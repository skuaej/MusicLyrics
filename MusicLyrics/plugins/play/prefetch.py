"""Background prefetching of the *next* queue item.

When a track starts playing, this module kicks off a background task that
resolves the media for the NEXT item in the queue so it is ready to stream
the instant the current track ends or the user presses /skip.

Strategy
========
The prefetcher runs in the background so latency doesn't matter to the
user, which lets us PREFER *downloads* over stream URLs.  A local file
never expires; a YouTube / SoundCloud stream URL typically expires within
minutes.  Sequential download-first is therefore safer than the concurrent
race used during the original ``/play`` resolve.

The prefetcher mutates ``item.media_path`` / ``item.is_stream_url`` /
``item.media_resolved_at`` in place so the player just needs to check
:func:`is_prefetched` and stream the existing path — no extra plumbing.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional

from MusicLyrics.plugins.play.queue import get_chat_queue, QueueItem

LOG = logging.getLogger(__name__)

# One worker task per chat — cancelled & replaced whenever a new track starts.
_prefetch_tasks: dict[int, asyncio.Task] = {}

# How long a stream URL is considered "fresh" after resolution.  Most CDN
# tokens (Google Video, SndCDN, JioSaavn) expire in 5-15 min — we use 90 s
# as a conservative bound so the FAST PATH always plays a still-valid URL.
URL_FRESHNESS_SEC = 90


def _is_valid_local_file(path: str) -> bool:
    try:
        return bool(path) and os.path.isfile(path) and os.path.getsize(path) > 1024
    except Exception:
        return False


def _is_fresh_url(item: QueueItem) -> bool:
    """A stream URL is considered fresh if it was resolved recently."""
    if not item.media_path or not item.media_path.startswith(("http://", "https://")):
        return False
    if item.media_resolved_at <= 0:
        return False
    return (time.time() - item.media_resolved_at) < URL_FRESHNESS_SEC


def is_prefetched(item: Optional[QueueItem]) -> bool:
    """Return True if *item* has a media path that will play instantly.

    Local files are always considered prefetched (they don't expire).
    Stream URLs only count as prefetched if they were resolved within the
    last ``URL_FRESHNESS_SEC`` seconds.
    """
    if item is None or not item.media_path:
        return False
    if _is_valid_local_file(item.media_path):
        return True
    if _is_fresh_url(item):
        return True
    return False


def mark_resolved(item: QueueItem) -> None:
    """Stamp *item* with the current resolution time."""
    item.media_resolved_at = time.time()


# ── Sequential resolvers (download-preferring) ──────────────────────────────

async def _dl_youtube(item: QueueItem):
    """Download via yt-dlp by title."""
    from MusicLyrics.plugins.play.platforms.youtube import (
        search_and_download_audio, search_and_download_video,
    )
    try:
        if item.stream_type == "video":
            p, _ = await search_and_download_video(item.title)
        else:
            p, _ = await search_and_download_audio(item.title)
        if p and _is_valid_local_file(str(p)):
            return str(p), False
    except Exception as e:
        LOG.debug("prefetch _dl_youtube failed: %s", e)
    return None


async def _dl_jiosaavn(item: QueueItem):
    if item.stream_type == "video":
        return None
    from MusicLyrics.plugins.play.platforms.jiosaavn import (
        search_and_download_jiosaavn,
    )
    try:
        p, _ = await search_and_download_jiosaavn(item.title)
        if p and _is_valid_local_file(str(p)):
            return str(p), False
    except Exception as e:
        LOG.debug("prefetch _dl_jiosaavn failed: %s", e)
    return None


async def _dl_soundcloud(item: QueueItem):
    from MusicLyrics.plugins.play.platforms.soundcloud import (
        search_and_download_soundcloud,
    )
    try:
        p, info = await search_and_download_soundcloud(item.title)
        if p:
            if _is_valid_local_file(str(p)):
                return str(p), False
            if info and info.get("_is_stream_url"):
                return str(p), True
    except Exception as e:
        LOG.debug("prefetch _dl_soundcloud failed: %s", e)
    return None


async def _url_youtube(item: QueueItem):
    from MusicLyrics.plugins.play.platforms.youtube import (
        get_audio_stream_url, get_video_stream_url,
        is_youtube_url, search_youtube,
    )
    try:
        if is_youtube_url(item.url):
            target_url = item.url
        else:
            yt = await search_youtube(item.title)
            target_url = yt.get("url") if yt else None
        if target_url:
            if item.stream_type == "video":
                u = await get_video_stream_url(target_url)
            else:
                u = await get_audio_stream_url(target_url)
            if u:
                return u, True
    except Exception as e:
        LOG.debug("prefetch _url_youtube failed: %s", e)
    return None


async def _url_jiosaavn(item: QueueItem):
    if item.stream_type == "video":
        return None
    from MusicLyrics.plugins.play.platforms.jiosaavn import search_jiosaavn
    try:
        r = await search_jiosaavn(item.title)
        if r and r.get("download_url"):
            return r["download_url"], True
    except Exception as e:
        LOG.debug("prefetch _url_jiosaavn failed: %s", e)
    return None


async def _resolve_item_media(item: QueueItem) -> bool:
    """Resolve media for *item* — prefer downloads over stream URLs.

    Mutates ``item.media_path`` / ``item.is_stream_url`` /
    ``item.media_resolved_at`` on success.
    """
    title = (item.title or "").strip()
    if not title:
        return False

    # Phase 1: try downloads sequentially — local files never expire.
    for resolver in (_dl_youtube, _dl_jiosaavn, _dl_soundcloud):
        try:
            result = await resolver(item)
        except asyncio.CancelledError:
            raise
        except Exception:
            result = None
        if result:
            path, is_stream = result
            item.media_path = path
            item.is_stream_url = bool(is_stream)
            mark_resolved(item)
            return True

    # Phase 2: fallback to stream URLs (will expire — kept fresh by re-prefetch).
    for resolver in (_url_youtube, _url_jiosaavn):
        try:
            result = await resolver(item)
        except asyncio.CancelledError:
            raise
        except Exception:
            result = None
        if result:
            path, is_stream = result
            item.media_path = path
            item.is_stream_url = bool(is_stream)
            mark_resolved(item)
            return True

    return False


# ── Public task management ─────────────────────────────────────────────────

async def prefetch_next(chat_id: int) -> None:
    """Kick off a background task to prefetch the NEXT item in *chat_id*'s queue.

    Safe to call repeatedly — cancels any previous prefetch for the chat first.
    """
    old = _prefetch_tasks.pop(chat_id, None)
    if old and not old.done():
        try:
            old.cancel()
        except Exception:
            pass

    async def _worker():
        try:
            cq = await get_chat_queue(chat_id)
            if len(cq.items) < 2:
                return
            next_item = cq.items[1]
            # If already prefetched (local file, or fresh URL), skip.  Even
            # then, schedule a refresh if it's a URL nearing expiry — that
            # way we never serve a stale URL to skip / auto-next.
            if is_prefetched(next_item) and not next_item.media_path.startswith(("http://", "https://")):
                LOG.info(
                    "Prefetch HIT (local file) for %s: '%s'", chat_id, next_item.title
                )
                return
            LOG.info("Prefetch START for %s: '%s'", chat_id, next_item.title)
            ok = await _resolve_item_media(next_item)
            if ok:
                LOG.info(
                    "Prefetch DONE for %s: '%s' -> %s%s",
                    chat_id, next_item.title,
                    "URL " if next_item.is_stream_url else "FILE ",
                    str(next_item.media_path)[:80],
                )
            else:
                LOG.warning(
                    "Prefetch MISS for %s: '%s' — will resolve at play time",
                    chat_id, next_item.title,
                )
        except asyncio.CancelledError:
            pass
        except Exception as e:
            LOG.debug("Prefetch worker error for %s: %s", chat_id, e)

    task = asyncio.create_task(_worker())
    _prefetch_tasks[chat_id] = task


async def refresh_item_if_stale(item: QueueItem) -> bool:
    """Synchronously refresh *item* iff its media is a stale URL.

    Called from the player's FAST PATH right before streaming.  Returns
    True if the item now has fresh media (either it was already fresh, or
    we refreshed it), False if refresh failed.
    """
    if is_prefetched(item):
        return True
    LOG.info("Item '%s' has stale/missing media — refreshing now", item.title)
    return await _resolve_item_media(item)


def cancel_prefetch(chat_id: int) -> None:
    """Cancel any pending prefetch task for *chat_id*."""
    t = _prefetch_tasks.pop(chat_id, None)
    if t and not t.done():
        try:
            t.cancel()
        except Exception:
            pass
