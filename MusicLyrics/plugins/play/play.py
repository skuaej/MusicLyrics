"""Handler for /play <query|url> — audio playback in voice chat.

Uses stream URL extraction as primary method (no download needed),
with download as fallback. Based on patterns from YukkiMusicBot.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re

from pyrogram import Client, filters
from pyrogram.types import (
    Message,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)

from MusicLyrics.bot import bot
from MusicLyrics.helpers.filters import not_edited
from config import Config

from MusicLyrics.plugins.play.queue import (
    QueueItem,
    add_to_queue,
    get_chat_queue,
    clear_queue,
    format_duration,
    MAX_QUEUE_SIZE,
)
from MusicLyrics.plugins.play.stream import (
    stream_audio,
    is_active,
    pre_join_vc,
    _now_playing_messages,
    _add_now_playing,
    _pop_now_playing,
    _add_queue_added,
    _control_keyboard,
    _queue_added_keyboard,
    _get_next_color,
    _get_current_theme,
    _start_progress_timer,
    _stop_progress_timer,
    _add_reaction,
    leave_voice_chat,
    _get_skip_lock,
    acquire_skip_lock,
    get_owner_mention,
)
from MusicLyrics.plugins.play.prefetch import prefetch_next, mark_resolved
from MusicLyrics.helpers.thumbnails import gen_thumbnail


async def _build_thumb(title: str, channel: str, duration: int, thumbnail_url: str, requester: str) -> str | None:
    """Generate a custom branded thumbnail. Returns local path or None on failure."""
    try:
        return await gen_thumbnail(
            title=title or "Unknown",
            artist=channel or "Unknown Artist",
            duration=duration or 0,
            thumbnail_url=thumbnail_url or "",
            requester=requester or "Unknown",
        )
    except Exception as _e:
        LOG.debug("gen_thumbnail failed: %s", _e)
        return None


async def _safe_edit(msg, text: str, **kwargs) -> bool:
    """Edit a status message, silently ignoring deleted/expired messages.

    Without this, MessageIdInvalid raised inside an error handler bubbles
    up as an unhandled exception and freezes the dispatcher.
    Uses safe_send to prevent RANDOM_ID_DUPLICATE storms.
    """
    from MusicLyrics.utils.safe_send import safe_send, safe_edit

    if msg is None:
        return False
    try:
        await safe_edit(msg, text, **kwargs)
        return True
    except Exception:
        try:
            chat_obj = getattr(msg, "chat", None)
            chat_id = getattr(chat_obj, "id", None) if chat_obj else None
            if chat_id is not None:
                await safe_send(bot, chat_id, text, **kwargs)
                return True
        except Exception:
            pass
        return False


from MusicLyrics.plugins.play.platforms.youtube import (
    search_youtube,
    get_audio_stream_url,
    download_audio,
    get_video_info,
    is_youtube_url,
    search_and_download_audio,
)
from MusicLyrics.plugins.play.platforms.spotify import (
    is_spotify_url,
    get_spotify_track,
    get_spotify_playlist,
)
from MusicLyrics.plugins.play.platforms.jiosaavn import (
    is_jiosaavn_url,
    get_jiosaavn_song,
    download_jiosaavn,
    search_and_download_jiosaavn,
    get_jiosaavn_stream_url,
    search_jiosaavn,
)
from MusicLyrics.plugins.play.platforms.apple_music import (
    is_apple_music_url,
    get_apple_music_track,
)
from MusicLyrics.plugins.play.platforms.soundcloud import (
    is_soundcloud_url,
    search_soundcloud,
    get_soundcloud_info,
    download_soundcloud,
    get_soundcloud_stream_url,
    search_and_download_soundcloud,
)
from MusicLyrics.utils.autodelete import (
    auto_delete_service,
    auto_delete_playing,
    auto_delete_cmd,
)

LOG = logging.getLogger(__name__)


# ── Global concurrency guard ────────────────────────────────────────────────
# Each /play resolves media by racing multiple yt-dlp / ffmpeg / network
# fallbacks in parallel. If many users in many groups add many songs in a
# short burst, the bot can spawn dozens of concurrent yt-dlp subprocesses
# and exhaust the host's RAM — the deployment platform (Heroku / Railway /
# Render) then OOM-kills the container, which the user sees as a crash.
#
# A small semaphore caps how many resolutions run at once. New /play
# requests still queue up almost instantly (they only wait inside the
# semaphore), but the bot never tries to download more than a handful of
# songs simultaneously.
#
# Tune via env var so heavier deployments can raise the cap without a
# code change.
_MAX_CONCURRENT_RESOLVES = max(
    1,
    int(os.environ.get("MAX_CONCURRENT_RESOLVES", "4") or "4"),
)
_resolve_semaphore = asyncio.Semaphore(_MAX_CONCURRENT_RESOLVES)
LOG.info(
    "Audio resolve concurrency cap: %d (override via MAX_CONCURRENT_RESOLVES)",
    _MAX_CONCURRENT_RESOLVES,
)


async def _resolve_query_guarded(query: str, platform: str, message):
    """Run :func:`_resolve_query` under the global resolve semaphore."""
    async with _resolve_semaphore:
        return await _resolve_query(query, platform, message)


def _detect_platform(text: str) -> str:
    """Return platform name from a URL or 'query' for plain text."""
    if is_youtube_url(text):
        return "youtube"
    if is_spotify_url(text):
        return "spotify"
    if is_jiosaavn_url(text):
        return "jiosaavn"
    if is_apple_music_url(text):
        return "apple_music"
    if is_soundcloud_url(text):
        return "soundcloud"
    if re.match(r"https?://", text):
        return "direct_url"
    return "query"


async def _get_audio_media(url: str) -> tuple[str, bool]:
    """Get media path for audio playback.

    Returns (media_path, is_stream_url).
    PRIORITY (user-requested, matches /vplay flow):
    Step 1: yt-dlp download FIRST (sequential await) — most reliable
    Step 2: ONLY if Step 1 fails: YouTube stream URL
    Step 3: ONLY if Step 2 fails: title-search + JioSaavn + SoundCloud
    """
    import asyncio as _aio

    # ── Step 1: yt-dlp direct download FIRST (strict priority) ──
    LOG.info("Trying yt-dlp direct download FIRST for: %s", url)
    try:
        fp = await download_audio(url)
        if fp and os.path.isfile(fp):
            LOG.info("yt-dlp direct download succeeded for: %s", url)
            return fp, False
    except Exception as e:
        LOG.debug("yt-dlp direct download failed for %s: %s", url, e)

    # ── Step 2: YouTube stream URL fallback ──
    LOG.info("Direct download failed, trying YouTube stream URL for: %s", url)
    try:
        su = await get_audio_stream_url(url)
        if su:
            LOG.info("Stream URL obtained for: %s", url)
            return su, True
    except Exception as e:
        LOG.debug("Stream URL extraction failed for %s: %s", url, e)

    # ── Step 3: YouTube failed — title-search + JioSaavn + SoundCloud ──
    LOG.info(
        "YouTube methods failed, trying title-search + JioSaavn + SoundCloud for: %s",
        url,
    )

    from MusicLyrics.plugins.play.platforms.youtube import get_video_info

    info = await get_video_info(url)
    title = info.get("title", "") if info else ""

    async def _try_ytdlp_search():
        if title and title not in ("YouTube Audio", "Unknown"):
            try:
                fp, _ = await search_and_download_audio(title)
                if fp and os.path.isfile(fp):
                    return fp, False
            except Exception:
                pass
        return None

    async def _try_jiosaavn():
        if not title or title in ("YouTube Audio", "Unknown"):
            return None
        try:
            jp, ji = await search_and_download_jiosaavn(title)
            if jp and os.path.isfile(jp):
                return jp, False
        except Exception:
            pass
        return None

    async def _try_soundcloud():
        if not title or title in ("YouTube Audio", "Unknown"):
            return None
        try:
            sp, si = await search_and_download_soundcloud(title)
            if sp:
                if si and si.get("_is_stream_url"):
                    return sp, True
                if os.path.isfile(sp):
                    return sp, False
        except Exception:
            pass
        return None

    fb_tasks = [
        _aio.create_task(_try_ytdlp_search()),
        _aio.create_task(_try_jiosaavn()),
        _aio.create_task(_try_soundcloud()),
    ]
    fb_pending = set(fb_tasks)
    while fb_pending:
        done, fb_pending = await _aio.wait(fb_pending, return_when=_aio.FIRST_COMPLETED)
        for task in done:
            try:
                result = task.result()
                if result:
                    for p in fb_pending:
                        p.cancel()
                    return result
            except Exception:
                pass

    return "", False


async def _resolve_query(query: str, platform: str, msg: Message):
    """Resolve the user query into (info_dict, media_path, is_stream_url) or raise."""

    # -- YouTube URL
    if platform == "youtube":
        info = await get_video_info(query)
        if not info:
            # Try searching by URL as query
            info = {
                "title": "YouTube Audio",
                "url": query,
                "duration": 0,
                "thumbnail": "",
                "channel": "",
            }
        if info["duration"] > Config.DURATION_LIMIT_MIN * 60 and info["duration"] > 0:
            raise ValueError(
                f"গানটি {Config.DURATION_LIMIT_MIN} মিনিটের বেশি, play করা যাবে না।"
            )
        media_path, is_stream = await _get_audio_media(query)
        if not media_path:
            # Fallback: try search+download with title as query
            title_query = info.get("title", "")
            channel_query = info.get("channel", "")
            if title_query and title_query != "YouTube Audio":
                LOG.info(
                    "YouTube URL extraction failed, trying search+download: %s",
                    title_query,
                )
                filepath, dl_info = await search_and_download_audio(
                    f"{title_query} {channel_query}".strip()
                )
                if filepath:
                    return (dl_info or info), filepath, False
            raise ValueError("YouTube থেকে audio পাওয়া যায়নি। আবার চেষ্টা করুন।")
        return info, media_path, is_stream

    # -- Spotify
    if platform == "spotify":
        track = await get_spotify_track(query)
        if not track:
            raise ValueError("Spotify link parse করা যায়নি।")
        # ── YouTube FIRST (user-requested: YouTube every time) ──
        LOG.info("Spotify: trying YouTube FIRST for: %s", track["query"])
        yt = await search_youtube(track["query"])
        if yt:
            media_path, is_stream = await _get_audio_media(yt["url"])
            if media_path:
                info = {**yt, "platform": "spotify"}
                return info, media_path, is_stream
        # ── YouTube yt-dlp search+download as second YT-path ──
        LOG.info(
            "Spotify -> YouTube search failed, trying yt-dlp search+download: %s",
            track["query"],
        )
        filepath, dl_info = await search_and_download_audio(track["query"])
        if filepath and dl_info:
            return dl_info, filepath, False
        # ── JioSaavn fallback (when YouTube fully fails) ──
        LOG.info(
            "Spotify -> YouTube all failed, trying JioSaavn for: %s", track["query"]
        )
        js_path, js_info = await search_and_download_jiosaavn(track["query"])
        if js_path and js_info:
            import os as _os

            if _os.path.isfile(js_path):
                info = {
                    "title": js_info.get("title", track.get("title", "Unknown")),
                    "url": js_info.get("url", query),
                    "duration": js_info.get("duration", track.get("duration", 0)),
                    "thumbnail": js_info.get("thumbnail", track.get("thumbnail", "")),
                    "channel": js_info.get("artist", track.get("artist", "")),
                    "platform": "jiosaavn",
                }
                return info, js_path, False
        # SoundCloud as last resort
        LOG.info("Spotify -> all failed, trying SoundCloud for: %s", track["query"])
        sc_path, sc_info = await search_and_download_soundcloud(track["query"])
        if sc_path and sc_info:
            is_stream = bool(sc_info.get("_is_stream_url"))
            info = {
                "title": sc_info.get("title", track.get("title", "Unknown")),
                "url": sc_info.get("url", query),
                "duration": sc_info.get("duration", track.get("duration", 0)),
                "thumbnail": sc_info.get("thumbnail", track.get("thumbnail", "")),
                "channel": sc_info.get("channel", track.get("artist", "")),
                "platform": "soundcloud",
            }
            return info, sc_path, is_stream
        raise ValueError("YouTube ও অন্য কোথাও গানটি পাওয়া যায়নি।")

    # -- JioSaavn
    if platform == "jiosaavn":
        song = await get_jiosaavn_song(query)
        if not song:
            raise ValueError("JioSaavn link থেকে তথ্য পাওয়া যায়নি।")
        yt_query = f"{song['title']} {song.get('artist', '')}".strip()
        # ── YouTube FIRST (user-requested: YouTube every time) ──
        LOG.info("JioSaavn URL: trying YouTube FIRST for: %s", yt_query)
        yt = await search_youtube(yt_query)
        if yt:
            media_path, is_stream = await _get_audio_media(yt["url"])
            if media_path:
                info = {
                    "title": song["title"],
                    "url": song["url"],
                    "duration": song["duration"],
                    "thumbnail": song.get("thumbnail", ""),
                    "channel": song.get("artist", ""),
                    "platform": "jiosaavn",
                }
                return info, media_path, is_stream
        # ── YouTube yt-dlp search+download as second YT-path ──
        LOG.info(
            "JioSaavn -> YouTube search failed, trying yt-dlp search+download: %s",
            yt_query,
        )
        try:
            filepath, dl_info = await search_and_download_audio(yt_query)
            if filepath and dl_info and os.path.isfile(filepath):
                info = {
                    "title": dl_info.get("title", song["title"]),
                    "url": song["url"],
                    "duration": dl_info.get("duration", song["duration"]),
                    "thumbnail": dl_info.get("thumbnail", song.get("thumbnail", "")),
                    "channel": dl_info.get("channel", song.get("artist", "")),
                    "platform": "jiosaavn",
                }
                return info, filepath, False
        except Exception as e:
            LOG.debug("yt-dlp search+download failed for JioSaavn URL: %s", e)
        # ── JioSaavn native fallback ──
        LOG.info(
            "JioSaavn -> YouTube failed, falling back to JioSaavn native: %s",
            song["title"],
        )
        filepath = await download_jiosaavn(query, song_info=song)
        if filepath:
            import os as _os

            if _os.path.isfile(filepath):
                info = {
                    "title": song["title"],
                    "url": song["url"],
                    "duration": song["duration"],
                    "thumbnail": song.get("thumbnail", ""),
                    "channel": song.get("artist", ""),
                    "platform": "jiosaavn",
                }
                return info, filepath, False
        # JioSaavn stream URL fallback (no disk write)
        if song.get("download_url"):
            LOG.info(
                "JioSaavn download failed, using JioSaavn stream URL for: %s",
                song["title"],
            )
            info = {
                "title": song["title"],
                "url": song["url"],
                "duration": song["duration"],
                "thumbnail": song.get("thumbnail", ""),
                "channel": song.get("artist", ""),
                "platform": "jiosaavn",
            }
            return info, song["download_url"], True
        # SoundCloud as LAST RESORT
        LOG.info("JioSaavn -> all failed, trying SoundCloud for: %s", song["title"])
        sc_query = f"{song['title']} {song.get('artist', '')}".strip()
        sc_path, sc_info = await search_and_download_soundcloud(sc_query)
        if sc_path and sc_info:
            is_stream = bool(sc_info.get("_is_stream_url"))
            info = {
                "title": song["title"],
                "url": song["url"],
                "duration": song["duration"],
                "thumbnail": song.get("thumbnail", ""),
                "channel": song.get("artist", ""),
                "platform": "soundcloud",
            }
            return info, sc_path, is_stream
        raise ValueError("গানটি কোথাও পাওয়া যায়নি।")

    # -- Apple Music
    if platform == "apple_music":
        track = await get_apple_music_track(query)
        if not track:
            raise ValueError("Apple Music link parse করা যায়নি।")
        # ── YouTube FIRST (user-requested: YouTube every time) ──
        LOG.info("Apple Music: trying YouTube FIRST for: %s", track["query"])
        yt = await search_youtube(track["query"])
        if yt:
            media_path, is_stream = await _get_audio_media(yt["url"])
            if media_path:
                info = {**yt, "platform": "apple_music"}
                return info, media_path, is_stream
        # ── YouTube yt-dlp search+download as second YT-path ──
        LOG.info(
            "Apple Music -> YouTube search failed, trying yt-dlp: %s", track["query"]
        )
        filepath, dl_info = await search_and_download_audio(track["query"])
        if filepath and dl_info:
            return dl_info, filepath, False
        # ── JioSaavn fallback ──
        LOG.info(
            "Apple Music -> YouTube all failed, trying JioSaavn for: %s", track["query"]
        )
        js_path, js_info = await search_and_download_jiosaavn(track["query"])
        if js_path and js_info:
            import os as _os

            if _os.path.isfile(js_path):
                info = {
                    "title": js_info.get("title", track.get("title", "Unknown")),
                    "url": js_info.get("url", query),
                    "duration": js_info.get("duration", 0),
                    "thumbnail": js_info.get("thumbnail", ""),
                    "channel": js_info.get("artist", track.get("artist", "")),
                    "platform": "jiosaavn",
                }
                return info, js_path, False
        # SoundCloud last resort
        LOG.info("Apple Music -> all failed, trying SoundCloud: %s", track["query"])
        sc_path, sc_info = await search_and_download_soundcloud(track["query"])
        if sc_path and sc_info:
            is_stream = bool(sc_info.get("_is_stream_url"))
            info = {
                "title": sc_info.get("title", track.get("title", "Unknown")),
                "url": sc_info.get("url", query),
                "duration": sc_info.get("duration", 0),
                "thumbnail": sc_info.get("thumbnail", ""),
                "channel": sc_info.get("channel", track.get("artist", "")),
                "platform": "soundcloud",
            }
            return info, sc_path, is_stream
        raise ValueError("Audio stream পাওয়া যায়নি।")

    # -- SoundCloud URL
    if platform == "soundcloud":
        sc_info = await get_soundcloud_info(query)
        if not sc_info:
            sc_info = {
                "title": "SoundCloud Audio",
                "url": query,
                "duration": 0,
                "thumbnail": "",
                "channel": "",
                "platform": "soundcloud",
            }
        if (
            sc_info.get("duration", 0) > Config.DURATION_LIMIT_MIN * 60
            and sc_info["duration"] > 0
        ):
            raise ValueError(
                f"গানটি {Config.DURATION_LIMIT_MIN} মিনিটের বেশি, play করা যাবে না।"
            )
        # Try downloading first
        filepath = await download_soundcloud(query)
        if filepath and os.path.isfile(filepath):
            return sc_info, filepath, False
        # Try stream URL
        stream_url = await get_soundcloud_stream_url(query)
        if stream_url:
            return sc_info, stream_url, True
        raise ValueError("SoundCloud থেকে audio পাওয়া যায়নি।")

    # -- Direct URL
    if platform == "direct_url":
        info = await get_video_info(query)
        if not info:
            info = {
                "title": "Direct Stream",
                "url": query,
                "duration": 0,
                "thumbnail": "",
                "channel": "",
            }
        media_path, is_stream = await _get_audio_media(query)
        if not media_path:
            raise ValueError("URL থেকে audio পাওয়া যায়নি।")
        return info, media_path, is_stream

    # -- Plain text query --
    # USER-REQUESTED ORDER: YouTube FIRST every time, then fall back to
    # JioSaavn, then SoundCloud only if YouTube fails. Sequential — not
    # racing — so a slow YouTube response never lets JioSaavn jump ahead.

    import asyncio as _aio

    DUR_LIMIT_SEC = Config.DURATION_LIMIT_MIN * 60

    def _duration_ok(d):
        return not (d and d > DUR_LIMIT_SEC)

    duration_exceeded = None

    # ── Step 1: YouTube search → stream URL ──
    async def _try_youtube_url():
        try:
            yt = await search_youtube(query)
            if not yt:
                return None
            if not _duration_ok(yt.get("duration", 0)):
                return ("__duration_exceeded__", yt["duration"], None)
            su = await get_audio_stream_url(yt["url"])
            if su:
                return yt, su, True
        except Exception as e:
            LOG.debug("youtube_url search failed: %s", e)
        return None

    # ── Step 2: yt-dlp YouTube search+download (reliable but slower) ──
    async def _try_youtube_download():
        try:
            fp, dl_info = await search_and_download_audio(query)
            if fp and os.path.isfile(fp):
                if dl_info and not _duration_ok(dl_info.get("duration", 0)):
                    return ("__duration_exceeded__", dl_info["duration"], None)
                info = dl_info or {
                    "title": "Unknown",
                    "url": "",
                    "duration": 0,
                    "thumbnail": "",
                    "channel": "",
                }
                return info, fp, False
        except Exception as e:
            LOG.debug("youtube download failed: %s", e)
        return None

    # ── Step 3 fallback: JioSaavn search ──
    async def _try_jiosaavn():
        try:
            js_search = await search_jiosaavn(query)
            if js_search and js_search.get("download_url"):
                if not _duration_ok(js_search.get("duration", 0)):
                    return ("__duration_exceeded__", js_search["duration"], None)
                info = {
                    "title": js_search.get("title", "Unknown"),
                    "url": js_search.get("url", ""),
                    "duration": js_search.get("duration", 0),
                    "thumbnail": js_search.get("thumbnail", ""),
                    "channel": js_search.get("artist", ""),
                    "platform": "jiosaavn",
                }
                return info, js_search["download_url"], True
        except Exception as e:
            LOG.debug("jiosaavn search failed: %s", e)
        try:
            js_path, js_info = await search_and_download_jiosaavn(query)
            if js_path and js_info and os.path.isfile(js_path):
                if not _duration_ok(js_info.get("duration", 0)):
                    return ("__duration_exceeded__", js_info["duration"], None)
                info = {
                    "title": js_info.get("title", "Unknown"),
                    "url": js_info.get("url", ""),
                    "duration": js_info.get("duration", 0),
                    "thumbnail": js_info.get("thumbnail", ""),
                    "channel": js_info.get("artist", ""),
                    "platform": "jiosaavn",
                }
                return info, js_path, False
        except Exception as e:
            LOG.debug("jiosaavn download failed: %s", e)
        return None

    # ── Step 4 last-resort: SoundCloud ──
    async def _try_soundcloud():
        try:
            sc_path, sc_info = await search_and_download_soundcloud(query)
            if sc_path and sc_info:
                if not _duration_ok(sc_info.get("duration", 0)):
                    return ("__duration_exceeded__", sc_info["duration"], None)
                is_stream = bool(sc_info.get("_is_stream_url"))
                if is_stream or os.path.isfile(str(sc_path)):
                    return sc_info, sc_path, is_stream
        except Exception as e:
            LOG.debug("soundcloud failed: %s", e)
        return None

    LOG.info(
        "Audio query: YouTube-first concurrent race (yt_url || yt_dl) -> JioSaavn -> SoundCloud for: %s",
        query,
    )

    # ── PHASE 1: YouTube FIRST — race yt_url and yt_dl concurrently so the
    # fastest YouTube path wins.  Previously these ran sequentially with very
    # tight (6s/8s) timeouts which caused YouTube to "time out" before it
    # even had a chance to complete, letting JioSaavn/SoundCloud win every
    # time.  Running them in parallel + generous budget makes YouTube win
    # reliably and fast.
    YT_TOTAL_BUDGET = 22.0  # Total time YouTube gets before falling back
    YT_URL_HEADSTART = 0.0  # Both start together
    yt_url_task = _aio.create_task(_try_youtube_url())
    yt_dl_task = _aio.create_task(_try_youtube_download())
    yt_tasks = {yt_url_task: "youtube_url", yt_dl_task: "youtube_dl"}
    yt_deadline = _aio.get_event_loop().time() + YT_TOTAL_BUDGET
    yt_winner = None
    try:
        pending = set(yt_tasks.keys())
        while pending:
            remaining = yt_deadline - _aio.get_event_loop().time()
            if remaining <= 0:
                break
            done, pending = await _aio.wait(
                pending,
                timeout=remaining,
                return_when=_aio.FIRST_COMPLETED,
            )
            if not done:
                break  # timeout
            for t in done:
                try:
                    r = t.result()
                except Exception:
                    r = None
                if not r:
                    continue
                if isinstance(r, tuple) and r and r[0] == "__duration_exceeded__":
                    duration_exceeded = r[1]
                    continue
                # Winner!
                yt_winner = (yt_tasks[t], r)
                break
            if yt_winner:
                break
    finally:
        # Cancel any still-pending YouTube tasks once we have a winner / time out
        for t in list(yt_tasks.keys()):
            if not t.done():
                t.cancel()
        # Drain cancellations
        for t in list(yt_tasks.keys()):
            try:
                await t
            except BaseException:
                pass

    if yt_winner:
        LOG.info("Audio query WON by %s for: %s", yt_winner[0], query)
        return yt_winner[1]
    else:
        LOG.info(
            "YouTube paths exhausted (budget %.1fs) for: %s — falling back",
            YT_TOTAL_BUDGET,
            query,
        )

    # ── PHASE 2: JioSaavn fallback (generous timeout) ──
    try:
        result = await _aio.wait_for(_try_jiosaavn(), timeout=12.0)
        if result and not (
            isinstance(result, tuple) and result[0] == "__duration_exceeded__"
        ):
            LOG.info("Audio query WON by jiosaavn for: %s", query)
            return result
        elif result and result[0] == "__duration_exceeded__":
            duration_exceeded = result[1]
    except _aio.TimeoutError:
        LOG.info("jiosaavn timed out, falling back for: %s", query)
    except Exception:
        pass

    # ── PHASE 3: SoundCloud last-resort (generous timeout) ──
    try:
        result = await _aio.wait_for(_try_soundcloud(), timeout=12.0)
        if result and not (
            isinstance(result, tuple) and result[0] == "__duration_exceeded__"
        ):
            LOG.info("Audio query WON by soundcloud for: %s", query)
            return result
        elif result and result[0] == "__duration_exceeded__":
            duration_exceeded = result[1]
    except _aio.TimeoutError:
        LOG.info("soundcloud timed out for: %s", query)
    except Exception:
        pass

    if duration_exceeded:
        raise ValueError(
            f"গানটি {Config.DURATION_LIMIT_MIN} মিনিটের বেশি, play করা যাবে না।"
        )

    raise ValueError("কোনো result পাওয়া যায়নি। অন্য keyword দিয়ে চেষ্টা করুন।")


@bot.on_message(filters.command(["play", "p"]) & not_edited)
async def play_command(client: Client, message: Message):
    """Handle /play <query|url>."""
    chat_id = message.chat.id
    user = message.from_user
    requester = user.mention if user else "Unknown"
    requester_id = user.id if user else 0

    # Extract query
    query = ""
    if len(message.command) > 1:
        query = " ".join(message.command[1:])
    elif message.reply_to_message and message.reply_to_message.text:
        query = message.reply_to_message.text
    elif message.reply_to_message and message.reply_to_message.audio:
        pass  # audio file reply not handled here

    if not query:
        usage_msg = await message.reply_text(
            "**Usage:** `/play <song name or URL>`\n\n"
            "Example:\n"
            "`/play Arijit Singh Tum Hi Ho`\n"
            "`/play https://youtu.be/...`"
        )
        await _add_reaction(chat_id, message.id)
        return

    # ── EARLY GATE: refuse to search if no assistant can ever play here ──
    # Without this check the bot wastes a full search + download cycle on
    # every /play in groups where the assistant isn't even a member.
    from MusicLyrics.userbot import assistant_in_chat, pool_size
    if pool_size() == 0:
        await message.reply_text(
            "❌ **Music feature off** — `STRING_SESSION` সেট করা নেই।\n"
            "Owner-কে বলুন assistant configure করতে।"
        )
        await _add_reaction(chat_id, message.id)
        return
    if not await assistant_in_chat(chat_id):
        await message.reply_text(
            "❌ **এই গ্রুপে আমার assistant নেই।**\n\n"
            "প্রথমে assistant-কে গ্রুপে add করো, তারপর `/play` দাও।\n"
            "Assistant ছাড়া গান বাজানো সম্ভব না — তাই search ও skip করছি।"
        )
        await _add_reaction(chat_id, message.id)
        return

    status_msg = await message.reply_text(
        f"🔍 **খুঁজছি:** `{query[:80]}`\n\nঅপেক্ষা করুন..."
    )
    await _add_reaction(chat_id, message.id)

    platform = _detect_platform(query)

    try:
        # Pre-join VC concurrently while resolving media (speed optimization)
        pre_join_task = asyncio.create_task(pre_join_vc(chat_id))
        try:
            info, media_path, is_stream = await _resolve_query_guarded(query, platform, message)
        finally:
            # Ensure pre-join task completes (or is cancelled)
            try:
                await pre_join_task
            except Exception:
                pass
    except ValueError as exc:
        await _safe_edit(status_msg, f"❌ **Error:** {exc}")
        return
    except Exception as exc:
        LOG.exception("Unexpected error in /play for %s", chat_id)
        await _safe_edit(
            status_msg,
            f"❌ কিছু একটা সমস্যা হয়েছে। পরে আবার চেষ্টা করুন।\n"
            f"**Details:** `{type(exc).__name__}: {str(exc)[:200]}`",
        )
        return

    title = info.get("title", "Unknown")
    duration = info.get("duration", 0)
    thumbnail = info.get("thumbnail", "")
    url = info.get("url", query)
    channel = info.get("channel", "")

    item = QueueItem(
        title=title,
        url=url,
        media_path=media_path,
        duration=duration,
        requester=requester,
        requester_id=requester_id,
        thumbnail=thumbnail,
        stream_type="audio",
        platform=platform if platform != "query" else "youtube",
        is_stream_url=is_stream,
    )
    mark_resolved(item)

    position = await add_to_queue(chat_id, item)

    # Queue is full — reject politely so the bot doesn't spawn unbounded work
    if position == 0:
        await _safe_edit(
            status_msg,
            f"⚠️ **Queue ভর্তি!** ({MAX_QUEUE_SIZE}টা গান সর্বাধিক)\n\n"
            f"আগের কোনো গান শেষ হলে আবার `/play` দিন।",
        )
        await _add_reaction(chat_id, message.id)
        return

    # If something is already playing, just queue it
    if position > 1 and is_active(chat_id):
        # Kick off prefetch so this item is ready instantly when it's its turn
        try:
            asyncio.create_task(prefetch_next(chat_id))
        except Exception:
            pass
        dur = format_duration(duration)
        color = _get_next_color()
        owner_mention = await get_owner_mention()
        await status_msg.edit_text(
            f"**📜 Queue-তে যোগ হয়েছে #{position}**\n\n"
            f"> 💿 **Title:** {title}\n"
            f"> ⏳ **Duration:** {dur}\n"
            f"> 👑 **Requested by:** {requester}\n\n"
            f"✨ ✦ᴘᴏᴡєʀєᴅ ʙʏ » ── {owner_mention}",
            reply_markup=_queue_added_keyboard(color),
        )
        # Track this queue-added notification so it gets cleaned up when the
        # corresponding song ends.
        await _add_queue_added(chat_id, status_msg)
        await _add_reaction(chat_id, message.id)
        return

    # Start streaming
    try:
        await stream_audio(
            chat_id,
            media_path,
            title=title,
            duration=duration,
            thumbnail=thumbnail,
            requester=requester,
            skip_url_check=True,  # URL was just resolved, no need for HEAD probe
        )
    except FileNotFoundError:
        LOG.exception("Media not found for stream in %s", chat_id)
        await _safe_edit(
            status_msg,
            "❌ মিডিয়া ফাইল/URL পাওয়া যায়নি।\nআবার `/play` দিয়ে চেষ্টা করুন।",
        )
        try:
            await leave_voice_chat(chat_id)
        except Exception:
            pass
        return
    except RuntimeError as exc:
        await _safe_edit(
            status_msg,
            f"❌ {exc}\n\nSTRING_SESSION সেট করা আছে কিনা চেক করুন।",
        )
        try:
            await leave_voice_chat(chat_id)
        except Exception:
            pass
        return
    except Exception as exc:
        LOG.exception("Stream start failed in %s", chat_id)
        await _safe_edit(
            status_msg,
            "❌ Voice chat-এ connect করা যাচ্ছে না।\n"
            "নিশ্চিত করুন voice chat চালু আছে এবং "
            "assistant গ্রুপে আছে।\n\n"
            f"**Error:** `{type(exc).__name__}: {str(exc)[:150]}`",
        )
        try:
            await leave_voice_chat(chat_id)
        except Exception:
            pass
        return

    # Start the progress timer for this track
    await _start_progress_timer(chat_id, duration)

    dur = format_duration(duration)
    color = _get_next_color()
    t = _get_current_theme()
    owner_mention = await get_owner_mention()
    text = (
        f"{t['header']} **ᴘʟᴀʏʙᴀᴄᴋ ᴀᴄᴛɪᴠᴀᴛᴇᴅ | ᴇɴᴊᴏʏ ᴛʜᴇ ᴍᴜꜱɪᴄ**\n\n"
        f"> {t['title_icon']}  **ᴛɪᴛʟᴇ :** [{title}]({url})\n"
        f"> {t['dur_icon']}  **ᴅᴜʀᴀᴛɪᴏɴ :** {dur}\n"
        f"> 👑  **ʀᴇǫᴜᴇꜱᴛᴇᴅ :** {requester}\n\n"
        f"✨ ✦ᴘᴏᴡєʀєᴅ ʙʏ » ── {owner_mention}"
    )

    try:
        # Always try to generate a custom branded thumbnail (with spoiler)
        custom_thumb = await _build_thumb(title, channel, duration, thumbnail, requester)
        photo_src = custom_thumb or (thumbnail if thumbnail else None)
        if photo_src:
            try:
                await status_msg.delete()
            except Exception:
                pass
            try:
                now_playing_msg = await bot.send_photo(
                    chat_id,
                    photo=photo_src,
                    caption=text,
                    reply_markup=_control_keyboard(color),
                    has_spoiler=True,
                )
            except Exception as send_exc:
                # send_photo can fail with WebpageMediaEmpty / MediaEmpty
                # when YouTube hands us a stale thumbnail. Fall back to a
                # plain message instead of letting the handler die.
                LOG.debug("play send_photo failed for %s: %s", chat_id, send_exc)
                # Last-ditch: try the raw URL if we were using the custom file
                if custom_thumb and thumbnail and photo_src != thumbnail:
                    try:
                        now_playing_msg = await bot.send_photo(
                            chat_id,
                            photo=thumbnail,
                            caption=text,
                            reply_markup=_control_keyboard(color),
                            has_spoiler=True,
                        )
                    except Exception:
                        now_playing_msg = await bot.send_message(
                            chat_id,
                            text,
                            reply_markup=_control_keyboard(color),
                            disable_web_page_preview=True,
                        )
                else:
                    now_playing_msg = await bot.send_message(
                        chat_id,
                        text,
                        reply_markup=_control_keyboard(color),
                        disable_web_page_preview=True,
                    )
            # Track this message so we can delete it when track ends (thread-safe)
            await _add_now_playing(chat_id, now_playing_msg)
            await _add_reaction(chat_id, message.id)
        else:
            try:
                await status_msg.edit_text(text, reply_markup=_control_keyboard(color))
                await _add_now_playing(chat_id, status_msg)
            except Exception:
                new_msg = await bot.send_message(
                    chat_id,
                    text,
                    reply_markup=_control_keyboard(color),
                    disable_web_page_preview=True,
                )
                await _add_now_playing(chat_id, new_msg)
            await _add_reaction(chat_id, message.id)
    except Exception as outer_exc:
        # Last-resort guard so concurrent now-playing render failures
        # across many groups cannot crash the deployment.
        LOG.warning("play now-playing render failed for %s: %s", chat_id, outer_exc)
        try:
            new_msg = await bot.send_message(
                chat_id,
                text,
                reply_markup=_control_keyboard(color),
                disable_web_page_preview=True,
            )
            await _add_now_playing(chat_id, new_msg)
            await _add_reaction(chat_id, message.id)
        except Exception:
            pass


@bot.on_message(filters.command(["playforce", "pf", "forceplay"]) & not_edited)
async def playforce_command(client: Client, message: Message):
    """Handle /playforce <query|url> — stop current and play immediately."""
    chat_id = message.chat.id
    user = message.from_user
    requester = user.mention if user else "Unknown"
    requester_id = user.id if user else 0

    # Extract query
    query = ""
    if len(message.command) > 1:
        query = " ".join(message.command[1:])
    elif message.reply_to_message and message.reply_to_message.text:
        query = message.reply_to_message.text

    if not query:
        usage_msg = await message.reply_text(
            "**Usage:** `/playforce <song name or URL>`\n\n"
            "বর্তমান গান বন্ধ করে সাথে সাথে নতুন গান চালায়।\n\n"
            "Example:\n"
            "`/playforce Arijit Singh Tum Hi Ho`"
        )
        await _add_reaction(chat_id, message.id)
        return

    # ── EARLY GATE: refuse if no assistant available for this chat ──
    from MusicLyrics.userbot import assistant_in_chat, pool_size
    if pool_size() == 0:
        await message.reply_text(
            "❌ **Music feature off** — `STRING_SESSION` সেট করা নেই।"
        )
        await _add_reaction(chat_id, message.id)
        return
    if not await assistant_in_chat(chat_id):
        await message.reply_text(
            "❌ **এই গ্রুপে আমার assistant নেই।**\n\n"
            "প্রথমে assistant-কে গ্রুপে add করো, তারপর `/playforce` দাও।"
        )
        await _add_reaction(chat_id, message.id)
        return

    status_msg = await message.reply_text(
        f"⚡ **Force Play:** `{query[:80]}`\n\nবর্তমান গান বন্ধ করছি..."
    )
    await _add_reaction(chat_id, message.id)

    # Stop current playback if active
    if is_active(chat_id):
        try:
            lock = await acquire_skip_lock(chat_id, timeout=15.0)
        except RuntimeError:
            await message.reply_text(
                "⏳ আগের command এখনো চলছে — একটু পরে আবার চেষ্টা করুন।"
            )
            return
        try:
            _stop_progress_timer(chat_id)
            # Delete previous "Now Playing" messages (thread-safe)
            old_msgs = await _pop_now_playing(chat_id)
            for old_msg in old_msgs:
                try:
                    await old_msg.delete()
                except Exception:
                    pass
            await leave_voice_chat(chat_id)
        finally:
            try:
                lock.release()
            except Exception:
                pass

    platform = _detect_platform(query)

    try:
        # Pre-join VC concurrently while resolving media (speed optimization)
        pre_join_task = asyncio.create_task(pre_join_vc(chat_id))
        try:
            info, media_path, is_stream = await _resolve_query_guarded(query, platform, message)
        finally:
            try:
                await pre_join_task
            except Exception:
                pass
    except ValueError as exc:
        await _safe_edit(status_msg, f"❌ **Error:** {exc}")
        return
    except Exception as exc:
        LOG.exception("Unexpected error in /playforce for %s", chat_id)
        await _safe_edit(
            status_msg,
            f"❌ কিছু একটা সমস্যা হয়েছে।\n"
            f"**Details:** `{type(exc).__name__}: {str(exc)[:200]}`",
        )
        return

    title = info.get("title", "Unknown")
    duration = info.get("duration", 0)
    thumbnail = info.get("thumbnail", "")
    url = info.get("url", query)
    channel = info.get("channel", "")

    item = QueueItem(
        title=title,
        url=url,
        media_path=media_path,
        duration=duration,
        requester=requester,
        requester_id=requester_id,
        thumbnail=thumbnail,
        stream_type="audio",
        platform=platform if platform != "query" else "youtube",
        is_stream_url=is_stream,
    )
    mark_resolved(item)

    # Clear queue and add as first item
    await clear_queue(chat_id)
    await add_to_queue(chat_id, item)

    # Start streaming
    try:
        await stream_audio(
            chat_id,
            media_path,
            title=title,
            duration=duration,
            thumbnail=thumbnail,
            requester=requester,
            skip_url_check=True,  # URL was just resolved, no need for HEAD probe
        )
    except FileNotFoundError:
        await _safe_edit(
            status_msg,
            "❌ মিডিয়া ফাইল/URL পাওয়া যায়নি।\nআবার `/playforce` দিয়ে চেষ্টা করুন।",
        )
        try:
            await leave_voice_chat(chat_id)
        except Exception:
            pass
        return
    except RuntimeError as exc:
        await _safe_edit(
            status_msg,
            f"❌ {exc}\n\nSTRING_SESSION সেট করা আছে কিনা চেক করুন।",
        )
        try:
            await leave_voice_chat(chat_id)
        except Exception:
            pass
        return
    except Exception as exc:
        LOG.exception("Stream start failed in /playforce %s", chat_id)
        await _safe_edit(
            status_msg,
            "❌ Voice chat-এ connect করা যাচ্ছে না।\n"
            "নিশ্চিত করুন voice chat চালু আছে।\n\n"
            f"**Error:** `{type(exc).__name__}: {str(exc)[:150]}`",
        )
        try:
            await leave_voice_chat(chat_id)
        except Exception:
            pass
        return

    # Start the progress timer
    await _start_progress_timer(chat_id, duration)

    dur = format_duration(duration)
    color = _get_next_color()
    t = _get_current_theme()
    owner_mention = await get_owner_mention()
    text = (
        f"⚡ **ꜰᴏʀᴄᴇ ᴘʟᴀʏ | ᴇɴᴊᴏʏ ᴛʜᴇ ᴍᴜꜱɪᴄ**\n\n"
        f"> {t['title_icon']}  **ᴛɪᴛʟᴇ :** [{title}]({url})\n"
        f"> {t['dur_icon']}  **ᴅᴜʀᴀᴛɪᴏɴ :** {dur}\n"
        f"> 👑  **ʀᴇǫᴜᴇꜱᴛᴇᴅ :** {requester}\n\n"
        f"✨ ✦ᴘᴏᴡєʀєᴅ ʙʏ » ── {owner_mention}"
    )

    try:
        # Always try to generate a custom branded thumbnail (with spoiler)
        custom_thumb = await _build_thumb(title, channel, duration, thumbnail, requester)
        photo_src = custom_thumb or (thumbnail if thumbnail else None)
        if photo_src:
            try:
                await status_msg.delete()
            except Exception:
                pass
            try:
                now_playing_msg = await bot.send_photo(
                    chat_id,
                    photo=photo_src,
                    caption=text,
                    reply_markup=_control_keyboard(color),
                    has_spoiler=True,
                )
            except Exception as send_exc:
                LOG.debug("playforce send_photo failed for %s: %s", chat_id, send_exc)
                if custom_thumb and thumbnail and photo_src != thumbnail:
                    try:
                        now_playing_msg = await bot.send_photo(
                            chat_id,
                            photo=thumbnail,
                            caption=text,
                            reply_markup=_control_keyboard(color),
                            has_spoiler=True,
                        )
                    except Exception:
                        now_playing_msg = await bot.send_message(
                            chat_id,
                            text,
                            reply_markup=_control_keyboard(color),
                            disable_web_page_preview=True,
                        )
                else:
                    now_playing_msg = await bot.send_message(
                        chat_id,
                        text,
                        reply_markup=_control_keyboard(color),
                        disable_web_page_preview=True,
                    )
            await _add_now_playing(chat_id, now_playing_msg)
            await _add_reaction(chat_id, message.id)
        else:
            try:
                await status_msg.edit_text(text, reply_markup=_control_keyboard(color))
                await _add_now_playing(chat_id, status_msg)
            except Exception:
                new_msg = await bot.send_message(
                    chat_id,
                    text,
                    reply_markup=_control_keyboard(color),
                    disable_web_page_preview=True,
                )
                await _add_now_playing(chat_id, new_msg)
            await _add_reaction(chat_id, message.id)
    except Exception as outer_exc:
        LOG.warning(
            "playforce now-playing render failed for %s: %s", chat_id, outer_exc
        )
        try:
            new_msg = await bot.send_message(
                chat_id,
                text,
                reply_markup=_control_keyboard(color),
                disable_web_page_preview=True,
            )
            await _add_now_playing(chat_id, new_msg)
            await _add_reaction(chat_id, message.id)
        except Exception:
            pass
