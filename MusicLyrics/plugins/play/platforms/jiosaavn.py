"""JioSaavn integration via the public saavn.dev API."""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Optional, Tuple

import aiohttp

LOG = logging.getLogger(__name__)

_API_BASE = "https://saavn.dev/api"
# Reduced fallback list — fewer concurrent requests = faster overall
# failure detection and less chance of the event loop hanging on a slow
# unreachable endpoint.  Only well-known reliable mirrors.
_API_FALLBACKS = [
    "https://jiosaavn-api-privatecvc2.vercel.app",
    "https://saavn.dev/api",
]
_API_FALLBACK = _API_FALLBACKS[0]

# Short timeouts — JioSaavn is OUTSIDE the critical YouTube path, so we
# want fast failure rather than long retries.  This prevents skip from
# hanging while waiting on a slow JioSaavn mirror.
_SAAVN_TIMEOUT = 3.0
_SAAVN_DL_TIMEOUT = 45.0


def is_jiosaavn_url(url: str) -> bool:
    return bool(re.match(r"https?://(www\.)?jiosaavn\.com/", url))


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _search_songs_from_base(session: aiohttp.ClientSession, base: str, query: str) -> Optional[dict]:
    """Attempt a song search against one API base URL. Returns parsed song or None."""
    try:
        async with session.get(
            f"{base}/api/search/songs" if "privatecvc2" in base else f"{base}/search/songs",
            params={"query": query, "limit": 1},
            timeout=aiohttp.ClientTimeout(total=_SAAVN_TIMEOUT),
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json(content_type=None)
        results = data.get("data", {}).get("results", [])
        if not results:
            return None
        return _parse_song(results[0])
    except Exception:
        return None


async def _fetch_song_from_base(session: aiohttp.ClientSession, base: str, url: str) -> Optional[dict]:
    """Attempt to fetch song details by JioSaavn URL from one API base. Returns parsed song or None."""
    try:
        endpoint = f"{base}/api/songs" if "privatecvc2" in base else f"{base}/songs"
        async with session.get(
            endpoint,
            params={"link": url},
            timeout=aiohttp.ClientTimeout(total=_SAAVN_TIMEOUT),
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json(content_type=None)
        songs = data.get("data", [])
        if not songs:
            return None
        return _parse_song(songs[0])
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def search_jiosaavn(query: str) -> Optional[dict]:
    """Search JioSaavn; return first result or None.

    Tries the primary API base (saavn.dev) first, then falls back to
    ALL alternate endpoints CONCURRENTLY for speed and reliability.

    Keys: title, url, duration (sec), thumbnail, artist, download_url.
    """
    try:
        async with aiohttp.ClientSession() as session:
            # Primary endpoint first (fastest)
            result = await _search_songs_from_base(session, _API_BASE, query)
            if result:
                return result

            LOG.warning("JioSaavn primary API returned no results for %r; trying fallbacks concurrently.", query)

            # Try ALL fallbacks concurrently — first result wins
            tasks = [
                asyncio.create_task(_search_songs_from_base(session, base, query))
                for base in _API_FALLBACKS
            ]
            pending = set(tasks)
            while pending:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    try:
                        res = task.result()
                        if res:
                            for p in pending:
                                p.cancel()
                            return res
                    except Exception:
                        pass

        LOG.warning("JioSaavn: no results found on any endpoint for %r", query)
        return None
    except Exception:
        LOG.exception("JioSaavn search failed: %s", query)
        return None


async def get_jiosaavn_song(url: str) -> Optional[dict]:
    """Get song details and direct download URL from a JioSaavn link.

    Tries the primary API base first, then ALL fallback endpoints concurrently.
    """
    try:
        async with aiohttp.ClientSession() as session:
            result = await _fetch_song_from_base(session, _API_BASE, url)
            if result:
                return result

            LOG.warning("JioSaavn primary API failed for URL %r; trying fallbacks concurrently.", url)

            # Try ALL fallbacks concurrently
            tasks = [
                asyncio.create_task(_fetch_song_from_base(session, base, url))
                for base in _API_FALLBACKS
            ]
            pending = set(tasks)
            while pending:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    try:
                        res = task.result()
                        if res:
                            for p in pending:
                                p.cancel()
                            return res
                    except Exception:
                        pass

        LOG.warning("JioSaavn: could not fetch song for URL %r on any endpoint.", url)
        return None
    except Exception:
        LOG.exception("JioSaavn song fetch failed: %s", url)
        return None


async def get_jiosaavn_stream_url(url_or_id: str) -> Optional[str]:
    """Return the direct CDN stream/download URL for a JioSaavn track.

    Accepts a full JioSaavn page URL or a song ID (as returned by the API).
    No file is written to disk — only the CDN URL string is returned.

    Returns the highest-quality CDN URL, or None if it cannot be determined.
    """
    try:
        # If it looks like a JioSaavn web URL, resolve via the songs endpoint.
        if is_jiosaavn_url(url_or_id):
            song = await get_jiosaavn_song(url_or_id)
            if song:
                return song.get("download_url") or None
            return None

        # Otherwise treat as a song ID and hit the /songs/{id} endpoint.
        async with aiohttp.ClientSession() as session:
            for base in (_API_BASE, _API_FALLBACK):
                try:
                    endpoint = (
                        f"{base}/api/songs/{url_or_id}"
                        if "privatecvc2" in base
                        else f"{base}/songs/{url_or_id}"
                    )
                    async with session.get(
                        endpoint,
                        timeout=aiohttp.ClientTimeout(total=_SAAVN_TIMEOUT + 2),
                    ) as resp:
                        if resp.status != 200:
                            continue
                        data = await resp.json(content_type=None)
                    songs = data.get("data", [])
                    if songs:
                        parsed = _parse_song(songs[0])
                        cdn_url = parsed.get("download_url")
                        if cdn_url:
                            return cdn_url
                except Exception:
                    continue

        return None
    except Exception:
        LOG.exception("JioSaavn stream URL lookup failed: %s", url_or_id)
        return None


async def download_jiosaavn(url: str, *, song_info: Optional[dict] = None) -> Optional[str]:
    """Download a song from JioSaavn and return the local file path.

    Parameters
    ----------
    url:
        The JioSaavn page URL (used to look up the song when *song_info* is
        not provided).
    song_info:
        Optional pre-fetched song dict (as returned by :func:`search_jiosaavn`
        or :func:`get_jiosaavn_song`).  When supplied and it contains a
        ``download_url`` key, the API lookup is skipped entirely, saving a
        network round-trip.
    """
    import os
    import aiofiles
    from config import Config

    # Resolve song metadata -------------------------------------------------
    if song_info and song_info.get("download_url"):
        song = song_info
    else:
        song = await get_jiosaavn_song(url)
        if not song or not song.get("download_url"):
            LOG.warning("download_jiosaavn: could not resolve download URL for %r", url)
            return None

    os.makedirs(Config.DOWNLOADS_DIR, exist_ok=True)

    # Build a safe filename --------------------------------------------------
    safe = re.sub(r"[^\w\s-]", "", song["title"])[:60].strip()
    filepath = os.path.join(Config.DOWNLOADS_DIR, f"{safe}.m4a")

    # Stream to disk ---------------------------------------------------------
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                song["download_url"],
                timeout=aiohttp.ClientTimeout(total=_SAAVN_DL_TIMEOUT),
            ) as resp:
                if resp.status != 200:
                    LOG.warning(
                        "download_jiosaavn: CDN returned HTTP %s for %r",
                        resp.status,
                        song["download_url"],
                    )
                    return None
                async with aiofiles.open(filepath, "wb") as f:
                    async for chunk in resp.content.iter_chunked(1024 * 64):
                        await f.write(chunk)
        return filepath
    except Exception:
        LOG.exception("JioSaavn download failed: %s", url)
        return None


async def search_and_download_jiosaavn(query: str) -> Tuple[Optional[str], Optional[dict]]:
    """Search JioSaavn for *query* and download the top result.

    This is the primary entry point for plain-text queries.  It combines
    :func:`search_jiosaavn` and :func:`download_jiosaavn` into a single
    convenience call and avoids the redundant API round-trip by passing the
    already-resolved *song_info* directly to the downloader.

    Returns
    -------
    (filepath, info_dict)
        *filepath* is the absolute path to the downloaded audio file.
        *info_dict* contains: title, url, duration, thumbnail, artist,
        platform="jiosaavn".
        Both values are ``None`` when the search yields no results or the
        download fails.
    """
    song = await search_jiosaavn(query)
    if not song:
        LOG.warning("search_and_download_jiosaavn: no JioSaavn result for %r", query)
        return None, None

    if not song.get("download_url"):
        LOG.warning(
            "search_and_download_jiosaavn: result for %r has no download URL", query
        )
        return None, None

    # Pass song_info so download_jiosaavn skips the extra API call.
    filepath = await download_jiosaavn(song.get("url", ""), song_info=song)
    if not filepath:
        return None, None

    info_dict = {
        "title": song.get("title", "Unknown"),
        "url": song.get("url", ""),
        "duration": song.get("duration", 0),
        "thumbnail": song.get("thumbnail", ""),
        "artist": song.get("artist", "Unknown"),
        "platform": "jiosaavn",
    }
    return filepath, info_dict


def _parse_song(song: dict) -> dict:
    """Normalise a JioSaavn API song object."""
    download_urls = song.get("downloadUrl", [])
    # pick highest quality
    dl_url = ""
    if download_urls:
        dl_url = download_urls[-1].get("url", "")

    images = song.get("image", [])
    thumb = images[-1].get("url", "") if images else ""

    artists = song.get("artists", {}).get("primary", [])
    artist_str = ", ".join(a.get("name", "") for a in artists) if artists else (
        song.get("primaryArtists", "Unknown")
    )

    return {
        "title": song.get("name", "Unknown"),
        "url": song.get("url", ""),
        "duration": int(song.get("duration", 0)),
        "thumbnail": thumb,
        "artist": artist_str,
        "download_url": dl_url,
    }
