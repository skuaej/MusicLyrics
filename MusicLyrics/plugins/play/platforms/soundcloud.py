"""SoundCloud integration via yt-dlp (which natively supports SoundCloud).

Used as the LAST RESORT fallback when all other platforms
(YouTube, Spotify, JioSaavn, Apple Music) fail to provide audio.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import Optional

from config import Config

LOG = logging.getLogger(__name__)

_DOWNLOADS = Config.DOWNLOADS_DIR
os.makedirs(_DOWNLOADS, exist_ok=True)


class _SoundCloudLogger:
    """Suppress noisy yt-dlp warnings for SoundCloud."""
    def debug(self, msg): LOG.debug("[sc-ytdlp] %s", msg)
    def info(self, msg): LOG.debug("[sc-ytdlp] %s", msg)
    def warning(self, msg): LOG.warning("[sc-ytdlp] %s", msg)
    def error(self, msg): LOG.warning("[sc-ytdlp] %s", msg)


_sc_logger = _SoundCloudLogger()

# SoundCloud is NOT in the YouTube critical path — it's a last-resort
# fallback.  Use aggressive timeouts / minimal retries so a slow or dead
# SoundCloud request can never hang the skip pipeline.
_SC_SOCKET_TIMEOUT = 6
_SC_RETRIES = 1
_SC_FRAGMENT_RETRIES = 1
_SC_EXTRACTOR_RETRIES = 1


def is_soundcloud_url(url: str) -> bool:
    """Check if the URL is a SoundCloud link."""
    return bool(re.match(r"https?://(www\.|m\.)?soundcloud\.com/", url))


async def search_soundcloud(query: str) -> Optional[dict]:
    """Search SoundCloud for a track using yt-dlp.

    Returns dict with keys: title, url, duration, thumbnail, channel
    or None if nothing found.
    """
    try:
        import yt_dlp

        opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": True,
            "logger": _sc_logger,
            "default_search": "scsearch1",  # SoundCloud search, 1 result
            "socket_timeout": _SC_SOCKET_TIMEOUT,
            "retries": _SC_RETRIES,
            "fragment_retries": _SC_FRAGMENT_RETRIES,
            "extractor_retries": _SC_EXTRACTOR_RETRIES,
        }

        loop = asyncio.get_running_loop()

        def _extract():
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(f"scsearch1:{query}", download=False)
                if not info:
                    return None
                entries = info.get("entries", [])
                if not entries:
                    return None
                entry = entries[0]
                return {
                    "title": entry.get("title", "Unknown"),
                    "url": entry.get("url") or entry.get("webpage_url", ""),
                    "duration": int(entry.get("duration", 0) or 0),
                    "thumbnail": entry.get("thumbnail", ""),
                    "channel": entry.get("uploader", entry.get("channel", "")),
                    "platform": "soundcloud",
                }

        result = await loop.run_in_executor(None, _extract)
        if result:
            LOG.info("SoundCloud search found: %s", result["title"])
        return result
    except Exception:
        LOG.exception("SoundCloud search failed for: %s", query)
        return None


async def get_soundcloud_info(url: str) -> Optional[dict]:
    """Get track info from a SoundCloud URL using yt-dlp.

    Returns dict with keys: title, url, duration, thumbnail, channel
    or None on failure.
    """
    try:
        import yt_dlp

        opts = {
            "quiet": True,
            "no_warnings": True,
            "logger": _sc_logger,
            "socket_timeout": _SC_SOCKET_TIMEOUT,
            "retries": _SC_RETRIES,
            "fragment_retries": _SC_FRAGMENT_RETRIES,
            "extractor_retries": _SC_EXTRACTOR_RETRIES,
        }

        loop = asyncio.get_running_loop()

        def _extract():
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
                if not info:
                    return None
                return {
                    "title": info.get("title", "Unknown"),
                    "url": info.get("webpage_url", url),
                    "duration": int(info.get("duration", 0) or 0),
                    "thumbnail": info.get("thumbnail", ""),
                    "channel": info.get("uploader", info.get("channel", "")),
                    "platform": "soundcloud",
                }

        result = await loop.run_in_executor(None, _extract)
        return result
    except Exception:
        LOG.exception("SoundCloud info extraction failed: %s", url)
        return None


async def get_soundcloud_stream_url(url: str) -> Optional[str]:
    """Extract a direct stream URL from SoundCloud via yt-dlp.

    Returns the direct audio URL or None.
    """
    try:
        import yt_dlp

        opts = {
            "quiet": True,
            "no_warnings": True,
            "format": "bestaudio/best",
            "logger": _sc_logger,
            "socket_timeout": _SC_SOCKET_TIMEOUT,
            "retries": _SC_RETRIES,
            "fragment_retries": _SC_FRAGMENT_RETRIES,
            "extractor_retries": _SC_EXTRACTOR_RETRIES,
        }

        loop = asyncio.get_running_loop()

        def _extract():
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
                if not info:
                    return None
                return info.get("url", "")

        stream_url = await loop.run_in_executor(None, _extract)
        if stream_url:
            LOG.info("SoundCloud stream URL obtained for: %s", url)
        return stream_url or None
    except Exception:
        LOG.exception("SoundCloud stream URL extraction failed: %s", url)
        return None


async def download_soundcloud(url: str) -> Optional[str]:
    """Download audio from SoundCloud using yt-dlp.

    Returns the local file path or None on failure.
    """
    try:
        import yt_dlp

        outtmpl = os.path.join(_DOWNLOADS, "sc_%(id)s.%(ext)s")
        opts = {
            "format": "bestaudio/best",
            "outtmpl": outtmpl,
            "quiet": True,
            "no_warnings": True,
            "logger": _sc_logger,
            "socket_timeout": _SC_SOCKET_TIMEOUT,
            "retries": _SC_RETRIES,
            "fragment_retries": _SC_FRAGMENT_RETRIES,
            "extractor_retries": _SC_EXTRACTOR_RETRIES,
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }],
            "max_filesize": 100 * 1024 * 1024,  # 100MB limit
        }

        loop = asyncio.get_running_loop()

        def _download():
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=True)
                if not info:
                    return None
                # yt-dlp may change extension after postprocessing
                vid_id = info.get("id", "unknown")
                # Check for the postprocessed mp3 file
                mp3_path = os.path.join(_DOWNLOADS, f"sc_{vid_id}.mp3")
                if os.path.isfile(mp3_path):
                    return mp3_path
                # Fallback: check common extensions
                for ext in ("mp3", "m4a", "opus", "ogg", "wav", "webm"):
                    path = os.path.join(_DOWNLOADS, f"sc_{vid_id}.{ext}")
                    if os.path.isfile(path):
                        return path
                return None

        filepath = await loop.run_in_executor(None, _download)
        if filepath:
            LOG.info("SoundCloud download complete: %s", filepath)
        return filepath
    except Exception:
        LOG.exception("SoundCloud download failed: %s", url)
        return None


async def search_and_download_soundcloud(query: str) -> tuple[Optional[str], Optional[dict]]:
    """Search SoundCloud and download the first result.

    Returns (file_path, info_dict) or (None, None).
    This is the ultimate fallback when all other platforms fail.
    """
    info = await search_soundcloud(query)
    if not info or not info.get("url"):
        return None, None

    filepath = await download_soundcloud(info["url"])
    if filepath and os.path.isfile(filepath):
        return filepath, info

    # If download failed, try getting stream URL instead
    stream_url = await get_soundcloud_stream_url(info["url"])
    if stream_url:
        return stream_url, {**info, "_is_stream_url": True}

    return None, None
