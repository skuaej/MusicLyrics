"""YouTube search and download helpers.

Search uses YouTube's innertube API directly via aiohttp (no external
library needed — avoids youtube-search-python httpx compatibility issues).

Stream URL extraction uses Piped/Invidious API proxies as PRIMARY method
(works on cloud servers without cookies), with yt-dlp as fallback.
"""

from __future__ import annotations

import asyncio
import glob
import json
import logging
import os
import random
import re
from typing import Optional

import aiohttp

from config import Config

LOG = logging.getLogger(__name__)

class _YtDlpLogger:
    """Custom logger for yt-dlp that suppresses noisy warnings."""
    def debug(self, msg): LOG.debug("[yt-dlp] %s", msg)
    def info(self, msg): LOG.debug("[yt-dlp] %s", msg)
    def warning(self, msg):
        if "is not a valid URL" in str(msg):
            return  # Suppress noisy generic extractor warnings
        LOG.warning("[yt-dlp] %s", msg)
    def error(self, msg): LOG.warning("[yt-dlp] %s", msg)

_ytdlp_logger = _YtDlpLogger()

_DOWNLOADS = Config.DOWNLOADS_DIR
os.makedirs(_DOWNLOADS, exist_ok=True)

# ══════════════════════════════════════════════════════════════════════════════
# PIPED / INVIDIOUS API — cookie-free YouTube proxy (PRIMARY method)
# ══════════════════════════════════════════════════════════════════════════════

# Multiple public Piped API instances for redundancy (updated May 2026)
# Ordered by reliability — verified working instances first
# Dead/unreliable instances removed. Timeout reduced from 15s to 8s.
_PIPED_INSTANCES = [
    "https://pipedapi.kavin.rocks",
    "https://pipedapi.r4fo.com",
    "https://pipedapi.adminforge.de",
    "https://api.piped.yt",
    "https://pipedapi.leptons.xyz",
    "https://pipedapi.drgns.space",
    "https://pipedapi.in.projectsegfau.lt",
    "https://pipedapi.us.projectsegfau.lt",
    "https://pipedapi.frontendfriendly.xyz",
    "https://pipedapi.syncpundit.io",
    "https://piped-api.lunar.icu",
    "https://pipedapi.moomoo.me",
    "https://pipedapi.darkness.services",
    "https://api.piped.private.coffee",
    "https://pipedapi.hostux.net",
    "https://pipedapi.ngn.tf",
    "https://watchapi.whatever.social",
    "https://pipedapi.smnz.de",
]

# Track dead Piped/Invidious instances at runtime to skip them
# Each entry is (instance_url, timestamp_when_marked_dead)
_dead_piped: dict[str, float] = {}
_dead_invidious: dict[str, float] = {}
_DEAD_INSTANCE_RECOVERY_SECONDS = 120  # Re-try dead instances after 2 minutes

# Invidious instances as additional fallback (updated May 2026)
# These act as YouTube proxies — no cookies needed
_INVIDIOUS_INSTANCES = [
    "https://inv.nadeko.net",
    "https://invidious.fdn.fr",
    "https://iv.datura.network",
    "https://vid.puffyan.us",
    "https://invidious.nerdvpn.de",
    "https://inv.tux.pizza",
    "https://invidious.perennialte.ch",
    "https://invidious.jing.rocks",
    "https://invidious.privacyredirect.com",
    "https://yt.artemislena.eu",
    "https://invidious.lunar.icu",
    "https://invidious.darkness.services",
    "https://inv.in.projectsegfau.lt",
    "https://invidious.private.coffee",
    "https://invidious.protokolla.fi",
    "https://iv.melmac.space",
    "https://invidious.io.lol",
]

# Cobalt API — reliable cloud-friendly YouTube proxy
# Requires API key since late 2024 — set COBALT_API_KEY env var
_COBALT_INSTANCES = [
    "https://api.cobalt.tools",
    "https://cobalt-api.kwiatekmiki.com",
    "https://cobalt.canine.tools",
    "https://cobalt-api.ayo.tf",
    "https://co.eepy.today",
    "https://cobalt-api.hyper.lol",
    "https://cobalt.tskau.team",
]
# Allow custom Cobalt instance via env var (e.g., self-hosted)
_cobalt_custom_url = os.environ.get("COBALT_API_URL", "").strip().rstrip("/")
if _cobalt_custom_url:
    _COBALT_INSTANCES.insert(0, _cobalt_custom_url)
_COBALT_API_KEY = os.environ.get("COBALT_API_KEY", "").strip()

# Validate Cobalt API key format — real keys are long hex/alphanumeric strings
if _COBALT_API_KEY and len(_COBALT_API_KEY) < 20:
    LOG.warning(
        "COBALT_API_KEY looks invalid (too short: %d chars). "
        "Real Cobalt API keys are typically 32+ character hex strings. "
        "Get a valid key from https://cobalt.tools — will try without key first.",
        len(_COBALT_API_KEY),
    )
    # Don't use an invalid key — it causes 401 errors everywhere
    _COBALT_API_KEY = ""

_PROXY_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/137.0.0.0 Safari/537.36"
    ),
}

# ── Proxy support for cloud deployments ──────────────────────────────────────
# Track proxy health — auto-disable proxies that return 402/403/407
_proxy_dead = False
_proxy_fail_count = 0
_proxy_dead_since: float = 0.0  # timestamp when proxy was disabled
_PROXY_FAIL_THRESHOLD = 3  # Disable after 3 failures (tolerate transient errors)
_PROXY_RECOVERY_SECONDS = 120  # Re-try proxy every 2 minutes


def _mark_proxy_failed():
    """Mark the proxy as potentially dead after a failure."""
    global _proxy_fail_count, _proxy_dead, _proxy_dead_since
    _proxy_fail_count += 1
    if _proxy_fail_count >= _PROXY_FAIL_THRESHOLD:
        _proxy_dead = True
        import time as _t
        _proxy_dead_since = _t.time()
        LOG.warning("Proxy disabled after %d consecutive failures. "
                    "Will auto-retry in %d seconds. "
                    "Check your YOUTUBE_PROXY subscription.",
                    _proxy_fail_count, _PROXY_RECOVERY_SECONDS)


def _mark_proxy_ok():
    """Reset proxy failure counter on success."""
    global _proxy_fail_count, _proxy_dead, _proxy_dead_since
    _proxy_fail_count = 0
    _proxy_dead = False
    _proxy_dead_since = 0.0


def _check_proxy_recovery():
    """Periodically re-enable proxy for retry (subscription might have been renewed)."""
    global _proxy_dead, _proxy_fail_count, _proxy_dead_since
    if _proxy_dead and _proxy_dead_since > 0:
        import time as _t
        elapsed = _t.time() - _proxy_dead_since
        if elapsed >= _PROXY_RECOVERY_SECONDS:
            LOG.info("Proxy recovery: re-enabling proxy for retry after %d seconds", int(elapsed))
            _proxy_dead = False
            _proxy_fail_count = 0
            _proxy_dead_since = 0.0


def _get_proxy() -> Optional[str]:
    """Get a random proxy URL from the proxy list or single proxy config.

    Returns None if the proxy has been auto-disabled due to failures
    (e.g., 402 Payment Required = expired subscription).
    Always ensures the returned proxy is a valid URL (http://user:pass@host:port).
    """
    _check_proxy_recovery()  # Re-enable proxy periodically for retry
    if _proxy_dead:
        return None  # Proxy is dead, go direct

    proxy = None
    # Priority 1: Proxy list (rotation)
    if Config.YOUTUBE_PROXIES:
        proxy = random.choice(Config.YOUTUBE_PROXIES)
    else:
        # Priority 2: Single proxy
        proxy = Config.YOUTUBE_PROXY or os.environ.get("YOUTUBE_PROXY", "") or None

    # Safety: ensure proxy is a valid URL, not raw ip:port:user:pass format
    if proxy and not proxy.startswith(("http://", "https://", "socks")):
        parts = proxy.split(":")
        if len(parts) == 4:
            # Webshare format: ip:port:user:pass
            ip, port, user, pw = parts
            proxy = f"http://{user}:{pw}@{ip}:{port}"
            LOG.info("Auto-converted Webshare proxy format to URL: %s", proxy[:40])
        elif "@" in proxy:
            proxy = f"http://{proxy}"
        else:
            proxy = f"http://{proxy}"

    return proxy if proxy else None


def _aio_session_kwargs() -> dict:
    """Return kwargs for aiohttp.ClientSession with proxy support."""
    return {}


def _aio_request_kwargs() -> dict:
    """Return kwargs for aiohttp request methods (get/post) with proxy.

    ONLY use this for direct YouTube API calls (Innertube).
    Do NOT use for third-party APIs (Piped, Invidious, Cobalt).
    """
    proxy = _get_proxy()
    if proxy:
        return {"proxy": proxy}
    return {}


def _extract_video_id(url: str) -> Optional[str]:
    """Extract YouTube video ID from various URL formats."""
    patterns = [
        r"(?:v=|/v/|youtu\.be/)([a-zA-Z0-9_-]{11})",
        r"(?:embed/|shorts/)([a-zA-Z0-9_-]{11})",
    ]
    for pat in patterns:
        m = re.search(pat, url)
        if m:
            return m.group(1)
    return None


async def _piped_get_streams(video_id: str) -> Optional[dict]:
    """Get stream info from Piped API. Returns dict with audioStreams, videoStreams, etc.

    NOTE: Does NOT use YOUTUBE_PROXY — Piped instances ARE the proxy.
    Uses concurrent requests to multiple instances for speed.
    """
    instances = list(_PIPED_INSTANCES)
    random.shuffle(instances)

    # Try instances in batches of 10 concurrently for speed
    batch_size = 10
    for i in range(0, len(instances), batch_size):
        batch = instances[i:i + batch_size]
        tasks = [_try_piped_instance(base_url, video_id) for base_url in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, dict) and (result.get("audioStreams") or result.get("videoStreams")):
                return result

    return None


async def _try_piped_instance(base_url: str, video_id: str) -> Optional[dict]:
    """Try a single Piped instance."""
    import time as _t
    # Skip known-dead instances, but auto-recover after timeout
    if base_url in _dead_piped:
        if _t.time() - _dead_piped[base_url] < _DEAD_INSTANCE_RECOVERY_SECONDS:
            return None  # Still in cooldown
        else:
            del _dead_piped[base_url]  # Recovery: give it another chance
            LOG.info("Piped instance %s recovered after cooldown", base_url)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{base_url}/streams/{video_id}",
                headers=_PROXY_HEADERS,
                timeout=aiohttp.ClientTimeout(total=6),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data and (data.get("audioStreams") or data.get("videoStreams")):
                        LOG.info("Piped stream obtained from %s for %s", base_url, video_id)
                        return data
                else:
                    LOG.debug("Piped %s returned HTTP %d for %s", base_url, resp.status, video_id)
                    if resp.status in (500, 502, 503, 520, 521, 522, 523, 524):
                        _dead_piped[base_url] = _t.time()
                        LOG.info("Piped instance %s marked dead (HTTP %d)", base_url, resp.status)
    except asyncio.TimeoutError:
        _dead_piped[base_url] = _t.time()
        LOG.debug("Piped %s timed out for %s — marked dead", base_url, video_id)
    except Exception as e:
        LOG.debug("Piped %s failed for %s: %s", base_url, video_id, e)
    return None


async def _invidious_get_streams(video_id: str) -> Optional[dict]:
    """Get stream info from Invidious API.

    NOTE: Does NOT use YOUTUBE_PROXY — Invidious instances ARE the proxy.
    Uses concurrent requests for speed.
    """
    instances = list(_INVIDIOUS_INSTANCES)
    random.shuffle(instances)

    # Try instances in batches of 10 concurrently
    batch_size = 10
    for i in range(0, len(instances), batch_size):
        batch = instances[i:i + batch_size]
        tasks = [_try_invidious_instance(base_url, video_id) for base_url in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, dict) and (result.get("adaptiveFormats") or result.get("formatStreams")):
                return result

    return None


async def _try_invidious_instance(base_url: str, video_id: str) -> Optional[dict]:
    """Try a single Invidious instance."""
    import time as _t
    # Skip known-dead instances, but auto-recover after timeout
    if base_url in _dead_invidious:
        if _t.time() - _dead_invidious[base_url] < _DEAD_INSTANCE_RECOVERY_SECONDS:
            return None  # Still in cooldown
        else:
            del _dead_invidious[base_url]  # Recovery: give it another chance
            LOG.info("Invidious instance %s recovered after cooldown", base_url)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{base_url}/api/v1/videos/{video_id}",
                headers=_PROXY_HEADERS,
                timeout=aiohttp.ClientTimeout(total=6),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data and (data.get("adaptiveFormats") or data.get("formatStreams")):
                        LOG.info("Invidious stream obtained from %s for %s", base_url, video_id)
                        return data
                else:
                    LOG.debug("Invidious %s returned HTTP %d for %s", base_url, resp.status, video_id)
                    if resp.status in (500, 502, 503, 520, 521, 522, 523, 524):
                        _dead_invidious[base_url] = _t.time()
                        LOG.info("Invidious instance %s marked dead (HTTP %d)", base_url, resp.status)
    except asyncio.TimeoutError:
        _dead_invidious[base_url] = _t.time()
        LOG.debug("Invidious %s timed out for %s — marked dead", base_url, video_id)
    except Exception as e:
        LOG.debug("Invidious %s failed for %s: %s", base_url, video_id, e)
    return None


def _best_piped_audio_url(data: dict) -> Optional[str]:
    """Pick the best audio stream URL from Piped response."""
    streams = data.get("audioStreams", [])
    if not streams:
        return None
    # Prefer opus/webm, then m4a, sorted by bitrate
    opus = [s for s in streams if s.get("codec", "").startswith("opus")]
    if opus:
        best = max(opus, key=lambda s: s.get("bitrate", 0))
        return best.get("url")
    # Fallback: any audio stream with highest bitrate
    best = max(streams, key=lambda s: s.get("bitrate", 0))
    return best.get("url")


def _best_piped_video_url(data: dict) -> Optional[str]:
    """Pick the best video stream URL from Piped response (with audio)."""
    # First try videoStreams (muxed — has both audio+video)
    streams = data.get("videoStreams", [])
    if streams:
        # Prefer mp4, max 720p
        candidates = [s for s in streams
                      if s.get("videoOnly") is not True
                      and (s.get("height", 0) or 0) <= 720]
        if not candidates:
            candidates = [s for s in streams if s.get("videoOnly") is not True]
        if candidates:
            best = max(candidates, key=lambda s: s.get("height", 0) or 0)
            return best.get("url")
    # Fallback: audio-only stream for video player
    return _best_piped_audio_url(data)


def _best_invidious_audio_url(data: dict) -> Optional[str]:
    """Pick best audio URL from Invidious response."""
    formats = data.get("adaptiveFormats", [])
    audio = [f for f in formats if f.get("type", "").startswith("audio/")]
    if not audio:
        return None
    # Prefer opus
    opus = [f for f in audio if "opus" in f.get("type", "")]
    if opus:
        best = max(opus, key=lambda f: int(f.get("bitrate", "0") or 0))
        return best.get("url")
    best = max(audio, key=lambda f: int(f.get("bitrate", "0") or 0))
    return best.get("url")


# ══════════════════════════════════════════════════════════════════════════════
# COBALT API — Reliable cloud-friendly YouTube proxy (no auth needed)
# ══════════════════════════════════════════════════════════════════════════════

async def _cobalt_get_stream(video_id: str, audio_only: bool = True) -> Optional[str]:
    """Get stream URL via Cobalt API. Works reliably on cloud servers.

    Tries with API key first, then without (for self-hosted/open instances).
    NOTE: Does NOT use YOUTUBE_PROXY — Cobalt IS the proxy to YouTube.
    """
    yt_url = f"https://www.youtube.com/watch?v={video_id}"

    # Try both v10+ endpoint (POST /) and legacy endpoint (POST /api/json)
    _endpoints = ["/", "/api/json"]

    for instance in _COBALT_INSTANCES:
        for endpoint in _endpoints:
            # Try without auth first (many instances are open), then with API key
            auth_options = [None]  # Always try without auth first
            if _COBALT_API_KEY:
                auth_options.append(_COBALT_API_KEY)  # Then try with key

            for api_key in auth_options:
                try:
                    # v10+ payload format
                    if endpoint == "/":
                        payload = {
                            "url": yt_url,
                            "downloadMode": "audio" if audio_only else "auto",
                            "audioFormat": "opus",
                            "youtubeVideoCodec": "h264",
                            "videoQuality": "720",
                        }
                    else:
                        # Legacy /api/json format
                        payload = {
                            "url": yt_url,
                            "isAudioOnly": audio_only,
                            "aFormat": "opus",
                            "vCodec": "h264",
                            "vQuality": "720",
                            "filenamePattern": "basic",
                        }

                    headers = {
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                        "User-Agent": _PROXY_HEADERS["User-Agent"],
                    }
                    if api_key:
                        headers["Authorization"] = f"Api-Key {api_key}"
                    async with aiohttp.ClientSession() as session:
                        async with session.post(
                            f"{instance}{endpoint}",
                            json=payload,
                            headers=headers,
                            timeout=aiohttp.ClientTimeout(total=6),
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                # v10+ format: {"url": "..."}
                                stream_url = data.get("url")
                                if stream_url:
                                    LOG.info("Cobalt stream obtained from %s%s for %s (audio=%s, auth=%s)",
                                             instance, endpoint, video_id, audio_only, bool(api_key))
                                    return stream_url
                                # Cobalt may return a picker for videos with separate streams
                                picker = data.get("picker")
                                if picker and isinstance(picker, list):
                                    for p_item in picker:
                                        if audio_only and p_item.get("type") == "audio":
                                            return p_item.get("url")
                                        if not audio_only and p_item.get("type") == "video":
                                            return p_item.get("url")
                                    # Fallback: first item
                                    if picker:
                                        return picker[0].get("url")
                                # Legacy format: {"status": "stream", "url": "..."}
                                if data.get("status") in ("stream", "redirect", "success"):
                                    stream_url = data.get("url")
                                    if stream_url:
                                        LOG.info("Cobalt legacy stream from %s for %s",
                                                 instance, video_id)
                                        return stream_url
                            else:
                                body = ""
                                try:
                                    body = await resp.text()
                                except Exception:
                                    pass
                                if resp.status in (401, 403):
                                    LOG.warning(
                                        "Cobalt %s%s returned HTTP %d (auth error) for %s. "
                                        "Your COBALT_API_KEY may be invalid or expired. "
                                        "Get a valid key from https://cobalt.tools",
                                        instance, endpoint, resp.status, video_id,
                                    )
                                    # Try without auth next
                                    continue
                                else:
                                    LOG.debug("Cobalt %s%s returned HTTP %d for %s: %s",
                                              instance, endpoint, resp.status, video_id, body[:100])
                except Exception as e:
                    LOG.debug("Cobalt %s%s failed for %s: %s", instance, endpoint, video_id, e)
                    continue
    return None


def _best_invidious_video_url(data: dict) -> Optional[str]:
    """Pick best video URL from Invidious response."""
    formats = data.get("formatStreams", [])
    if formats:
        candidates = [f for f in formats if (int(f.get("resolution", "0p").rstrip("p") or 0)) <= 720]
        if not candidates:
            candidates = formats
        if candidates:
            best = max(candidates, key=lambda f: int(f.get("resolution", "0p").rstrip("p") or 0))
            return best.get("url")
    return _best_invidious_audio_url(data)


def _piped_video_info(data: dict, video_id: str) -> dict:
    """Extract video info dict from Piped response."""
    return {
        "title": data.get("title", "Unknown"),
        "url": f"https://www.youtube.com/watch?v={video_id}",
        "duration": data.get("duration", 0),
        "thumbnail": data.get("thumbnailUrl", ""),
        "channel": data.get("uploader", "Unknown"),
        "video_id": video_id,
    }


def _invidious_video_info(data: dict, video_id: str) -> dict:
    """Extract video info dict from Invidious response."""
    thumbs = data.get("videoThumbnails", [])
    thumbnail = ""
    for t in thumbs:
        if t.get("quality") == "maxresdefault":
            thumbnail = t.get("url", "")
            break
    if not thumbnail and thumbs:
        thumbnail = thumbs[0].get("url", "")

    return {
        "title": data.get("title", "Unknown"),
        "url": f"https://www.youtube.com/watch?v={video_id}",
        "duration": data.get("lengthSeconds", 0),
        "thumbnail": thumbnail,
        "channel": data.get("author", "Unknown"),
        "video_id": video_id,
    }

# ══════════════════════════════════════════════════════════════════════════════
# INNERTUBE PLAYER API — Direct YouTube stream extraction (no yt-dlp needed)
# Mobile clients return direct stream URLs without signature cipher.
# This is the same method NewPipe and Piped use internally.
# ══════════════════════════════════════════════════════════════════════════════

_INNERTUBE_PLAYER_URL = "https://www.youtube.com/youtubei/v1/player"

# Mobile/TV/Web clients that return direct (non-cipher) stream URLs
# Updated May 2026 with latest client versions to avoid 403/bot detection
# NOTE: YouTube aggressively blocks cloud IPs. Order matters — most reliable first.
_PLAYER_CLIENTS = [
    # ANDROID_TESTSUITE — least blocked, returns direct URLs without cipher
    {
        "name": "ANDROID_TESTSUITE",
        "context": {
            "client": {
                "clientName": "ANDROID_TESTSUITE",
                "clientVersion": "1.9",
                "androidSdkVersion": 35,
                "hl": "en",
                "gl": "US",
                "osName": "Android",
                "osVersion": "15",
                "platform": "MOBILE",
            }
        },
        "key": "AIzaSyA8eiZmM1FaDVjRy-df2KTyQ_vz_yYM39w",
        "ua": "com.google.android.youtube/1.9 (Linux; U; Android 15; en_US) gzip",
    },
    # ANDROID_VR — less monitored, good for cloud IPs
    {
        "name": "ANDROID_VR",
        "context": {
            "client": {
                "clientName": "ANDROID_VR",
                "clientVersion": "1.62.28",
                "androidSdkVersion": 35,
                "hl": "en",
                "gl": "US",
                "osName": "Android",
                "osVersion": "15",
                "platform": "MOBILE",
            }
        },
        "key": "AIzaSyA8eiZmM1FaDVjRy-df2KTyQ_vz_yYM39w",
        "ua": "com.google.android.apps.youtube.vr.oculus/1.62.28 (Linux; U; Android 15) gzip",
    },
    # IOS — reliable for direct stream URLs (updated to latest version)
    {
        "name": "IOS",
        "context": {
            "client": {
                "clientName": "IOS",
                "clientVersion": "20.26.6",
                "deviceMake": "Apple",
                "deviceModel": "iPhone17,1",
                "hl": "en",
                "gl": "US",
                "osName": "iOS",
                "osVersion": "18.5.1",
                "platform": "MOBILE",
            }
        },
        "key": "AIzaSyB-63vPrdThhKuerbB2N_l7Kwwcxj6yUAc",
        "ua": "com.google.ios.youtube/20.26.6 (iPhone17,1; U; CPU iOS 18_5_1 like Mac OS X)",
    },
    # IOS_MUSIC — good for audio streams (updated)
    {
        "name": "IOS_MUSIC",
        "context": {
            "client": {
                "clientName": "IOS_MUSIC",
                "clientVersion": "7.48.0",
                "deviceMake": "Apple",
                "deviceModel": "iPhone17,1",
                "hl": "en",
                "gl": "US",
                "osName": "iOS",
                "osVersion": "18.5.1",
                "platform": "MOBILE",
            }
        },
        "key": "AIzaSyBAETezhkwP0ZWA02RsqT1zu78Fpt0bC_s",
        "ua": "com.google.ios.youtubemusic/7.48.0 (iPhone17,1; U; CPU iOS 18_5_1 like Mac OS X)",
    },
    # ANDROID_MUSIC — alternative mobile music client (updated)
    {
        "name": "ANDROID_MUSIC",
        "context": {
            "client": {
                "clientName": "ANDROID_MUSIC",
                "clientVersion": "7.43.50",
                "androidSdkVersion": 35,
                "hl": "en",
                "gl": "US",
                "osName": "Android",
                "osVersion": "15",
                "platform": "MOBILE",
            }
        },
        "key": "AIzaSyAOghZGza2MQSZkY_zfZ370N-PUdXEo8AI",
        "ua": "com.google.android.apps.youtube.music/7.43.50 (Linux; U; Android 15) gzip",
    },
    # TV_EMBEDDED — works for some videos, no cipher needed
    {
        "name": "TV_EMBEDDED",
        "context": {
            "client": {
                "clientName": "TVHTML5_SIMPLY_EMBEDDED_PLAYER",
                "clientVersion": "2.0",
                "hl": "en",
                "gl": "US",
                "platform": "TV",
            }
        },
        "key": "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8",
        "ua": "Mozilla/5.0 (SMART-TV; Linux; Tizen 7.0)",
        "embed": True,
    },
    # MEDIACONNECT — newer client, often bypasses restrictions
    {
        "name": "MEDIACONNECT",
        "context": {
            "client": {
                "clientName": "MEDIA_CONNECT_FRONTEND",
                "clientVersion": "0.1",
                "hl": "en",
                "gl": "US",
            }
        },
        "key": "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8",
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
    },
]


def _parse_netscape_cookies(cookie_file: str) -> dict[str, str]:
    """Parse Netscape cookie file into a dict of name->value for youtube.com."""
    cookies = {}
    try:
        with open(cookie_file, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    # But parse #HttpOnly_ lines
                    if line.startswith("#HttpOnly_"):
                        line = line[len("#HttpOnly_"):]
                    else:
                        continue
                parts = line.split("\t")
                if len(parts) >= 7 and "youtube" in parts[0]:
                    cookies[parts[5]] = parts[6]
    except Exception:
        pass
    return cookies


async def _innertube_web_with_cookies(video_id: str, cookie_file: str) -> Optional[dict]:
    """Try WEB client InnerTube with cookie authentication.

    This is the most reliable method for cloud servers when cookies are available.
    YouTube trusts WEB client requests with valid login cookies even from cloud IPs.
    """
    cookies = _parse_netscape_cookies(cookie_file)
    if not cookies.get("SID") and not cookies.get("__Secure-1PSID"):
        return None  # No valid login cookies

    # Build cookie header string
    cookie_header = "; ".join(f"{k}={v}" for k, v in cookies.items())

    # Generate SAPISIDHASH for authenticated requests
    sapisid = cookies.get("SAPISID") or cookies.get("__Secure-3PAPISID", "")
    import hashlib
    import time as _time
    origin = "https://www.youtube.com"
    timestamp = str(int(_time.time()))
    hash_input = f"{timestamp} {sapisid} {origin}"
    sapisidhash = hashlib.sha1(hash_input.encode()).hexdigest()
    auth_header = f"SAPISIDHASH {timestamp}_{sapisidhash}"

    _web_version = "2.20260525.01.00"
    # signatureTimestamp is typically derived from YouTube's player JS.
    # A reasonable approximation: days since 2020-01-01 (YouTube epoch).
    # Real value is in player JS as `signatureTimestamp:NNNNN`.
    import time as _time2
    _sts = (int(_time2.time()) - 1577836800) // 86400  # Days since 2020-01-01
    payload = {
        "context": {
            "client": {
                "clientName": "WEB",
                "clientVersion": _web_version,
                "hl": "en",
                "gl": "US",
            }
        },
        "videoId": video_id,
        "playbackContext": {
            "contentPlaybackContext": {
                "html5Preference": "HTML5_PREF_WANTS",
                "signatureTimestamp": _sts,
            }
        },
        "contentCheckOk": True,
        "racyCheckOk": True,
    }

    headers = {
        "Content-Type": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/137.0.0.0 Safari/537.36"
        ),
        "Origin": origin,
        "Referer": f"{origin}/",
        "Cookie": cookie_header,
        "Authorization": auth_header,
        "X-Youtube-Client-Name": "1",
        "X-Youtube-Client-Version": _web_version,
        "X-Goog-Visitor-Id": cookies.get("VISITOR_INFO1_LIVE", ""),
        "X-Goog-AuthUser": "0",
    }

    api_url = f"{_INNERTUBE_PLAYER_URL}?key=AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                api_url,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=6),
                **_aio_request_kwargs(),
            ) as resp:
                if resp.status != 200:
                    LOG.debug("Innertube WEB+cookies HTTP %d for %s", resp.status, video_id)
                    return None
                data = await resp.json()

                ps = data.get("playabilityStatus", {})
                if ps.get("status") != "OK":
                    LOG.debug("Innertube WEB+cookies: status=%s for %s",
                             ps.get("status"), video_id)
                    return None

                sd = data.get("streamingData", {})
                all_fmts = sd.get("adaptiveFormats", []) + sd.get("formats", [])

                # With WEB client, formats may have signatureCipher — we accept both
                if all_fmts:
                    # Prefer direct URLs
                    direct = [f for f in all_fmts
                              if f.get("url") and not f.get("signatureCipher")]
                    if direct:
                        LOG.info("Innertube WEB+cookies: %d direct formats for %s",
                                len(direct), video_id)
                        return data
                    else:
                        # Accept cipher formats too — yt-dlp can decrypt them
                        LOG.info("Innertube WEB+cookies: %d cipher formats for %s (accepted)",
                                 len(all_fmts), video_id)
                        return data
    except asyncio.TimeoutError:
        LOG.warning("Innertube WEB+cookies TIMEOUT for %s — proxy may be dead", video_id)
        _mark_proxy_failed()
    except (aiohttp.ClientConnectorError, aiohttp.ClientOSError, OSError) as e:
        LOG.warning("Innertube WEB+cookies connection error for %s: %s", video_id, e)
        _mark_proxy_failed()
    except Exception as e:
        LOG.debug("Innertube WEB+cookies error for %s: %s", video_id, e)

    return None


async def _innertube_player(video_id: str) -> Optional[dict]:
    """Get player response from YouTube Innertube Player API directly.

    Tries WEB client with cookies first (best for cloud servers),
    then mobile clients for direct stream URLs without signature cipher.
    Auto-detects and disables broken proxies (402 Payment Required).
    Skips entirely if proxy is dead (Innertube needs proxy on cloud).
    """
    # If proxy is dead, try Innertube WITHOUT proxy — cloud IPs may work
    # for some clients (especially mobile clients and TV_EMBEDDED).
    # Previously we skipped Innertube entirely when proxy was dead, but
    # some clients work without proxy even from cloud IPs.
    _skip_proxy_for_innertube = _proxy_dead

    # Track cipher-only data as fallback (better than nothing)
    _last_innertube_cipher_data = {}

    # Try WEB client with cookies first (works on cloud IPs when authenticated)
    cookie_file = _get_cookie()
    if cookie_file:
        try:
            result = await _innertube_web_with_cookies(video_id, cookie_file)
            if result:
                return result
        except Exception:
            LOG.debug("Innertube WEB+cookies failed for %s", video_id)

    # Try all mobile/TV clients (no cookies needed — direct stream URLs)
    for client in _PLAYER_CLIENTS:
        try:
            payload = {
                "context": client["context"],
                "videoId": video_id,
                "playbackContext": {
                    "contentPlaybackContext": {
                        "html5Preference": "HTML5_PREF_WANTS",
                    }
                },
                "contentCheckOk": True,
                "racyCheckOk": True,
            }
            # TV_EMBEDDED needs thirdParty.embedUrl
            if client.get("embed"):
                payload["thirdParty"] = {"embedUrl": "https://www.google.com"}

            headers = {
                "Content-Type": "application/json",
                "User-Agent": client["ua"],
                "Origin": "https://www.youtube.com",
                "Referer": "https://www.youtube.com/",
            }
            api_url = f"{_INNERTUBE_PLAYER_URL}?key={client['key']}"

            async with aiohttp.ClientSession() as session:
                req_kwargs = {} if _skip_proxy_for_innertube else _aio_request_kwargs()
                async with session.post(
                    api_url,
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=5),
                    **req_kwargs,
                ) as resp:
                    if resp.status != 200:
                        if resp.status in (402, 407):
                            _mark_proxy_failed()
                        LOG.debug("Innertube player %s HTTP %d for %s",
                                 client["name"], resp.status, video_id)
                        continue
                    data = await resp.json()

                    # Check playability
                    ps = data.get("playabilityStatus", {})
                    if ps.get("status") != "OK":
                        LOG.debug("Innertube %s: status=%s for %s (reason: %s)",
                                 client["name"], ps.get("status"), video_id,
                                 ps.get("reason", "unknown"))
                        continue

                    # Check for HLS manifest URL first (no signature needed)
                    sd = data.get("streamingData", {})
                    hls_url = sd.get("hlsManifestUrl")
                    if hls_url:
                        LOG.info("Innertube %s: HLS manifest available for %s",
                                 client["name"], video_id)
                        # Store HLS URL in data for extraction
                        data["_hls_manifest_url"] = hls_url
                        return data

                    all_fmts = sd.get("adaptiveFormats", []) + sd.get("formats", [])

                    # Only use formats with direct URL (no signatureCipher)
                    direct = [f for f in all_fmts
                              if f.get("url") and not f.get("signatureCipher")]
                    if direct:
                        LOG.info("Innertube player: %d direct formats via %s for %s",
                                len(direct), client["name"], video_id)
                        return data
                    else:
                        # Store cipher format count for logging
                        cipher_count = len([f for f in all_fmts if f.get("signatureCipher")])
                        LOG.debug("Innertube %s: %d formats but all cipher for %s "
                                 "(cipher=%d, total=%d)",
                                 client["name"], len(all_fmts), video_id,
                                 cipher_count, len(all_fmts))
                        # Accept cipher formats as last resort — yt-dlp can
                        # decrypt these in the fallback path. Better than
                        # returning None and skipping to slower methods.
                        if not _last_innertube_cipher_data:
                            _last_innertube_cipher_data.update(data)

        except asyncio.TimeoutError:
            LOG.warning("Innertube player %s TIMEOUT for %s — marking proxy dead",
                       client["name"], video_id)
            _mark_proxy_failed()
            continue
        except (aiohttp.ClientConnectorError, aiohttp.ClientOSError, OSError) as e:
            LOG.warning("Innertube player %s connection error for %s: %s — marking proxy dead",
                       client["name"], video_id, e)
            _mark_proxy_failed()
            continue
        except Exception as e:
            LOG.debug("Innertube player %s error for %s: %s",
                     client["name"], video_id, e)
            continue

    # If we got cipher-only formats from any client, return them
    # yt-dlp can handle cipher decryption in the fallback path
    if _last_innertube_cipher_data:
        LOG.info("Innertube: returning cipher-only data for %s (yt-dlp will decrypt)", video_id)
        return _last_innertube_cipher_data

    return None


def _best_innertube_audio(data: dict) -> Optional[str]:
    """Extract best direct audio URL from Innertube player response.

    Priority: direct adaptive audio → direct combined formats → HLS manifest.
    HLS is last resort — direct URLs give better quality and lower latency,
    but HLS still works via ffmpeg when direct URLs are unavailable (common
    on cloud IPs where only mobile/TV Innertube clients return results).
    """
    sd = data.get("streamingData", {})

    # 1. Adaptive audio-only formats (best quality)
    adaptive = sd.get("adaptiveFormats", [])
    audio = [f for f in adaptive
             if f.get("url") and not f.get("signatureCipher")
             and f.get("mimeType", "").startswith("audio/")]
    if audio:
        # Prefer opus/webm
        opus = [f for f in audio if "opus" in f.get("mimeType", "")]
        pool = opus if opus else audio
        best = max(pool, key=lambda f: int(f.get("bitrate", 0)))
        return best.get("url")

    # 2. Combined formats (audio+video muxed)
    combined = sd.get("formats", [])
    direct = [f for f in combined
              if f.get("url") and not f.get("signatureCipher")]
    if direct:
        return direct[0].get("url")

    # 3. HLS manifest as last resort (ffmpeg can handle .m3u8)
    hls_url = data.get("_hls_manifest_url")
    if hls_url:
        LOG.info("Using HLS manifest URL as last resort for audio")
        return hls_url

    return None


def _best_innertube_video(data: dict) -> Optional[str]:
    """Extract best direct video URL from Innertube player response.

    Priority: direct combined → direct adaptive video → HLS manifest.
    HLS is last resort but still works via ffmpeg when direct URLs are
    unavailable (common on cloud IPs).
    """
    sd = data.get("streamingData", {})

    # 1. Combined formats first (has audio+video — best for VC streaming)
    combined = sd.get("formats", [])
    direct = [f for f in combined
              if f.get("url") and not f.get("signatureCipher")]
    if direct:
        candidates = [f for f in direct if (f.get("height", 0) or 0) <= 720]
        if not candidates:
            candidates = direct
        best = max(candidates, key=lambda f: f.get("height", 0) or 0)
        return best.get("url")

    # Adaptive video-only
    adaptive = sd.get("adaptiveFormats", [])
    video = [f for f in adaptive
             if f.get("url") and not f.get("signatureCipher")
             and f.get("mimeType", "").startswith("video/")]
    if video:
        candidates = [f for f in video if (f.get("height", 0) or 0) <= 720]
        if not candidates:
            candidates = video
        best = max(candidates, key=lambda f: f.get("height", 0) or 0)
        return best.get("url")

    # 3. HLS manifest as last resort (ffmpeg can handle .m3u8)
    hls_url = data.get("_hls_manifest_url")
    if hls_url:
        LOG.info("Using HLS manifest URL as last resort for video")
        return hls_url

    # 4. Audio-only fallback
    return _best_innertube_audio(data)


def _innertube_video_info(data: dict, video_id: str) -> dict:
    """Extract video info from Innertube player response."""
    vd = data.get("videoDetails", {})
    thumbs = vd.get("thumbnail", {}).get("thumbnails", [])
    thumbnail = thumbs[-1].get("url", "") if thumbs else ""
    return {
        "title": vd.get("title", "Unknown"),
        "url": f"https://www.youtube.com/watch?v={video_id}",
        "duration": int(vd.get("lengthSeconds", 0)),
        "thumbnail": thumbnail,
        "channel": vd.get("author", "Unknown"),
        "video_id": video_id,
    }

# ── Cookie support ────────────────────────────────────────────────────────────
_COOKIES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "..", "..", "..", "cookies")
os.makedirs(_COOKIES_DIR, exist_ok=True)

_cookie_files: list[str] = []
_cookies_loaded = False


def _write_env_cookies():
    """Write cookies from COOKIES_TXT env var to a file (for cloud deploys
    like Heroku where cookie files can't be committed to git).

    Supports both single-line (escaped \\n) and multi-line env var values.
    Always rewrites the file to pick up env var changes on dyno restart.
    """
    raw = os.environ.get("COOKIES_TXT", "").strip()
    if not raw:
        return
    # Handle escaped newlines (common when setting env vars via CLI)
    if "\\n" in raw and "\n" not in raw:
        raw = raw.replace("\\n", "\n")
    # Ensure the cookie file starts with proper Netscape header
    if not raw.startswith("# Netscape HTTP Cookie File") and not raw.startswith("# HTTP Cookie File"):
        raw = "# Netscape HTTP Cookie File\n# https://curl.haxx.se/docs/http-cookies.html\n# This file was generated automatically.\n\n" + raw
    env_cookie_path = os.path.join(_COOKIES_DIR, "_env_cookies.txt")
    try:
        with open(env_cookie_path, "w") as fp:
            fp.write(raw)
        LOG.info("Wrote COOKIES_TXT env var to %s (%d bytes)", env_cookie_path, len(raw))
    except Exception:
        LOG.exception("Failed to write COOKIES_TXT env var to file")


def _load_cookie_files():
    global _cookies_loaded
    if _cookies_loaded:
        return
    _cookies_loaded = True
    # First, materialise cookies from env var (if set)
    _write_env_cookies()
    for f in os.listdir(_COOKIES_DIR):
        if f.endswith(".txt"):
            _cookie_files.append(os.path.join(_COOKIES_DIR, f))
    if _cookie_files:
        LOG.info("Loaded %d cookie file(s)", len(_cookie_files))
    else:
        LOG.warning(
            "No cookie files found. YouTube may block requests on cloud servers. "
            "Set the COOKIES_TXT env var or add .txt files to %s",
            _COOKIES_DIR,
        )


def _get_cookie() -> Optional[str]:
    _load_cookie_files()
    return random.choice(_cookie_files) if _cookie_files else None


# ── yt-dlp player client rotation ────────────────────────────────────────────
# YouTube aggressively blocks certain clients on cloud IPs.
# Updated May 2026 — with cookies, "web" client works best on cloud.
# Without cookies, mobile/TV clients are tried.
_CLIENT_COMBOS_WITH_COOKIES: list[list[str]] = [
    ["web"],                           # Web client — BEST with cookies on cloud
    ["web_music"],                     # YouTube Music web — good with cookies
    ["web_creator"],                   # Creator Studio client — good for restricted
    ["ios"],                           # iOS client
    ["android_vr"],                    # Android VR — less monitored by YouTube
    ["web_safari"],                    # Safari — cookies help
    ["mediaconnect"],                  # MediaConnect — newer, less blocked
    ["tv"],                            # Smart TV — fewer restrictions
    ["android_testsuite"],             # Android Testsuite — direct URLs
    ["web", "ios"],                    # Web+iOS combo — double coverage
    ["web_music", "android_vr"],       # Music+VR combo — alternate fallback
    ["mweb"],                          # Mobile web — fallback with cookies
    ["tv_embedded"],                   # TV embedded player — last resort
]

_CLIENT_COMBOS_NO_COOKIES: list[list[str]] = [
    ["ios"],                           # iOS — best without cookies
    ["android_vr"],                    # Android VR — less monitored
    ["android_testsuite"],             # Android Testsuite — direct URLs
    ["mweb"],                          # Mobile web — works well on cloud
    ["mediaconnect"],                  # MediaConnect — newer client
    ["tv"],                            # Smart TV — fewer restrictions
    ["tv_embedded"],                   # TV embedded player
    ["web_music"],                     # YouTube Music web client
    ["web_creator"],                   # Creator Studio — works without cookies too
    ["ios", "android_vr"],             # iOS+VR combo — double coverage
    ["android_testsuite", "tv"],       # Testsuite+TV combo — alternate fallback
    ["mweb", "mediaconnect"],          # Mobile+MediaConnect — final combos
]


def _get_client_combos() -> list[list[str]]:
    """Return appropriate client combos based on cookie availability."""
    if _get_cookie():
        return _CLIENT_COMBOS_WITH_COOKIES
    return _CLIENT_COMBOS_NO_COOKIES


def _base_ytdlp_opts(client_combo: Optional[list[str]] = None) -> dict:
    combos = _get_client_combos()
    if client_combo is None:
        client_combo = combos[0]
    opts = {
        "quiet": True,
        "no_warnings": True,
        "geo_bypass": True,
        "geo_bypass_country": "US",
        "nocheckcertificate": True,
        "socket_timeout": 12,
        "retries": 5,
        "fragment_retries": 5,
        "noplaylist": True,
        "no_color": True,
        "noprogress": True,
        "logger": _ytdlp_logger,
        # CRITICAL: Do NOT verify formats from cloud IPs — YouTube returns
        # valid format lists but blocks actual stream verification requests
        # from cloud servers, causing "Requested format is not available".
        "check_formats": False,
        "allow_unplayable_formats": False,
        # Ignore format errors — try to download even if format check fails
        "ignore_no_formats_error": True,
        # Accept the FIRST available format rather than checking all
        "format_sort": [
            "proto:https",             # prefer HTTPS streams
            "proto:m3u8_native",       # prefer HLS (no signature needed)
            "hasaud",                  # prefer formats with audio
            "source",                  # prefer higher quality source
        ],
        "extractor_args": {
            "youtube": {
                "player_client": client_combo,
                # Skip HTML5 player JS download when possible (faster, avoids
                # signature cipher issues on cloud servers)
                "player_skip": ["configs"],
            },
        },
        "hls_prefer_native": True,  # Use native HLS downloader (more reliable)
        "http_headers": {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/137.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        },
        # Workaround: skip signature decryption issues
        "extractor_retries": 8,
        # IMPORTANT: Don't check certificates on stream URLs
        # (some Piped/Invidious proxies have self-signed certs)
        "nocheckcertificate": True,
    }
    # PO token support (if set via env var)
    # Format: "web+VISITOR_DATA:PO_TOKEN" (yt-dlp 2024.09+ format)
    po_token = os.environ.get("YT_PO_TOKEN", "").strip()
    if po_token:
        opts["extractor_args"]["youtube"]["po_token"] = [po_token]

    # Visitor data support (optional, used with PO token)
    visitor_data = os.environ.get("YT_VISITOR_DATA", "").strip()
    if visitor_data:
        opts["extractor_args"]["youtube"]["visitor_data"] = [visitor_data]

    cookie = _get_cookie()
    if cookie:
        opts["cookiefile"] = cookie

    # Proxy support — essential for Heroku/cloud deployments
    proxy = _get_proxy()
    if proxy:
        opts["proxy"] = proxy
        LOG.debug("Using proxy for yt-dlp: %s", proxy[:30])

    return opts


# ══════════════════════════════════════════════════════════════════════════════
# SEARCH — YouTube Innertube API (direct, no external library)
# ══════════════════════════════════════════════════════════════════════════════

_INNERTUBE_SEARCH_URL = "https://www.youtube.com/youtubei/v1/search"

_INNERTUBE_CONTEXT = {
    "client": {
        "clientName": "WEB",
        "clientVersion": "2.20260525.01.00",
        "hl": "en",
        "gl": "US",
    }
}

_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/137.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.youtube.com",
    "Referer": "https://www.youtube.com/",
}


async def search_youtube(query: str, max_results: int = 1) -> Optional[dict]:
    """Search YouTube via innertube API.  Returns the first result.

    This calls YouTube's own search endpoint directly using aiohttp.
    It does NOT trigger bot detection (search != player).

    Fetches extra results internally so lyrics/lyrical videos can be
    deprioritised — _parse_innertube_results sorts originals first.

    Keys: title, url, duration (seconds), thumbnail, channel, video_id.
    """
    try:
        # Append "official audio" hint to nudge YouTube toward original songs
        # (only if the query doesn't already contain platform/filter keywords)
        search_query = query
        q_lower = query.lower()
        if not any(kw in q_lower for kw in ("official", "audio", "video", "lyrics",
                                             "remix", "cover", "live", "mv")):
            search_query = f"{query} official audio"

        # Fetch more results than requested so the lyrics filter can
        # pick the best original song from a wider pool.
        fetch_count = max(max_results, 5)
        results = await _innertube_search(search_query, fetch_count)
        if results:
            return results[0]

        # If "official audio" query returned nothing, try original query
        if search_query != query:
            results = await _innertube_search(query, fetch_count)
            if results:
                return results[0]
    except Exception:
        LOG.exception("Innertube search failed for: %s", query)

    # Fallback: yt-dlp flat search
    try:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None, _ytdlp_search_sync, query, max_results
        )
        return result
    except Exception:
        LOG.exception("yt-dlp fallback search also failed: %s", query)
        return None


async def search_youtube_many(query: str, limit: int = 5) -> list[dict]:
    """Return up to *limit* search results."""
    try:
        return await _innertube_search(query, limit)
    except Exception:
        LOG.exception("Multi-search failed: %s", query)
        return []


async def _innertube_search(query: str, limit: int = 5) -> list[dict]:
    """Call YouTube innertube search API and parse results.

    NOTE: Does NOT use proxy — YouTube search API works fine from cloud IPs.
    Only the player/stream API blocks cloud IPs.
    """
    payload = {
        "context": _INNERTUBE_CONTEXT,
        "query": query,
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            _INNERTUBE_SEARCH_URL,
            json=payload,
            headers=_HEADERS,
            timeout=aiohttp.ClientTimeout(total=6),
        ) as resp:
            if resp.status != 200:
                LOG.warning("Innertube search HTTP %d for: %s", resp.status, query)
                return []
            data = await resp.json()

    return _parse_innertube_results(data, limit)


_LYRICS_KEYWORDS = re.compile(
    r'\b(lyrics?|lyrical|lyric\s*video|lyrics?\s*video|with\s+lyrics?)\b',
    re.IGNORECASE,
)


def _is_lyrics_video(title: str, channel: str = "") -> bool:
    """Check if a video is likely a lyrics/lyrical version rather than original."""
    # Check title for lyrics keywords
    if _LYRICS_KEYWORDS.search(title):
        return True
    # Check channel name for common lyrics channels
    ch_lower = channel.lower()
    for kw in ("lyrics", "lyrical", "lyric"):
        if kw in ch_lower:
            return True
    return False


def _parse_innertube_results(data: dict, limit: int) -> list[dict]:
    """Extract video results from innertube search response.

    PRIORITISES original songs over lyrics/lyrical videos.
    Collects all results, then sorts: original songs first, lyrics videos last.
    """
    all_results = []

    try:
        contents = (
            data.get("contents", {})
            .get("twoColumnSearchResultsRenderer", {})
            .get("primaryContents", {})
            .get("sectionListRenderer", {})
            .get("contents", [])
        )
    except (AttributeError, TypeError):
        LOG.warning("Unexpected innertube response structure")
        return []

    for section in contents:
        items = (
            section.get("itemSectionRenderer", {})
            .get("contents", [])
        )
        for item in items:
            vr = item.get("videoRenderer")
            if not vr:
                continue

            video_id = vr.get("videoId", "")
            if not video_id:
                continue

            # Title
            title_runs = vr.get("title", {}).get("runs", [])
            title = title_runs[0]["text"] if title_runs else "Unknown"

            # Duration
            length_text = (
                vr.get("lengthText", {}).get("simpleText", "")
                or vr.get("lengthText", {}).get("accessibility", {})
                .get("accessibilityData", {}).get("label", "")
            )
            duration = _parse_duration(length_text)

            # Thumbnail
            thumbs = vr.get("thumbnail", {}).get("thumbnails", [])
            thumbnail = thumbs[-1]["url"] if thumbs else ""
            # Clean thumbnail URL
            if thumbnail and "?" in thumbnail:
                thumbnail = thumbnail.split("?")[0]

            # Channel
            owner_runs = vr.get("ownerText", {}).get("runs", [])
            channel = owner_runs[0]["text"] if owner_runs else "Unknown"

            all_results.append({
                "title": title,
                "url": f"https://www.youtube.com/watch?v={video_id}",
                "duration": duration,
                "thumbnail": thumbnail,
                "channel": channel,
                "video_id": video_id,
                "_is_lyrics": _is_lyrics_video(title, channel),
            })

    # Sort: original songs first, lyrics videos last
    all_results.sort(key=lambda r: (r.get("_is_lyrics", False),))

    # Remove internal flag and limit results
    results = []
    for r in all_results[:limit]:
        r.pop("_is_lyrics", None)
        results.append(r)

    return results


def _ytdlp_search_sync(query: str, max_results: int = 1) -> Optional[dict]:
    """Fallback: search using yt-dlp with extract_flat (lightweight).

    Fetches extra results to filter out lyrics/lyrical videos.
    """
    import yt_dlp

    # Fetch more results to filter lyrics videos
    fetch_count = max(max_results * 5, 5)
    opts = {
        **_base_ytdlp_opts(),
        "extract_flat": True,
        "default_search": f"ytsearch{fetch_count}",
    }

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(query, download=False)
            if not info:
                return None
            entries = info.get("entries", [])
            if not entries:
                # Single result
                item = info
                if not item:
                    return None
                vid = item.get("id", "")
                return {
                    "title": item.get("title", "Unknown"),
                    "url": item.get("webpage_url") or item.get("url", ""),
                    "duration": int(item.get("duration") or 0),
                    "thumbnail": item.get("thumbnail", ""),
                    "channel": item.get("uploader") or item.get("channel", "Unknown"),
                    "video_id": vid,
                }

            # Prefer non-lyrics videos
            non_lyrics = []
            lyrics_items = []
            for item in entries:
                if not item:
                    continue
                title = item.get("title", "")
                channel = item.get("uploader") or item.get("channel", "")
                if _is_lyrics_video(title, channel):
                    lyrics_items.append(item)
                else:
                    non_lyrics.append(item)

            # Pick from non-lyrics first, then lyrics as fallback
            best = (non_lyrics + lyrics_items)[0] if (non_lyrics or lyrics_items) else None
            if not best:
                return None

            vid = best.get("id", "")
            return {
                "title": best.get("title", "Unknown"),
                "url": best.get("webpage_url") or best.get("url", ""),
                "duration": int(best.get("duration") or 0),
                "thumbnail": best.get("thumbnail", ""),
                "channel": best.get("uploader") or best.get("channel", "Unknown"),
                "video_id": vid,
            }
    except Exception:
        LOG.exception("yt-dlp search failed: %s", query)
        return None


# ══════════════════════════════════════════════════════════════════════════════
# STREAM URL EXTRACTION — Piped/Invidious first, yt-dlp fallback
# ══════════════════════════════════════════════════════════════════════════════


async def _validate_stream_url(url: str) -> bool:
    """Quick validation that a stream URL is reachable.

    LENIENT by design — only rejects clearly dead URLs (403, 410, 404).
    On any doubt (timeout, unexpected status, connection error), returns
    True and lets py-tgcalls / ffmpeg handle it.  The previous version
    was too strict and rejected valid YouTube CDN URLs.
    """
    if not url or not url.startswith("http"):
        return False
    # HLS manifests (.m3u8) are always accepted — ffmpeg handles them
    if ".m3u8" in url or "manifest/hls" in url:
        return True
    # Known good CDN domains — skip validation entirely
    if any(d in url for d in ("googlevideo.com", "ytimg.com", "ggpht.com",
                               "sndcdn.com", "soundcloud.com", "saavn.com",
                               "cobalt.tools")):
        return True
    try:
        async with aiohttp.ClientSession() as session:
            async with session.head(
                url,
                headers=_PROXY_HEADERS,
                timeout=aiohttp.ClientTimeout(total=3, connect=1.5),
                allow_redirects=True,
            ) as resp:
                if resp.status < 400:
                    return True
                if resp.status in (403, 404, 410, 451):
                    LOG.warning("Stream URL returned HTTP %d (dead): %s",
                               resp.status, url[:80])
                    return False
                # Other 4xx/5xx — let py-tgcalls try anyway
                return True
    except asyncio.TimeoutError:
        return True  # Slow CDN — let py-tgcalls handle it
    except Exception:
        return True  # Connection error — try anyway


async def get_audio_stream_url(url: str) -> Optional[str]:
    """Extract direct audio stream URL (no download).

    SPEED OPTIMISED: Cobalt + Innertube + Piped run CONCURRENTLY.
    First successful result wins — no waiting for sequential failures.

    Stream URLs are validated with a HEAD request before returning to
    avoid passing expired/dead URLs to py-tgcalls (which causes no-sound).
    """
    video_id = _extract_video_id(url)

    if video_id:
        # Phase 1: Run Cobalt, Innertube, and Piped CONCURRENTLY
        # Whichever returns first wins — massive speed improvement
        async def _try_cobalt():
            try:
                return await _cobalt_get_stream(video_id, audio_only=True)
            except Exception:
                return None

        async def _try_innertube():
            try:
                data = await _innertube_player(video_id)
                if data:
                    return _best_innertube_audio(data)
            except Exception:
                pass
            return None

        async def _try_piped():
            try:
                data = await _piped_get_streams(video_id)
                if data:
                    return _best_piped_audio_url(data)
            except Exception:
                pass
            return None

        async def _try_invidious():
            try:
                data = await _invidious_get_streams(video_id)
                if data:
                    return _best_invidious_audio_url(data)
            except Exception:
                pass
            return None

        # Fire all four concurrently, return first non-None result
        tasks = [
            asyncio.create_task(_try_cobalt()),
            asyncio.create_task(_try_innertube()),
            asyncio.create_task(_try_piped()),
            asyncio.create_task(_try_invidious()),
        ]

        # Use asyncio.wait with FIRST_COMPLETED to get fastest result
        done_results = []
        pending = set(tasks)
        while pending:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                try:
                    result = task.result()
                    if result:
                        # Validate the stream URL before returning
                        if await _validate_stream_url(result):
                            # Cancel remaining tasks
                            for p in pending:
                                p.cancel()
                            LOG.info("Audio stream URL obtained and validated (concurrent) for %s", video_id)
                            return result
                        else:
                            LOG.warning("Audio stream URL failed validation, trying next: %s", result[:80])
                except Exception:
                    pass

    # Phase 2: yt-dlp (last resort — only try first 2 client combos for speed)
    LOG.info("All direct APIs failed, trying yt-dlp for audio: %s", url)
    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(
            None, _get_stream_url_sync, url, True
        )
        if result and await _validate_stream_url(result):
            return result
        elif result:
            LOG.warning("yt-dlp stream URL failed validation: %s", result[:80])
    except Exception:
        LOG.exception("Audio stream URL extraction failed: %s", url)

    return None


async def get_video_stream_url(url: str) -> Optional[str]:
    """Extract direct video stream URL (no download).

    SPEED OPTIMISED: Cobalt + Innertube + Piped run CONCURRENTLY.
    First successful result wins — no waiting for sequential failures.

    Stream URLs are validated with a HEAD request before returning.
    """
    video_id = _extract_video_id(url)

    if video_id:
        # Phase 1: Run Cobalt, Innertube, and Piped CONCURRENTLY
        async def _try_cobalt():
            try:
                return await _cobalt_get_stream(video_id, audio_only=False)
            except Exception:
                return None

        async def _try_innertube():
            try:
                data = await _innertube_player(video_id)
                if data:
                    return _best_innertube_video(data)
            except Exception:
                pass
            return None

        async def _try_piped():
            try:
                data = await _piped_get_streams(video_id)
                if data:
                    return _best_piped_video_url(data)
            except Exception:
                pass
            return None

        async def _try_invidious():
            try:
                data = await _invidious_get_streams(video_id)
                if data:
                    return _best_invidious_video_url(data)
            except Exception:
                pass
            return None

        tasks = [
            asyncio.create_task(_try_cobalt()),
            asyncio.create_task(_try_innertube()),
            asyncio.create_task(_try_piped()),
            asyncio.create_task(_try_invidious()),
        ]

        pending = set(tasks)
        while pending:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                try:
                    result = task.result()
                    if result:
                        if await _validate_stream_url(result):
                            for p in pending:
                                p.cancel()
                            LOG.info("Video stream URL obtained and validated (concurrent) for %s", video_id)
                            return result
                        else:
                            LOG.warning("Video stream URL failed validation, trying next: %s", result[:80])
                except Exception:
                    pass

    # Phase 2: yt-dlp (last resort)
    LOG.info("All direct APIs failed, trying yt-dlp for video: %s", url)
    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(
            None, _get_stream_url_sync, url, False
        )
        if result and await _validate_stream_url(result):
            return result
        elif result:
            LOG.warning("yt-dlp video stream URL failed validation: %s", result[:80])
    except Exception:
        LOG.exception("Video stream URL extraction failed: %s", url)

    return None


async def get_video_info(url: str) -> Optional[dict]:
    """Get video metadata. SPEED OPTIMISED: Innertube + Piped run concurrently."""
    video_id = _extract_video_id(url)

    if video_id:
        # Run Innertube and Piped concurrently for info
        async def _info_innertube():
            try:
                data = await _innertube_player(video_id)
                if data and data.get("videoDetails", {}).get("title"):
                    return _innertube_video_info(data, video_id)
            except Exception:
                pass
            return None

        async def _info_piped():
            try:
                data = await _piped_get_streams(video_id)
                if data and data.get("title"):
                    return _piped_video_info(data, video_id)
            except Exception:
                pass
            return None

        tasks = [
            asyncio.create_task(_info_innertube()),
            asyncio.create_task(_info_piped()),
        ]
        pending = set(tasks)
        while pending:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                try:
                    result = task.result()
                    if result:
                        for p in pending:
                            p.cancel()
                        return result
                except Exception:
                    pass

        # Invidious fallback (less reliable)
        try:
            data = await _invidious_get_streams(video_id)
            if data and data.get("title"):
                return _invidious_video_info(data, video_id)
        except Exception:
            LOG.debug("Invidious info failed for %s", video_id)

    # Fallback: yt-dlp
    loop = asyncio.get_running_loop()
    try:
        return await loop.run_in_executor(None, _get_info_sync, url)
    except Exception:
        LOG.exception("Video info extraction failed: %s", url)
        return None


def _extract_stream_from_info(info: dict, audio_only: bool) -> Optional[str]:
    """Extract the best stream URL from yt-dlp info dict."""
    if not info:
        return None

    # Check for HLS manifest URL first (works without signature decryption)
    manifest_url = info.get("manifest_url")
    if manifest_url and "manifest/hls" in manifest_url:
        LOG.info("Using HLS manifest URL from yt-dlp info")
        return manifest_url

    # Direct URL
    stream_url = info.get("url")
    if stream_url:
        return stream_url

    # From requested_formats
    for fmt_info in info.get("requested_formats", []):
        fmt_url = fmt_info.get("url")
        if fmt_url:
            if audio_only and fmt_info.get("acodec", "none") != "none":
                return fmt_url
            if not audio_only and fmt_info.get("vcodec", "none") != "none":
                return fmt_url

    # From all formats — be very permissive
    formats = info.get("formats", [])
    if audio_only:
        audio_fmts = [f for f in formats
                      if f.get("url")
                      and f.get("acodec", "none") != "none"]
        if audio_fmts:
            best = max(audio_fmts, key=lambda f: f.get("abr", 0) or f.get("tbr", 0) or 0)
            return best.get("url")
    else:
        video_fmts = [f for f in formats
                      if f.get("url")
                      and f.get("vcodec", "none") != "none"]
        if video_fmts:
            best = max(video_fmts, key=lambda f: f.get("height", 0) or 0)
            return best.get("url")

    # Last resort: ANY format with a URL
    any_fmt = [f for f in formats if f.get("url")]
    if any_fmt:
        LOG.info("Using fallback format (any available) for stream")
        return any_fmt[-1].get("url")

    return None


def _get_stream_url_sync(url: str, audio_only: bool) -> Optional[str]:
    import yt_dlp

    if audio_only:
        fmt = "ba/b"  # best audio, fallback to best anything
    else:
        fmt = "bv*[height<=720]+ba/bv+ba/b"  # video+audio with fallback

    # If proxy is dead, skip all proxy attempts and go direct immediately
    if _proxy_dead:
        LOG.info("Proxy is dead — skipping proxy attempts, going direct for: %s", url)
        combos = _get_client_combos()
        for combo in combos[:7]:  # Try first 7 combos (expanded)
            opts = {**_base_ytdlp_opts(client_combo=combo), "format": fmt}
            opts.pop("proxy", None)  # Force no proxy
            try:
                with yt_dlp.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                    result = _extract_stream_from_info(info, audio_only)
                    if result:
                        LOG.info("Stream URL obtained (no proxy) for %s (client: %s)", url, combo)
                        return result
            except Exception as exc:
                LOG.warning("Stream URL no-proxy attempt failed (client %s): %s", combo, exc)
                continue
        return None

    last_err = None
    # Try first 7 client combos for faster failure detection
    for combo in _get_client_combos()[:7]:
        opts = {**_base_ytdlp_opts(client_combo=combo), "format": fmt}
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
                result = _extract_stream_from_info(info, audio_only)
                if result:
                    LOG.info("Stream URL obtained for %s (client: %s)", url, combo)
                    _mark_proxy_ok()
                    return result
        except Exception as exc:
            last_err = exc
            exc_str = str(exc)
            # Detect proxy payment/auth failures and auto-disable
            if "402" in exc_str or "Payment Required" in exc_str or \
               "407" in exc_str or "Proxy Authentication" in exc_str:
                _mark_proxy_failed()
                LOG.warning("Proxy payment/auth error detected: %s", exc_str[:100])
            LOG.warning("Stream URL attempt failed with client %s: %s", combo, exc)
            continue

    # Fallback: try without proxy if proxy was being used
    if last_err and _get_proxy():
        LOG.info("Retrying stream URL WITHOUT proxy for: %s", url)
        try:
            no_proxy_opts = _base_ytdlp_opts()
            no_proxy_opts.pop("proxy", None)  # Force no proxy
            no_proxy_opts["format"] = "ba/b" if audio_only else "bv+ba/b"
            no_proxy_opts["check_formats"] = False
            with yt_dlp.YoutubeDL(no_proxy_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                result = _extract_stream_from_info(info, audio_only)
                if result:
                    LOG.info("Stream URL obtained WITHOUT proxy for %s", url)
                    _mark_proxy_failed()  # Mark proxy as bad
                    return result
        except Exception as exc2:
            LOG.warning("No-proxy fallback also failed: %s", exc2)

    # Fallback: try "b" (best anything) with default client, no restrictions
    if last_err:
        LOG.info("Retrying with permissive format 'b' for: %s", url)
        try:
            fb_opts = _base_ytdlp_opts()
            fb_opts["format"] = "b"
            fb_opts["check_formats"] = False
            fb_opts.pop("proxy", None)
            # Remove player_client restriction — let yt-dlp decide
            fb_opts.get("extractor_args", {}).get("youtube", {}).pop("player_client", None)
            with yt_dlp.YoutubeDL(fb_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                result = _extract_stream_from_info(info, audio_only)
                if result:
                    LOG.info("Stream URL obtained via permissive fallback for %s", url)
                    return result
        except Exception as exc2:
            LOG.warning("Permissive fallback also failed: %s", exc2)

    if last_err:
        LOG.error("All yt-dlp stream URL attempts failed: %s — %s", url, last_err)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# DOWNLOAD — Innertube/Piped stream download, yt-dlp fallback
# ══════════════════════════════════════════════════════════════════════════════


async def _piped_or_invidious_audio(video_id: str) -> Optional[str]:
    """Try Piped then Invidious for audio stream URL."""
    try:
        data = await _piped_get_streams(video_id)
        if data:
            url = _best_piped_audio_url(data)
            if url:
                return url
    except Exception:
        pass
    try:
        data = await _invidious_get_streams(video_id)
        if data:
            url = _best_invidious_audio_url(data)
            if url:
                return url
    except Exception:
        pass
    return None


async def _piped_or_invidious_video(video_id: str) -> Optional[str]:
    """Try Piped then Invidious for video stream URL."""
    try:
        data = await _piped_get_streams(video_id)
        if data:
            url = _best_piped_video_url(data)
            if url:
                return url
    except Exception:
        pass
    try:
        data = await _invidious_get_streams(video_id)
        if data:
            url = _best_invidious_video_url(data)
            if url:
                return url
    except Exception:
        pass
    return None


async def download_audio(url: str) -> Optional[str]:
    """Download audio. Tries Cobalt/Innertube/Piped stream + download, yt-dlp fallback."""
    video_id = _extract_video_id(url)

    if video_id:
        # Try 1: Cobalt API -> download stream (most reliable on cloud)
        try:
            stream_url = await _cobalt_get_stream(video_id, audio_only=True)
            if stream_url:
                filepath = os.path.join(_DOWNLOADS, f"{video_id}_cobalt.opus")
                downloaded = await _download_stream(stream_url, filepath)
                if downloaded:
                    LOG.info("Audio downloaded via Cobalt for %s", video_id)
                    return downloaded
        except Exception:
            LOG.debug("Cobalt audio download failed for %s", video_id)

        # Try 2: Innertube Player API -> download stream
        try:
            data = await _innertube_player(video_id)
            if data:
                stream_url = _best_innertube_audio(data)
                if stream_url:
                    filepath = os.path.join(_DOWNLOADS, f"{video_id}_innertube.m4a")
                    # Innertube URLs are YouTube CDN — NEED proxy on cloud
                    downloaded = await _download_stream(stream_url, filepath,
                                                        use_proxy=True)
                    if downloaded:
                        LOG.info("Audio downloaded via Innertube for %s", video_id)
                        return downloaded
        except Exception:
            LOG.debug("Innertube audio download failed for %s", video_id)

        # Try 3: Piped/Invidious stream URL + download
        try:
            stream_url = await _piped_or_invidious_audio(video_id)
            if stream_url:
                filepath = os.path.join(_DOWNLOADS, f"{video_id}.opus")
                downloaded = await _download_stream(stream_url, filepath)
                if downloaded:
                    LOG.info("Audio downloaded via proxy for %s", video_id)
                    return downloaded
        except Exception:
            LOG.debug("Proxy audio download failed for %s", video_id)

    # Try 4: yt-dlp (last resort)
    LOG.info("Direct download failed, trying yt-dlp for audio: %s", url)
    opts = {
        **_base_ytdlp_opts(),
        "format": "ba/b",
        "outtmpl": os.path.join(_DOWNLOADS, "%(id)s.%(ext)s"),
        "overwrites": False,
        # Convert to a format py-tgcalls can stream reliably
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "opus",
            "preferredquality": "128",
        }],
    }
    return await _run_ytdlp(url, opts)


async def download_video(url: str) -> Optional[str]:
    """Download video. Tries Cobalt/Innertube/Piped stream + download, yt-dlp fallback."""
    video_id = _extract_video_id(url)

    if video_id:
        # Try 1: Cobalt API -> download stream (most reliable on cloud)
        try:
            stream_url = await _cobalt_get_stream(video_id, audio_only=False)
            if stream_url:
                filepath = os.path.join(_DOWNLOADS, f"{video_id}_cobalt_video.mp4")
                downloaded = await _download_stream(stream_url, filepath)
                if downloaded:
                    LOG.info("Video downloaded via Cobalt for %s", video_id)
                    return downloaded
        except Exception:
            LOG.debug("Cobalt video download failed for %s", video_id)

        # Try 2: Innertube Player API -> download stream
        try:
            data = await _innertube_player(video_id)
            if data:
                stream_url = _best_innertube_video(data)
                if stream_url:
                    filepath = os.path.join(_DOWNLOADS, f"{video_id}_innertube_video.mp4")
                    # Innertube URLs are YouTube CDN — NEED proxy on cloud
                    downloaded = await _download_stream(stream_url, filepath,
                                                        use_proxy=True)
                    if downloaded:
                        LOG.info("Video downloaded via Innertube for %s", video_id)
                        return downloaded
        except Exception:
            LOG.debug("Innertube video download failed for %s", video_id)

        # Try 3: Piped/Invidious stream URL + download
        try:
            stream_url = await _piped_or_invidious_video(video_id)
            if stream_url:
                filepath = os.path.join(_DOWNLOADS, f"{video_id}_video.mp4")
                downloaded = await _download_stream(stream_url, filepath)
                if downloaded:
                    LOG.info("Video downloaded via proxy for %s", video_id)
                    return downloaded
        except Exception:
            LOG.debug("Proxy video download failed for %s", video_id)

    # Try 4: yt-dlp (last resort)
    LOG.info("Direct download failed, trying yt-dlp for video: %s", url)
    opts = {
        **_base_ytdlp_opts(),
        "format": "bv*[height<=720]+ba/bv+ba/b",
        "outtmpl": os.path.join(_DOWNLOADS, "%(id)s_video.%(ext)s"),
        "merge_output_format": "mp4",
        "overwrites": False,
        # Remux to mp4 for py-tgcalls compatibility
        "postprocessors": [{
            "key": "FFmpegVideoRemuxer",
            "preferedformat": "mp4",
        }],
    }
    return await _run_ytdlp(url, opts)


async def search_and_download_audio(query: str) -> tuple[Optional[str], Optional[dict]]:
    """Search YouTube and download audio in one step using yt-dlp's ytsearch.

    This is the most reliable fallback for cloud servers where separate
    search -> extract URL -> download flow fails due to IP blocking.
    yt-dlp handles search + download atomically.

    First searches for multiple results (extract_flat) to find the best
    non-lyrics original song, then downloads that specific video.

    Returns (filepath, info_dict) or (None, None).
    """
    import yt_dlp
    loop = asyncio.get_running_loop()

    # Step 1: Search for the best non-lyrics video first
    best_url = None
    try:
        search_result = await search_youtube(query, max_results=1)
        if search_result and search_result.get("url"):
            best_url = search_result["url"]
            LOG.info("search_and_download_audio: using filtered result: %s", search_result.get("title", "?"))
    except Exception:
        pass

    # Try first 4 client combos for search+download (expanded for reliability)
    # Alternate between two format strings for wider coverage
    _AUDIO_FORMATS = ["ba/b", "ba[ext=m4a]/ba/b", "ba[ext=webm]/ba/b", "b"]
    combos = _get_client_combos()[:4]
    for idx, combo in enumerate(combos):
        fmt = _AUDIO_FORMATS[idx % len(_AUDIO_FORMATS)]
        opts = {
            **_base_ytdlp_opts(client_combo=combo),
            "format": fmt,
            "outtmpl": os.path.join(_DOWNLOADS, "%(id)s.%(ext)s"),
            "noplaylist": True,
            "overwrites": False,
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "opus",
                "preferredquality": "128",
            }],
        }
        # If we don't have a pre-selected URL, use ytsearch
        if not best_url:
            opts["default_search"] = "ytsearch"

        dl_query = best_url or query
        try:
            def _do_search_dl(_opts=opts, _query=dl_query):
                with yt_dlp.YoutubeDL(_opts) as ydl:
                    info = ydl.extract_info(_query, download=True)
                    if not info:
                        return None, None
                    # ytsearch returns a playlist-like result
                    entries = info.get("entries")
                    item = entries[0] if entries else info
                    if not item:
                        return None, None
                    path = ydl.prepare_filename(item)
                    if not os.path.exists(path):
                        base = os.path.splitext(path)[0]
                        for ext in (".opus", ".m4a", ".webm", ".mp3", ".ogg", ".mp4"):
                            candidate = base + ext
                            if os.path.exists(candidate):
                                path = candidate
                                break
                        else:
                            matches = sorted(glob.glob(f"{base}.*"),
                                             key=os.path.getmtime, reverse=True)
                            if matches:
                                path = matches[0]
                    if not os.path.exists(path):
                        return None, None
                    result_info = {
                        "title": item.get("title", "Unknown"),
                        "url": item.get("webpage_url") or item.get("url", ""),
                        "duration": int(item.get("duration") or 0),
                        "thumbnail": item.get("thumbnail", ""),
                        "channel": item.get("uploader") or item.get("channel", "Unknown"),
                        "video_id": item.get("id", ""),
                    }
                    return path, result_info

            filepath, info = await loop.run_in_executor(None, _do_search_dl)
            if filepath and os.path.isfile(filepath):
                LOG.info("search_and_download_audio succeeded (client: %s, fmt: %s): %s", combo, fmt, query)
                return filepath, info
        except Exception as exc:
            LOG.warning("search_and_download_audio failed (client %s, fmt %s): %s", combo, fmt, exc)
            continue

    LOG.error("search_and_download_audio: all %d combo attempts failed for: %s", len(combos), query)

    # Last resort: retry first 2 client combos without proxy, alternate formats
    if _get_proxy() or _proxy_dead:
        LOG.info("search_and_download_audio: retrying WITHOUT proxy for: %s", query)
        import yt_dlp as _yt_dlp
        _noproxy_combos = _get_client_combos()[:2]
        _noproxy_fmts = ["ba/b", "ba[ext=m4a]/ba/b"]
        for _np_idx, _np_combo in enumerate(_noproxy_combos):
            _np_fmt = _noproxy_fmts[_np_idx % len(_noproxy_fmts)]
            opts = {
                **_base_ytdlp_opts(client_combo=_np_combo),
                "format": _np_fmt,
                "outtmpl": os.path.join(_DOWNLOADS, "%(id)s.%(ext)s"),
                "noplaylist": True,
                "overwrites": False,
                "postprocessors": [{
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "opus",
                    "preferredquality": "128",
                }],
            }
            # Use pre-selected URL if available, otherwise ytsearch
            noproxy_query = best_url or query
            if not best_url:
                opts["default_search"] = "ytsearch"
            opts.pop("proxy", None)  # Force no proxy
            try:
                def _do_noproxy_dl(_opts=opts, _query=noproxy_query):
                    with _yt_dlp.YoutubeDL(_opts) as ydl:
                        info = ydl.extract_info(_query, download=True)
                        if not info:
                            return None, None
                        entries = info.get("entries")
                        item = entries[0] if entries else info
                        if not item:
                            return None, None
                        path = ydl.prepare_filename(item)
                        if not os.path.exists(path):
                            base = os.path.splitext(path)[0]
                            for ext in (".opus", ".m4a", ".webm", ".mp3", ".ogg", ".mp4"):
                                candidate = base + ext
                                if os.path.exists(candidate):
                                    path = candidate
                                    break
                            else:
                                matches = sorted(glob.glob(f"{base}.*"),
                                                 key=os.path.getmtime, reverse=True)
                                if matches:
                                    path = matches[0]
                        if not os.path.exists(path):
                            return None, None
                        result_info = {
                            "title": item.get("title", "Unknown"),
                            "url": item.get("webpage_url") or item.get("url", ""),
                            "duration": int(item.get("duration") or 0),
                            "thumbnail": item.get("thumbnail", ""),
                            "channel": item.get("uploader") or item.get("channel", "Unknown"),
                            "video_id": item.get("id", ""),
                        }
                        return path, result_info
                filepath, info = await loop.run_in_executor(None, _do_noproxy_dl)
                if filepath and os.path.isfile(filepath):
                    LOG.info("search_and_download_audio succeeded WITHOUT proxy (client %s): %s", _np_combo, query)
                    _mark_proxy_failed()
                    return filepath, info
            except Exception as exc:
                LOG.warning("search_and_download_audio no-proxy (client %s) failed: %s", _np_combo, exc)

    # Ultimate last resort: try with NO player_client restriction
    # This lets yt-dlp auto-detect the best client for the current IP
    LOG.info("search_and_download_audio: trying with auto client detection for: %s", query)
    try:
        import yt_dlp as _yt_dlp2
        auto_opts = {
            "quiet": True,
            "no_warnings": True,
            "geo_bypass": True,
            "geo_bypass_country": "US",
            "nocheckcertificate": True,
            "socket_timeout": 15,
            "retries": 5,
            "fragment_retries": 5,
            "noplaylist": True,
            "no_color": True,
            "noprogress": True,
            "logger": _ytdlp_logger,
            "check_formats": False,
            "ignore_no_formats_error": True,
            "format": "ba/b",
            "outtmpl": os.path.join(_DOWNLOADS, "%(id)s.%(ext)s"),
            "overwrites": False,
            "default_search": "ytsearch",
            "hls_prefer_native": True,
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "opus",
                "preferredquality": "128",
            }],
        }
        cookie = _get_cookie()
        if cookie:
            auto_opts["cookiefile"] = cookie
        auto_query = best_url or query
        def _do_auto_dl():
            with _yt_dlp2.YoutubeDL(auto_opts) as ydl:
                info = ydl.extract_info(auto_query, download=True)
                if not info:
                    return None, None
                entries = info.get("entries")
                item = entries[0] if entries else info
                if not item:
                    return None, None
                path = ydl.prepare_filename(item)
                if not os.path.exists(path):
                    base = os.path.splitext(path)[0]
                    for ext in (".opus", ".m4a", ".webm", ".mp3", ".ogg", ".mp4"):
                        candidate = base + ext
                        if os.path.exists(candidate):
                            path = candidate
                            break
                    else:
                        matches = sorted(glob.glob(f"{base}.*"),
                                         key=os.path.getmtime, reverse=True)
                        if matches:
                            path = matches[0]
                if not os.path.exists(path):
                    return None, None
                result_info = {
                    "title": item.get("title", "Unknown"),
                    "url": item.get("webpage_url") or item.get("url", ""),
                    "duration": int(item.get("duration") or 0),
                    "thumbnail": item.get("thumbnail", ""),
                    "channel": item.get("uploader") or item.get("channel", "Unknown"),
                    "video_id": item.get("id", ""),
                }
                return path, result_info
        filepath, info = await loop.run_in_executor(None, _do_auto_dl)
        if filepath and os.path.isfile(filepath):
            LOG.info("search_and_download_audio succeeded with auto client: %s", query)
            return filepath, info
    except Exception as exc:
        LOG.warning("search_and_download_audio auto-client also failed: %s", exc)

    return None, None


async def search_and_download_video(query: str) -> tuple[Optional[str], Optional[dict]]:
    """Search YouTube and download video in one step using yt-dlp's ytsearch.

    Prefers original songs over lyrics/lyrical videos.

    Returns (filepath, info_dict) or (None, None).
    """
    import yt_dlp
    loop = asyncio.get_running_loop()

    # Step 1: Search for the best non-lyrics video first
    best_url = None
    try:
        search_result = await search_youtube(query, max_results=1)
        if search_result and search_result.get("url"):
            best_url = search_result["url"]
            LOG.info("search_and_download_video: using filtered result: %s", search_result.get("title", "?"))
    except Exception:
        pass

    # Try first 4 client combos for video search+download (expanded for reliability)
    _VIDEO_FORMATS = [
        "bv*[height<=720]+ba/bv+ba/b",
        "bv[height<=720]+ba/b",
        "bv*[height<=480]+ba/bv+ba/b",
        "b",
    ]
    combos = _get_client_combos()[:4]
    for idx, combo in enumerate(combos):
        fmt = _VIDEO_FORMATS[idx % len(_VIDEO_FORMATS)]
        opts = {
            **_base_ytdlp_opts(client_combo=combo),
            "format": fmt,
            "outtmpl": os.path.join(_DOWNLOADS, "%(id)s_video.%(ext)s"),
            "merge_output_format": "mp4",
            "noplaylist": True,
            "overwrites": False,
            "postprocessors": [{
                "key": "FFmpegVideoRemuxer",
                "preferedformat": "mp4",
            }],
        }
        # If we don't have a pre-selected URL, use ytsearch
        if not best_url:
            opts["default_search"] = "ytsearch"

        dl_query = best_url or query
        try:
            def _do_search_dl(_opts=opts, _query=dl_query):
                with yt_dlp.YoutubeDL(_opts) as ydl:
                    info = ydl.extract_info(_query, download=True)
                    if not info:
                        return None, None
                    entries = info.get("entries")
                    item = entries[0] if entries else info
                    if not item:
                        return None, None
                    path = ydl.prepare_filename(item)
                    if not os.path.exists(path):
                        base = os.path.splitext(path)[0]
                        for ext in (".mp4", ".mkv", ".webm", ".flv"):
                            candidate = base + ext
                            if os.path.exists(candidate):
                                path = candidate
                                break
                        else:
                            matches = sorted(glob.glob(f"{base}.*"),
                                             key=os.path.getmtime, reverse=True)
                            if matches:
                                path = matches[0]
                    if not os.path.exists(path):
                        return None, None
                    result_info = {
                        "title": item.get("title", "Unknown"),
                        "url": item.get("webpage_url") or item.get("url", ""),
                        "duration": int(item.get("duration") or 0),
                        "thumbnail": item.get("thumbnail", ""),
                        "channel": item.get("uploader") or item.get("channel", "Unknown"),
                        "video_id": item.get("id", ""),
                    }
                    return path, result_info

            filepath, info = await loop.run_in_executor(None, _do_search_dl)
            if filepath and os.path.isfile(filepath):
                LOG.info("search_and_download_video succeeded (client: %s, fmt: %s): %s", combo, fmt, query)
                return filepath, info
        except Exception as exc:
            LOG.warning("search_and_download_video failed (client %s, fmt %s): %s", combo, fmt, exc)
            continue

    LOG.error("search_and_download_video: all %d combo attempts failed for: %s", len(combos), query)

    # Last resort: retry first 2 combos without proxy, alternate formats
    if _get_proxy() or _proxy_dead:
        LOG.info("search_and_download_video: retrying WITHOUT proxy for: %s", query)
        import yt_dlp as _yt_dlp
        _noproxy_combos = _get_client_combos()[:2]
        _noproxy_vfmts = ["bv*[height<=720]+ba/bv+ba/b", "bv[height<=480]+ba/b"]
        for _np_idx, _np_combo in enumerate(_noproxy_combos):
            _np_fmt = _noproxy_vfmts[_np_idx % len(_noproxy_vfmts)]
            opts = {
                **_base_ytdlp_opts(client_combo=_np_combo),
                "format": _np_fmt,
                "outtmpl": os.path.join(_DOWNLOADS, "%(id)s_video.%(ext)s"),
                "merge_output_format": "mp4",
                "noplaylist": True,
                "overwrites": False,
                "postprocessors": [{
                    "key": "FFmpegVideoRemuxer",
                    "preferedformat": "mp4",
                }],
            }
            # Use pre-selected URL if available, otherwise ytsearch
            noproxy_query = best_url or query
            if not best_url:
                opts["default_search"] = "ytsearch"
            opts.pop("proxy", None)  # Force no proxy
            try:
                def _do_noproxy_dl(_opts=opts, _query=noproxy_query):
                    with _yt_dlp.YoutubeDL(_opts) as ydl:
                        info = ydl.extract_info(_query, download=True)
                        if not info:
                            return None, None
                        entries = info.get("entries")
                        item = entries[0] if entries else info
                        if not item:
                            return None, None
                        path = ydl.prepare_filename(item)
                        if not os.path.exists(path):
                            base = os.path.splitext(path)[0]
                            for ext in (".mp4", ".mkv", ".webm", ".flv"):
                                candidate = base + ext
                                if os.path.exists(candidate):
                                    path = candidate
                                    break
                            else:
                                matches = sorted(glob.glob(f"{base}.*"),
                                                 key=os.path.getmtime, reverse=True)
                                if matches:
                                    path = matches[0]
                        if not os.path.exists(path):
                            return None, None
                        result_info = {
                            "title": item.get("title", "Unknown"),
                            "url": item.get("webpage_url") or item.get("url", ""),
                            "duration": int(item.get("duration") or 0),
                            "thumbnail": item.get("thumbnail", ""),
                            "channel": item.get("uploader") or item.get("channel", "Unknown"),
                            "video_id": item.get("id", ""),
                        }
                        return path, result_info
                filepath, info = await loop.run_in_executor(None, _do_noproxy_dl)
                if filepath and os.path.isfile(filepath):
                    LOG.info("search_and_download_video succeeded WITHOUT proxy (client %s): %s", _np_combo, query)
                    _mark_proxy_failed()
                    return filepath, info
            except Exception as exc:
                LOG.warning("search_and_download_video no-proxy (client %s) failed: %s", _np_combo, exc)

    # Ultimate last resort: try with NO player_client restriction (auto-detect)
    LOG.info("search_and_download_video: trying with auto client detection for: %s", query)
    try:
        import yt_dlp as _yt_dlp2
        auto_opts = {
            "quiet": True,
            "no_warnings": True,
            "geo_bypass": True,
            "geo_bypass_country": "US",
            "nocheckcertificate": True,
            "socket_timeout": 15,
            "retries": 5,
            "fragment_retries": 5,
            "noplaylist": True,
            "no_color": True,
            "noprogress": True,
            "logger": _ytdlp_logger,
            "check_formats": False,
            "ignore_no_formats_error": True,
            "format": "bv*[height<=720]+ba/bv+ba/b",
            "outtmpl": os.path.join(_DOWNLOADS, "%(id)s_video.%(ext)s"),
            "merge_output_format": "mp4",
            "overwrites": False,
            "default_search": "ytsearch",
            "hls_prefer_native": True,
            "postprocessors": [{
                "key": "FFmpegVideoRemuxer",
                "preferedformat": "mp4",
            }],
        }
        cookie = _get_cookie()
        if cookie:
            auto_opts["cookiefile"] = cookie
        auto_query = best_url or query
        def _do_auto_vdl():
            with _yt_dlp2.YoutubeDL(auto_opts) as ydl:
                info = ydl.extract_info(auto_query, download=True)
                if not info:
                    return None, None
                entries = info.get("entries")
                item = entries[0] if entries else info
                if not item:
                    return None, None
                path = ydl.prepare_filename(item)
                if not os.path.exists(path):
                    base = os.path.splitext(path)[0]
                    for ext in (".mp4", ".mkv", ".webm", ".flv"):
                        candidate = base + ext
                        if os.path.exists(candidate):
                            path = candidate
                            break
                    else:
                        matches = sorted(glob.glob(f"{base}.*"),
                                         key=os.path.getmtime, reverse=True)
                        if matches:
                            path = matches[0]
                if not os.path.exists(path):
                    return None, None
                result_info = {
                    "title": item.get("title", "Unknown"),
                    "url": item.get("webpage_url") or item.get("url", ""),
                    "duration": int(item.get("duration") or 0),
                    "thumbnail": item.get("thumbnail", ""),
                    "channel": item.get("uploader") or item.get("channel", "Unknown"),
                    "video_id": item.get("id", ""),
                }
                return path, result_info
        filepath, info = await loop.run_in_executor(None, _do_auto_vdl)
        if filepath and os.path.isfile(filepath):
            LOG.info("search_and_download_video succeeded with auto client: %s", query)
            return filepath, info
    except Exception as exc:
        LOG.warning("search_and_download_video auto-client also failed: %s", exc)

    return None, None


async def _download_stream(stream_url: str, filepath: str,
                           use_proxy: bool = False) -> Optional[str]:
    """Download a stream URL directly via aiohttp.

    use_proxy=True for Innertube/YouTube CDN URLs (need proxy on cloud).
    use_proxy=False for Piped/Invidious/Cobalt URLs (already proxied).
    Automatically retries without proxy if proxied download fails.
    """
    for attempt_proxy in ([True, False] if use_proxy else [False]):
        try:
            req_kwargs = {}
            if attempt_proxy:
                proxy = _get_proxy()
                if proxy:
                    req_kwargs["proxy"] = proxy
                    LOG.debug("Downloading stream via proxy: %s", proxy[:30])
                elif attempt_proxy:
                    continue  # No proxy available, skip to no-proxy attempt

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    stream_url,
                    headers=_PROXY_HEADERS,
                    timeout=aiohttp.ClientTimeout(total=60, connect=8),
                    **req_kwargs,
                ) as resp:
                    if resp.status != 200:
                        LOG.debug("Stream download HTTP %d (proxy=%s) for: %s",
                                  resp.status, attempt_proxy, stream_url[:80])
                        if resp.status in (403, 410, 429, 451):
                            break  # URL is dead/blocked, don't retry with different proxy
                        continue
                    import aiofiles
                    total_bytes = 0
                    async with aiofiles.open(filepath, "wb") as fp:
                        async for chunk in resp.content.iter_chunked(64 * 1024):
                            await fp.write(chunk)
                            total_bytes += len(chunk)
            if os.path.exists(filepath) and total_bytes > 1000:
                LOG.info("Downloaded %d bytes to %s (proxy=%s)",
                         total_bytes, filepath, attempt_proxy)
                return filepath
            LOG.warning("Downloaded file too small (%d bytes, proxy=%s): %s",
                       total_bytes, attempt_proxy, stream_url[:80])
            if os.path.exists(filepath):
                os.remove(filepath)
        except Exception:
            LOG.debug("Stream download failed (proxy=%s): %s",
                     attempt_proxy, stream_url[:80])
            continue
    return None


def _get_info_sync(url: str) -> Optional[dict]:
    import yt_dlp

    last_err = None
    # SPEED: Only try first 2 client combos for info (metadata is simple)
    for combo in _get_client_combos()[:2]:
        opts = {**_base_ytdlp_opts(client_combo=combo), "skip_download": True}
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
                if not info:
                    continue
                return {
                    "title": info.get("title", "Unknown"),
                    "url": info.get("webpage_url", url),
                    "duration": int(info.get("duration") or 0),
                    "thumbnail": info.get("thumbnail", ""),
                    "channel": info.get("uploader") or info.get("channel", "Unknown"),
                    "video_id": info.get("id", ""),
                }
        except Exception as exc:
            last_err = exc
            LOG.warning("yt-dlp info attempt failed (client %s): %s", combo, exc)
            continue

    if last_err:
        LOG.error("All yt-dlp info attempts failed: %s — %s", url, last_err)
    return None


async def _run_ytdlp(url: str, opts: dict) -> Optional[str]:
    import yt_dlp
    loop = asyncio.get_running_loop()

    # If proxy is dead, remove proxy from opts immediately to avoid timeouts
    if _proxy_dead:
        opts.pop("proxy", None)
        LOG.info("Proxy is dead — running yt-dlp without proxy for: %s", url)

    last_err = None
    # Try first 7 client combos for download (expanded for maximum reliability)
    for combo in _get_client_combos()[:7]:
        run_opts = {**opts}
        # Build extractor_args preserving PO token and visitor data
        yt_args = {"player_client": combo}
        po_token = os.environ.get("YT_PO_TOKEN", "").strip()
        if po_token:
            yt_args["po_token"] = [po_token]
        visitor_data = os.environ.get("YT_VISITOR_DATA", "").strip()
        if visitor_data:
            yt_args["visitor_data"] = [visitor_data]
        run_opts["extractor_args"] = {"youtube": yt_args}
        # Preserve cookies
        cookie = _get_cookie()
        if cookie:
            run_opts["cookiefile"] = cookie

        try:
            with yt_dlp.YoutubeDL(run_opts) as ydl:
                info = await loop.run_in_executor(
                    None, lambda: ydl.extract_info(url, download=True)
                )
                if not info:
                    continue
                path = ydl.prepare_filename(info)
                if os.path.exists(path):
                    return path
                base = os.path.splitext(path)[0]
                for ext in (".opus", ".m4a", ".webm", ".mp3", ".ogg",
                            ".mp4", ".mkv", ".flv"):
                    candidate = base + ext
                    if os.path.exists(candidate):
                        return candidate
                matches = sorted(glob.glob(f"{base}.*"),
                                 key=os.path.getmtime, reverse=True)
                if matches:
                    return matches[0]
        except Exception as exc:
            last_err = exc
            exc_str = str(exc)
            if "402" in exc_str or "Payment Required" in exc_str or \
               "407" in exc_str or "Proxy Authentication" in exc_str:
                _mark_proxy_failed()
            LOG.warning("yt-dlp download attempt failed (client %s): %s", combo, exc)
            continue

    # Retry without proxy if proxy was being used
    if last_err and not _proxy_dead and _get_proxy():
        LOG.info("Retrying download WITHOUT proxy for: %s", url)
        try:
            no_proxy_opts = {**opts}
            no_proxy_opts.pop("proxy", None)
            # Also remove proxy from any nested opts that _base_ytdlp_opts may have added
            cookie = _get_cookie()
            if cookie:
                no_proxy_opts["cookiefile"] = cookie
            no_proxy_opts["format"] = "b"  # Most permissive format
            no_proxy_opts["check_formats"] = False
            with yt_dlp.YoutubeDL(no_proxy_opts) as ydl:
                info = await loop.run_in_executor(
                    None, lambda: ydl.extract_info(url, download=True)
                )
                if info:
                    path = ydl.prepare_filename(info)
                    if os.path.exists(path):
                        _mark_proxy_failed()
                        return path
                    base = os.path.splitext(path)[0]
                    matches = sorted(glob.glob(f"{base}.*"),
                                     key=os.path.getmtime, reverse=True)
                    if matches:
                        _mark_proxy_failed()
                        return matches[0]
        except Exception as exc_np:
            LOG.warning("No-proxy download fallback also failed: %s", exc_np)

    # Fallback: try "b" format with default client, no proxy, no restrictions
    if last_err:
        LOG.info("Retrying download with permissive format 'b' for: %s", url)
        try:
            fallback_opts = {**opts, "format": "b",
                             "check_formats": False}
            fallback_opts.pop("proxy", None)
            cookie = _get_cookie()
            if cookie:
                fallback_opts["cookiefile"] = cookie
            # Remove player_client restriction — let yt-dlp decide
            fallback_opts.get("extractor_args", {}).get("youtube", {}).pop("player_client", None)
            with yt_dlp.YoutubeDL(fallback_opts) as ydl:
                info = await loop.run_in_executor(
                    None, lambda: ydl.extract_info(url, download=True)
                )
                if info:
                    path = ydl.prepare_filename(info)
                    if os.path.exists(path):
                        return path
                    base = os.path.splitext(path)[0]
                    matches = sorted(glob.glob(f"{base}.*"),
                                     key=os.path.getmtime, reverse=True)
                    if matches:
                        return matches[0]
        except Exception as exc2:
            LOG.warning("Permissive download fallback also failed: %s", exc2)

    if last_err:
        LOG.error("All yt-dlp download attempts failed: %s — %s", url, last_err)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _parse_duration(duration_str: str) -> int:
    """Parse 'M:SS', 'H:MM:SS', or accessibility label to seconds."""
    if not duration_str:
        return 0
    # Handle accessibility label like "3 minutes, 45 seconds"
    if "minute" in duration_str or "hour" in duration_str:
        total = 0
        import re as _re
        for match in _re.finditer(r"(\d+)\s*(hour|minute|second)", duration_str):
            val, unit = int(match.group(1)), match.group(2)
            if unit == "hour":
                total += val * 3600
            elif unit == "minute":
                total += val * 60
            else:
                total += val
        return total
    # Handle "M:SS" or "H:MM:SS"
    parts = duration_str.split(":")
    try:
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        elif len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        else:
            return int(parts[0])
    except (ValueError, IndexError):
        return 0


def is_youtube_url(url: str) -> bool:
    return bool(
        re.match(
            r"https?://(www\.)?(youtube\.com|youtu\.be|music\.youtube\.com)/",
            url,
        )
    )
