"""Sticker pool manager for MusicLyrics bot.

Loads sticker file_ids from configured Telegram sticker packs at startup,
caches them, and provides a smart selection helper so the bot can reply
with contextually-appropriate stickers (matched by emoji mood) instead of
random dice / big-emoji grids.

The sticker packs are resolved via raw API ``messages.GetStickerSet``,
which is supported by bot tokens (read-only). Each pack yields a list of
``(file_id, emoji)`` tuples that get bucketed into mood categories.

Public API
----------
``load_all_packs(client)``     – call once at startup
``pick_sticker(mood=None)``    – returns a random file_id or None
``pool_size()``                – total cached stickers
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import defaultdict
from typing import Optional

LOG = logging.getLogger(__name__)

# ── Configured sticker pack short_names ──────────────────────────────────
# These are the pack short_names parsed from the t.me/addstickers/<name>
# URLs provided by the bot owner. Edit this list to add/remove packs.
STICKER_PACK_NAMES: list[str] = [
    "S_U_K_H",
    "Ajajjsjd_by_fStikBot",
    "Aestheticaddahee_by_fStikBot",
    "d2cmp_by_CalsiBot",
    "kyyji_by_CalsiBot",
    "UzzzzDancing",
    "Videopack3_by_stickersthiefbot",
]

# ── Emoji → mood bucket mapping ──────────────────────────────────────────
# Stickers are categorised by their attached emoji so the reactor can
# pick a sticker that vibes with the situation (happy / love / sad /
# dance / cool / etc.).
_MOOD_MAP: dict[str, str] = {
    # happy / laugh
    "😀": "happy", "😃": "happy", "😄": "happy", "😁": "happy",
    "😆": "happy", "😅": "happy", "🤣": "happy", "😂": "happy",
    "🙂": "happy", "😊": "happy", "😇": "happy", "🥹": "happy",
    "😺": "happy", "😸": "happy",

    # love
    "🥰": "love", "😍": "love", "🤩": "love", "😘": "love",
    "😗": "love", "😙": "love", "😚": "love", "❤️": "love",
    "🧡": "love", "💛": "love", "💚": "love", "💙": "love",
    "💜": "love", "🖤": "love", "🤍": "love", "🤎": "love",
    "💕": "love", "💞": "love", "💓": "love", "💗": "love",
    "💖": "love", "💘": "love", "💝": "love", "😻": "love",

    # cool / confident
    "😎": "cool", "🤙": "cool", "👌": "cool", "🆒": "cool",
    "💪": "cool", "🦾": "cool", "🔥": "cool", "⚡": "cool",
    "🤘": "cool", "🤟": "cool", "👑": "cool", "💎": "cool",

    # celebrate / hype
    "🥳": "hype", "🎉": "hype", "🎊": "hype", "🏆": "hype",
    "🎁": "hype", "✨": "hype", "🌟": "hype", "💫": "hype",
    "🎶": "hype", "🎵": "hype", "🎤": "hype", "🎸": "hype",

    # sad / cry
    "😢": "sad", "😭": "sad", "🥺": "sad", "😔": "sad",
    "😞": "sad", "😟": "sad", "😕": "sad", "🙁": "sad",
    "☹️": "sad", "💔": "sad",

    # dance / party
    "💃": "dance", "🕺": "dance", "🪩": "dance",

    # think / mind
    "🤔": "think", "🧐": "think", "🤨": "think", "🙄": "think",

    # surprise / shock
    "😮": "shock", "😯": "shock", "😲": "shock", "😱": "shock",
    "🤯": "shock", "😳": "shock",

    # rude / angry (we treat as low-priority bucket; not sent for greetings)
    "😡": "angry", "🤬": "angry", "👿": "angry", "😈": "angry",
}

# Moods that are generally SAFE for any command reply.
# "angry" is intentionally excluded so the bot doesn't fire a rude sticker
# in response to /start or /help. Use `pick_sticker(mood="angry")` only
# from explicit angry contexts if you add them later.
_SAFE_MOODS = ("happy", "love", "cool", "hype", "dance")

# Command → preferred mood mapping (best-effort). Missing commands fall
# back to a random safe mood.
_COMMAND_MOOD: dict[str, str] = {
    "start": "happy", "help": "happy", "alive": "cool", "ping": "cool",
    "play": "hype", "p": "hype", "vplay": "hype", "vp": "hype",
    "song": "hype", "vsong": "hype",
    "stop": "cool", "end": "cool", "pause": "cool", "resume": "hype",
    "skip": "cool", "next": "cool",
    "queue": "happy", "nowplaying": "love", "np": "love",
    "loop": "cool", "shuffle": "hype",
    "ban": "cool", "mute": "cool", "warn": "cool",
    "unban": "happy", "unmute": "happy",
    "react": "love", "emoji": "love", "mixemoji": "love",
    "truth": "think", "dare": "hype",
    "flip": "hype", "dice": "hype", "quiz": "think",
    "ttt": "hype", "rps": "hype", "guess": "think",
    "afk": "sad", "tagall": "hype",
    "broadcast": "hype", "stats": "cool",
}


# ── Internal cache ──────────────────────────────────────────────────────
# _pool["happy"] -> list of file_id strings
_pool: dict[str, list[str]] = defaultdict(list)
_all_ids: list[str] = []
_loaded: bool = False
_last_sent: dict[int, str] = {}  # chat_id -> last sent file_id (avoid repeats)

# Refresh state
_REFRESH_INTERVAL_SEC = 6 * 60 * 60   # auto-refresh every 6 hours
_REFRESH_MIN_GAP_SEC = 60             # never refresh more often than this
_last_refresh_ts: float = 0.0
_refresh_lock: Optional[asyncio.Lock] = None  # lazy-created on first use
_refresh_task: Optional[asyncio.Task] = None
_stale: bool = False                  # flipped on by mark_stale() after send error


def _get_lock() -> asyncio.Lock:
    """Lazily create the refresh lock on the currently-running event loop."""
    global _refresh_lock
    if _refresh_lock is None:
        _refresh_lock = asyncio.Lock()
    return _refresh_lock


# Errors that indicate the cached file_ids / file_references are stale.
# Used by reply_sticker callers to trigger a pool refresh.
STALE_ERROR_KEYWORDS = (
    "FILE_REFERENCE_EXPIRED",
    "FILE_REFERENCE_INVALID",
    "FILE_REFERENCE_EMPTY",
    "FILE_ID_INVALID",
    "STICKER_INVALID",
    "MEDIA_INVALID",
)


def is_stale_error(exc: BaseException) -> bool:
    """Return True if the exception text matches a file-reference-stale error."""
    text = f"{type(exc).__name__}: {exc}".upper()
    return any(k in text for k in STALE_ERROR_KEYWORDS)


def mark_stale() -> None:
    """Mark the pool as stale so the next send attempt triggers a refresh."""
    global _stale
    _stale = True



def _bucket(emoji: Optional[str]) -> str:
    if not emoji:
        return "neutral"
    # Try exact match first
    if emoji in _MOOD_MAP:
        return _MOOD_MAP[emoji]
    # Try first codepoint (strips skin-tone modifiers, variation selectors)
    return _MOOD_MAP.get(emoji[0], "neutral")


async def load_all_packs(client, packs: Optional[list[str]] = None) -> int:
    """Load every configured sticker pack into the in-memory pool.

    Safe to call multiple times — subsequent calls reload from scratch.
    Returns total sticker count loaded.
    """
    global _loaded, _last_refresh_ts, _stale

    pack_list = packs if packs is not None else STICKER_PACK_NAMES

    # Build into a fresh staging area so the live pool stays usable
    # if the reload fails halfway (e.g., network blip).
    tasks = [_fetch_pack_into(client, name) for name in pack_list]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    fresh_pool: dict[str, list[str]] = defaultdict(list)
    fresh_all: list[str] = []
    total = 0
    for r in results:
        if isinstance(r, Exception) or r is None:
            continue
        added, per_pack = r
        total += added
        for mood, ids in per_pack.items():
            fresh_pool[mood].extend(ids)
            fresh_all.extend(ids)

    if total == 0 and _all_ids:
        # Reload failed completely but we still have a previous pool — keep it
        LOG.warning("Sticker pool reload returned 0 stickers; keeping previous cache (%d).", len(_all_ids))
        _last_refresh_ts = time.time()  # avoid hammering on repeated failures
        return len(_all_ids)

    if total == 0:
        # No stickers loaded and no previous cache. Keep retrying later.
        LOG.warning("Sticker pool load returned 0 stickers and no previous cache; will retry later.")
        _stale = True
        return 0

    _pool.clear()
    _all_ids.clear()
    for k, v in fresh_pool.items():
        _pool[k].extend(v)
    _all_ids.extend(fresh_all)
    _loaded = total > 0
    _last_refresh_ts = time.time()
    _stale = False

    mood_summary = ", ".join(f"{m}:{len(v)}" for m, v in sorted(_pool.items()))
    LOG.info(
        "Sticker pool ready — %d total stickers from %d pack(s). Moods: %s",
        total, len(pack_list), mood_summary or "none",
    )
    return total


async def _fetch_pack_into(client, short_name: str):
    """Fetch a single pack and return (count, {mood: [file_id, ...]}).

    Separate from the public _fetch_pack so the live pool isn't mutated
    during a reload — the caller swaps in the staged data only on success.
    """
    from pyrogram.raw.functions.messages import GetStickerSet
    from pyrogram.raw.types import InputStickerSetShortName
    from pyrogram.file_id import FileId, FileType

    try:
        result = await client.invoke(
            GetStickerSet(
                stickerset=InputStickerSetShortName(short_name=short_name),
                hash=0,
            )
        )
    except Exception as e:
        LOG.warning("Sticker pack '%s' load failed: %s", short_name, e)
        return 0, {}

    documents = getattr(result, "documents", None) or []
    packs = getattr(result, "packs", None) or []

    doc_emoji: dict[int, str] = {}
    for p in packs:
        emoji = getattr(p, "emoticon", None)
        for doc_id in getattr(p, "documents", []) or []:
            if doc_id not in doc_emoji and emoji:
                doc_emoji[doc_id] = emoji

    per_pack: dict[str, list[str]] = defaultdict(list)
    added = 0
    for doc in documents:
        try:
            file_id_obj = FileId(
                file_type=FileType.STICKER,
                dc_id=doc.dc_id,
                media_id=doc.id,
                access_hash=doc.access_hash,
                file_reference=doc.file_reference,
            )
            file_id_str = file_id_obj.encode()
        except Exception as e:
            LOG.debug("Could not encode sticker file_id in '%s': %s", short_name, e)
            continue
        mood = _bucket(doc_emoji.get(doc.id))
        per_pack[mood].append(file_id_str)
        added += 1

    LOG.info("Sticker pack '%s' loaded: %d stickers.", short_name, added)
    return added, per_pack


async def refresh_if_needed(client, force: bool = False) -> bool:
    """Refresh the pool if it's stale or older than the interval.

    Returns True if a refresh actually ran (success or no-op-keep), False
    if the call was skipped (lock contention or rate-limit window).
    """
    now = time.time()
    if not force:
        age = now - _last_refresh_ts
        if not _stale and age < _REFRESH_INTERVAL_SEC:
            return False
        if age < _REFRESH_MIN_GAP_SEC:
            return False  # rate-limit: avoid refresh storms

    lock = _get_lock()
    if lock.locked():
        return False  # another refresh already in flight
    async with lock:
        # Re-check after acquiring lock
        if not force and not _stale and (time.time() - _last_refresh_ts) < _REFRESH_INTERVAL_SEC:
            return False
        try:
            await load_all_packs(client)
        except Exception as e:
            LOG.warning("Sticker pool refresh failed: %s", e)
    return True


async def _background_refresh_loop(client):
    """Periodic background refresh — fires every REFRESH_INTERVAL_SEC."""
    while True:
        try:
            await asyncio.sleep(_REFRESH_INTERVAL_SEC)
            await refresh_if_needed(client, force=True)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            LOG.warning("Background sticker refresh tick failed: %s", e)


def start_background_refresh(client) -> None:
    """Start the periodic refresh task (idempotent — safe to call repeatedly)."""
    global _refresh_task
    if _refresh_task is not None and not _refresh_task.done():
        return
    try:
        loop = asyncio.get_event_loop()
        _refresh_task = loop.create_task(_background_refresh_loop(client))
        LOG.info("Sticker pool background refresh started (interval=%ds).",
                 _REFRESH_INTERVAL_SEC)
    except Exception as e:
        LOG.warning("Could not start sticker background refresh: %s", e)


def pool_size() -> int:
    return len(_all_ids)


def is_ready() -> bool:
    return _loaded and bool(_all_ids)


def pick_sticker(
    mood: Optional[str] = None,
    command: Optional[str] = None,
    chat_id: Optional[int] = None,
) -> Optional[str]:
    """Return a sticker file_id chosen with simple heuristics.

    Priority order:
      1. If ``command`` is given and mapped, use that command's mood.
      2. Else if ``mood`` is given and has entries, pick from that bucket.
      3. Else pick from a random safe mood bucket.
      4. Fall back to any sticker.

    Avoids returning the same sticker as the previous reply in the same chat.
    """
    if not _all_ids:
        return None

    # Resolve target mood
    if command and not mood:
        mood = _COMMAND_MOOD.get(command.lower().lstrip("/"))

    candidates: list[str] = []
    if mood and _pool.get(mood):
        candidates = _pool[mood]
    else:
        # Random safe mood with content
        safe_with_content = [m for m in _SAFE_MOODS if _pool.get(m)]
        if safe_with_content:
            chosen = random.choice(safe_with_content)
            candidates = _pool[chosen]

    if not candidates:
        candidates = _all_ids

    pick = random.choice(candidates)
    # Avoid immediate repeat per chat
    if chat_id is not None and len(candidates) > 1:
        tries = 0
        while pick == _last_sent.get(chat_id) and tries < 4:
            pick = random.choice(candidates)
            tries += 1
        _last_sent[chat_id] = pick

    return pick
