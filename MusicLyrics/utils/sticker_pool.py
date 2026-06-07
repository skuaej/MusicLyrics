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


def _bucket(emoji: Optional[str]) -> str:
    if not emoji:
        return "neutral"
    # Try exact match first
    if emoji in _MOOD_MAP:
        return _MOOD_MAP[emoji]
    # Try first codepoint (strips skin-tone modifiers, variation selectors)
    return _MOOD_MAP.get(emoji[0], "neutral")


async def _fetch_pack(client, short_name: str) -> int:
    """Fetch a single sticker pack, populate the cache. Returns count added."""
    from pyrogram.raw.functions.messages import GetStickerSet
    from pyrogram.raw.types import InputStickerSetShortName
    from pyrogram.file_id import FileId, FileType, ThumbnailSource

    try:
        result = await client.invoke(
            GetStickerSet(
                stickerset=InputStickerSetShortName(short_name=short_name),
                hash=0,
            )
        )
    except Exception as e:
        LOG.warning("Sticker pack '%s' load failed: %s", short_name, e)
        return 0

    documents = getattr(result, "documents", None) or []
    packs = getattr(result, "packs", None) or []

    # Build doc_id -> emoji map from pack metadata
    doc_emoji: dict[int, str] = {}
    for p in packs:
        emoji = getattr(p, "emoticon", None)
        for doc_id in getattr(p, "documents", []) or []:
            if doc_id not in doc_emoji and emoji:
                doc_emoji[doc_id] = emoji

    added = 0
    for doc in documents:
        try:
            # Build a Pyrogram-compatible file_id string from the raw Document
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

        emoji = doc_emoji.get(doc.id)
        mood = _bucket(emoji)
        _pool[mood].append(file_id_str)
        _all_ids.append(file_id_str)
        added += 1

    LOG.info("Sticker pack '%s' loaded: %d stickers.", short_name, added)
    return added


async def load_all_packs(client, packs: Optional[list[str]] = None) -> int:
    """Load every configured sticker pack into the in-memory pool.

    Safe to call multiple times — subsequent calls reload from scratch.
    Returns total sticker count loaded.
    """
    global _loaded
    _pool.clear()
    _all_ids.clear()

    pack_list = packs if packs is not None else STICKER_PACK_NAMES
    tasks = [_fetch_pack(client, name) for name in pack_list]
    counts = await asyncio.gather(*tasks, return_exceptions=True)

    total = sum(c for c in counts if isinstance(c, int))
    _loaded = total > 0

    mood_summary = ", ".join(f"{m}:{len(v)}" for m, v in sorted(_pool.items()))
    LOG.info(
        "Sticker pool ready — %d total stickers from %d pack(s). Moods: %s",
        total, len(pack_list), mood_summary or "none",
    )
    return total


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
