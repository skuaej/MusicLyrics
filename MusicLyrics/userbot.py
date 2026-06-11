from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

from pyrogram import Client
from pytgcalls import PyTgCalls

from config import Config

LOG = logging.getLogger(__name__)

# Collect all available STRING_SESSION* environment variables.
_sessions: list[str] = []
if Config.STRING_SESSION:
    _sessions.append(Config.STRING_SESSION)
for i in range(2, 11):
    session_value = os.environ.get(f"STRING_SESSION_{i}", "").strip()
    if session_value:
        _sessions.append(session_value)

_userbot_pool: list[Client] = []
_pytgcalls_pool: list[PyTgCalls] = []

for idx, session_value in enumerate(_sessions, start=1):
    name = f"MusicLyricsUser{idx}" if idx > 1 else "MusicLyricsUser"
    try:
        ub = Client(
            name=name,
            api_id=Config.API_ID,
            api_hash=Config.API_HASH,
            session_string=session_value,
        )
        _userbot_pool.append(ub)
        _pytgcalls_pool.append(PyTgCalls(ub))
        LOG.info("Loaded assistant #%d (%s)", idx, name)
    except Exception as exc:
        LOG.exception("Failed to load assistant #%d (%s): %s", idx, name, exc)

userbot: Optional[Client] = _userbot_pool[0] if _userbot_pool else None
pytgcalls: Optional[PyTgCalls] = _pytgcalls_pool[0] if _pytgcalls_pool else None


def get_assistant(chat_id: int) -> tuple[Optional[Client], Optional[PyTgCalls]]:
    if not _userbot_pool:
        return None, None
    idx = abs(chat_id) % len(_userbot_pool)
    return _userbot_pool[idx], _pytgcalls_pool[idx]


def get_all_userbots() -> list[Client]:
    return list(_userbot_pool)


def get_all_pytgcalls() -> list[PyTgCalls]:
    return list(_pytgcalls_pool)


def pool_size() -> int:
    return len(_userbot_pool)


# Cache of assistant user-ids per pool index so we don't call get_me() on every
# play command (it triggers a network round-trip).
_assistant_id_cache: dict[int, int] = {}
# Cache of "is assistant in chat" results so repeated /play commands in the
# same group don't hammer Telegram with get_chat_member calls.  Entries
# expire after a short TTL.
_membership_cache: dict[tuple[int, int], tuple[float, bool]] = {}
_MEMBERSHIP_TTL_SEC = 60.0  # 1 minute is plenty — admins rarely move assistants


async def _get_assistant_id(ub: Client, idx: int) -> Optional[int]:
    cached = _assistant_id_cache.get(idx)
    if cached:
        return cached
    try:
        me = await asyncio.wait_for(ub.get_me(), timeout=4.0)
        if me and getattr(me, "id", None):
            _assistant_id_cache[idx] = me.id
            return me.id
    except Exception as e:
        LOG.debug("_get_assistant_id failed for pool idx %d: %s", idx, e)
    return None


async def assistant_in_chat(chat_id: int) -> bool:
    """Return True iff the assistant assigned to *chat_id* is a usable member.

    Used as an EARLY GATE before any search / download work — if there's no
    assistant in the group there's no point spending CPU & bandwidth resolving
    a song that can never be played.

    Returns False when:
    * No STRING_SESSION is configured (assistant pool is empty).
    * The assistant has left, been kicked, banned, or restricted in the chat.
    * The membership lookup raises (treated as "not present" rather than
      hopefully retrying — keeps the gate strict).
    """
    if not _userbot_pool:
        return False

    idx = abs(chat_id) % len(_userbot_pool)
    ub = _userbot_pool[idx]

    # Cache hit?
    now = asyncio.get_event_loop().time()
    cached = _membership_cache.get((chat_id, idx))
    if cached and (now - cached[0]) < _MEMBERSHIP_TTL_SEC:
        return cached[1]

    aid = await _get_assistant_id(ub, idx)
    if not aid:
        _membership_cache[(chat_id, idx)] = (now, False)
        return False

    # Use the BOT client (not the assistant) to look up membership — the bot
    # is guaranteed to be in the chat (it received the /play command).
    try:
        from MusicLyrics.bot import bot

        member = await asyncio.wait_for(
            bot.get_chat_member(chat_id, aid),
            timeout=3.5,
        )
        status_str = ""
        if member and getattr(member, "status", None) is not None:
            status_str = str(member.status).split(".")[-1].lower()
        present = bool(
            status_str
            and status_str not in ("left", "kicked", "banned", "restricted")
        )
        _membership_cache[(chat_id, idx)] = (now, present)
        return present
    except Exception as e:
        # UserNotParticipant, ChatAdminRequired, PeerIdInvalid, etc. all
        # mean "assistant cannot play here" — treat as not present.
        LOG.debug("assistant_in_chat(%s) lookup failed: %s", chat_id, e)
        _membership_cache[(chat_id, idx)] = (now, False)
        return False


def invalidate_assistant_membership(chat_id: int) -> None:
    """Forget the cached membership result for *chat_id*.

    Call this after a successful join (so the next /play sees the fresh
    membership immediately) or after a failed play attempt where we suspect
    the assistant is no longer in the chat.
    """
    if not _userbot_pool:
        return
    idx = abs(chat_id) % len(_userbot_pool)
    _membership_cache.pop((chat_id, idx), None)
