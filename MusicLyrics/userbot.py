from __future__ import annotations

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
