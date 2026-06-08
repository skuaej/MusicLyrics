"""Safe send wrappers to avoid RANDOM_ID_DUPLICATE storms and per-chat flood waits.

This module centralizes send_message/reply/edit operations so concurrent
message sends to the same chat are serialized and flood waits are respected.
"""
from __future__ import annotations

import asyncio
import logging
import secrets
import time
from typing import Any

from pyrogram.errors import FloodWait

LOG = logging.getLogger(__name__)

_chat_send_locks: dict[int, asyncio.Lock] = {}
_global_send_sem = asyncio.Semaphore(20)
_flood_until: dict[int, float] = {}


def _get_lock(chat_id: int) -> asyncio.Lock:
    lock = _chat_send_locks.get(chat_id)
    if lock is None:
        lock = asyncio.Lock()
        _chat_send_locks[chat_id] = lock
    return lock


async def _maybe_wait_flood(chat_id: int) -> None:
    until = _flood_until.get(chat_id, 0.0)
    now = time.time()
    if until > now:
        await asyncio.sleep(until - now)


async def safe_send(client, chat_id: int, text: str, **kwargs) -> Any:
    await _maybe_wait_flood(chat_id)
    kwargs.setdefault("disable_web_page_preview", True)
    async with _global_send_sem:
        async with _get_lock(chat_id):
            await asyncio.sleep(0)
            for attempt in range(3):
                try:
                    return await client.send_message(chat_id, text, **kwargs)
                except FloodWait as fw:
                    wait = int(getattr(fw, "value", 5)) + 1
                    LOG.warning("FloodWait %ds for chat %s — backing off", wait, chat_id)
                    _flood_until[chat_id] = time.time() + wait
                    if attempt == 2:
                        raise
                    await asyncio.sleep(wait)
                except Exception as e:
                    err = str(e)
                    if "RANDOM_ID_DUPLICATE" in err and attempt < 2:
                        await asyncio.sleep(0.05 * (attempt + 1))
                        continue
                    raise
            return None


async def safe_reply(message, text: str, **kwargs) -> Any:
    return await safe_send(message._client, message.chat.id, text,
                           reply_to_message_id=message.id, **kwargs)


async def safe_edit(message, text: str, **kwargs) -> Any:
    await _maybe_wait_flood(message.chat.id)
    async with _global_send_sem:
        async with _get_lock(message.chat.id):
            await asyncio.sleep(0)
            try:
                return await message.edit_text(text, **kwargs)
            except FloodWait as fw:
                wait = int(getattr(fw, "value", 5)) + 1
                _flood_until[message.chat.id] = time.time() + wait
                LOG.warning("FloodWait %ds on edit for chat %s", wait, message.chat.id)
                return None
            except Exception as e:
                if "MESSAGE_NOT_MODIFIED" in str(e):
                    return message
                LOG.debug("safe_edit: %s", e)
                return None


def clear_chat_state(chat_id: int) -> None:
    _chat_send_locks.pop(chat_id, None)
    _flood_until.pop(chat_id, None)
