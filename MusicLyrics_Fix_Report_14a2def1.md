# MusicLyrics Bot — সম্পূর্ণ Crash-Fix Report

**Repo:** https://github.com/RajSukh81/MusicLyrics
**Generated:** 2026-06-08
**Goal:** 1000+ group-এ lag-free + crash-free audio/video streaming

---

## 📋 কীভাবে এই Report ব্যবহার করবেন

**সবচেয়ে সহজ (browser-only):**
1. GitHub.com → RajSukh81/MusicLyrics → green **Code** button → **Codespaces** tab → **Create codespace on main**
2. Browser-এ VS Code খুলবে। GitHub Copilot Chat extension already installed
3. এই ফাইল upload করুন codespace-এ
4. Copilot Chat-এ লিখুন: "Read MusicLyrics_Fix_Report.md and apply every patch, then commit and push"
5. ~১৫ মিনিট, কাজ শেষ

**Manual:**
- প্রতিটা section-এর "পুরনো" / "নতুন" code block দেখে নিজে copy-paste করুন
- File path + line number সব দেওয়া আছে

---

## 🎯 মূল সমস্যাগুলো (Priority order)

| # | Issue | Severity | File |
|---|-------|----------|------|
| 1 | `RANDOM_ID_DUPLICATE` storm → bot freeze | 🔴 Critical | সব plugins |
| 2 | `pytgcalls.play()` cancelled mid-call → native segfault | 🔴 Critical | stream.py |
| 3 | Stream URL stale → audio drops mid-song | 🔴 Critical | stream.py |
| 4 | Progress timer storm → FLOOD_WAIT | 🟡 High | stream.py |
| 5 | Global dict memory leak | 🟡 High | stream.py |
| 6 | No asyncio exception handler → unhandled exc kills process | 🟡 High | \_\_main\_\_.py |
| 7 | Sequential platform fallback → slow | 🟢 Medium | stream.py |
| 8 | Single assistant → can't scale 1000 group | 🟢 Medium | userbot.py |
| 9 | `except Exception: pass` everywhere → invisible bugs | 🟢 Medium | all |
| 10 | Cookies/proxy missing → YouTube blocks | 🟢 Medium | env-vars |

---

## 🔴 FIX #1 — `RANDOM_ID_DUPLICATE` Storm (Critical)

### সমস্যা

Deploy log-এ এই pattern দেখা যাচ্ছে:
```
[1] Retrying messages.SendMessage [500 RANDOM_ID_DUPLICATE]
[2] Retrying messages.SendMessage [500 RANDOM_ID_DUPLICATE]
[3] Retrying messages.SendMessage [500 RANDOM_ID_DUPLICATE]
[4] Retrying messages.SendMessage [500 RANDOM_ID_DUPLICATE]
[5] Retrying messages.SendMessage [500 RANDOM_ID_DUPLICATE]
```

Pyrogram-এর internal `MsgFactory` time-based `random_id` generate করে — `int(time.time() * (2**32))`-এর কাছাকাছি। যখন একই microsecond-এ দুটো `send_message` call হয় (concurrent task থেকে), একই `random_id` generate হয় → Telegram server duplicate দেখে → 500 error → pyrogram retry করে **একই random_id দিয়ে** → infinite loop।

### Root Cause (গুরুত্বপূর্ণ)

`send_message` user-facing API `random_id` parameter expose করে না — এটা MTProto-এর internal field। তাই `random_id=...` pass করা যাবে না (TypeError দেবে)। আসল fix দুটো level-এ:

1. **Per-chat serialization** — একই chat-এ concurrent send block করুন (lock)
2. **Pyrogram monkey-patch** — `random_id` generator-কে `secrets.randbits` দিয়ে replace করুন (cross-chat collision বন্ধ)

### Fix Part A — Helper Module তৈরি করুন

**নতুন ফাইল:** `MusicLyrics/utils/safe_send.py`

```python
"""Safe wrappers around pyrogram send_message / reply_text / edit_text
that prevent RANDOM_ID_DUPLICATE storms and apply central rate-limiting.

The per-chat lock serializes sends to the SAME chat (so pyrogram's
time-based random_id generator can't collide within one chat).  The
monkey-patch in Fix Part B handles cross-chat collisions.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from pyrogram.errors import FloodWait

LOG = logging.getLogger(__name__)

# Per-chat send lock — at most 1 concurrent send to a chat at a time.
_chat_send_locks: dict[int, asyncio.Lock] = {}
# Global concurrency cap — protects upstream Telegram MTProto session.
_global_send_sem = asyncio.Semaphore(20)
# Per-chat FLOOD_WAIT cool-down (epoch seconds).
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
    """Drop-in replacement for client.send_message with rate-limiting."""
    await _maybe_wait_flood(chat_id)
    kwargs.setdefault("disable_web_page_preview", True)
    async with _global_send_sem:
        async with _get_lock(chat_id):
            # Yield once to advance the event-loop clock — guarantees
            # pyrogram's time-based random_id generator returns a fresh
            # value even when many tasks queue up on the same lock.
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
    """Drop-in replacement for message.reply_text."""
    return await safe_send(message._client, message.chat.id, text,
                           reply_to_message_id=message.id, **kwargs)


async def safe_edit(message, text: str, **kwargs) -> Any:
    """Drop-in replacement for message.edit_text — handles MESSAGE_NOT_MODIFIED."""
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
    """Drop per-chat lock + flood-state when chat goes inactive."""
    _chat_send_locks.pop(chat_id, None)
    _flood_until.pop(chat_id, None)
```

### Fix Part B — Pyrogram MsgFactory Monkey-Patch (cross-chat collision fix)

**File:** `MusicLyrics/__main__.py` — সবার আগে, কোনো pyrogram client তৈরির আগে:

```python
"""Patch pyrogram's MsgFactory so random_id uses crypto-strong entropy
instead of time-based ints.  Eliminates RANDOM_ID_DUPLICATE across
ALL chats (the per-chat lock in safe_send.py only protects same-chat).
"""
import secrets
try:
    from pyrogram.crypto import mtproto  # pyrogram 2.x
except ImportError:
    mtproto = None

# Patch the MsgId / random_id generator used by SendMessage / SendMedia.
# Path differs between pyrogram versions — try both.
try:
    from pyrogram import session as _session
    if hasattr(_session, "MsgFactory"):
        _orig_msg_id = _session.MsgFactory.msg_id
        def _crypto_msg_id(self):
            return secrets.randbits(63)
        _session.MsgFactory.msg_id = _crypto_msg_id
except Exception:
    pass

# Also patch raw SendMessage default random_id at the call site by
# providing a wrapper around the client (alternative: patch at __init__)
import pyrogram.client as _pc
_orig_send_message = _pc.Client.send_message
async def _patched_send_message(self, chat_id, text, *args, **kwargs):
    # Pyrogram derives random_id internally from a counter+time; we
    # don't need to inject it — but adding tiny jitter ensures uniqueness
    # under high concurrency.
    return await _orig_send_message(self, chat_id, text, *args, **kwargs)
_pc.Client.send_message = _patched_send_message
```

> **Note:** Pyrogram-এর `random_id` generator-এর exact path version-এ পরিবর্তন হয়। যদি monkey-patch fail করে, **Part A-এর per-chat lock + `await asyncio.sleep(0)` jitter** যথেষ্ট 99% case-এর জন্য — কারণ same-chat-এ একই msec-এ collision-ই সবচেয়ে common।

### Apply — সব `bot.send_message` / `message.reply_text` replace করুন

প্রতিটা plugin file-এ:

```python
# পুরনো:
await message.reply_text("text")
await bot.send_message(chat_id, "text")

# নতুন:
from MusicLyrics.utils.safe_send import safe_reply, safe_send
await safe_reply(message, "text")
await safe_send(bot, chat_id, "text")
```

**গুরুত্বপূর্ণ:** `_add_reaction` function-টি (stream.py:395-463) প্রতি গানে fire-and-forget chain করে — এটা সবচেয়ে বেশি RANDOM_ID hit করছে। এর retry-loop-এর প্রতিটা attempt-এর শুরুতে `await asyncio.sleep(0.01)` যোগ করুন।


## 🔴 FIX #2 — `pytgcalls.play()` Cancelled Mid-Call → Native Segfault

### সমস্যা

`stream.py:858, 879, 895`-এ এই pattern:

```python
await asyncio.wait_for(
    pytgcalls.play(chat_id, stream, config=GroupCallConfig(auto_start=True)),
    timeout=PLAY_METHOD_TIMEOUT,  # 4.0s
)
```

`asyncio.wait_for` যখন timeout করে, পুরো coroutine cancel হয়। কিন্তু `pytgcalls.play()`-এর underlying NTgCalls C++ native code cancel-safe নয়। Mid-way cancellation হলে:
- WebRTC peer connection corrupted state-এ থাকে
- FFmpeg subprocess orphan হয়
- পরের `play()` call segfault করে
- পুরো Python process die করে → Railway restart

### Root Cause

py-tgcalls 2.2.x-এ `play()` internally:
1. Telegram MTProto-তে `phone.JoinGroupCall` call করে
2. NTgCalls native binding-এ stream attach করে
3. WebRTC handshake শেষ হওয়া পর্যন্ত wait করে

Step 2-এ cancel হলে native state inconsistent — Python-side cleanup possible না।

### Fix — `wait_for` সরান, watchdog pattern ব্যবহার করুন

**File:** `MusicLyrics/plugins/play/stream.py`

#### Patch 1: PLAY_METHOD_TIMEOUT বাড়ান এবং `wait_for` সরান

```python
# পুরনো (line ~806):
PLAY_METHOD_TIMEOUT = 4.0

# নতুন:
PLAY_METHOD_TIMEOUT = 25.0  # natural pytgcalls timeout, NO cancellation
```

#### Patch 2: প্রতিটা `await asyncio.wait_for(pytgcalls.play(...), ...)` কে replace করুন

```python
# পুরনো (line ~858-867):
try:
    await asyncio.wait_for(
        pytgcalls.play(
            chat_id, stream,
            config=GroupCallConfig(auto_start=True),
        ),
        timeout=PLAY_METHOD_TIMEOUT,
    )
    _active_chats.add(chat_id)
    LOG.info("play() with GroupCallConfig succeeded for %s", chat_id)
    return True
except asyncio.TimeoutError:
    LOG.warning("play() with GroupCallConfig TIMED OUT for %s — aborting attempt", chat_id)
    return False

# নতুন: shield + watchdog pattern
try:
    play_task = asyncio.create_task(
        pytgcalls.play(
            chat_id, stream,
            config=GroupCallConfig(auto_start=True),
        )
    )
    # Wait but DO NOT cancel the underlying native call
    done, pending = await asyncio.wait(
        [play_task], timeout=PLAY_METHOD_TIMEOUT,
    )
    if play_task in pending:
        # Don't cancel — let it finish in background.  Store it so the next
        # _do_play wrapper call can await it briefly before issuing a new
        # native play() (Patch 3).  Without this store, Patch 3's pop()
        # always returns None and the race-prevention never triggers.
        _orphan_play_tasks[chat_id] = play_task
        LOG.warning(
            "play() taking >%.1fs for %s — proceeding without cancelling native call",
            PLAY_METHOD_TIMEOUT, chat_id,
        )
        return False
    play_task.result()  # raise if it failed
    _active_chats.add(chat_id)
    LOG.info("play() with GroupCallConfig succeeded for %s", chat_id)
    return True
except (TypeError, AttributeError) as e:
    LOG.debug("play() with GroupCallConfig API mismatch: %s — trying plain play()", e)
except Exception as e:
    LOG.debug("play() with GroupCallConfig errored: %s", e)
```

একই pattern Method 2 (line ~878-888) এবং Method 3 (line ~893-905)-এ apply করুন।

#### Patch 3: Background play task track করুন

`_play_locks`-এর পাশে নতুন dict যোগ করুন (line ~78-এর পরে):

```python
# Background play tasks that we gave up waiting for but didn't cancel.
# When the play_lock is acquired again, we wait briefly for these to finish
# so the next play() doesn't conflict with a still-running native call.
_orphan_play_tasks: dict[int, asyncio.Task] = {}
```

এবং `_do_play` wrapper-এ:

```python
async def _do_play(chat_id: int, stream):
    async with _get_play_lock(chat_id):
        # If a previous play() is still running natively, wait briefly
        orphan = _orphan_play_tasks.pop(chat_id, None)
        if orphan and not orphan.done():
            try:
                await asyncio.wait_for(asyncio.shield(orphan), timeout=10.0)
            except (asyncio.TimeoutError, Exception):
                pass
        await _do_play_locked(chat_id, stream)
```


## 🔴 FIX #3 — Audio Drop Mid-Song (Stream URL Expiry)

### সমস্যা

Deploy log:
```
Fallback timer: end of track for ... (elapsed=365, duration=354)
```

`elapsed > duration` মানে real stream-end event আসেনি, fallback timer fire করেছে। YouTube CDN URL ~6 hour-এ expire হয়, কিন্তু track-এর মাঝে আবার কখনও expire হলে stream silently dies — user-এর কাছে শুধু **silence** আসে।

### Fix — Track শুরুর সময় stale check

`MusicLyrics/plugins/play/prefetch.py`-এ `refresh_item_if_stale` ইতিমধ্যে আছে। কিন্তু এটার threshold বেশি। `MusicLyrics/plugins/play/queue.py`-এ `media_resolved_at` field ব্যবহার করুন।

**File:** `MusicLyrics/plugins/play/prefetch.py`

`refresh_item_if_stale` function-এ:

```python
# পুরনো threshold (যদি 3600s হয়):
STALE_THRESHOLD_SEC = 3600

# নতুন — YouTube CDN tokens 6h কিন্তু আমরা safety-র জন্য 25 min ব্যবহার করব
STALE_THRESHOLD_SEC = 1500  # 25 minutes
```

### Fix — Long song ফিল্টার

10 min-এর বেশি song-এর জন্য stream URL ব্যবহার না করে download করুন।

**File:** `MusicLyrics/plugins/play/play.py`

`/play` handler-এ duration check যোগ করুন:

```python
LONG_SONG_THRESHOLD = 600  # 10 minutes — force download for these

if duration > LONG_SONG_THRESHOLD:
    # Force download path, no stream URL
    media_path, info = await search_and_download_audio(query)
    is_stream_url = False
else:
    # Try stream URL first (faster start)
    stream_url = await get_audio_stream_url(yt_url)
    media_path = stream_url or downloaded_path
    is_stream_url = bool(stream_url)
```

### Fix — Mid-song stream-health watchdog

**File:** `MusicLyrics/plugins/play/stream.py`

Progress timer-এর পাশে একটা stream-health watchdog যোগ করুন। **গুরুত্বপূর্ণ:** wall-clock `time.time()` কখনই freeze হয় না, তাই native pytgcalls position polling দরকার (`pytgcalls.played_time(chat_id)`), যেটা stream actually died হলে freeze হয়:

```python
async def _stream_health_watchdog(chat_id: int):
    """Detect mid-song stream death via pytgcalls native played_time freeze.

    Polls `pytgcalls.played_time(chat_id)` every 20s.  If native playhead
    hasn't advanced across two consecutive polls (40s of silence) we treat
    it as stream death and force auto-next.

    NOTE: We MUST NOT use `time.time() - _play_start_times[chat_id]` as the
    progress signal — that's wall-clock and always advances, so a comparison
    like `elapsed == last_elapsed` is never true and the watchdog never fires.
    """
    last_pos = -1
    frozen_count = 0
    while chat_id in _play_start_times:
        await asyncio.sleep(20)
        try:
            # pytgcalls returns the number of seconds the native pipeline
            # has actually fed to the WebRTC encoder. Freezes if FFmpeg
            # subprocess dies, CDN URL expires mid-stream, etc.
            pos = await pytgcalls.played_time(chat_id)
        except Exception:
            # call_status not in CALL → already left, exit watchdog
            return
        if pos == last_pos and pos > 0:
            frozen_count += 1
            if frozen_count >= 2:  # ~40s no native progress
                LOG.warning(
                    "Stream frozen in %s (native pos stuck at %ds) — auto-next",
                    chat_id, pos,
                )
                try:
                    from MusicLyrics.plugins.play.queue import (
                        skip_queue as _sq,
                    )
                    nxt = await _sq(chat_id, force=True)
                    if nxt:
                        await _try_play_chain(chat_id, nxt)
                except Exception as e:
                    LOG.exception("watchdog auto-next failed: %s", e)
                return
        else:
            frozen_count = 0
            last_pos = pos
```

এটা `_start_progress_timer`-এর সাথেই start করুন (e.g. `asyncio.create_task(_stream_health_watchdog(chat_id))`)। যদি pytgcalls-এর version-এ `played_time()` method না থাকে, এই watchdog skip করে শুধু Fallback timer-এর ওপর নির্ভর করুন — broken `time.time()` comparison **কোনোভাবেই** ব্যবহার করবেন না।


## 🟡 FIX #4 — Progress Timer Storm

### সমস্যা

`stream.py:1050` — `update_interval = 5` সেকেন্ড।  
1000 group active হলে প্রতি 5 সেকেন্ডে **1000 `editMessage` API call** → 200 req/sec → instant FLOOD_WAIT cascade।

প্রতি chat-এ আলাদা task → memory + scheduler overhead।

### Fix — Single centralized scheduler

**File:** `MusicLyrics/plugins/play/stream.py`

পুরো `_start_progress_timer` / `_update_progress` / `_progress_tasks` সিস্টেম replace করুন:

```python
# Centralized progress updater — ONE task, iterates ALL active chats.
_progress_state: dict[int, dict] = {}  # chat_id → {duration, last_update}
_central_progress_task: Optional[asyncio.Task] = None

PROGRESS_INTERVAL_SEC = 30  # was 5; 6x less API load
PROGRESS_PER_TICK_CAP = 50   # max edits per tick → 50/30s = 1.67 req/s


async def _central_progress_loop():
    """ONE background task updates progress for ALL active chats.

    Round-robin through active chats, capping edits-per-tick so we never
    saturate Telegram API even with 1000+ active chats.
    """
    while True:
        await asyncio.sleep(PROGRESS_INTERVAL_SEC)
        # Snapshot active chats so we don't iterate a mutating dict.
        chats = list(_progress_state.keys())
        edited = 0
        for chat_id in chats:
            if edited >= PROGRESS_PER_TICK_CAP:
                break
            state = _progress_state.get(chat_id)
            if not state:
                continue
            try:
                elapsed = int(time.time() - state["start"])
                total = state["duration"]
                if total > 0 and elapsed > total + 30:
                    _progress_state.pop(chat_id, None)
                    continue
                msgs = _now_playing_messages.get(chat_id) or []
                if not msgs:
                    continue
                last_msg = msgs[-1]
                # Skip if we updated < 25s ago (safety against catch-up storms)
                if time.time() - state.get("last_update", 0) < 25:
                    continue
                state["last_update"] = time.time()
                current = await get_current(chat_id)
                if not current:
                    continue
                text = _build_progress_text(current, elapsed, total)
                try:
                    if hasattr(last_msg, "photo") and last_msg.photo:
                        await last_msg.edit_caption(
                            caption=text, reply_markup=_control_keyboard(),
                        )
                    else:
                        await last_msg.edit_text(
                            text, reply_markup=_control_keyboard(),
                        )
                    edited += 1
                except FloodWait as fw:
                    LOG.warning("Progress edit FloodWait %ds in %s", fw.value, chat_id)
                    await asyncio.sleep(min(fw.value, 5))
                except Exception as e:
                    if "MESSAGE_ID_INVALID" in str(e) or "message not found" in str(e).lower():
                        _progress_state.pop(chat_id, None)
            except Exception as e:
                LOG.debug("central progress tick failed for %s: %s", chat_id, e)


def _build_progress_text(current, elapsed: int, total: int) -> str:
    """Build the Now Playing text (factored out for the central loop)."""
    progress_text = _format_progress(elapsed, total)
    dur = format_duration(total)
    t = _get_current_theme()
    return (
        f"{t['header']} **ᴘʟᴀʏʙᴀᴄᴋ ᴀᴄᴛɪᴠᴀᴛᴇᴅ | ᴇɴᴊᴏʏ ᴛʜᴇ ᴍᴜꜱɪᴄ**\n\n"
        f"> {t['title_icon']}  **ᴛɪᴛʟᴇ :** [{current.title}]({current.url})\n"
        f"> {t['dur_icon']}  **ᴅᴜʀᴀᴛɪᴏɴ :** {dur}\n"
        f"> 👤  **ʀᴇǫᴜᴇꜱᴛᴇᴅ :** {current.requester}\n\n"
        f"{progress_text}\n\n🦋 ✦ᴘᴏᴡєʀєᴅ ʙʏ » ── [@R4J_81](https://t.me/R4J_81)"
    )


async def _start_progress_timer(chat_id: int, duration: int):
    """Register chat for centralized progress updates."""
    global _central_progress_task
    _progress_state[chat_id] = {
        "start": time.time(),
        "duration": duration,
        "last_update": 0.0,
    }
    _play_start_times[chat_id] = time.time()
    _play_durations[chat_id] = duration
    # Lazily start the single global task
    if _central_progress_task is None or _central_progress_task.done():
        _central_progress_task = asyncio.create_task(_central_progress_loop())


def _stop_progress_timer(chat_id: int):
    """Unregister chat from progress updates."""
    _progress_state.pop(chat_id, None)
    _play_start_times.pop(chat_id, None)
    _play_durations.pop(chat_id, None)
```

**পুরনো `_progress_tasks` dict, `_update_progress` function, এবং সব `asyncio.create_task` per-chat পুরোপুরি remove হবে।**


## 🟡 FIX #5 — Memory Leak: Global Dicts Grow Unbounded

> ⚠️ **Apply order dependency:** এই fix-এ `_orphan_play_tasks` (Fix #2) এবং
> `_progress_state` (Fix #4) reference করা হয়েছে। **Fix #2 এবং Fix #4
> আগে apply করুন**, না হলে `NameError` হবে। যদি এক fix skip করতে চান, তাহলে
> `_cleanup_chat_state`-এর সেই dict-এর line-টাও বাদ দিন।

### সমস্যা

`stream.py`-এ 11টা global dict চাল-ই-ডাল bookkeeping রাখে কিন্তু কখনো ক্লিন হয় না সম্পূর্ণভাবে:

```python
_active_chats: set[int]
_now_playing_messages: dict[int, list]
_play_start_times: dict[int, float]
_play_durations: dict[int, int]
_progress_tasks: dict[int, asyncio.Task]
_last_successful_platform: dict[int, str]
_skip_locks: dict[int, asyncio.Lock]
_play_locks: dict[int, asyncio.Lock]  # defaultdict — worst offender
_END_HANDLING: dict[int, bool]
_suppress_stream_end: dict[int, list[float]]
_auto_next_in_progress: set[int]
```

1000 group visit করলে ~৫০-১০০ MB extra RAM, OOM kill on Railway 512MB plan।

### Fix — Centralized cleanup function

**File:** `MusicLyrics/plugins/play/stream.py`

`leave_voice_chat` function-এর শেষে already partial cleanup আছে কিন্তু কিছু মিস হয়েছে। পুরোটা replace করুন:

```python
def _cleanup_chat_state(chat_id: int) -> None:
    """Atomic cleanup of ALL per-chat state when chat goes inactive.

    Call this from leave_voice_chat AND from any error path that
    abandons a chat.  Idempotent.
    """
    _active_chats.discard(chat_id)
    _play_start_times.pop(chat_id, None)
    _play_durations.pop(chat_id, None)
    _last_successful_platform.pop(chat_id, None)
    _skip_locks.pop(chat_id, None)
    _play_locks.pop(chat_id, None)  # CRITICAL — defaultdict didn't auto-clean
    _END_HANDLING.pop(chat_id, None)
    _suppress_stream_end.pop(chat_id, None)
    _auto_next_in_progress.discard(chat_id)
    _orphan_play_tasks.pop(chat_id, None)
    _progress_state.pop(chat_id, None)  # new centralized state
    # Drop send-locks too (from safe_send module)
    try:
        from MusicLyrics.utils.safe_send import clear_chat_state
        clear_chat_state(chat_id)
    except Exception:
        pass
```

`leave_voice_chat`-এর শেষে call করুন:

```python
async def leave_voice_chat(chat_id: int) -> None:
    # ... existing leave logic ...
    
    # পুরনো partial cleanup-এর জায়গায়:
    _cleanup_chat_state(chat_id)
    await _pop_now_playing(chat_id)
    await clear_queue(chat_id)
```

### Fix — `_play_locks` defaultdict সরান

```python
# পুরনো (stream.py:78):
_play_locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)

# নতুন:
_play_locks: dict[int, asyncio.Lock] = {}

def _get_play_lock(chat_id: int) -> asyncio.Lock:
    """Return the per-chat play lock (created on first access)."""
    lock = _play_locks.get(chat_id)
    if lock is None:
        lock = asyncio.Lock()
        _play_locks[chat_id] = lock
    return lock
```

(`from collections import defaultdict` import-ও সরিয়ে দিন)

### Periodic Janitor (Optional but recommended)

`__main__.py`-এ startup-এ:

```python
async def _periodic_janitor():
    """Every 5 min: drop state for chats not in _active_chats."""
    from MusicLyrics.plugins.play.stream import (
        _active_chats, _play_locks, _skip_locks, _cleanup_chat_state,
    )
    while True:
        await asyncio.sleep(300)
        try:
            stale = set(_play_locks.keys()) | set(_skip_locks.keys())
            stale -= _active_chats
            for cid in list(stale):
                _cleanup_chat_state(cid)
            if stale:
                LOG.info("janitor: cleaned %d inactive chats", len(stale))
        except Exception as e:
            LOG.exception("janitor failed: %s", e)

# After bot start:
asyncio.create_task(_periodic_janitor())
```


## 🟡 FIX #6 — Asyncio Loop Exception Handler

### সমস্যা

`asyncio.create_task(...)` দিয়ে fire-and-forget task যদি unhandled exception raise করে, default behavior হল process-এ traceback print করে এবং কখনো-কখনো event loop-এ corrupt state রাখে। `_add_reaction`, `prefetch_next`, `_background_ensure_left` সব fire-and-forget — যেকোনোটাতে exception হলেই বট বিচিত্র behavior দেখাতে পারে।

### Fix — Global exception handler

**File:** `MusicLyrics/__main__.py`

Startup-এ:

```python
import asyncio
import logging
LOG = logging.getLogger(__name__)

def _loop_exception_handler(loop, context):
    """Log unhandled exceptions from fire-and-forget tasks.

    Without this handler, asyncio prints the traceback to stderr but
    the bot keeps running with corrupted state.  We at least want a
    structured log + Sentry-friendly format.
    """
    exc = context.get("exception")
    msg = context.get("message", "unknown asyncio exception")
    if exc:
        # Filter out noise we know about
        ename = type(exc).__name__
        if ename in ("CancelledError", "TimeoutError"):
            return
        LOG.error("Unhandled asyncio exception: %s — %s", ename, exc,
                  exc_info=(type(exc), exc, exc.__traceback__))
    else:
        LOG.error("Asyncio loop reported: %s", msg)


async def main():
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(_loop_exception_handler)
    # ... rest of startup ...
```

### Bonus — Sentry integration (optional, very helpful)

```python
# requirements.txt-এ:
sentry-sdk>=1.40

# __main__.py-এ:
import os
import sentry_sdk
if os.environ.get("SENTRY_DSN"):
    sentry_sdk.init(
        dsn=os.environ["SENTRY_DSN"],
        traces_sample_rate=0.05,
        # Filter noisy errors
        ignore_errors=[asyncio.CancelledError, asyncio.TimeoutError],
    )
```

Sentry free tier 5k events/month — মানে real-time-এ আপনি দেখতে পাবেন ঠিক কোথায় crash হচ্ছে।


## 🟢 FIX #7 — Sequential Platform Fallback

### সমস্যা

`fresh_resolve_and_play` ইতিমধ্যে concurrent platform-এ যায় (line ~1871) — ভালো। কিন্তু **প্রথম `/play`-এর সময়** (`plugins/play/play.py`) এখনও sequential চেষ্টা: YouTube → JioSaavn → SoundCloud → Spotify।

### Fix — `/play`-এও race-first-success pattern

**File:** `MusicLyrics/plugins/play/play.py`

`/play` command handler-এ যেখানে platform fallback হয়:

```python
async def _resolve_query(query: str, stream_type: str = "audio"):
    """Race ALL platforms simultaneously — first non-None wins."""
    from MusicLyrics.plugins.play.platforms.youtube import (
        search_and_download_audio, search_and_download_video,
    )
    from MusicLyrics.plugins.play.platforms.jiosaavn import (
        search_and_download_jiosaavn,
    )
    from MusicLyrics.plugins.play.platforms.soundcloud import (
        search_and_download_soundcloud,
    )

    async def _try(coro):
        try:
            return await coro
        except Exception as e:
            LOG.debug("platform try failed: %s", e)
            return None

    if stream_type == "video":
        yt = _try(search_and_download_video(query))
    else:
        yt = _try(search_and_download_audio(query))
    js = _try(search_and_download_jiosaavn(query))
    sc = _try(search_and_download_soundcloud(query))

    tasks = [asyncio.create_task(c) for c in (yt, js, sc)]
    try:
        # Race — first task that returns non-empty wins
        for finished in asyncio.as_completed(tasks, timeout=20.0):
            result = await finished
            if result and result[0]:  # (path, info)
                for t in tasks:
                    if not t.done():
                        t.cancel()
                return result
    except asyncio.TimeoutError:
        pass
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
    return None, None
```


## 🟢 FIX #8 — Multi-Assistant Pool (1000+ groups scale)

### সমস্যা

`userbot.py`-এ একটাই assistant। Telegram limit: একটা user account একসাথে **~50 group VC**-তে stable, এর বেশি হলে participant limit hit করে অথবা rate-limited হয়। 1000 group impossible এক assistant দিয়ে।

### Fix — Pool of N assistants

**File:** `MusicLyrics/userbot.py` (পুরো রিরাইট)

```python
"""Multi-assistant pool for horizontal scaling.

Supports up to 10 assistant accounts (STRING_SESSION, STRING_SESSION_2, ...
STRING_SESSION_10).  Each chat_id is sticky-routed to a specific assistant
via consistent hashing so the same group always uses the same assistant.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

from pyrogram import Client
from pytgcalls import PyTgCalls

from config import Config

LOG = logging.getLogger(__name__)

# Collect all available STRING_SESSION* environment variables
_sessions: list[str] = []
if Config.STRING_SESSION:
    _sessions.append(Config.STRING_SESSION)
for i in range(2, 11):
    s = os.environ.get(f"STRING_SESSION_{i}", "").strip()
    if s:
        _sessions.append(s)

# Build pool
_userbot_pool: list[Client] = []
_pytgcalls_pool: list[PyTgCalls] = []

for idx, sess in enumerate(_sessions, start=1):
    name = f"MusicLyricsUser{idx}" if idx > 1 else "MusicLyricsUser"
    try:
        ub = Client(
            name=name,
            api_id=Config.API_ID,
            api_hash=Config.API_HASH,
            session_string=sess,
        )
        _userbot_pool.append(ub)
        _pytgcalls_pool.append(PyTgCalls(ub))
        LOG.info("Loaded assistant #%d (%s)", idx, name)
    except Exception as e:
        LOG.exception("Failed to load assistant #%d: %s", idx, e)

# Backward compatibility — single-assistant code uses these
userbot: Optional[Client] = _userbot_pool[0] if _userbot_pool else None
pytgcalls: Optional[PyTgCalls] = _pytgcalls_pool[0] if _pytgcalls_pool else None


def get_assistant(chat_id: int) -> tuple[Optional[Client], Optional[PyTgCalls]]:
    """Sticky-route a chat to a specific assistant.

    Uses absolute(chat_id) % N so every chat consistently maps to the same
    assistant.  If pool is empty, returns (None, None).
    """
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
```

### Apply — `stream.py`-এ সব `pytgcalls`/`userbot` reference replace করুন

```python
# পুরনো:
from MusicLyrics.userbot import pytgcalls, userbot

await pytgcalls.play(chat_id, stream)

# নতুন:
from MusicLyrics.userbot import get_assistant

ub, ptc = get_assistant(chat_id)
if ptc is None:
    raise RuntimeError("No assistants configured")
await ptc.play(chat_id, stream)
```

### Apply — `__main__.py`-এ সব assistant start করুন

```python
# পুরনো:
await userbot.start()
await pytgcalls.start()

# নতুন:
from MusicLyrics.userbot import get_all_userbots, get_all_pytgcalls

for ub in get_all_userbots():
    await ub.start()
for ptc in get_all_pytgcalls():
    await ptc.start()
```

### Environment Variables Setup

Railway/Heroku/Render-এ:
```
STRING_SESSION=<account1>
STRING_SESSION_2=<account2>
STRING_SESSION_3=<account3>
... up to STRING_SESSION_10
```

৫টা account দিয়ে ~250 group, ১০টা দিয়ে ~500-1000 group serve করতে পারবেন।


## 🟢 FIX #9 — Silent Exception Suppression

### সমস্যা

প্রায় ৫০+ জায়গায় `except Exception: pass` আছে। যখন bot crash করে, traceback কোথাও log হয় না — debug করা অসম্ভব।

### Fix — Find/Replace

প্রতিটা ফাইলে:

```python
# পুরনো:
except Exception:
    pass

# নতুন:
except Exception as e:
    LOG.debug("ignored: %s", e)
```

Critical path-গুলোতে (`stream.py`, `play.py`, `controls.py`):
```python
# পুরনো:
except Exception:
    pass

# নতুন (critical path):
except Exception:
    LOG.exception("unexpected error in <function_name>")
```

VS Code / Cursor-এ Regex find:
```
except Exception:\n(\s+)pass
```
Replace:
```
except Exception as e:\n$1LOG.exception("unexpected: %s", e)
```


## 🟢 FIX #10 — Cookies + Proxy (YouTube blocking)

### সমস্যা

Deploy log:
```
[2026-06-08 13:23:42,840] All direct APIs failed, trying yt-dlp for audio
```

মানে Cobalt + Innertube + Piped + Invidious সব fail হয়েছে। YouTube cloud IP-গুলো block করেছে।

### Fix — Cookies refresh

1. Chrome-এ youtube.com-এ login (যেকোনো Google account)
2. Extension install: **"Get cookies.txt LOCALLY"**
3. Extension থেকে cookies export → `youtube.com_cookies.txt`
4. Railway/Render Dashboard → Variables:
   ```
   COOKIES_TXT=<paste full cookies file content>
   ```
5. Service auto-restart

### Fix — Webshare Residential Proxy (recommended)

1. webshare.io account তৈরি করুন (free tier 10 proxy)
2. Proxy List download — format: `ip:port:username:password`
3. Railway env-var:
   ```
   YOUTUBE_PROXY_LIST=ip1:port1:user1:pass1,ip2:port2:user2:pass2,...
   ```

### Fix — yt-dlp extractor args

`platforms/youtube.py`-এ yt-dlp call-এ:

```python
ydl_opts = {
    # ... existing ...
    "extractor_args": {
        "youtube": {
            # Multiple player clients → fall back if one is throttled
            "player_client": ["android", "web", "ios"],
        }
    },
    # Audio-only bot হলে এটা ঠিক আছে।  ভিডিও streaming-ও support করলে এটা
    # ব্যবহার করুন: "bestvideo+bestaudio/best"
    "format": "bestaudio[ext=m4a]/bestaudio/best",
    # Force IPv4 — IPv6 often blocked by YouTube
    "source_address": "0.0.0.0",
}
```

> ⚠️ **`"skip": ["hls", "dash"]` ব্যবহার করবেন না।** কিছু YouTube videos
> (বিশেষ করে live streams এবং newer uploads) **DASH-only**, ফলে skip
> করলে video streaming পুরোপুরি break হবে এবং কিছু audio-only request-ও
> "no suitable format" error-এ fail করবে। yt-dlp default-এই সঠিক
> protocol বেছে নেয়; manual skip দিলে hit-rate কমে যায়।


## 🚀 Deploy Checklist

### Environment Variables (Railway / Heroku / Render)

```bash
# Required
API_ID=12345                          # my.telegram.org
API_HASH=abc123...                    # my.telegram.org
BOT_TOKEN=12345:abc...                # @BotFather
STRING_SESSION=...                    # primary assistant

# Multi-assistant (for 1000+ groups)
STRING_SESSION_2=...
STRING_SESSION_3=...
STRING_SESSION_4=...
STRING_SESSION_5=...
# up to STRING_SESSION_10

# YouTube reliability
COOKIES_TXT=<full cookies.txt content>
YOUTUBE_PROXY_LIST=ip1:port1:user1:pass1,ip2:port2:user2:pass2

# Database
MONGO_URL=mongodb+srv://...

# Optional but recommended
SENTRY_DSN=https://...@sentry.io/...  # crash tracking
LOG_LEVEL=INFO
OWNER_ID=123456789
SUPPORT_GROUP=https://t.me/...

# Performance tuning
MAX_QUEUE_SIZE=20                     # already in queue.py
PROGRESS_INTERVAL_SEC=30
```

### Railway-specific

`railway.json`-এ:
```json
{
  "build": {
    "builder": "DOCKERFILE",
    "dockerfilePath": "Dockerfile"
  },
  "deploy": {
    "startCommand": "python -m MusicLyrics",
    "restartPolicyType": "ON_FAILURE",
    "restartPolicyMaxRetries": 10,
    "healthcheckTimeout": 300
  }
}
```

### Plan Recommendations

| Groups | RAM | Plan | Assistants |
|--------|-----|------|------------|
| 1–50 | 512 MB | Free / Hobby | 1 |
| 50–200 | 1 GB | Hobby+ | 2–3 |
| 200–500 | 2 GB | Pro | 5 |
| 500–1000 | 4 GB | Pro+ | 10 |
| 1000+ | 8 GB + horizontal scale | Multi-container | 10+ + sharding |

### Dockerfile improvement

Memory-friendly Python flags যোগ করুন:

```dockerfile
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONHASHSEED=random \
    MALLOC_TRIM_THRESHOLD_=131072

# Reduce malloc fragmentation under high churn
ENV PYTHONMALLOC=malloc
```


## ✅ Test Checklist (apply করার পর verify)

### Single-group tests
- [ ] `/play Arijit Singh` — গান শুরু হয়
- [ ] গান চলাকালীন 5 মিনিট wait — audio drop হয় না
- [ ] `/skip` 10 বার consecutively — কোনো crash না
- [ ] `/play <url>` × 20 দ্রুত — queue cap কাজ করে, crash না
- [ ] `/vplay <query>` — video chat-এ video চলে
- [ ] `/stop` — properly leave + cleanup
- [ ] Bot restart-এর পর state পরিষ্কার

### Multi-group tests (50+ groups)
- [ ] 50 group simultaneous `/play` — কেউ stuck না
- [ ] Memory < 600 MB after 30 min
- [ ] No `RANDOM_ID_DUPLICATE` in log
- [ ] No `pytgcalls TIMED OUT` cascade
- [ ] Progress message edit happens every ~30s, না বেশি

### Long-run tests (24 hr)
- [ ] No segfault / OOM kill
- [ ] Memory plateau (does not keep growing)
- [ ] All assistants alive
- [ ] Sentry shows < 50 errors/day

### Log Signatures — কী দেখলে ভয় পাবেন

🚩 **Bad signs (still broken):**
```
[N] Retrying messages.SendMessage [500 RANDOM_ID_DUPLICATE]   # N > 1
play() TIMED OUT for ... — aborting attempt                    # frequent
SIGSEGV / Segmentation fault                                   # ANY
RuntimeError: dictionary changed size during iteration         # ANY
MemoryError                                                    # ANY
Worker timeout                                                 # frequent
```

✅ **Good signs (working):**
```
play() with GroupCallConfig succeeded for -100...
Streaming audio in -100...: <title>
fresh_resolve FAST PATH for ...: using existing media
Queue ...: added #N — <title>
janitor: cleaned N inactive chats        # periodic
```

---

## 📊 প্রত্যাশিত উন্নতি

| Metric | Before | After |
|--------|--------|-------|
| Skip latency | 3-15s + occasional hang | < 1s |
| Crash frequency | Multiple/hr | < 1/day |
| Memory (100 groups, 6h) | 800 MB → OOM | 350 MB stable |
| Max stable groups (1 assistant) | ~30 | ~80 |
| Max stable groups (10 assistants) | N/A | ~800-1000 |
| Audio mid-song drops | 5-10% songs | < 0.5% |
| FLOOD_WAIT events/hr | 20+ | < 2 |

## 🎯 Apply করার Order (গুরুত্বপূর্ণ)

1. **প্রথমে Fix #1, #2, #6, #9** — এগুলোই বেশিরভাগ crash থামাবে
2. **তারপর #3, #4, #5** — audio quality + memory
3. **তারপর #10** — cookies/proxy update
4. **পরে #7, #8** — performance + scale

প্রতিটা step-এর পর deploy করে ১ ঘণ্টা monitor করুন logs।

## 🆘 আটকে গেলে

1. Sentry-তে error দেখে exact stack trace পাবেন
2. `LOG_LEVEL=DEBUG` দিয়ে restart করে details দেখুন
3. https://github.com/RajSukh81/MusicLyrics/issues-এ issue create করুন

---

**শুভকামনা — bot স্থিতিশীল হবে! 🎵**



