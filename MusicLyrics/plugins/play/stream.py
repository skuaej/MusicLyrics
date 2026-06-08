"""Core streaming logic -- join/leave voice chats, stream audio/video.

Supports both local file paths and direct stream URLs (e.g. from YouTube).
Uses the py-tgcalls 2.x MediaStream API with proper flags.
Includes progress timer and auto-leave on track end.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional

from pyrogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)

from config import Config
from MusicLyrics.bot import bot
from MusicLyrics.userbot import get_assistant, pytgcalls, userbot


def _assistant_for_chat(chat_id: int):
    ub, ptc = get_assistant(chat_id)
    if ub is None or ptc is None:
        return userbot, pytgcalls
    return ub, ptc

from MusicLyrics.plugins.play.queue import (
    get_current,
    skip_queue,
    clear_queue,
    format_duration,
)
from MusicLyrics.utils.downloader import cleanup
from MusicLyrics.utils.autodelete import auto_delete_service, auto_delete_playing

# SoundCloud & JioSaavn fallbacks — ultimate last resort when all other methods fail
from MusicLyrics.plugins.play.platforms.soundcloud import (
    search_and_download_soundcloud,
    get_soundcloud_stream_url,
    is_soundcloud_url,
)
from MusicLyrics.plugins.play.platforms.jiosaavn import (
    search_and_download_jiosaavn,
)
from MusicLyrics.plugins.play.prefetch import (
    prefetch_next,
    cancel_prefetch,
    is_prefetched,
    refresh_item_if_stale,
)

LOG = logging.getLogger(__name__)

# Track active chats so we know whether to join or change stream
_active_chats: set[int] = set()
# Lock guarding all mutations of _active_chats / _progress_state.
# Without this, a tick of the central progress loop iterating over the dict
# can race with stream_audio / _on_stream_end and crash with
# "RuntimeError: Set/dictionary changed size during iteration".
_STATE_LOCK = asyncio.Lock()

# Track "Now Playing" messages for each chat so we can delete them when track ends
_now_playing_messages: dict[int, list] = {}

# Track playback start times for progress display
_play_start_times: dict[int, float] = {}

# Track current track durations for progress display
_play_durations: dict[int, int] = {}

# Centralized progress update state for all active chats.
_progress_state: dict[int, dict] = {}
_central_progress_task: Optional[asyncio.Task] = None

# Chats that have already received the first-play "warm-up" cycle.
# Workaround for the well-known py-tgcalls cold-start bug where the very
# first play() in a chat returns success but no audio is actually bound
# to the WebRTC track ("first song no sound" symptom). A brief
# pause→resume cycle right after the initial play forces py-tgcalls to
# (re)bind the audio stream, making the first track audible.
_warmed_up_chats: set[int] = set()

PROGRESS_INTERVAL_SEC = 5  # Update the "now playing" progress every 5 seconds.
PROGRESS_PER_TICK_CAP = 50  # Max edits per tick to avoid burst FLOOD.

# Track which platform last succeeded for each chat — prioritize it next time
_last_successful_platform: dict[int, str] = {}

# Per-chat lock to prevent race conditions between auto-next and manual skip/stop
_skip_locks: dict[int, asyncio.Lock] = {}

# Per-chat lock to prevent two concurrent pytgcalls.play() calls in the
# same chat.  Concurrent calls into pytgcalls' native C extension can
# segfault and kill the entire Python process — the most common cause of
# the bot vanishing mid-skip / mid-auto-next.
_play_locks: dict[int, asyncio.Lock] = {}
# Background play tasks that we gave up waiting for but didn't cancel.
# The next _do_play invocation will wait briefly for these to finish before
# issuing a new native play() on the same chat.
_orphan_play_tasks: dict[int, asyncio.Task] = {}
# Hard cap so a long-running deployment over thousands of groups cannot
# leak unbounded native-state references into Python and OOM Railway.
_ORPHAN_TASKS_HARD_CAP = 2000


def _reap_orphan_tasks() -> None:
    """Drop completed entries from _orphan_play_tasks; FIFO-evict if oversized."""
    try:
        done_keys = [k for k, t in _orphan_play_tasks.items() if t.done()]
        for k in done_keys:
            _orphan_play_tasks.pop(k, None)
        # If we are STILL over the cap (e.g., many genuinely stuck tasks),
        # cancel the oldest ones so memory cannot grow without bound.
        if len(_orphan_play_tasks) > _ORPHAN_TASKS_HARD_CAP:
            excess = len(_orphan_play_tasks) - _ORPHAN_TASKS_HARD_CAP
            for k in list(_orphan_play_tasks.keys())[:excess]:
                t = _orphan_play_tasks.pop(k, None)
                if t is not None and not t.done():
                    try:
                        t.cancel()
                    except Exception:
                        pass
    except Exception:
        pass


def _get_play_lock(chat_id: int) -> asyncio.Lock:
    """Return the per-chat play lock (created on first access)."""
    lock = _play_locks.get(chat_id)
    if lock is None:
        lock = asyncio.Lock()
        _play_locks[chat_id] = lock
    return lock


# Global lock for _now_playing_messages dict mutations.  Without this,
# multiple coroutines (skip / stop / auto-next / _on_stream_end) mutate
# the same list concurrently producing
#     RuntimeError: dictionary changed size during iteration
# and KeyError, both of which crash the affected handler.
_NPM_LOCK = asyncio.Lock()


async def _add_now_playing(chat_id: int, msg) -> None:
    """Append a Now Playing message under the global NPM lock."""
    async with _NPM_LOCK:
        _now_playing_messages.setdefault(chat_id, []).append(msg)


async def _pop_now_playing(chat_id: int) -> list:
    """Atomically remove and return all Now Playing messages for chat."""
    async with _NPM_LOCK:
        return _now_playing_messages.pop(chat_id, [])


async def _remove_now_playing(chat_id: int, msg_id: int) -> None:
    """Remove a single message by id under the NPM lock."""
    async with _NPM_LOCK:
        if chat_id in _now_playing_messages:
            _now_playing_messages[chat_id] = [
                m for m in _now_playing_messages[chat_id]
                if getattr(m, "id", None) != msg_id
            ]
            if not _now_playing_messages[chat_id]:
                _now_playing_messages.pop(chat_id, None)


# Per-chat flag so _on_stream_end cannot double-fire for the same chat.
# py-tgcalls occasionally raises StreamAudioEnded twice in rapid
# succession when a stream is replaced; without this guard each event
# advances the queue and the user sees songs skipped.
_END_HANDLING: dict[int, bool] = {}

# TTL-based suppression for stream-end events caused by manual skip/stop
# replacing streams.  When _do_play replaces the current stream, py-tgcalls
# fires StreamAudioEnded for the OLD stream.  Without suppression this causes
# _on_stream_end to double-advance the queue and leave the VC.
#
# We use a list of expiry timestamps per chat instead of a raw counter so
# stale suppressions cannot accumulate and silently swallow REAL stream-end
# events (which would cause "bot stuck in VC" symptoms).
_suppress_stream_end: dict[int, list[float]] = {}
# TTL for suppressing the OLD stream's end-event when we replace the
# current stream.  Must cover a worst-case pytgcalls.play() cascade.
_SUPPRESS_TTL_SEC = 45.0


def suppress_next_stream_end(chat_id: int) -> None:
    """Tell _on_stream_end to ignore the next stream-end event for *chat_id*.

    Call this BEFORE _do_play when you are intentionally replacing the current
    stream (skip / force-play) so the old stream's end event is swallowed.

    Each suppression has a TTL — if the old-stream-end never arrives within
    that window we forget about it so future real events are NOT swallowed.
    """
    deadlines = _suppress_stream_end.setdefault(chat_id, [])
    now = time.time()
    # Drop any expired entries first
    deadlines[:] = [d for d in deadlines if d > now]
    deadlines.append(now + _SUPPRESS_TTL_SEC)
    LOG.debug("suppress_next_stream_end(%s) → pending=%d", chat_id, len(deadlines))


def _consume_suppression(chat_id: int) -> bool:
    """If there is a non-expired suppression, consume one and return True."""
    deadlines = _suppress_stream_end.get(chat_id)
    if not deadlines:
        return False
    now = time.time()
    # Drop expired
    deadlines[:] = [d for d in deadlines if d > now]
    if not deadlines:
        _suppress_stream_end.pop(chat_id, None)
        return False
    deadlines.pop(0)
    if not deadlines:
        _suppress_stream_end.pop(chat_id, None)
    return True


def _get_skip_lock(chat_id: int) -> asyncio.Lock:
    """Get or create a per-chat skip lock."""
    if chat_id not in _skip_locks:
        _skip_locks[chat_id] = asyncio.Lock()
    return _skip_locks[chat_id]


async def acquire_skip_lock(chat_id: int, timeout: float = 15.0) -> asyncio.Lock:
    """Acquire the skip lock with a generous timeout.

    Previously this function "force-replaced" a stuck lock with a fresh
    one after 5 s — which let the caller proceed while the previous
    operation was still running, causing two concurrent pytgcalls.play()
    calls and an eventual segfault.

    Now we simply wait up to *timeout* seconds for the genuine lock; on
    timeout we raise so the caller can tell the user to retry instead of
    racing the in-flight operation.

    The returned Lock is ALREADY acquired — caller must release it
    (use ``try/finally`` instead of ``async with``).
    """
    lock = _get_skip_lock(chat_id)
    try:
        await asyncio.wait_for(lock.acquire(), timeout=timeout)
        return lock
    except asyncio.TimeoutError:
        LOG.warning(
            "acquire_skip_lock: lock for %s busy > %.1fs — refusing to "
            "force-replace (would race in-flight play()).",
            chat_id, timeout,
        )
        raise RuntimeError(f"skip_lock timeout in chat {chat_id}")

async def pre_join_vc(chat_id: int) -> None:
    """Pre-join the voice chat so streaming can start instantly.

    Ensures the assistant is in the group before play() is called,
    eliminating the group-join delay from the critical path.
    If already in VC or group, does nothing.

    PARALLEL join: all 4 join methods race simultaneously; the FIRST
    that succeeds wins and the rest are cancelled.  Each method has a
    hard timeout so a hung Telegram API call cannot block the others.
    This collapses the worst-case wait from ~25 s (sequential) down to
    the slowest single method (~5 s).
    """
    ub, ptc = _assistant_for_chat(chat_id)
    if ptc is None or ub is None:
        return

    # Already active? Nothing to do
    if chat_id in _active_chats:
        return

    try:
        # Check if already in a call
        try:
            calls = ptc.calls
            if asyncio.iscoroutine(calls):
                calls = await calls
            if chat_id in calls:
                _active_chats.add(chat_id)
                return
        except Exception:
            pass

        # Check if assistant is already a member of the group
        try:
            me = await asyncio.wait_for(ub.get_me(), timeout=3.0)
            try:
                member = await asyncio.wait_for(
                    bot.get_chat_member(chat_id, me.id), timeout=2.5,
                )
                if member and getattr(member, "status", None) and \
                        str(member.status).split(".")[-1].lower() not in ("left", "kicked", "banned", "restricted"):
                    LOG.debug("Pre-join: Assistant already in group %s", chat_id)
                    return
            except Exception:
                pass
        except Exception:
            me = None

        # PARALLEL JOIN — race all methods, first success wins.
        async def _m_direct():
            await asyncio.wait_for(ub.join_chat(chat_id), timeout=5.0)
            return "direct"

        async def _m_invite():
            invite_link = await asyncio.wait_for(
                bot.export_chat_invite_link(chat_id), timeout=3.0,
            )
            if not invite_link:
                raise RuntimeError("no invite link")
            await asyncio.wait_for(ub.join_chat(invite_link), timeout=5.0)
            return "invite"

        async def _m_fresh_invite():
            new_link = await asyncio.wait_for(
                bot.create_chat_invite_link(
                    chat_id, name="Assistant Auto-Join", member_limit=1,
                ),
                timeout=3.0,
            )
            if not (new_link and new_link.invite_link):
                raise RuntimeError("no fresh invite")
            link = new_link.invite_link
            try:
                await asyncio.wait_for(ub.join_chat(link), timeout=5.0)
                return "fresh_invite"
            finally:
                try:
                    await asyncio.wait_for(
                        bot.revoke_chat_invite_link(chat_id, link), timeout=2.0,
                    )
                except Exception:
                    pass

        async def _m_add():
            if me is None:
                _me = await asyncio.wait_for(ub.get_me(), timeout=2.5)
                uid = _me.id
            else:
                uid = me.id
            await asyncio.wait_for(
                bot.add_chat_members(chat_id, uid), timeout=4.0,
            )
            return "add_chat_members"

        tasks = [
            asyncio.create_task(_m_direct()),
            asyncio.create_task(_m_invite()),
            asyncio.create_task(_m_fresh_invite()),
            asyncio.create_task(_m_add()),
        ]
        winner = None
        last_errors: list[str] = []
        pending = set(tasks)
        try:
            while pending:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED,
                )
                for t in done:
                    try:
                        winner = t.result()
                        break
                    except Exception as e:
                        last_errors.append(f"{type(e).__name__}: {e}")
                if winner is not None:
                    break
        finally:
            for t in pending:
                t.cancel()
            # Drain cancellations quietly
            if pending:
                try:
                    await asyncio.gather(*pending, return_exceptions=True)
                except Exception:
                    pass

        if winner is not None:
            LOG.info("Pre-join: Assistant joined group %s via %s", chat_id, winner)
        else:
            LOG.warning(
                "Pre-join: All parallel methods failed for %s (errors=%s) — "
                "will retry inside _do_play",
                chat_id, "; ".join(last_errors[:4]) or "none",
            )

    except Exception as e:
        LOG.debug("Pre-join VC failed for %s: %s", chat_id, e)


# Guard: if pytgcalls is None (no STRING_SESSION), music features are disabled
if pytgcalls is None:
    LOG.warning("STRING_SESSION not set -- music streaming features are disabled.")


# -- Import py-tgcalls types with compatibility handling --
_HAS_MEDIA_STREAM = False
_HAS_FLAGS = False
_HAS_GROUP_CALL_CONFIG = False
MediaStream = None
AudioQuality = None
VideoQuality = None

try:
    from pytgcalls.types import MediaStream as _MS, AudioQuality as _AQ, VideoQuality as _VQ
    MediaStream = _MS
    AudioQuality = _AQ
    VideoQuality = _VQ
    _HAS_MEDIA_STREAM = True
except ImportError:
    LOG.warning("Could not import MediaStream/AudioQuality/VideoQuality from pytgcalls.types")

# Check for Flags support (py-tgcalls >= 2.1)
if _HAS_MEDIA_STREAM:
    try:
        _ = MediaStream.Flags.IGNORE
        _HAS_FLAGS = True
    except AttributeError:
        _HAS_FLAGS = False
        LOG.info("MediaStream.Flags not available (older py-tgcalls version)")

# Check for GroupCallConfig
try:
    from pytgcalls.types import GroupCallConfig
    _HAS_GROUP_CALL_CONFIG = True
except ImportError:
    GroupCallConfig = None
    _HAS_GROUP_CALL_CONFIG = False

try:
    from pytgcalls.types.stream import StreamAudioEnded
    _STREAM_END_TYPE = StreamAudioEnded
except ImportError:
    _STREAM_END_TYPE = None


# Track chats where auto-next is already being processed (prevents double-skip)
_auto_next_in_progress: set[int] = set()


async def _add_reaction(chat_id: int, message_id: int) -> None:
    """Add a random reaction to a bot message (fire-and-forget).

    Compatible with ALL pyrogram versions — no hard dependency on
    ReactionTypeEmoji.  Uses a large pool of trending emojis.
    """
    import random as _rand
    _react_pool = [
        "\U0001f44d",  # 👍
        "\u2764\ufe0f",  # ❤️
        "\U0001f525",  # 🔥
        "\U0001f389",  # 🎉
        "\U0001f929",  # 🤩
        "\U0001f60d",  # 😍
        "\U0001f44f",  # 👏
        "\U0001f970",  # 🥰
        "\U0001f4af",  # 💯
        "\u26a1",      # ⚡
        "\U0001f3c6",  # 🏆
        "\U0001f601",  # 😁
        "\U0001f923",  # 🤣
        "\U0001f60e",  # 😎
        "\U0001f618",  # 😘
        "\U0001f64f",  # 🙏
        "\U0001f48b",  # 💋
        "\U0001f37e",  # 🍾
        "\U0001f31a",  # 🌚
        "\U0001f44c",  # 👌
        "\U0001f607",  # 😇
        "\u2764\ufe0f\u200d\U0001f525",  # ❤️‍🔥
        "\U0001f60b",  # 😋
        "\U0001f633",  # 😳
        "\U0001f47b",  # 👻
        "\U0001f383",  # 🎃
        "\U0001f913",  # 🤓
        "\U0001f92f",  # 🤯
        "\U0001f62d",  # 😭
        "\U0001f608",  # 😈
        "\U0001f440",  # 👀
        "\U0001f353",  # 🍓
        "\U0001f34c",  # 🍌
        "\U0001f494",  # 💔
        "\U0001f648",  # 🙈
        "\U0001f634",  # 😴
        "\U0001f928",  # 🧐
        "\U0001f32d",  # 🌭
        "\U0001f973",  # 🥳
        "\U0001f480",  # 💀
    ]
    emoji = _rand.choice(_react_pool)
    await asyncio.sleep(0.01)
    # Try multiple methods for compatibility with all pyrogram versions
    for attempt in range(4):
        try:
            if attempt == 0:
                await bot.send_reaction(chat_id, message_id, emoji=emoji)
            elif attempt == 1:
                await bot.send_reaction(chat_id, message_id, emoji=[emoji])
            elif attempt == 2:
                await bot.send_reaction(chat_id, message_id, reaction=emoji)
            else:
                # Last resort: try with ReactionTypeEmoji if available
                try:
                    from pyrogram.types import ReactionTypeEmoji
                    await bot.send_reaction(chat_id, message_id, emoji=[ReactionTypeEmoji(emoji=emoji)])
                except ImportError:
                    pass
            return  # Success, stop trying
        except Exception:
            continue  # Try next method


# ── Stylish animated button themes that rotate ──────────────────────────────
# Each theme defines emoji icons for buttons — emojis cycle through themes
# every 30 seconds creating a "moving" animation effect.
_BUTTON_THEMES = [
    {
        "resume": "🌙", "mute": "🔇", "song": "🎵", "skip": "🎶",
        "yorsa": "🎶", "home": "🔮", "close": "🌙",
        "bar_left": "🐻", "bar_dot": "🍃",
        "header": "🎧", "title_icon": "🎵", "dur_icon": "⏱",
        "label": "Theme 1",
    },
    {
        "resume": "🦋", "mute": "🔕", "song": "🎧", "skip": "🎼",
        "yorsa": "🦋", "home": "🌍", "close": "🦋",
        "bar_left": "🦊", "bar_dot": "🔥",
        "header": "🔥", "title_icon": "🎧", "dur_icon": "⌛",
        "label": "Theme 2",
    },
    {
        "resume": "🌸", "mute": "🚫", "song": "🎹", "skip": "🎻",
        "yorsa": "🌸", "home": "💎", "close": "🌸",
        "bar_left": "🐱", "bar_dot": "⭐",
        "header": "💫", "title_icon": "🎹", "dur_icon": "🕐",
        "label": "Theme 3",
    },
    {
        "resume": "🔮", "mute": "🔈", "song": "🎺", "skip": "🎷",
        "yorsa": "🔮", "home": "🌟", "close": "🔮",
        "bar_left": "🐼", "bar_dot": "💫",
        "header": "✨", "title_icon": "🎺", "dur_icon": "⏰",
        "label": "Theme 4",
    },
    {
        "resume": "🌊", "mute": "🔕", "song": "🎻", "skip": "🎶",
        "yorsa": "🌊", "home": "🏠", "close": "🌊",
        "bar_left": "🐬", "bar_dot": "🌟",
        "header": "🌈", "title_icon": "🎻", "dur_icon": "⏳",
        "label": "Theme 5",
    },
    {
        "resume": "🍀", "mute": "🔇", "song": "🎤", "skip": "🎵",
        "yorsa": "🍀", "home": "💝", "close": "🍀",
        "bar_left": "🐢", "bar_dot": "🌺",
        "header": "🎊", "title_icon": "🎤", "dur_icon": "🕰",
        "label": "Theme 6",
    },
]
_current_theme_index: int = 0


def _get_next_color() -> str:
    """Advance theme index and return current theme label."""
    global _current_theme_index
    _current_theme_index += 1
    return _BUTTON_THEMES[_current_theme_index % len(_BUTTON_THEMES)]["label"]


def _get_current_theme() -> dict:
    """Get the current button theme."""
    return _BUTTON_THEMES[_current_theme_index % len(_BUTTON_THEMES)]


def _control_keyboard(color: str = "") -> InlineKeyboardMarkup:
    """Build stylish premium control keyboard with animated emoji icons.

    Buttons match the reference photo style — colorful emojis that rotate
    through themes on each progress update creating animation effect.
    YORSA button links to user's GitHub repo.
    """
    t = _get_current_theme()
    bot_username = bot.me.username if bot.me else "MusicLyrics"
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(f"{t['resume']} Resume", callback_data="ctl_resume"),
                InlineKeyboardButton(f"{t['mute']} Pause", callback_data="ctl_pause"),
                InlineKeyboardButton(f"{t['song']} Queue", callback_data="ctl_queue"),
                InlineKeyboardButton(f"{t['skip']} Skip", callback_data="ctl_skip"),
            ],
            [
                InlineKeyboardButton(
                    f"➕ ᴀᴅᴅ ᴛᴏ ɢʀᴏᴜᴘ {t['yorsa']}",
                    url=f"https://t.me/{bot_username}?startgroup=true",
                ),
                InlineKeyboardButton(
                    f"{t['home']} 💬 ꜱᴜᴘᴘᴏʀᴛ",
                    url=Config.SUPPORT_GROUP,
                ),
            ],
            [
                InlineKeyboardButton(
                    f"{t['close']}  ✧ CLOSE ✧  {t['close']}",
                    callback_data="ctl_stop",
                ),
            ],
        ]
    )


def _queue_added_keyboard(color: str = "") -> InlineKeyboardMarkup:
    """Minimal keyboard for the "added to queue" notification.

    Only the essentials — Queue (to peek at the list) and Close (to dismiss
    the message). Playback controls (pause/resume/skip/stop) intentionally
    omitted because they are irrelevant to a queue notification and would
    only make the message visually crowded.
    """
    t = _get_current_theme()
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(f"{t['song']} Queue", callback_data="ctl_queue"),
                InlineKeyboardButton(
                    f"{t['close']} Close", callback_data="ctl_stop",
                ),
            ],
        ]
    )


def _song_ended_keyboard() -> InlineKeyboardMarkup:
    """Build the 'song ended' keyboard with Add to Group button."""
    bot_username = bot.me.username if bot.me else "MusicLyrics"
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "➕ ᴀᴅᴅ ᴛᴏ ɢʀᴏᴜᴘ",
                    url=f"https://t.me/{bot_username}?startgroup=true",
                ),
                InlineKeyboardButton(
                    "💬 ꜱᴜᴘᴘᴏʀᴛ",
                    url=Config.SUPPORT_GROUP,
                ),
            ],
        ]
    )


def _is_url(path: str) -> bool:
    """Check if path is a URL (not a local file)."""
    return path.startswith("http://") or path.startswith("https://")


async def _check_stream_url(url: str) -> bool:
    """Quick HEAD check to see if a stream URL is still valid.

    Returns True if URL is reachable (2xx/3xx), False otherwise.
    SPEED OPTIMISED: 0.6s timeout, HEAD-only, assume OK on timeout.
    """
    if not _is_url(url):
        return True  # Not a URL, skip check
    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            try:
                async with session.head(
                    url,
                    timeout=aiohttp.ClientTimeout(total=0.6, connect=0.4),
                    allow_redirects=True,
                ) as resp:
                    if resp.status < 400:
                        return True
                    LOG.warning("Stream URL failed (HTTP %d): %s", resp.status, url[:80])
                    return False
            except asyncio.TimeoutError:
                # On timeout, assume URL is OK — slow CDN, let py-tgcalls try
                return True
            except Exception:
                pass
    except Exception:
        pass
    # If we can't check at all, assume the URL is OK
    return True


def _validate_media(media_path: str) -> None:
    """Validate media path -- file must exist or be a URL."""
    if not media_path or not isinstance(media_path, str):
        raise FileNotFoundError("No media path provided.")
    media_path = media_path.strip()
    if not media_path:
        raise FileNotFoundError("Empty media path provided.")
    if _is_url(media_path):
        # Basic URL sanity checks
        if len(media_path) < 10:
            raise FileNotFoundError(f"Invalid media URL: {media_path}")
        return  # URLs are accepted
    if not os.path.isfile(media_path):
        raise FileNotFoundError(f"File not found: {media_path}")
    # Reject empty files (corrupted downloads)
    if os.path.getsize(media_path) < 1000:
        raise FileNotFoundError(f"File too small (likely corrupted): {media_path}")


def _make_audio_stream(media_path: str):
    """Create an audio-only MediaStream (file or URL).

    Uses video_flags=IGNORE for audio-only mode, matching
    the approach used by AnonXMusic.
    """
    if not _HAS_MEDIA_STREAM:
        raise RuntimeError("py-tgcalls MediaStream not available.")

    if _HAS_FLAGS:
        try:
            return MediaStream(
                media_path,
                audio_parameters=AudioQuality.HIGH,
                video_flags=MediaStream.Flags.IGNORE,
            )
        except (AttributeError, TypeError) as e:
            LOG.debug("MediaStream with Flags failed: %s", e)

    # Fallback: just audio parameters
    try:
        return MediaStream(
            media_path,
            audio_parameters=AudioQuality.HIGH,
        )
    except (AttributeError, TypeError) as e:
        LOG.debug("MediaStream with AudioQuality failed: %s", e)

    # Last resort
    return MediaStream(media_path)


def _make_video_stream(media_path: str):
    """Create a video+audio MediaStream (file or URL).

    Uses video_flags=AUTO_DETECT for video mode, matching
    the approach used by AnonXMusic.
    """
    if not _HAS_MEDIA_STREAM:
        raise RuntimeError("py-tgcalls MediaStream not available.")

    if _HAS_FLAGS:
        try:
            return MediaStream(
                media_path,
                audio_parameters=AudioQuality.HIGH,
                video_parameters=VideoQuality.HD_720p,
                video_flags=MediaStream.Flags.AUTO_DETECT,
            )
        except (AttributeError, TypeError) as e:
            LOG.debug("MediaStream video with Flags failed: %s", e)

    # Fallback
    try:
        return MediaStream(
            media_path,
            audio_parameters=AudioQuality.HIGH,
            video_parameters=VideoQuality.SD_480p,
        )
    except (AttributeError, TypeError) as e:
        LOG.debug("MediaStream video fallback failed: %s", e)

    return MediaStream(media_path)


async def _ensure_in_vc(chat_id: int):
    """Ensure the userbot is in the voice chat for the given chat.

    If not already in the VC, join it first before playing.
    This fixes the issue where pytgcalls.play() with auto_start
    sometimes fails to join the VC.
    """
    ub, ptc = _assistant_for_chat(chat_id)
    if ptc is None:
        raise RuntimeError("Music streaming is disabled -- STRING_SESSION not configured.")

    # Check if already in a call for this chat
    try:
        calls = ptc.calls
        if asyncio.iscoroutine(calls):
            calls = await calls
        if chat_id in calls:
            LOG.debug("Already in voice chat for %s, no need to join", chat_id)
            return
    except Exception:
        pass

    # Try to check active calls via pytgcalls
    try:
        active_calls = ptc.active_calls
        if asyncio.iscoroutine(active_calls):
            active_calls = await active_calls
        elif isinstance(active_calls, (list, dict, set)):
            if chat_id in active_calls:
                return
    except Exception:
        pass

    LOG.info("Ensuring userbot is in voice chat for chat %s", chat_id)


async def _warmup_first_play_if_needed(chat_id: int) -> None:
    """Force py-tgcalls to bind audio after the very first play in a chat.

    The well-known "first song no sound" symptom on py-tgcalls 2.x: the
    initial play() call returns success, but because the WebRTC
    negotiation with Telegram completed *after* pytgcalls latched the
    media source, the audio track never actually gets bound — listeners
    in the voice chat hear silence for the entire first track.

    A tiny pause→resume cycle right after the first successful play
    forces pytgcalls to re-bind the audio track on the active call,
    which makes the very first song audible. Subsequent plays in the
    same chat are not affected, so we only do this once per chat.
    """
    if chat_id in _warmed_up_chats:
        return
    _warmed_up_chats.add(chat_id)
    _, ptc = _assistant_for_chat(chat_id)
    if ptc is None:
        return
    try:
        # Let the call finish negotiating before we toggle the stream.
        await asyncio.sleep(0.4)
        pause_fn = getattr(ptc, "pause", None) or getattr(ptc, "pause_stream", None)
        resume_fn = getattr(ptc, "resume", None) or getattr(ptc, "resume_stream", None)
        if pause_fn is None or resume_fn is None:
            return
        await asyncio.wait_for(pause_fn(chat_id), timeout=2.0)
        await asyncio.sleep(0.15)
        await asyncio.wait_for(resume_fn(chat_id), timeout=2.0)
        LOG.info("First-play audio warm-up completed for %s", chat_id)
    except Exception as e:
        # Warm-up is best-effort: even if it fails the original play()
        # is still running, so we never want to break playback here.
        LOG.debug("First-play warm-up failed for %s: %s", chat_id, e)


async def _do_play(chat_id: int, stream):
    """Public _do_play wrapper that serializes pytgcalls.play() per chat.

    Two concurrent pytgcalls.play() calls in the same chat can crash the
    native NTgCalls extension and kill the Python process.  A per-chat
    asyncio.Lock guarantees at most one play() runs at a time for a chat,
    while other chats remain fully parallel.
    """
    async with _get_play_lock(chat_id):
        orphan = _orphan_play_tasks.pop(chat_id, None)
        if orphan and not orphan.done():
            try:
                await asyncio.wait_for(asyncio.shield(orphan), timeout=10.0)
            except (asyncio.TimeoutError, Exception):
                pass
        await _do_play_locked(chat_id, stream)


async def _drain_orphan_play_task(chat_id: int, timeout: float = 10.0) -> None:
    """Wait briefly for any previously orphaned play() task to finish."""
    orphan = _orphan_play_tasks.pop(chat_id, None)
    if orphan and not orphan.done():
        try:
            await asyncio.wait_for(asyncio.shield(orphan), timeout=timeout)
        except (asyncio.TimeoutError, Exception):
            pass


async def _do_play_locked(chat_id: int, stream):
    """Call pytgcalls.play — join VC if needed, then start streaming.

    Compatible with py-tgcalls 2.1.x and 2.2.x APIs.
    Single fast attempt per method.  On total failure performs a hard
    reset (raw-API leave) so the next call starts from a clean state
    instead of inheriting a wedged pytgcalls connection.

    The total wall-clock budget is bounded:
        3 methods × PLAY_METHOD_TIMEOUT (4s) + auto-join + 1 retry
        ≤ ~22 s in the worst case.

    Callers wrap _do_play with their own outer ``asyncio.wait_for`` —
    keep that outer timeout STRICTLY greater than this budget (>= 30s)
    or pytgcalls will be cancelled mid-``play()`` and left wedged for
    the next track.
    """
    _, ptc = _assistant_for_chat(chat_id)
    if ptc is None:
        raise RuntimeError("Music streaming is disabled -- STRING_SESSION not configured.")

    # Ensure the assistant is actually in the group/VC before issuing play().
    try:
        await pre_join_vc(chat_id)
    except Exception as e:
        LOG.debug("_do_play_locked: pre_join_vc failed for %s: %s", chat_id, e)

    # If a previous timed-out play() is still running, wait briefly so we
    # do not launch a second native play() on the same chat at the same time.
    await _drain_orphan_play_task(chat_id)

    # If we are already streaming in this chat, the new play() call will
    # terminate the old stream causing a StreamAudioEnded event.  Suppress
    # that event so _on_stream_end does not mistakenly advance the queue.
    if chat_id in _active_chats:
        suppress_next_stream_end(chat_id)

    # Per-method hard timeout.  4 s is enough for a healthy play() to
    # respond — a slower response almost always means the call is wedged
    # and the next method/reset will work better than waiting longer.
    # Larger values (we tried 12s) made the wedge cascade painfully slow
    # (3 × 12 = 36 s of dead air before recovery even started); 4s keeps
    # the recovery snappy and the outer chain still retries with a fresh
    # assistant join, which is what actually fixes wedged calls.
    PLAY_METHOD_TIMEOUT = 4.0

    async def _reset_call_state():
        """Leave the call to clear py-tgcalls internal state.

        When pytgcalls.play() hangs on a track replacement, retrying the
        same call usually hangs too because py-tgcalls' internal stream
        state is wedged.  A fresh leave+rejoin almost always unblocks it.

        We try BOTH pytgcalls leave methods AND the raw API leave so a
        wedged pytgcalls connection cannot keep us stuck in the call.
        """
        for method_name in ("leave_call", "leave_group_call"):
            fn = getattr(ptc, method_name, None)
            if fn is None:
                continue
            try:
                await asyncio.wait_for(fn(chat_id), timeout=2.5)
                LOG.info("_reset_call_state: %s succeeded for %s", method_name, chat_id)
                return
            except Exception as e:
                LOG.debug("_reset_call_state: %s failed for %s: %s", method_name, chat_id, e)
        # pytgcalls leaves failed — try raw API so we definitely leave.
        try:
            await _raw_leave_group_call(chat_id)
        except Exception as e:
            LOG.debug("_reset_call_state: raw leave failed for %s: %s", chat_id, e)

    async def _try_play(reset_first: bool = False):
        """Attempt all play methods — returns True on success.

        Once any method TIMES OUT we immediately reset and break out so
        the remaining methods don't pile up on a wedged call.  Other
        exceptions (TypeError / AttributeError from older pytgcalls) are
        non-fatal and fall through to the next method.
        """
        if reset_first:
            await _reset_call_state()
            _active_chats.discard(chat_id)
            # After a hard reset there is NO old stream left whose end-event
            # we need to suppress.  Any suppression we queued earlier (for
            # the OLD stream that existed before the reset) is now stale —
            # if we keep it, the NEXT real stream-end of the NEW track will
            # be swallowed and auto-next won't fire (= "song stuck / queue
            # frozen mid-track" symptom).  Drop the stale bucket here.
            _suppress_stream_end.pop(chat_id, None)
            # Brief pause for Telegram to register the leave before rejoin
            await asyncio.sleep(0.3)

        async def _play_and_wait(coro, label: str) -> bool:
            play_task = asyncio.create_task(coro)

            def _orphan_done(task: asyncio.Task) -> None:
                try:
                    task.result()
                    _active_chats.add(chat_id)
                    LOG.info("Background play task succeeded for %s", chat_id)
                except Exception as exc:
                    LOG.debug("Background play task failed for %s: %s", chat_id, exc)

            play_task.add_done_callback(_orphan_done)
            done, pending = await asyncio.wait(
                [play_task], timeout=PLAY_METHOD_TIMEOUT,
            )
            if play_task in pending:
                _orphan_play_tasks[chat_id] = play_task
                LOG.warning(
                    "play() taking >%.1fs for %s — proceeding without cancelling native call",
                    PLAY_METHOD_TIMEOUT, chat_id,
                )
                return False
            play_task.result()
            _active_chats.add(chat_id)
            LOG.info("%s succeeded for %s", label, chat_id)
            return True

        # Method 1: play() with GroupCallConfig (py-tgcalls >= 2.1)
        if _HAS_GROUP_CALL_CONFIG:
            try:
                if await _play_and_wait(
                    ptc.play(
                        chat_id, stream,
                        config=GroupCallConfig(auto_start=True),
                    ),
                    "play() with GroupCallConfig",
                ):
                    return True
                LOG.warning("play() with GroupCallConfig TIMED OUT for %s — aborting attempt", chat_id)
                return False
            except (TypeError, AttributeError) as e:
                LOG.debug("play() with GroupCallConfig API mismatch: %s — trying plain play()", e)
            except Exception as e:
                LOG.debug("play() with GroupCallConfig errored: %s", e)

        # Method 2: plain play() (py-tgcalls 2.2.x)
        try:
            if await _play_and_wait(
                ptc.play(chat_id, stream), "play()"
            ):
                return True
            LOG.warning("plain play() TIMED OUT for %s — aborting attempt", chat_id)
            return False
        except Exception as e:
            LOG.debug("play() failed: %s", e)

        # Method 3: explicit join_group_call (older py-tgcalls)
        if hasattr(ptc, 'join_group_call'):
            try:
                if await _play_and_wait(
                    ptc.join_group_call(chat_id, stream), "join_group_call()"
                ):
                    return True
                LOG.warning("join_group_call() TIMED OUT for %s", chat_id)
            except Exception as e:
                LOG.debug("join_group_call() also failed: %s", e)

        return False

    # First attempt — no reset needed
    if await _try_play():
        await _warmup_first_play_if_needed(chat_id)
        return

    # If the first attempt timed out and left a native play task orphaned,
    # wait briefly before retrying so we don't race another play() call.
    await _drain_orphan_play_task(chat_id)

    # Second attempt — reset call state first (handles wedged py-tgcalls)
    LOG.info("First play attempt failed for %s — resetting call state and retrying", chat_id)
    if await _try_play(reset_first=True):
        await _warmup_first_play_if_needed(chat_id)
        return

    # If first attempt failed, assistant might not be in the group yet.
    # Run all join methods in PARALLEL — first success wins.  This cuts
    # the worst-case wait from ~25 s (sequential 5+8+9+3) down to the
    # slowest single method (~5 s).
    LOG.info("Play failed for %s — trying to auto-join assistant to the group", chat_id)

    async def _aj_direct():
        await asyncio.wait_for(ub.join_chat(chat_id), timeout=5.0)
        return "chat_id"

    async def _aj_invite():
        invite_link = await asyncio.wait_for(
            bot.export_chat_invite_link(chat_id), timeout=3.0,
        )
        if not invite_link:
            raise RuntimeError("no invite link")
        await asyncio.wait_for(ub.join_chat(invite_link), timeout=5.0)
        return "invite_link"

    async def _aj_fresh():
        new_link = await asyncio.wait_for(
            bot.create_chat_invite_link(
                chat_id, name="Auto-Join", member_limit=1,
            ),
            timeout=3.0,
        )
        if not (new_link and new_link.invite_link):
            raise RuntimeError("no fresh invite")
        link = new_link.invite_link
        try:
            await asyncio.wait_for(ub.join_chat(link), timeout=5.0)
            return "fresh_invite"
        finally:
            try:
                await asyncio.wait_for(
                    bot.revoke_chat_invite_link(chat_id, link), timeout=2.0,
                )
            except Exception:
                pass

    async def _aj_add():
        _me = await asyncio.wait_for(ub.get_me(), timeout=2.5)
        await asyncio.wait_for(
            bot.add_chat_members(chat_id, _me.id), timeout=4.0,
        )
        return "add_chat_members"

    join_method = None
    aj_tasks = [
        asyncio.create_task(_aj_direct()),
        asyncio.create_task(_aj_invite()),
        asyncio.create_task(_aj_fresh()),
        asyncio.create_task(_aj_add()),
    ]
    aj_pending = set(aj_tasks)
    try:
        while aj_pending:
            done, aj_pending = await asyncio.wait(
                aj_pending, return_when=asyncio.FIRST_COMPLETED,
            )
            for t in done:
                try:
                    join_method = t.result()
                    break
                except Exception as e:
                    LOG.debug("auto-join method failed for %s: %s", chat_id, e)
            if join_method is not None:
                break
    finally:
        for t in aj_pending:
            t.cancel()
        if aj_pending:
            try:
                await asyncio.gather(*aj_pending, return_exceptions=True)
            except Exception:
                pass

    if join_method is not None:
        LOG.info("Assistant auto-joined group %s via %s", chat_id, join_method)
        # Brief pause so Telegram registers the join before pytgcalls
        # tries to discover the group call.
        await asyncio.sleep(0.4)
        # Reset pytgcalls' native state so the retry play() doesn't
        # inherit the wedged session from the timed-out attempt above.
        # Without this reset, pytgcalls keeps thinking it's still in
        # the call and the next play() hangs again.
        if await _try_play(reset_first=True):
            await _warmup_first_play_if_needed(chat_id)
            return

    # All play methods failed — HARD RESET so we never inherit a wedged
    # pytgcalls connection on the next attempt.  This is the key fix that
    # stops "skip pressed → next song doesn't play → bot stuck" loops.
    _active_chats.discard(chat_id)
    _suppress_stream_end.pop(chat_id, None)
    try:
        await _reset_call_state()
    except Exception:
        pass

    raise RuntimeError(f"All play methods failed for chat {chat_id}")


# ── Progress Timer ────────────────────────────────────────────────────────────

def _format_progress(elapsed: int, total: int) -> str:
    """Format a decorated progress bar with animated emoji style."""
    t = _get_current_theme()
    if total <= 0:
        return f"{t['bar_left']} {format_duration(elapsed)} {t['bar_dot']}━━━━━━━━━━━━ ʟɪᴠᴇ"

    elapsed_str = format_duration(elapsed)
    total_str = format_duration(total)

    # Create visual progress bar with animated emoji decoration
    bar_length = 12
    progress = min(elapsed / total, 1.0)
    filled = int(bar_length * progress)

    bar = "━" * filled + f" {t['bar_dot']} " + "━" * (bar_length - filled)
    return f"{t['bar_left']}  {elapsed_str}  {bar}  {total_str}"


def _build_progress_text(current, elapsed: int, total: int) -> str:
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


async def _central_progress_loop():
    while True:
        try:
            await asyncio.sleep(PROGRESS_INTERVAL_SEC)
            # Snapshot keys under the state lock so we never iterate a dict
            # that is being mutated concurrently (RuntimeError otherwise).
            async with _STATE_LOCK:
                chats = list(_progress_state.keys())
            # Opportunistic reaper — keeps memory bounded on long uptimes.
            _reap_orphan_tasks()
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
                    if time.time() - state.get("last_update", 0) < 4:
                        continue
                    state["last_update"] = time.time()
                    current = await get_current(chat_id)
                    if not current:
                        continue
                    text = _build_progress_text(current, elapsed, total)
                    last_msg = msgs[-1]
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
                    except Exception as e:
                        LOG.warning("Progress edit failed for %s: %s", chat_id, e)
                        if "MESSAGE_ID_INVALID" in str(e) or "message not found" in str(e).lower():
                            _progress_state.pop(chat_id, None)
                except Exception as e:
                    LOG.debug("central progress tick failed for %s: %s", chat_id, e)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # NEVER let the central progress loop die — without it, all groups
            # silently lose their "now playing" updates and users assume the
            # bot has crashed.
            LOG.exception("central progress loop crashed; restarting in 5s: %s", e)
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                raise


async def _stream_health_watchdog(chat_id: int):
    last_pos = -1
    frozen_count = 0
    while chat_id in _play_start_times:
        await asyncio.sleep(20)
        _, ptc = _assistant_for_chat(chat_id)
        if ptc is None:
            return
        try:
            pos = await ptc.played_time(chat_id)
        except Exception:
            return
        if pos == last_pos and pos > 0:
            frozen_count += 1
            if frozen_count >= 2:
                LOG.warning(
                    "Stream frozen in %s (native pos stuck at %ds) — auto-next", chat_id, pos,
                )
                try:
                    from MusicLyrics.plugins.play.queue import skip_queue as _sq
                    nxt = await _sq(chat_id, force=True)
                    if nxt:
                        await _try_play_chain(chat_id, nxt)
                except Exception as e:
                    LOG.exception("watchdog auto-next failed: %s", e)
                return
        else:
            frozen_count = 0
            last_pos = pos


async def _start_progress_timer(chat_id: int, duration: int):
    """Register chat for centralized progress updates."""
    global _central_progress_task
    async with _STATE_LOCK:
        _progress_state[chat_id] = {
            "start": time.time(),
            "duration": duration,
            "last_update": 0.0,
        }
        _play_start_times[chat_id] = time.time()
        _play_durations[chat_id] = duration
    if _central_progress_task is None or _central_progress_task.done():
        _central_progress_task = asyncio.create_task(_central_progress_loop())
    try:
        asyncio.create_task(_stream_health_watchdog(chat_id))
    except Exception:
        pass


def _stop_progress_timer(chat_id: int):
    """Unregister chat from progress updates.

    Synchronous because callers (incl. error paths) often run in non-await
    contexts; dict.pop is atomic so this is safe without the state lock.
    """
    _progress_state.pop(chat_id, None)
    _play_start_times.pop(chat_id, None)
    _play_durations.pop(chat_id, None)


def _cleanup_chat_state(chat_id: int) -> None:
    """Atomic cleanup of all per-chat state when a chat goes inactive."""
    _active_chats.discard(chat_id)
    _play_start_times.pop(chat_id, None)
    _play_durations.pop(chat_id, None)
    _last_successful_platform.pop(chat_id, None)
    _skip_locks.pop(chat_id, None)
    _play_locks.pop(chat_id, None)
    _END_HANDLING.pop(chat_id, None)
    _suppress_stream_end.pop(chat_id, None)
    _auto_next_in_progress.discard(chat_id)
    # Drop the warm-up flag so the next session in this chat (after
    # leaving + rejoining the VC) re-applies the first-play audio
    # warm-up. Without this the bug would re-appear on rejoin.
    _warmed_up_chats.discard(chat_id)
    # Cancel any orphan play task we still hold for this chat so it does
    # not linger forever consuming pytgcalls native state.
    t = _orphan_play_tasks.pop(chat_id, None)
    if t is not None and not t.done():
        try:
            t.cancel()
        except Exception:
            pass
    _progress_state.pop(chat_id, None)
    try:
        from MusicLyrics.utils.safe_send import clear_chat_state
        clear_chat_state(chat_id)
    except Exception:
        pass
    # Drop prefetch state (cancels any in-flight prefetch + drains it).
    try:
        from MusicLyrics.plugins.play.prefetch import clear_prefetch_state
        clear_prefetch_state(chat_id)
    except Exception:
        pass


# -- Public API ---

async def stream_audio(
    chat_id: int,
    media_path: str,
    title: str = "",
    duration: int = 0,
    thumbnail: str = "",
    requester: str = "",
    skip_url_check: bool = False,
) -> None:
    """Join voice chat (if needed) and start audio stream.

    media_path can be a local file path or a direct stream URL.
    If streaming a URL fails, automatically downloads the file
    and retries with the local path.

    skip_url_check=True bypasses the 0.6 s HEAD probe — pass True for
    freshly-resolved / prefetched URLs to make /skip near-instant.
    """
    if pytgcalls is None:
        raise RuntimeError("Music streaming is disabled -- STRING_SESSION not configured.")
    _validate_media(media_path)

    # Pre-check stream URL validity to prevent ffprobe/JSONDecodeError crashes
    if _is_url(media_path) and not skip_url_check:
        url_ok = await _check_stream_url(media_path)
        if not url_ok:
            LOG.warning("Stream URL pre-check failed in %s — trying download fallbacks concurrently", chat_id)
            if title:
                # Run all fallbacks CONCURRENTLY — prefer downloads over stream URLs
                async def _fb_youtube():
                    try:
                        from MusicLyrics.plugins.play.platforms.youtube import search_and_download_audio
                        p, _ = await search_and_download_audio(title)
                        if p and os.path.isfile(str(p)):
                            return p
                    except Exception:
                        pass
                    return None

                async def _fb_jiosaavn():
                    try:
                        p, _ = await search_and_download_jiosaavn(title)
                        if p and os.path.isfile(str(p)):
                            return p
                    except Exception:
                        pass
                    return None

                async def _fb_soundcloud():
                    try:
                        p, info = await search_and_download_soundcloud(title)
                        if p and (os.path.isfile(str(p)) or (info and info.get("_is_stream_url"))):
                            return p
                    except Exception:
                        pass
                    return None

                tasks = [
                    asyncio.create_task(_fb_youtube()),
                    asyncio.create_task(_fb_jiosaavn()),
                    asyncio.create_task(_fb_soundcloud()),
                ]
                # Wait for ALL tasks, prefer local file results
                results = await asyncio.gather(*tasks, return_exceptions=True)
                recovered = False
                for result in results:
                    if isinstance(result, str) and result and os.path.isfile(result):
                        try:
                            audio = _make_audio_stream(result)
                            await _do_play(chat_id, audio)
                            _active_chats.add(chat_id)
                            LOG.info("Streaming audio (pre-check download recovery) in %s: %s", chat_id, title)
                            recovered = True
                            break
                        except Exception:
                            pass
                # If no local file, try stream URL results
                if not recovered:
                    for result in results:
                        if isinstance(result, str) and result and not os.path.isfile(result):
                            try:
                                audio = _make_audio_stream(result)
                                await _do_play(chat_id, audio)
                                _active_chats.add(chat_id)
                                LOG.info("Streaming audio (pre-check stream recovery) in %s: %s", chat_id, title)
                                recovered = True
                                break
                            except Exception:
                                pass
                if recovered:
                    return
            raise FileNotFoundError(f"Stream URL expired and all fallbacks failed for: {title or media_path[:80]}")

    try:
        audio = _make_audio_stream(media_path)
        await _do_play(chat_id, audio)
        LOG.info("Streaming audio in %s: %s (%s)",
                 chat_id, title, media_path[:100])
        # Track which platform succeeded for auto-next priority
        if "soundcloud" in media_path.lower() or "sndcdn" in media_path.lower():
            _last_successful_platform[chat_id] = "soundcloud"
        elif "jiosaavn" in media_path.lower() or "saavn" in media_path.lower():
            _last_successful_platform[chat_id] = "jiosaavn"
        # Kick off prefetch for the NEXT queue item — makes skip/auto-next instant
        try:
            asyncio.create_task(prefetch_next(chat_id))
        except Exception:
            pass
    except Exception as exc:
        # If stream URL failed, try all fallback sources CONCURRENTLY
        if _is_url(media_path) and title:
            LOG.warning(
                "%s with stream URL in %s — trying all fallbacks concurrently...",
                type(exc).__name__, chat_id,
            )

            async def _dl_youtube():
                try:
                    from MusicLyrics.plugins.play.platforms.youtube import download_audio, search_and_download_audio
                    # Try direct URL download first
                    try:
                        lp = await download_audio(media_path)
                        if lp and os.path.isfile(str(lp)):
                            return lp
                    except Exception:
                        pass
                    # Try search+download with title
                    lp, _ = await search_and_download_audio(title)
                    if lp and os.path.isfile(str(lp)):
                        return lp
                except Exception:
                    pass
                return None

            async def _dl_jiosaavn():
                try:
                    p, _ = await search_and_download_jiosaavn(title)
                    if p and os.path.isfile(str(p)):
                        return p
                except Exception:
                    pass
                return None

            async def _dl_soundcloud():
                try:
                    p, info = await search_and_download_soundcloud(title)
                    if p:
                        if (info and info.get("_is_stream_url")) or os.path.isfile(str(p)):
                            return p
                except Exception:
                    pass
                return None

            fb_tasks = [
                asyncio.create_task(_dl_youtube()),
                asyncio.create_task(_dl_jiosaavn()),
                asyncio.create_task(_dl_soundcloud()),
            ]
            fb_pending = set(fb_tasks)
            fb_recovered = False
            while fb_pending:
                fb_done, fb_pending = await asyncio.wait(fb_pending, return_when=asyncio.FIRST_COMPLETED)
                for task in fb_done:
                    try:
                        local_path = task.result()
                        if local_path:
                            for p in fb_pending:
                                p.cancel()
                            audio = _make_audio_stream(local_path)
                            await _do_play(chat_id, audio)
                            _active_chats.add(chat_id)
                            LOG.info("Streaming audio (concurrent fallback) in %s: %s", chat_id, title)
                            fb_recovered = True
                            fb_pending = set()
                            break
                    except Exception:
                        pass
            if fb_recovered:
                return

        LOG.exception("Failed to stream audio in %s: %s", chat_id, exc)
        raise


async def stream_video(
    chat_id: int,
    media_path: str,
    title: str = "",
    duration: int = 0,
    thumbnail: str = "",
    requester: str = "",
    skip_url_check: bool = False,
) -> None:
    """Join voice chat (if needed) and start video stream.

    media_path can be a local file path or a direct stream URL.
    If streaming a URL fails, automatically downloads the file
    and retries with the local path.

    skip_url_check=True bypasses the 0.6 s HEAD probe — pass True for
    freshly-resolved / prefetched URLs to make /skip near-instant.
    """
    if pytgcalls is None:
        raise RuntimeError("Music streaming is disabled -- STRING_SESSION not configured.")
    _validate_media(media_path)

    # Pre-check stream URL validity to prevent ffprobe/JSONDecodeError crashes
    if _is_url(media_path) and not skip_url_check:
        url_ok = await _check_stream_url(media_path)
        if not url_ok:
            LOG.warning("Video stream URL pre-check failed in %s — going directly to fallback", chat_id)
            try:
                from MusicLyrics.plugins.play.platforms.youtube import download_video, search_and_download_video
                local_path = None
                if title:
                    local_path, _ = await search_and_download_video(title)
                if local_path and os.path.isfile(str(local_path)):
                    stream = _make_video_stream(local_path)
                    await _do_play(chat_id, stream)
                    _active_chats.add(chat_id)
                    LOG.info("Streaming video (URL pre-check recovery) in %s: %s", chat_id, title)
                    return
            except Exception:
                LOG.debug("Video URL pre-check recovery download failed for %s", chat_id)
            # Try SoundCloud
            if title:
                try:
                    sc_path, sc_info = await search_and_download_soundcloud(title)
                    if sc_path and (os.path.isfile(str(sc_path)) or (sc_info and sc_info.get("_is_stream_url"))):
                        stream = _make_audio_stream(sc_path)
                        await _do_play(chat_id, stream)
                        _active_chats.add(chat_id)
                        LOG.info("Streaming audio via SoundCloud (video pre-check recovery) in %s: %s", chat_id, title)
                        return
                except Exception:
                    pass
            raise FileNotFoundError(f"Video stream URL expired and all fallbacks failed for: {title or media_path[:80]}")

    try:
        stream = _make_video_stream(media_path)
        await _do_play(chat_id, stream)
        _active_chats.add(chat_id)
        LOG.info("Streaming video in %s: %s (%s)",
                 chat_id, title, media_path[:100])
        # Kick off prefetch for the NEXT queue item — makes skip/auto-next instant
        try:
            asyncio.create_task(prefetch_next(chat_id))
        except Exception:
            pass
    except Exception as exc:
        # If stream URL failed, try downloading and playing local file
        if _is_url(media_path):
            LOG.warning(
                "%s with video stream URL in %s — downloading file and retrying...",
                type(exc).__name__, chat_id,
            )
            try:
                from MusicLyrics.plugins.play.platforms.youtube import download_video, search_and_download_video
                local_path = await download_video(media_path)
                if not local_path or not os.path.isfile(str(local_path)):
                    # URL download failed, try search+download with title
                    if title:
                        LOG.info("Video URL download failed, trying search+download for: %s", title)
                        local_path, _ = await search_and_download_video(title)
                if local_path and os.path.isfile(str(local_path)):
                    stream = _make_video_stream(local_path)
                    await _do_play(chat_id, stream)
                    _active_chats.add(chat_id)
                    LOG.info("Streaming video (downloaded) in %s: %s (%s)",
                             chat_id, title, str(local_path)[:100])
                    return
            except Exception as dl_exc:
                LOG.exception("Video download fallback also failed in %s: %s",
                             chat_id, dl_exc)

            # LAST RESORT: SoundCloud fallback for video too (plays audio)
            if title:
                try:
                    LOG.info("All video methods failed, trying SoundCloud fallback for: %s", title)
                    sc_path, sc_info = await search_and_download_soundcloud(title)
                    if sc_path:
                        if sc_info and sc_info.get("_is_stream_url"):
                            stream = _make_audio_stream(sc_path)
                        elif os.path.isfile(str(sc_path)):
                            stream = _make_audio_stream(sc_path)
                        else:
                            stream = None
                        if stream:
                            await _do_play(chat_id, stream)
                            _active_chats.add(chat_id)
                            LOG.info("Streaming audio via SoundCloud (video fallback) in %s: %s (%s)",
                                     chat_id, title, str(sc_path)[:100])
                            return
                except Exception as sc_exc:
                    LOG.exception("SoundCloud video fallback also failed in %s: %s",
                                 chat_id, sc_exc)

        LOG.exception("Failed to stream video in %s: %s", chat_id, exc)
        raise


async def stream_audio_with_image(
    chat_id: int,
    file_path: str,
    image_path: str,
    title: str = "",
) -> None:
    """Stream audio with a static thumbnail image in video chat."""
    if pytgcalls is None:
        raise RuntimeError("Music streaming is disabled -- STRING_SESSION not configured.")
    _validate_media(file_path)
    try:
        stream = _make_audio_stream(file_path)
        await _do_play(chat_id, stream)
        _active_chats.add(chat_id)
        LOG.info("Streaming audio+image in %s: %s", chat_id, title)
    except Exception as exc:
        LOG.exception("Failed to stream audio+image in %s: %s", chat_id, exc)
        raise


async def pause_stream(chat_id: int) -> bool:
    try:
        # py-tgcalls 2.2.x uses pause(), older uses pause_stream()
        if hasattr(pytgcalls, 'pause'):
            await pytgcalls.pause(chat_id)
        else:
            await pytgcalls.pause_stream(chat_id)
        return True
    except Exception:
        LOG.exception("Pause failed: %s", chat_id)
        return False


async def resume_stream(chat_id: int) -> bool:
    try:
        # py-tgcalls 2.2.x uses resume(), older uses resume_stream()
        if hasattr(pytgcalls, 'resume'):
            await pytgcalls.resume(chat_id)
        else:
            await pytgcalls.resume_stream(chat_id)
        return True
    except Exception:
        LOG.exception("Resume failed: %s", chat_id)
        return False


async def seek_stream(chat_id: int, seconds: int) -> bool:
    """Seek is not natively supported by all py-tgcalls versions."""
    try:
        LOG.warning("Seek requested but not natively supported in this version.")
        return False
    except Exception:
        LOG.exception("Seek failed: %s", chat_id)
        return False


async def set_volume(chat_id: int, volume: int) -> bool:
    """Set playback volume (1-200)."""
    volume = max(1, min(200, volume))
    try:
        # py-tgcalls 2.2.x uses change_volume_call(), older uses change_volume()
        if hasattr(pytgcalls, 'change_volume_call'):
            await pytgcalls.change_volume_call(chat_id, volume)
        else:
            await pytgcalls.change_volume(chat_id, volume)
        return True
    except Exception:
        LOG.exception("Volume change failed: %s", chat_id)
        return False


async def _raw_leave_group_call(chat_id: int) -> bool:
    """Last-ditch leave via Telegram raw API.

    When pytgcalls' ``leave_call`` / ``leave_group_call`` keep timing out
    (the most common cause of "bot says it left but is still in VC"), the
    underlying GroupCall on Telegram's side has not actually been left.
    Hitting ``phone.LeaveGroupCall`` directly through the userbot bypasses
    pytgcalls' wedged state and forces the server to drop the participant.

    Returns True on success.
    """
    ub, _ = get_assistant(chat_id)
    if ub is None:
        return False
    try:
        from pyrogram.raw import functions, types as _raw_types  # noqa: F401
    except Exception:
        return False
    try:
        peer = await ub.resolve_peer(chat_id)
    except Exception as e:
        LOG.debug("raw_leave: resolve_peer failed for %s: %s", chat_id, e)
        return False

    # Fetch the InputGroupCall — works for both basic groups and channels
    input_call = None
    try:
        if hasattr(peer, "channel_id"):
            full = await ub.invoke(
                functions.channels.GetFullChannel(channel=peer)
            )
            call = getattr(full.full_chat, "call", None)
            if call is not None:
                input_call = call
        else:
            full = await ub.invoke(
                functions.messages.GetFullChat(chat_id=peer.chat_id)
            )
            call = getattr(full.full_chat, "call", None)
            if call is not None:
                input_call = call
    except Exception as e:
        LOG.debug("raw_leave: GetFull*Chat failed for %s: %s", chat_id, e)
        return False

    if input_call is None:
        LOG.debug("raw_leave: no active group call on %s", chat_id)
        # No active call from the server's POV → effectively already left.
        return True

    try:
        await asyncio.wait_for(
            ub.invoke(
                functions.phone.LeaveGroupCall(call=input_call, source=0)
            ),
            timeout=5.0,
        )
        LOG.info("raw_leave: phone.LeaveGroupCall succeeded for %s", chat_id)
        return True
    except asyncio.TimeoutError:
        LOG.warning("raw_leave: phone.LeaveGroupCall TIMED OUT for %s", chat_id)
        return False
    except Exception as e:
        err = type(e).__name__.lower()
        # "GROUPCALL_JOIN_MISSING" or similar → already out
        if "missing" in err or "notjoined" in err or "groupcall" in err:
            LOG.info("raw_leave: server reports already-left for %s (%s)", chat_id, err)
            return True
        LOG.debug("raw_leave: LeaveGroupCall failed for %s: %s", chat_id, e)
        return False


async def _background_ensure_left(chat_id: int, attempts: int = 6) -> None:
    """Keep retrying leave in the background for chats that wouldn't leave.

    Runs detached so it never blocks the caller — the user already got the
    "leaving voice chat" message; we just need the userbot to actually
    drop out of the call.
    """
    _, ptc = get_assistant(chat_id)
    for i in range(attempts):
        await asyncio.sleep(2.5 * (i + 1))  # 2.5, 5, 7.5, 10, 12.5, 15s
        still_in = False
        try:
            if ptc is not None:
                calls = ptc.calls
                if asyncio.iscoroutine(calls):
                    calls = await calls
                if isinstance(calls, dict) and chat_id in calls:
                    still_in = True
                elif isinstance(calls, (list, set, tuple)) and chat_id in calls:
                    still_in = True
        except Exception:
            pass
        if not still_in and not await _raw_leave_check(chat_id):
            LOG.info("background_ensure_left: %s confirmed out (attempt %d)", chat_id, i + 1)
            return
        LOG.info("background_ensure_left: %s still in VC — retry %d/%d",
                 chat_id, i + 1, attempts)
        if ptc is not None:
            for method_name in ("leave_call", "leave_group_call"):
                fn = getattr(ptc, method_name, None)
                if fn is None:
                    continue
                try:
                    await asyncio.wait_for(fn(chat_id), timeout=4.0)
                    LOG.info("background_ensure_left: %s succeeded via %s", chat_id, method_name)
                    break
                except Exception:
                    continue
        try:
            await _raw_leave_group_call(chat_id)
        except Exception:
            pass


async def _raw_leave_check(chat_id: int) -> bool:
    """Return True if the userbot is still listed as a participant in *chat_id*'s GroupCall."""
    ub, _ = get_assistant(chat_id)
    if ub is None:
        return False
    try:
        from pyrogram.raw import functions
    except Exception:
        return False
    try:
        peer = await ub.resolve_peer(chat_id)
        if hasattr(peer, "channel_id"):
            full = await ub.invoke(functions.channels.GetFullChannel(channel=peer))
        else:
            full = await ub.invoke(functions.messages.GetFullChat(chat_id=peer.chat_id))
        return getattr(full.full_chat, "call", None) is not None
    except Exception:
        return False


async def leave_voice_chat(chat_id: int) -> None:
    """Leave the voice chat and clean up — best-effort, never raises.

    Always attempts every leave method available on pytgcalls regardless of
    our local ``_active_chats`` bookkeeping, because the two can drift out
    of sync (e.g. pytgcalls.play() partially succeeded, or we lost track of
    a leave event).  Cleanup of local state runs unconditionally.
    """
    # Stop progress timer & cancel any pending prefetch immediately
    _stop_progress_timer(chat_id)
    try:
        cancel_prefetch(chat_id)
    except Exception:
        pass

    left = False

    # Issue leave commands aggressively — both APIs, multiple retries.
    # CRITICAL: every call MUST be wrapped with asyncio.wait_for, otherwise
    # a wedged pytgcalls connection hangs leave_call() forever and the
    # entire bot stops responding (no skip, no /play, no leave).
    # 2.5s per method is enough — anything slower means the call is dead
    # and we should fall through to the raw API leave fast.
    LEAVE_METHOD_TIMEOUT = 2.5
    _, ptc = get_assistant(chat_id)
    if ptc is not None:
        for attempt in range(2):
            for method_name in ("leave_call", "leave_group_call"):
                fn = getattr(ptc, method_name, None)
                if fn is None:
                    continue
                try:
                    await asyncio.wait_for(fn(chat_id), timeout=LEAVE_METHOD_TIMEOUT)
                    LOG.info(
                        "leave_voice_chat: %s succeeded for %s (attempt %d)",
                        method_name, chat_id, attempt + 1,
                    )
                    left = True
                    break
                except asyncio.TimeoutError:
                    LOG.warning(
                        "leave_voice_chat: %s TIMED OUT for %s (attempt %d) — moving on",
                        method_name, chat_id, attempt + 1,
                    )
                    continue
                except Exception as e:
                    # "NotInGroupCallError" etc. count as success — we're out.
                    err_name = type(e).__name__.lower()
                    if "notingroup" in err_name or "notjoined" in err_name or "no active call" in str(e).lower():
                        LOG.info(
                            "leave_voice_chat: %s reports %s already not in call for %s",
                            method_name, chat_id, err_name,
                        )
                        left = True
                        break
                    LOG.debug(
                        "leave_voice_chat: %s attempt %d failed for %s: %s",
                        method_name, attempt + 1, chat_id, e,
                    )
            if left:
                break
            if attempt < 1:
                await asyncio.sleep(0.15)

    if not left:
        LOG.warning(
            "leave_voice_chat: pytgcalls leave methods failed for %s — trying raw API",
            chat_id,
        )
        try:
            left = await _raw_leave_group_call(chat_id)
        except Exception as e:
            LOG.debug("leave_voice_chat: raw API leave threw for %s: %s", chat_id, e)

    if not left:
        LOG.error(
            "leave_voice_chat: could NOT leave %s after retries — scheduling background retry",
            chat_id,
        )
        # Don't block the user's command — keep trying in the background so the
        # userbot actually drops out of the VC even if pytgcalls is wedged.
        try:
            asyncio.create_task(_background_ensure_left(chat_id))
        except Exception:
            pass

    # Always clean up state regardless of whether leave succeeded
    _cleanup_chat_state(chat_id)
    await _pop_now_playing(chat_id)
    await clear_queue(chat_id)


def is_active(chat_id: int) -> bool:
    return chat_id in _active_chats


# -- Fresh resolve helper used by skip and auto-next --

async def _fresh_resolve_and_play(chat_id: int, item) -> bool:
    """Try ALL platforms CONCURRENTLY for fastest results.

    YouTube, JioSaavn, and SoundCloud all run at the same time.
    First successful result wins — no waiting for sequential failures.

    Returns True on success, False on failure.  Caller is responsible for
    sending UI messages (Now Playing / error).
    """
    import os as _os

    # ── FAST PATH: use prefetched / initially resolved media if still valid ──
    # This is what makes /skip and auto-next feel instant: when the next item
    # already has a usable media_path (either set during the original /play
    # resolve, or by the background prefetcher), we go straight to streaming
    # instead of re-running search+download.
    #
    # Stream URLs go stale (CDN tokens expire) so we call refresh_item_if_stale
    # first — it returns immediately for fresh URLs / local files and does a
    # quick re-resolve only when needed.
    if await refresh_item_if_stale(item):
        try:
            LOG.info(
                "fresh_resolve FAST PATH for %s: using existing media for '%s'",
                chat_id, item.title,
            )
            if item.stream_type == "video":
                await stream_video(
                    chat_id, item.media_path,
                    title=item.title, duration=item.duration,
                    skip_url_check=True,
                )
            else:
                await stream_audio(
                    chat_id, item.media_path,
                    title=item.title, duration=item.duration,
                    skip_url_check=True,
                )
            return True
        except Exception as fast_exc:
            LOG.warning(
                "FAST PATH failed for '%s' (%s) — falling back to full resolve",
                item.title, fast_exc,
            )
            # Force re-resolve below
            item.media_path = ""
            item.is_stream_url = False

    fresh_path = None
    fresh_is_stream = False

    # ── Run ALL platforms concurrently ──────────────────────────
    async def _try_youtube():
        """Try YouTube: download first (reliable), stream URL as backup."""
        try:
            from MusicLyrics.plugins.play.platforms.youtube import (
                get_audio_stream_url, get_video_stream_url,
                is_youtube_url, search_and_download_audio as yt_search_dl,
                search_and_download_video as yt_search_dl_video,
                search_youtube as _yt_search,
                download_audio as _yt_dl_audio,
            )

            # Priority 1: Download by title (most reliable — no expiring URLs)
            if item.stream_type == "video":
                path, info = await yt_search_dl_video(item.title)
            else:
                path, info = await yt_search_dl(item.title)
            if path and _os.path.isfile(str(path)):
                return path, False, "youtube"

            # Priority 2: Try re-fetch stream URL if we have the original YouTube URL
            if is_youtube_url(item.url):
                if item.stream_type == "video":
                    new_url = await get_video_stream_url(item.url)
                else:
                    new_url = await get_audio_stream_url(item.url)
                if new_url:
                    return new_url, True, "youtube"

            # Priority 3: Try search by title -> get stream URL
            yt_result = await _yt_search(item.title)
            if yt_result and yt_result.get("url"):
                if item.stream_type == "video":
                    new_url = await get_video_stream_url(yt_result["url"])
                else:
                    new_url = await get_audio_stream_url(yt_result["url"])
                if new_url:
                    return new_url, True, "youtube"

        except Exception as e:
            LOG.debug("fresh_resolve: YouTube failed for '%s': %s", item.title, e)
        return None, False, ""

    async def _try_jiosaavn():
        """Try JioSaavn: stream URL first, then download."""
        try:
            from MusicLyrics.plugins.play.platforms.jiosaavn import search_jiosaavn as _js_search
            js_result = await _js_search(item.title)
            if js_result and js_result.get("download_url"):
                return js_result["download_url"], True, "jiosaavn"
        except Exception:
            pass
        try:
            js_path, js_info = await search_and_download_jiosaavn(item.title)
            if js_path and _os.path.isfile(str(js_path)):
                return js_path, False, "jiosaavn"
        except Exception:
            pass
        return None, False, ""

    async def _try_soundcloud():
        """Try SoundCloud."""
        try:
            sc_path, sc_info = await search_and_download_soundcloud(item.title)
            if sc_path:
                if sc_info and sc_info.get("_is_stream_url"):
                    return sc_path, True, "soundcloud"
                if _os.path.isfile(str(sc_path)):
                    return sc_path, False, "soundcloud"
        except Exception:
            pass
        return None, False, ""

    LOG.info("fresh_resolve: trying ALL platforms concurrently for %s: '%s'", chat_id, item.title)

    tasks = {
        asyncio.create_task(_try_youtube()): "youtube",
        asyncio.create_task(_try_jiosaavn()): "jiosaavn",
        asyncio.create_task(_try_soundcloud()): "soundcloud",
    }

    pending = set(tasks.keys())
    while pending:
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            try:
                result_path, result_stream, platform = task.result()
                if result_path:
                    fresh_path = result_path
                    fresh_is_stream = result_stream
                    _last_successful_platform[chat_id] = platform
                    LOG.info("fresh_resolve: %s succeeded for '%s'", platform, item.title)
                    for p in pending:
                        p.cancel()
                    pending = set()
                    break
            except Exception:
                pass

    if not fresh_path:
        LOG.error("All platforms failed for fresh_resolve_and_play: %s", item.title)
        return False

    item.media_path = fresh_path
    item.is_stream_url = fresh_is_stream

    if item.stream_type == "video":
        await stream_video(chat_id, item.media_path, title=item.title, duration=item.duration, skip_url_check=True)
    else:
        await stream_audio(chat_id, item.media_path, title=item.title, duration=item.duration, skip_url_check=True)

    return True


async def _ensure_assistant_in_vc(chat_id: int) -> None:
    """Strong-arm rejoin: make absolutely sure the assistant is back in the
    voice chat before the next ``_do_play`` attempt.

    The auto-next / chain-play path can leave the assistant outside the VC
    when ``_do_play`` did a hard reset (raw-API leave) after a wedged
    pytgcalls.play().  Without an explicit rejoin, the next play attempt
    has to wait for the slow auto-join chain inside ``_do_play``, and may
    even timeout entirely — leaving the bot "stuck outside the VC and not
    playing the next song".

    Steps performed (in order — each is best-effort, failures are logged
    and ignored so we always reach pre_join_vc):

    1. Discard stale ``_active_chats`` bookkeeping.
    2. Drop the stale suppression bucket (otherwise the NEXT play's first
       real stream-end is silently swallowed).
    3. Tell pytgcalls to leave the call — clears its NATIVE C-extension
       call state for this chat so the next ``play()`` starts fresh.
       Without this, pytgcalls keeps thinking the assistant is in a call
       and the next play() inherits a wedged WebRTC session.
    4. Run pre_join_vc so the assistant is a member of the group.
    """
    ub, ptc = _assistant_for_chat(chat_id)
    if ptc is None or ub is None:
        return

    # 1 + 2: drop stale bookkeeping so the next play() doesn't suppress
    # the new track's first real stream-end and pre_join_vc actually runs.
    _active_chats.discard(chat_id)
    _suppress_stream_end.pop(chat_id, None)

    # 3: clear pytgcalls' native call state for this chat.  Try both
    # pytgcalls leave methods AND the raw API leave so a wedged pytgcalls
    # connection cannot keep us "stuck in a phantom call".
    for method_name in ("leave_call", "leave_group_call"):
        fn = getattr(ptc, method_name, None)
        if fn is None:
            continue
        try:
            await asyncio.wait_for(fn(chat_id), timeout=2.0)
            LOG.debug("_ensure_assistant_in_vc: %s ok for %s", method_name, chat_id)
            break
        except Exception as e:
            LOG.debug("_ensure_assistant_in_vc: %s failed for %s: %s",
                      method_name, chat_id, e)
    # Raw-API leave as belt-and-suspenders — succeeds even if pytgcalls is wedged.
    try:
        await _raw_leave_group_call(chat_id)
    except Exception as e:
        LOG.debug("_ensure_assistant_in_vc: raw leave failed for %s: %s", chat_id, e)

    # Give Telegram a beat to register the leave before we ask to rejoin.
    await asyncio.sleep(0.3)

    # 4: ensure assistant is actually a member of the group.
    try:
        await asyncio.wait_for(pre_join_vc(chat_id), timeout=8.0)
    except asyncio.TimeoutError:
        LOG.warning("_ensure_assistant_in_vc: pre_join_vc timed out for %s", chat_id)
    except Exception as e:
        LOG.debug("_ensure_assistant_in_vc: pre_join_vc failed for %s: %s", chat_id, e)


# -- Auto-recovering chain player --------------------------------------------

async def _try_play_chain(chat_id: int, first_item, max_attempts: int = 5):
    """Try playing queue items one after another until one succeeds.

    Starts with *first_item* (already popped from the queue by the caller).
    On failure, pops the next item from the queue and tries that one,
    repeating until either an item starts playing OR the queue is fully
    exhausted.  ``max_attempts`` is a hard safety ceiling so a runaway
    queue can never spin forever, but in practice the chain stops as
    soon as the queue is empty.

    Default lowered from 25 → 5: each attempt fans out to 4-5 platform
    fallbacks internally (YouTube → JioSaavn → Piped → Invidious →
    SoundCloud), so 25 attempts produced 100+ blocking network calls
    and exhausted the executor thread pool.  5 is plenty in practice.

    Crucially, every attempt routes through
    ``_fresh_resolve_and_play → stream_audio/_video → _do_play``.  Between
    failed attempts we ALSO call ``_ensure_assistant_in_vc`` to rejoin
    the voice chat explicitly — without that explicit rejoin, the chain
    occasionally leaves the bot sitting outside the VC because a prior
    ``_do_play`` did a raw-API leave after a wedged ``pytgcalls.play()``.
    The explicit rejoin guarantees the very next attempt streams into
    a live VC connection instead of stalling.

    Per-attempt wall-clock budget is bounded by ``ATTEMPT_TIMEOUT`` so
    one bad track can never block the whole chain for more than a few
    seconds.

    Returns the QueueItem that successfully started playing, or ``None``
    if the queue is exhausted / all attempts failed.  Only when this
    function returns ``None`` should the caller leave the VC — and even
    then only because there genuinely are no more songs to try.
    """
    from MusicLyrics.plugins.play.queue import skip_queue as _sq  # avoid circular import

    # Per-attempt wall-clock cap.  Lower than before (was 35s) so a single
    # bad track can't hang the skip pipeline; 22s comfortably covers a
    # healthy resolve+play even on slow networks.
    ATTEMPT_TIMEOUT = 22.0

    item = first_item
    attempt = 0
    while item is not None and attempt < max_attempts:
        attempt += 1
        title = getattr(item, "title", "?")

        # Brief breath between attempts so we don't hammer the network
        # / the executor pool with back-to-back fallback chains.
        if attempt > 1:
            await asyncio.sleep(0.5)

        # Between FAILED attempts (attempt 2+), strong-arm the rejoin so
        # the next _do_play starts from a clean call state.  The first
        # attempt skips this for SPEED — healthy auto-next / skip needs
        # to be near-instant, and _do_play already handles its own
        # reset+rejoin internally if the play() call wedges.
        if attempt > 1:
            try:
                await _ensure_assistant_in_vc(chat_id)
            except Exception as e:
                LOG.debug("_try_play_chain: rejoin between attempts failed: %s", e)

        try:
            success = await asyncio.wait_for(
                _fresh_resolve_and_play(chat_id, item),
                timeout=ATTEMPT_TIMEOUT,
            )
        except asyncio.TimeoutError:
            LOG.warning(
                "_try_play_chain: resolve+play TIMED OUT for '%s' in %s (attempt %d/%d)",
                title, chat_id, attempt, max_attempts,
            )
            success = False
        except Exception as e:
            LOG.warning(
                "_try_play_chain: resolve+play raised for '%s' in %s (attempt %d/%d): %s",
                title, chat_id, attempt, max_attempts, e,
            )
            success = False

        if success:
            # New track is playing — start prefetching the FOLLOWING item so
            # the next /skip or auto-next is also instant.
            try:
                asyncio.create_task(prefetch_next(chat_id))
            except Exception:
                pass
            return item

        LOG.info(
            "_try_play_chain: '%s' failed in %s — popping next item and retrying "
            "(attempt %d/%d, will rejoin VC before next attempt)",
            title, chat_id, attempt, max_attempts,
        )
        # Pop next item from queue and try it
        try:
            item = await _sq(chat_id, force=True)
        except Exception as e:
            LOG.debug("_try_play_chain: skip_queue failed: %s", e)
            item = None

    return None


# -- Stream-end callback ---

async def _on_stream_end(client, update):
    """When current track ends, play next in queue or leave."""
    chat_id = None

    try:
        # Try various ways to get chat_id from the update object
        if isinstance(update, int):
            chat_id = update
        elif hasattr(update, "chat_id"):
            _cid = update.chat_id
            # Handle property that might return a coroutine
            if asyncio.iscoroutine(_cid):
                _cid = await _cid
            chat_id = _cid
        elif hasattr(update, "chat"):
            chat_obj = update.chat
            if asyncio.iscoroutine(chat_obj):
                chat_obj = await chat_obj
            if isinstance(chat_obj, dict):
                chat_id = chat_obj.get("id")
            elif isinstance(chat_obj, int):
                chat_id = chat_obj
            elif hasattr(chat_obj, "id"):
                chat_id = chat_obj.id
        elif isinstance(update, dict):
            chat_id = update.get("chat_id") or update.get("chat", {}).get("id")
    except TypeError:
        # Handle "abstract ... can't be used in 'await' expression"
        # by trying direct attribute access without await
        try:
            if hasattr(update, "chat_id"):
                chat_id = update.chat_id
            elif hasattr(update, "chat"):
                chat_obj = update.chat
                if isinstance(chat_obj, int):
                    chat_id = chat_obj
                elif hasattr(chat_obj, "id"):
                    chat_id = chat_obj.id
        except Exception:
            pass
    except Exception as e:
        LOG.warning("Error extracting chat_id from stream end event: %s", e)
        return

    if chat_id is None:
        LOG.warning("Stream end event with unknown chat_id: %s (type: %s)", update, type(update).__name__)
        return

    LOG.info("Stream end event for chat %s", chat_id)

    # Prevent double-fire: py-tgcalls + the fallback timer can both
    # deliver an end-event for the same track in rapid succession.
    # Without this guard the queue advances twice and songs get skipped.
    if _END_HANDLING.get(chat_id):
        LOG.debug("Stream-end already being handled for %s, ignoring duplicate", chat_id)
        return
    _END_HANDLING[chat_id] = True

    try:
        # Suppress events caused by manual skip/stop replacing the current stream.
        # _do_play triggers StreamAudioEnded for the OLD stream — swallow it here.
        if _consume_suppression(chat_id):
            LOG.info("Suppressed stream-end event for %s (TTL-based)", chat_id)
            return

        # This is a REAL stream-end (track finished naturally).
        # Defensive: clear any stale suppression bucket so no future event is swallowed.
        _suppress_stream_end.pop(chat_id, None)

        # Prevent double-processing: if auto-next is already running for this chat, skip
        if chat_id in _auto_next_in_progress:
            LOG.info("Auto-next already in progress for %s — ignoring duplicate stream-end event", chat_id)
            return

        _auto_next_in_progress.add(chat_id)
        try:
            # Acquire per-chat skip lock — waits if manual skip/stop is in progress.
            # Raises RuntimeError if a previous play() is still in flight; in that
            # case the in-flight call will produce its own stream-end so we can
            # safely skip handling this one.
            try:
                lock = await acquire_skip_lock(chat_id, timeout=15.0)
            except RuntimeError:
                LOG.warning(
                    "auto-next: skip_lock busy for %s — deferring to in-flight op",
                    chat_id,
                )
                return

            # ── Phase A: state mutations under the skip lock (FAST) ─────────
            # Anything slow (downloads, network) MUST happen OUTSIDE the lock.
            # Holding the skip lock during _try_play_chain (which can run for
            # 15+ s of yt-dlp / JioSaavn / SoundCloud network I/O) starves
            # user /skip commands and causes a cascade of acquire_skip_lock
            # timeouts that crash the bot at scale.
            next_item = None
            queue_was_empty_before = False
            finished_title = "Unknown"
            finished_requester = ""
            try:
                # Re-check if chat is still active (may have been stopped while waiting for lock)
                if chat_id not in _active_chats:
                    LOG.info("Chat %s no longer active, skipping auto-next", chat_id)
                    return

                # Stop the progress timer
                _stop_progress_timer(chat_id)

                # Get the finished track info BEFORE cleaning up
                finished = await get_current(chat_id)
                finished_title = finished.title if finished else "Unknown"
                finished_requester = finished.requester if finished else ""

                # Determine next item FIRST so we can tell loop mode from advance.
                next_item = await skip_queue(chat_id, force=False)

                # Clean up the finished track's file ONLY if we're actually
                # advancing to a different item.  In loop mode skip_queue
                # returns the SAME item — deleting its file before replay
                # would break the loop.
                is_loop_replay = (
                    finished is not None
                    and next_item is not None
                    and finished is next_item
                )
                if (
                    finished
                    and not finished.is_stream_url
                    and finished.media_path
                    and not is_loop_replay
                ):
                    cleanup(finished.media_path)

                # Delete previous "Now Playing" / thumbnail messages (thread-safe)
                old_msgs = await _pop_now_playing(chat_id)
                for old_msg in old_msgs:
                    try:
                        await old_msg.delete()
                        LOG.debug("Deleted previous Now Playing message in %s", chat_id)
                    except Exception:
                        pass

                if next_item is None:
                    queue_was_empty_before = True
                else:
                    # Remove from _active_chats so _do_play does NOT add a false
                    # suppress_next_stream_end (the old stream already ended naturally).
                    _active_chats.discard(chat_id)
            finally:
                # Release the skip lock NOW — before any slow network I/O.
                try:
                    lock.release()
                except Exception:
                    pass

            # ── Phase B: slow work OUTSIDE the skip lock ────────────────────
            if queue_was_empty_before:
                # Queue is empty — send "song ended" message with add-to-group button
                try:
                    t = _get_current_theme()
                    finish_msg = await bot.send_message(
                        chat_id,
                        f"▸ **ꜱᴏɴɢ ᴇɴᴅᴇᴅ** ✅\n\n"
                        f"{t['title_icon']} **ꜱʜᴇꜱʜ ɢᴀᴀɴ:** {finished_title}\n"
                        f"👤 **ꜱʜᴜɴɪʏᴇᴄʜɪʟᴇɴ:** {finished_requester}\n\n"
                        f"🔄 আবার গান শুনতে `/play` কমান্ড দিন।\n\n"
                        f"🦋 ✦ᴘᴏᴡєʀєᴅ ʙʏ » ── [@R4J_81](https://t.me/R4J_81)",
                        reply_markup=_song_ended_keyboard(),
                    )
                    await _add_reaction(chat_id, finish_msg.id)
                except Exception:
                    pass
                # Now leave the voice chat
                await leave_voice_chat(chat_id)
                LOG.info("Queue empty, left voice chat in %s", chat_id)
                return

            try:
                # 5 attempts is plenty — 25 was overkill and exhausted the
                # extractor pool (each attempt fans out to 4-5 platforms).
                played = await _try_play_chain(chat_id, next_item, max_attempts=5)

                if played is None:
                    try:
                        err_msg = await bot.send_message(
                            chat_id,
                            "❌ **Queue শেষ — পরের কোনো গান চালানো যায়নি।**\n\n"
                            "Voice chat থেকে বের হচ্ছি। আবার `/play` দিন।",
                        )
                        await _add_reaction(chat_id, err_msg.id)
                    except Exception:
                        pass
                    await leave_voice_chat(chat_id)
                    return

                # The item that actually started playing may not be the
                # first one we tried — use the returned item for UI.
                next_item = played

                dur = format_duration(next_item.duration)
                color = _get_next_color()

                # Start progress timer for the new track
                await _start_progress_timer(chat_id, next_item.duration)

                t = _get_current_theme()
                np_msg = await bot.send_message(
                    chat_id,
                    f"{t['header']} **ᴘʟᴀʏʙᴀᴄᴋ ᴀᴄᴛɪᴠᴀᴛᴇᴅ | ᴇɴᴊᴏʏ ᴛʜᴇ ᴍᴜꜱɪᴄ**\n\n"
                    f"> {t['title_icon']}  **ᴛɪᴛʟᴇ :** [{next_item.title}]({next_item.url})\n"
                    f"> {t['dur_icon']}  **ᴅᴜʀᴀᴛɪᴏɴ :** {dur}\n"
                    f"> 👤  **ʀᴇǫᴜᴇꜱᴛᴇᴅ :** {next_item.requester}"
                    f"\n\n🦋 ✦ᴘᴏᴡєʀєᴅ ʙʏ » ── [@R4J_81](https://t.me/R4J_81)",
                    reply_markup=_control_keyboard(color),
                )
                # Add reaction to the now playing message
                await _add_reaction(chat_id, np_msg.id)
                # Track this message (thread-safe)
                await _add_now_playing(chat_id, np_msg)
            except Exception:
                LOG.exception("Failed to play next in queue for %s", chat_id)
                # Send error message before leaving
                try:
                    err_msg = await bot.send_message(
                        chat_id,
                        f"❌ **পরের গানটি চলানো যায়নি:** {next_item.title}\n\n"
                        "Voice chat থেকে বের হচ্ছে। আবার `/play` দিন।",
                    )
                    await _add_reaction(chat_id, err_msg.id)
                except Exception:
                    pass
                await leave_voice_chat(chat_id)
        finally:
            _auto_next_in_progress.discard(chat_id)
    finally:
        _END_HANDLING.pop(chat_id, None)


# Register the stream-end callback with compatibility for multiple py-tgcalls versions
if pytgcalls is not None:
    _registered = False

    # Wrapper that handles both 1-arg and 2-arg callback signatures
    # and swallows TypeError from py-tgcalls internal issues
    async def _safe_stream_end_1arg(update):
        """1-arg handler for newer py-tgcalls versions."""
        try:
            await _on_stream_end(None, update)
        except TypeError as e:
            # Handle "abstract stream.end can't be used in 'await' expression"
            LOG.debug("TypeError in stream-end handler (handled): %s", e)
            # Try extracting chat_id directly without async
            try:
                cid = getattr(update, 'chat_id', None)
                if cid is not None:
                    await _on_stream_end(None, cid)
            except Exception:
                pass
        except Exception as e:
            LOG.exception("Error in _safe_stream_end_1arg: %s", e)

    async def _safe_stream_end_2arg(client, update):
        """2-arg handler for older py-tgcalls versions."""
        try:
            await _on_stream_end(client, update)
        except TypeError as e:
            LOG.debug("TypeError in stream-end handler (handled): %s", e)
            try:
                cid = getattr(update, 'chat_id', None)
                if cid is not None:
                    await _on_stream_end(client, cid)
            except Exception:
                pass
        except Exception as e:
            LOG.exception("Error in _safe_stream_end_2arg: %s", e)

    # Method 1: pytgcalls.on_update with filters.stream_end (py-tgcalls >= 2.1)
    if not _registered:
        try:
            from pytgcalls import filters as _ptg_filters
            if hasattr(_ptg_filters, "stream_end"):
                # Try 1-arg signature first (newer py-tgcalls)
                try:
                    @pytgcalls.on_update(_ptg_filters.stream_end)
                    async def _stream_end_handler_1(update):
                        await _safe_stream_end_1arg(update)
                    _registered = True
                    LOG.info("Stream-end callback registered via filters.stream_end (1-arg)")
                except TypeError:
                    # Try 2-arg signature
                    @pytgcalls.on_update(_ptg_filters.stream_end)
                    async def _stream_end_handler_2(client, update):
                        await _safe_stream_end_2arg(client, update)
                    _registered = True
                    LOG.info("Stream-end callback registered via filters.stream_end (2-arg)")
        except (ImportError, AttributeError, TypeError) as e:
            LOG.debug("Method 1 (filters.stream_end) failed: %s", e)

    # Method 2: pytgcalls.on_stream_end decorator
    if not _registered:
        try:
            if hasattr(pytgcalls, "on_stream_end"):
                try:
                    @pytgcalls.on_stream_end()
                    async def _stream_end_handler3(update):
                        await _safe_stream_end_1arg(update)
                    _registered = True
                    LOG.info("Stream-end callback registered via on_stream_end() (1-arg)")
                except TypeError:
                    @pytgcalls.on_stream_end()
                    async def _stream_end_handler4(client, update):
                        await _safe_stream_end_2arg(client, update)
                    _registered = True
                    LOG.info("Stream-end callback registered via on_stream_end() (2-arg)")
        except (AttributeError, TypeError) as e:
            LOG.debug("Method 2 (on_stream_end) failed: %s", e)

    # Method 3: pytgcalls.on_closed_voice_chat
    if not _registered:
        try:
            if hasattr(pytgcalls, "on_closed_voice_chat"):
                try:
                    @pytgcalls.on_closed_voice_chat()
                    async def _stream_end_handler5(update):
                        await _safe_stream_end_1arg(update)
                    _registered = True
                except TypeError:
                    @pytgcalls.on_closed_voice_chat()
                    async def _stream_end_handler6(client, update):
                        await _safe_stream_end_2arg(client, update)
                    _registered = True
                LOG.info("Stream-end callback registered via on_closed_voice_chat()")
        except (AttributeError, TypeError) as e:
            LOG.debug("Method 3 (on_closed_voice_chat) failed: %s", e)

    # Method 4: py-tgcalls >= 2.1 raw on_update without filter
    if not _registered:
        try:
            @pytgcalls.on_update()
            async def _raw_update_handler(client_or_update, update_or_none=None):
                # Handle both 1-arg and 2-arg signatures
                if update_or_none is not None:
                    update = update_or_none
                else:
                    update = client_or_update
                try:
                    update_type = type(update).__name__.lower()
                    if update_type in ("streamaudioended", "streamvideoended", "streamended", "stream_end"):
                        await _on_stream_end(None, update)
                    elif "end" in update_type and ("stream" in update_type or "audio" in update_type):
                        await _on_stream_end(None, update)
                except Exception as e:
                    LOG.exception("Error in raw_update_handler: %s", e)
            _registered = True
            LOG.info("Stream-end callback registered via raw pytgcalls.on_update()")
        except (AttributeError, TypeError) as e:
            LOG.debug("Method 4 (raw on_update) failed: %s", e)

    if not _registered:
        LOG.warning("Could not register stream-end callback -- timer fallback will handle it.")

    # ALWAYS enable timer-based stream-end detection as a safety net.
    # Even when the callback IS registered, it may never fire if the
    # stream URL is broken (e.g. HLS manifest, expired CDN URL) — in
    # that case the bot stays in VC forever.  This timer catches those
    # cases and triggers auto-next or leave after duration + buffer.
    async def _fallback_stream_end_checker():
        """Periodically check if tracks have finished playing.

        Triggers _on_stream_end when:
        * a track has been playing past its duration + 10 s buffer, OR
        * a track with unknown duration (0) has been playing for > 15 min
          (safety cap — prevents the bot sitting forever in VC if the
          stream-end event never fires for HLS / live URLs).
        """
        UNKNOWN_DURATION_CAP_SEC = 8 * 60  # 8 minutes hard ceiling
        while True:
            await asyncio.sleep(3)  # Check every 3 seconds
            try:
                # Import locally to avoid circular import at module load
                from MusicLyrics.plugins.play.queue import get_chat_queue as _gcq
                for chat_id in list(_active_chats):
                    if chat_id not in _play_start_times:
                        continue
                    elapsed = time.time() - _play_start_times[chat_id]
                    duration = _play_durations.get(chat_id, 0) or 0
                    if duration > 0 and elapsed > duration + 10:
                        LOG.info(
                            "Fallback timer: end-of-track for %s (elapsed=%.0f, duration=%d)",
                            chat_id, elapsed, duration,
                        )
                        await _on_stream_end(None, chat_id)
                    elif duration <= 0 and elapsed > UNKNOWN_DURATION_CAP_SEC:
                        LOG.warning(
                            "Fallback timer: unknown-duration safety cap hit for %s (elapsed=%.0f)",
                            chat_id, elapsed,
                        )
                        await _on_stream_end(None, chat_id)
            except Exception as e:
                LOG.debug("Fallback stream-end checker error: %s", e)

    asyncio.get_event_loop().create_task(_fallback_stream_end_checker())

    # ── Safety net: periodically ensure we have not been left sitting in VC ──
    # If _active_chats holds a chat but its queue is empty AND no progress
    # timer is running (no track playing), force a leave.  We also probe
    # pytgcalls.calls directly so chats that drifted out of our local
    # bookkeeping still get cleaned up.
    async def _vc_orphan_reaper():
        from MusicLyrics.plugins.play.queue import get_chat_queue as _gcq
        while True:
            await asyncio.sleep(5)
            try:
                # Build a union: chats we *think* are active + chats pytgcalls
                # thinks are active.  This catches drift in either direction.
                suspects: set[int] = set(_active_chats)
                try:
                    calls = pytgcalls.calls
                    if asyncio.iscoroutine(calls):
                        calls = await calls
                    if isinstance(calls, (list, set, tuple)):
                        suspects.update(int(c) for c in calls if isinstance(c, (int, str)))
                    elif isinstance(calls, dict):
                        suspects.update(int(c) for c in calls.keys())
                except Exception:
                    pass

                for chat_id in list(suspects):
                    cq = await _gcq(chat_id)
                    # Orphan = no queue items AND no playback progress timer
                    if not cq.items and chat_id not in _play_start_times:
                        LOG.warning(
                            "VC orphan detected for %s — queue empty, no playback; leaving",
                            chat_id,
                        )
                        try:
                            await leave_voice_chat(chat_id)
                        except Exception as le:
                            LOG.debug("Orphan leave failed for %s: %s", chat_id, le)
            except Exception as e:
                LOG.debug("VC orphan reaper error: %s", e)

    asyncio.get_event_loop().create_task(_vc_orphan_reaper())

    # ── Watchdog: age out wedged _auto_next_in_progress flags ──
    # If anything inside _on_stream_end's lock body crashed before the
    # `finally` ran, the chat would be marked auto-next forever and all
    # subsequent natural stream-end events would be silently dropped.
    # This watchdog drops entries older than the limit.
    async def _auto_next_watchdog():
        AGE_LIMIT_SEC = 45.0
        seen_at: dict[int, float] = {}
        while True:
            await asyncio.sleep(10)
            try:
                now = time.time()
                # Stamp newly-seen chats; clear stamps for chats no longer in the set.
                for cid in list(_auto_next_in_progress):
                    seen_at.setdefault(cid, now)
                for cid in list(seen_at):
                    if cid not in _auto_next_in_progress:
                        seen_at.pop(cid, None)
                        continue
                    if now - seen_at[cid] > AGE_LIMIT_SEC:
                        LOG.warning(
                            "auto_next watchdog: clearing stuck flag for %s (age=%.0fs)",
                            cid, now - seen_at[cid],
                        )
                        _auto_next_in_progress.discard(cid)
                        seen_at.pop(cid, None)
            except Exception as e:
                LOG.debug("auto_next watchdog error: %s", e)

    asyncio.get_event_loop().create_task(_auto_next_watchdog())

    # ── ALSO register on_kicked / on_left to clean up ──
    try:
        if hasattr(pytgcalls, "on_kicked"):
            @pytgcalls.on_kicked()
            async def _on_kicked(client, chat_id: int):
                LOG.info("Userbot kicked from voice chat in %s — cleaning up", chat_id)
                _stop_progress_timer(chat_id)
                _active_chats.discard(chat_id)
                await _pop_now_playing(chat_id)
                _auto_next_in_progress.discard(chat_id)
                await clear_queue(chat_id)
    except (AttributeError, TypeError) as e:
        LOG.debug("on_kicked registration failed: %s", e)

    # ── Register on_group_call_left for when the userbot leaves/is removed ──
    try:
        from pytgcalls import filters as _ptg_filters2
        if hasattr(_ptg_filters2, "left"):
            @pytgcalls.on_update(_ptg_filters2.left)
            async def _on_left_handler(client, update):
                left_chat = None
                if hasattr(update, "chat_id"):
                    left_chat = update.chat_id
                elif isinstance(update, int):
                    left_chat = update
                if left_chat:
                    LOG.info("Userbot left voice chat in %s — cleaning up", left_chat)
                    _stop_progress_timer(left_chat)
                    _active_chats.discard(left_chat)
                    await _pop_now_playing(left_chat)
                    _auto_next_in_progress.discard(left_chat)
                    await clear_queue(left_chat)
            LOG.info("Left voice chat handler registered via pytgcalls.filters.left")
    except (ImportError, AttributeError, TypeError) as e:
        LOG.debug("Left filter registration failed: %s", e)
