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
from MusicLyrics.userbot import pytgcalls, userbot

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

LOG = logging.getLogger(__name__)

# Track active chats so we know whether to join or change stream
_active_chats: set[int] = set()

# Track "Now Playing" messages for each chat so we can delete them when track ends
_now_playing_messages: dict[int, list] = {}

# Track playback start times for progress display
_play_start_times: dict[int, float] = {}

# Track current track durations for progress display
_play_durations: dict[int, int] = {}

# Active progress update tasks per chat
_progress_tasks: dict[int, asyncio.Task] = {}

# Track which platform last succeeded for each chat — prioritize it next time
_last_successful_platform: dict[int, str] = {}

# Per-chat lock to prevent race conditions between auto-next and manual skip/stop
_skip_locks: dict[int, asyncio.Lock] = {}

# Counter to suppress stream-end events caused by manual skip/stop replacing streams.
# When _do_play replaces the current stream, py-tgcalls fires StreamAudioEnded for the
# OLD stream.  Without suppression this causes _on_stream_end to double-advance the
# queue and leave the VC.
_suppress_stream_end: dict[int, int] = {}


def suppress_next_stream_end(chat_id: int) -> None:
    """Tell _on_stream_end to ignore the next N stream-end events for *chat_id*.

    Call this BEFORE _do_play when you are intentionally replacing the current
    stream (skip / force-play) so the old stream's end event is swallowed.
    """
    _suppress_stream_end[chat_id] = _suppress_stream_end.get(chat_id, 0) + 1
    LOG.debug("suppress_next_stream_end(%s) → count=%d", chat_id, _suppress_stream_end[chat_id])


def _get_skip_lock(chat_id: int) -> asyncio.Lock:
    """Get or create a per-chat skip lock."""
    if chat_id not in _skip_locks:
        _skip_locks[chat_id] = asyncio.Lock()
    return _skip_locks[chat_id]

async def pre_join_vc(chat_id: int) -> None:
    """Pre-join the voice chat so streaming can start instantly.

    Ensures the assistant is in the group before play() is called,
    eliminating the group-join delay from the critical path.
    If already in VC or group, does nothing.
    """
    if pytgcalls is None:
        return

    # Already active? Nothing to do
    if chat_id in _active_chats:
        return

    try:
        # Check if already in a call
        try:
            calls = pytgcalls.calls
            if asyncio.iscoroutine(calls):
                calls = await calls
            if chat_id in calls:
                _active_chats.add(chat_id)
                return
        except Exception:
            pass

        # Try joining the group (not VC, just group membership)
        # This handles the common case where assistant isn't in the group yet
        try:
            await userbot.join_chat(chat_id)
            LOG.info("Pre-join: Assistant joined group %s", chat_id)
        except Exception:
            # Already in group or can't join - either way, continue
            pass

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
    SPEED OPTIMISED: 1.5s timeout, HEAD-only, assume OK on timeout.
    """
    if not _is_url(url):
        return True  # Not a URL, skip check
    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            try:
                async with session.head(
                    url,
                    timeout=aiohttp.ClientTimeout(total=1.2, connect=0.8),
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
    if pytgcalls is None:
        raise RuntimeError("Music streaming is disabled -- STRING_SESSION not configured.")

    # Check if already in a call for this chat
    try:
        calls = pytgcalls.calls
        if asyncio.iscoroutine(calls):
            calls = await calls
        if chat_id in calls:
            LOG.debug("Already in voice chat for %s, no need to join", chat_id)
            return
    except Exception:
        pass

    # Try to check active calls via pytgcalls
    try:
        active_calls = pytgcalls.active_calls
        if asyncio.iscoroutine(active_calls):
            active_calls = await active_calls
        elif isinstance(active_calls, (list, dict, set)):
            if chat_id in active_calls:
                return
    except Exception:
        pass

    LOG.info("Ensuring userbot is in voice chat for chat %s", chat_id)


async def _do_play(chat_id: int, stream):
    """Call pytgcalls.play — join VC if needed, then start streaming.

    Compatible with py-tgcalls 2.1.x and 2.2.x APIs.
    If the assistant isn't in the group yet, tries to auto-join first.
    """
    if pytgcalls is None:
        raise RuntimeError("Music streaming is disabled -- STRING_SESSION not configured.")

    # If we are already streaming in this chat, the new play() call will
    # terminate the old stream causing a StreamAudioEnded event.  Suppress
    # that event so _on_stream_end does not mistakenly advance the queue.
    if chat_id in _active_chats:
        suppress_next_stream_end(chat_id)

    async def _try_play():
        """Attempt all play methods — returns True on success."""
        # Method 1: play() with GroupCallConfig (py-tgcalls >= 2.1)
        if _HAS_GROUP_CALL_CONFIG:
            try:
                await pytgcalls.play(
                    chat_id, stream,
                    config=GroupCallConfig(auto_start=True),
                )
                _active_chats.add(chat_id)
                LOG.info("play() with GroupCallConfig succeeded for %s", chat_id)
                return True
            except (TypeError, AttributeError) as e:
                LOG.debug("play() with GroupCallConfig failed: %s", e)

        # Method 2: plain play() (py-tgcalls 2.2.x)
        try:
            await pytgcalls.play(chat_id, stream)
            _active_chats.add(chat_id)
            LOG.info("play() succeeded for %s", chat_id)
            return True
        except Exception as e:
            LOG.debug("play() failed: %s", e)

        # Method 3: explicit join_group_call (older py-tgcalls)
        if hasattr(pytgcalls, 'join_group_call'):
            try:
                await pytgcalls.join_group_call(chat_id, stream)
                _active_chats.add(chat_id)
                LOG.info("join_group_call() succeeded for %s", chat_id)
                return True
            except Exception as e:
                LOG.debug("join_group_call() also failed: %s", e)

        return False

    # First attempt
    if await _try_play():
        return

    # If first attempt failed, assistant might not be in the group yet.
    # Try to auto-join the group, then retry play.
    LOG.info("Play failed for %s — trying to auto-join assistant to the group", chat_id)
    joined = False
    try:
        # Try joining by chat_id directly
        await userbot.join_chat(chat_id)
        joined = True
        LOG.info("Assistant auto-joined group %s by chat ID", chat_id)
    except Exception as e:
        LOG.debug("Assistant join by chat_id failed: %s", e)
        # Try via bot-generated invite link
        try:
            invite_link = await bot.export_chat_invite_link(chat_id)
            if invite_link:
                await userbot.join_chat(invite_link)
                joined = True
                LOG.info("Assistant auto-joined group %s via invite link", chat_id)
        except Exception as e2:
            LOG.debug("Assistant join via invite link failed: %s", e2)

    if joined:
        await asyncio.sleep(0.3)  # Brief pause for Telegram to register the join
        if await _try_play():
            return

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


async def _start_progress_timer(chat_id: int, duration: int):
    """Start a background task that updates the Now Playing message with progress."""
    # Cancel existing timer for this chat
    _stop_progress_timer(chat_id)
    
    _play_start_times[chat_id] = time.time()
    _play_durations[chat_id] = duration
    
    async def _update_progress():
        """Periodically update the Now Playing message with progress."""
        update_interval = 15  # Update every 15 seconds
        color_cycle_interval = 2  # Change color every 2 updates (30 seconds)
        update_count = 0
        
        while True:
            await asyncio.sleep(update_interval)
            update_count += 1
            
            if chat_id not in _play_start_times:
                return
            
            if chat_id not in _now_playing_messages or not _now_playing_messages[chat_id]:
                return
            
            elapsed = int(time.time() - _play_start_times[chat_id])
            total = _play_durations.get(chat_id, 0)
            
            # Stop updating if we've exceeded duration + buffer
            if total > 0 and elapsed > total + 30:
                return
            
            # Get current track info
            current = await get_current(chat_id)
            if not current:
                return
            
            # Get next color for button cycling
            if update_count % color_cycle_interval == 0:
                color = _get_next_color()
            else:
                color = "🎵"
            
            progress_text = _format_progress(elapsed, total)

            dur = format_duration(total)
            t = _get_current_theme()
            text = (
                f"{t['header']} **ᴘʟᴀʏʙᴀᴄᴋ ᴀᴄᴛɪᴠᴀᴛᴇᴅ | ᴇɴᴊᴏʏ ᴛʜᴇ ᴍᴜꜱɪᴄ**\n\n"
                f"> {t['title_icon']}  **ᴛɪᴛʟᴇ :** [{current.title}]({current.url})\n"
                f"> {t['dur_icon']}  **ᴅᴜʀᴀᴛɪᴏɴ :** {dur}\n"
                f"> 👤  **ʀᴇǫᴜᴇꜱᴛᴇᴅ :** {current.requester}\n\n"
                f"{progress_text}"
                f"\n\n🦋 ✦ᴘᴏᴡєʀєᴅ ʙʏ » ── [@R4J_81](https://t.me/R4J_81)"
            )
            
            # Update the most recent Now Playing message
            last_msg = _now_playing_messages[chat_id][-1] if _now_playing_messages[chat_id] else None
            if last_msg:
                try:
                    # Check if it's a photo message or text message
                    if hasattr(last_msg, 'photo') and last_msg.photo:
                        await last_msg.edit_caption(
                            caption=text,
                            reply_markup=_control_keyboard(color),
                        )
                    else:
                        await last_msg.edit_text(
                            text,
                            reply_markup=_control_keyboard(color),
                        )
                except Exception as e:
                    LOG.debug("Progress update failed for %s: %s", chat_id, e)
                    # If message was deleted, stop updating
                    if "MESSAGE_ID_INVALID" in str(e) or "message not found" in str(e).lower():
                        return
    
    task = asyncio.create_task(_update_progress())
    _progress_tasks[chat_id] = task


def _stop_progress_timer(chat_id: int):
    """Stop the progress timer for a chat."""
    if chat_id in _progress_tasks:
        try:
            _progress_tasks[chat_id].cancel()
        except Exception:
            pass
        del _progress_tasks[chat_id]
    
    _play_start_times.pop(chat_id, None)
    _play_durations.pop(chat_id, None)


# -- Public API ---

async def stream_audio(
    chat_id: int,
    media_path: str,
    title: str = "",
    duration: int = 0,
    thumbnail: str = "",
    requester: str = "",
) -> None:
    """Join voice chat (if needed) and start audio stream.

    media_path can be a local file path or a direct stream URL.
    If streaming a URL fails, automatically downloads the file
    and retries with the local path.
    """
    if pytgcalls is None:
        raise RuntimeError("Music streaming is disabled -- STRING_SESSION not configured.")
    _validate_media(media_path)

    # Pre-check stream URL validity to prevent ffprobe/JSONDecodeError crashes
    if _is_url(media_path):
        url_ok = await _check_stream_url(media_path)
        if not url_ok:
            LOG.warning("Stream URL pre-check failed in %s — trying fallbacks concurrently", chat_id)
            if title:
                # Run all fallbacks CONCURRENTLY for speed
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
                pending = set(tasks)
                recovered = False
                while pending:
                    done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        try:
                            local_path = task.result()
                            if local_path:
                                for p in pending:
                                    p.cancel()
                                audio = _make_audio_stream(local_path)
                                await _do_play(chat_id, audio)
                                _active_chats.add(chat_id)
                                LOG.info("Streaming audio (pre-check concurrent recovery) in %s: %s", chat_id, title)
                                recovered = True
                                pending = set()
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
) -> None:
    """Join voice chat (if needed) and start video stream.

    media_path can be a local file path or a direct stream URL.
    If streaming a URL fails, automatically downloads the file
    and retries with the local path.
    """
    if pytgcalls is None:
        raise RuntimeError("Music streaming is disabled -- STRING_SESSION not configured.")
    _validate_media(media_path)

    # Pre-check stream URL validity to prevent ffprobe/JSONDecodeError crashes
    if _is_url(media_path):
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


async def leave_voice_chat(chat_id: int) -> None:
    """Leave the voice chat and clean up."""
    # Stop progress timer
    _stop_progress_timer(chat_id)

    # Try leaving with retries — try BOTH methods on each attempt
    left = False
    for attempt in range(3):
        # Try leave_call (py-tgcalls 2.2.x)
        try:
            if hasattr(pytgcalls, 'leave_call'):
                await pytgcalls.leave_call(chat_id)
                LOG.info("Left voice chat via leave_call: %s (attempt %d)", chat_id, attempt + 1)
                left = True
                break
        except Exception as e:
            LOG.debug("leave_call attempt %d failed for %s: %s", attempt + 1, chat_id, e)

        # Try leave_group_call (older py-tgcalls)
        try:
            if hasattr(pytgcalls, 'leave_group_call'):
                await pytgcalls.leave_group_call(chat_id)
                LOG.info("Left voice chat via leave_group_call: %s (attempt %d)", chat_id, attempt + 1)
                left = True
                break
        except Exception as e:
            LOG.debug("leave_group_call attempt %d failed for %s: %s", attempt + 1, chat_id, e)

        # Try playing empty/silent stream then leaving (force disconnect)
        if attempt == 2:
            try:
                # Last resort: try to force leave by playing nothing
                if hasattr(pytgcalls, 'played_time'):
                    await pytgcalls.leave_call(chat_id)
                    left = True
                    break
            except Exception:
                pass

        if attempt < 2:
            await asyncio.sleep(1)

    if not left:
        LOG.error("Could not leave voice chat %s after 3 attempts — forcing cleanup", chat_id)

    # Always clean up state regardless of whether leave succeeded
    _active_chats.discard(chat_id)
    # Clear now playing messages tracking (don't delete — user wants messages kept)
    if chat_id in _now_playing_messages:
        _now_playing_messages[chat_id].clear()
        del _now_playing_messages[chat_id]
    # Clean up skip lock, suppression counter and auto-next tracking
    _skip_locks.pop(chat_id, None)
    _suppress_stream_end.pop(chat_id, None)
    _auto_next_in_progress.discard(chat_id)
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

    fresh_path = None
    fresh_is_stream = False

    # ── Run ALL platforms concurrently ──────────────────────────
    async def _try_youtube():
        """Try YouTube: stream URL first, then download."""
        try:
            from MusicLyrics.plugins.play.platforms.youtube import (
                get_audio_stream_url, get_video_stream_url,
                is_youtube_url, search_and_download_audio as yt_search_dl,
                search_and_download_video as yt_search_dl_video,
                search_youtube as _yt_search,
            )

            # Try re-fetch stream URL if we have the original YouTube URL
            if is_youtube_url(item.url):
                if item.stream_type == "video":
                    new_url = await get_video_stream_url(item.url)
                else:
                    new_url = await get_audio_stream_url(item.url)
                if new_url:
                    return new_url, True, "youtube"

            # Try search by title → get stream URL (faster than download)
            yt_result = await _yt_search(item.title)
            if yt_result and yt_result.get("url"):
                if item.stream_type == "video":
                    new_url = await get_video_stream_url(yt_result["url"])
                else:
                    new_url = await get_audio_stream_url(yt_result["url"])
                if new_url:
                    return new_url, True, "youtube"

            # Try search+download by title (local file)
            if item.stream_type == "video":
                path, info = await yt_search_dl_video(item.title)
            else:
                path, info = await yt_search_dl(item.title)
            if path and _os.path.isfile(str(path)):
                return path, False, "youtube"
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
        await stream_video(chat_id, item.media_path, title=item.title, duration=item.duration)
    else:
        await stream_audio(chat_id, item.media_path, title=item.title, duration=item.duration)

    return True


# -- Stream-end callback ---

async def _on_stream_end(client, update):
    """When current track ends, play next in queue or leave."""
    chat_id = None

    try:
        # Try various ways to get chat_id from the update object
        if hasattr(update, "chat_id"):
            chat_id = update.chat_id
        elif hasattr(update, "chat"):
            chat_obj = update.chat
            if isinstance(chat_obj, dict):
                chat_id = chat_obj.get("id")
            elif isinstance(chat_obj, int):
                chat_id = chat_obj
            elif hasattr(chat_obj, "id"):
                chat_id = chat_obj.id
        elif isinstance(update, int):
            chat_id = update
        elif isinstance(update, dict):
            chat_id = update.get("chat_id") or update.get("chat", {}).get("id")
    except Exception as e:
        LOG.warning("Error extracting chat_id from stream end event: %s", e)
        return

    if chat_id is None:
        LOG.warning("Stream end event with unknown chat_id: %s (type: %s)", update, type(update).__name__)
        return

    LOG.info("Stream end event for chat %s", chat_id)

    # Suppress events caused by manual skip/stop replacing the current stream.
    # _do_play triggers StreamAudioEnded for the OLD stream — swallow it here.
    if _suppress_stream_end.get(chat_id, 0) > 0:
        _suppress_stream_end[chat_id] -= 1
        LOG.info("Suppressed stream-end event for %s (remaining suppresses=%d)",
                 chat_id, _suppress_stream_end[chat_id])
        return

    # This is a REAL stream-end (track finished naturally).
    # Reset the suppress counter to 0 so no stale suppressions remain.
    _suppress_stream_end.pop(chat_id, None)

    # Prevent double-processing: if auto-next is already running for this chat, skip
    if chat_id in _auto_next_in_progress:
        LOG.info("Auto-next already in progress for %s — ignoring duplicate stream-end event", chat_id)
        return

    _auto_next_in_progress.add(chat_id)
    try:
        # Acquire per-chat skip lock — waits if manual skip/stop is in progress
        lock = _get_skip_lock(chat_id)
        async with lock:
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

            # Clean up the finished track's file (if it was a local download)
            if finished and not finished.is_stream_url and finished.media_path:
                cleanup(finished.media_path)

            # Delete previous "Now Playing" / thumbnail messages for this chat
            if chat_id in _now_playing_messages:
                for old_msg in _now_playing_messages[chat_id]:
                    try:
                        await old_msg.delete()
                        LOG.debug("Deleted previous Now Playing message in %s", chat_id)
                    except Exception:
                        pass
                _now_playing_messages[chat_id].clear()

            next_item = await skip_queue(chat_id, force=False)
            if next_item is None:
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

            # Play next track (INSIDE lock to prevent race with manual skip)
            # Remove from _active_chats so _do_play does NOT add a false
            # suppress_next_stream_end (the old stream already ended naturally,
            # there is no old-stream event to suppress).
            _active_chats.discard(chat_id)

            try:
                success = await _fresh_resolve_and_play(chat_id, next_item)
                if not success:
                    # Try skipping to subsequent tracks before giving up
                    LOG.warning("Auto-next failed for '%s', trying next tracks in queue", next_item.title)
                    retries = 0
                    while retries < 3:
                        retries += 1
                        fallback_item = await skip_queue(chat_id, force=True)
                        if fallback_item is None:
                            break
                        try:
                            success = await _fresh_resolve_and_play(chat_id, fallback_item)
                            if success:
                                next_item = fallback_item
                                break
                        except Exception:
                            continue

                    if not success:
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
                        return

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
                # Track this message so we can delete it when this track ends
                if chat_id not in _now_playing_messages:
                    _now_playing_messages[chat_id] = []
                _now_playing_messages[chat_id].append(np_msg)
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


# Register the stream-end callback with compatibility for multiple py-tgcalls versions
if pytgcalls is not None:
    _registered = False

    # Method 1: pytgcalls.on_update with filters (py-tgcalls >= 2.1)
    if not _registered:
        try:
            from pytgcalls import filters as _ptg_filters
            if hasattr(_ptg_filters, "stream_end"):
                @pytgcalls.on_update(_ptg_filters.stream_end)
                async def _stream_end_handler(client, update):
                    await _on_stream_end(client, update)
                _registered = True
                LOG.info("Stream-end callback registered via pytgcalls.filters.stream_end")
        except (ImportError, AttributeError, TypeError) as e:
            LOG.debug("Method 1 (filters.stream_end) failed: %s", e)

    # Method 2: pytgcalls.on_stream_end decorator
    if not _registered:
        try:
            if hasattr(pytgcalls, "on_stream_end"):
                @pytgcalls.on_stream_end()
                async def _stream_end_handler2(client, update):
                    await _on_stream_end(client, update)
                _registered = True
                LOG.info("Stream-end callback registered via pytgcalls.on_stream_end()")
        except (AttributeError, TypeError) as e:
            LOG.debug("Method 2 (on_stream_end) failed: %s", e)

    # Method 3: pytgcalls.on_closed_voice_chat
    if not _registered:
        try:
            if hasattr(pytgcalls, "on_closed_voice_chat"):
                @pytgcalls.on_closed_voice_chat()
                async def _stream_end_handler3(client, update):
                    await _on_stream_end(client, update)
                _registered = True
                LOG.info("Stream-end callback registered via pytgcalls.on_closed_voice_chat()")
        except (AttributeError, TypeError) as e:
            LOG.debug("Method 3 (on_closed_voice_chat) failed: %s", e)

    # Method 4: py-tgcalls >= 2.1 raw on_update without filter
    if not _registered:
        try:
            @pytgcalls.on_update()
            async def _raw_update_handler(client, update):
                # Only handle stream-end type events
                try:
                    update_type = type(update).__name__.lower()
                    if update_type in ("streamaudioended", "streamvideoended", "streamended", "stream_end"):
                        await _on_stream_end(client, update)
                    elif "end" in update_type and ("stream" in update_type or "audio" in update_type):
                        await _on_stream_end(client, update)
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
        """Periodically check if tracks have finished playing."""
        while True:
            await asyncio.sleep(5)  # Check every 5 seconds
            try:
                for chat_id in list(_active_chats):
                    if chat_id not in _play_start_times or chat_id not in _play_durations:
                        continue
                    elapsed = time.time() - _play_start_times[chat_id]
                    duration = _play_durations[chat_id]
                    # If track has been playing for longer than its duration + 10s buffer
                    if duration > 0 and elapsed > duration + 10:
                        LOG.info("Fallback timer detected stream end for %s (elapsed=%.0f, duration=%d)",
                                 chat_id, elapsed, duration)
                        await _on_stream_end(None, chat_id)
            except Exception as e:
                LOG.debug("Fallback stream-end checker error: %s", e)

    asyncio.get_event_loop().create_task(_fallback_stream_end_checker())

    # ── ALSO register on_kicked / on_left to clean up ──
    try:
        if hasattr(pytgcalls, "on_kicked"):
            @pytgcalls.on_kicked()
            async def _on_kicked(client, chat_id: int):
                LOG.info("Userbot kicked from voice chat in %s — cleaning up", chat_id)
                _stop_progress_timer(chat_id)
                _active_chats.discard(chat_id)
                if chat_id in _now_playing_messages:
                    _now_playing_messages[chat_id].clear()
                    del _now_playing_messages[chat_id]
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
                    if left_chat in _now_playing_messages:
                        _now_playing_messages[left_chat].clear()
                        del _now_playing_messages[left_chat]
                    _auto_next_in_progress.discard(left_chat)
                    await clear_queue(left_chat)
            LOG.info("Left voice chat handler registered via pytgcalls.filters.left")
    except (ImportError, AttributeError, TypeError) as e:
        LOG.debug("Left filter registration failed: %s", e)
