"""Playback control commands and inline callback handlers."""

from __future__ import annotations

import asyncio
import logging

from pyrogram import Client, filters
from pyrogram.types import (
    CallbackQuery,
    Message,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from pyrogram.enums import ChatType, ChatMemberStatus

from MusicLyrics.bot import bot
from MusicLyrics.helpers.filters import not_edited
from config import Config
from MusicLyrics.plugins.play.queue import (
    get_queue,
    get_current,
    clear_queue,
    skip_queue,
    toggle_loop,
    shuffle_queue,
    format_duration,
    get_chat_queue,
)
from MusicLyrics.plugins.play.stream import (
    pause_stream,
    resume_stream,
    seek_stream,
    set_volume,
    leave_voice_chat,
    stream_audio,
    stream_video,
    is_active,
    _now_playing_messages,
    _add_now_playing,
    _pop_now_playing,
    _remove_now_playing,
    _control_keyboard,
    _get_next_color,
    _get_current_theme,
    _start_progress_timer,
    _stop_progress_timer,
    _get_skip_lock,
    acquire_skip_lock,
    _add_reaction,
    suppress_next_stream_end,
    _fresh_resolve_and_play,
    _try_play_chain,
)
from MusicLyrics.plugins.play.prefetch import prefetch_next
from MusicLyrics.utils.autodelete import (
    auto_delete_service,
    auto_delete_playing,
    auto_delete_cmd,
)

LOG = logging.getLogger(__name__)


# ── Admin check for inline-keyboard callbacks ────────────────────────────────

async def _is_admin_callback(client: Client, callback: CallbackQuery) -> bool:
    """Return True if the callback user may use restricted control buttons.

    Allowed: bot OWNER_ID, SUDO_USERS, chat administrators / owner.
    In a private chat anyone can use the buttons.
    """
    if not callback.from_user:
        return False
    uid = callback.from_user.id
    if uid == Config.OWNER_ID or uid in getattr(Config, "SUDO_USERS", []):
        return True
    chat = callback.message.chat
    if chat.type == ChatType.PRIVATE:
        return True
    try:
        member = await client.get_chat_member(chat.id, uid)
        return member.status in (
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.OWNER,
        )
    except Exception:
        return False


async def _deny_non_admin(callback: CallbackQuery) -> None:
    """Send a popup explaining the button is admin-only."""
    try:
        await callback.answer(
            "⚠️ শুধু গ্রুপের admin-রা এই বাটন ব্যবহার করতে পারবেন।",
            show_alert=True,
        )
    except Exception:
        pass


# ── /pause ───────────────────────────────────────────────────────────────────

@bot.on_message(filters.command("pause") & not_edited)
async def pause_cmd(client: Client, message: Message):
    chat_id = message.chat.id
    if not is_active(chat_id):
        reply = await message.reply_text("❌ কিছু চলছে না এখন।")
        await _add_reaction(chat_id, message.id)
        return
    ok = await pause_stream(chat_id)
    if ok:
        reply = await message.reply_text("⏸ **Paused!**\nResume করতে `/resume` দিন।")
    else:
        reply = await message.reply_text("❌ Pause করা যায়নি।")
    await _add_reaction(chat_id, message.id)


# ── /resume ──────────────────────────────────────────────────────────────────

@bot.on_message(filters.command("resume") & not_edited)
async def resume_cmd(client: Client, message: Message):
    chat_id = message.chat.id
    if not is_active(chat_id):
        reply = await message.reply_text("❌ কিছু চলছে না এখন।")
        await _add_reaction(chat_id, message.id)
        return
    ok = await resume_stream(chat_id)
    if ok:
        reply = await message.reply_text("▶️ **Resumed!**")
    else:
        reply = await message.reply_text("❌ Resume করা যায়নি।")
    await _add_reaction(chat_id, message.id)


# ── /skip | /next ────────────────────────────────────────────────────────────

@bot.on_message(filters.command(["skip", "next"]) & not_edited)
async def skip_cmd(client: Client, message: Message):
    chat_id = message.chat.id
    if not is_active(chat_id):
        reply = await message.reply_text("❌ কিছু চলছে না এখন।")
        await _add_reaction(chat_id, message.id)
        return

    # Acquire skip lock to prevent race with auto-next.
    # Generous 15 s timeout; if a previous operation is genuinely still
    # running we tell the user to retry instead of racing it (which used
    # to crash pytgcalls).
    try:
        lock = await acquire_skip_lock(chat_id, timeout=15.0)
    except RuntimeError:
        await message.reply_text(
            "⏳ আগের command এখনো চলছে — একটু পরে আবার চেষ্টা করুন।"
        )
        return
    try:
        # Stop progress timer
        _stop_progress_timer(chat_id)

        # Delete previous "Now Playing" messages (thread-safe)
        old_msgs = await _pop_now_playing(chat_id)
        for old_msg in old_msgs:
            try:
                await old_msg.delete()
            except Exception:
                pass

        next_item = await skip_queue(chat_id, force=True)
        if next_item is None:
            await leave_voice_chat(chat_id)
            reply = await message.reply_text(
                "✅ **Queue শেষ হয়ে গেছে!**\n\n"
                "Voice chat থেকে বের হচ্ছি।"
            )
            await _add_reaction(chat_id, message.id)
            return

        try:
            # NOTE: Do NOT call suppress_next_stream_end here!
            # _do_play() (called inside _fresh_resolve_and_play → stream_audio)
            # already suppresses the stream-end event for the OLD stream.
            # Adding it here causes DOUBLE suppression — the real stream-end
            # for the NEW track also gets swallowed, breaking auto-next.

            # Walk a small number of items if the picked one fails.  25 was
            # excessive — each attempt does 4-5 platform fallbacks so 25
            # produced 100+ blocking network calls and exhausted the
            # extractor pool.  5 attempts is plenty in practice.
            played = await _try_play_chain(chat_id, next_item, max_attempts=5)

            if played is None:
                # Queue truly exhausted or every attempt failed.  Only
                # now do we leave the voice chat.
                await leave_voice_chat(chat_id)
                reply = await message.reply_text(
                    "❌ Queue শেষ — কোনো গান চালানো যায়নি।\n"
                    "Voice chat থেকে বের হচ্ছি — আবার `/play` দিন।"
                )
                await _add_reaction(chat_id, message.id)
                return

            # Use the item that actually started playing for the UI message.
            next_item = played

            # Start progress timer for the new track
            await _start_progress_timer(chat_id, next_item.duration)

            dur = format_duration(next_item.duration)
            color = _get_next_color()
            t = _get_current_theme()
            reply = await message.reply_text(
                f"⏭ **ꜱᴋɪᴘᴘᴇᴅ!**\n\n"
                f"> {t['title_icon']}  **ᴛɪᴛʟᴇ :** [{next_item.title}]({next_item.url})\n"
                f"> {t['dur_icon']}  **ᴅᴜʀᴀᴛɪᴏɴ :** {dur}\n"
                f"> 👤  **ʀᴇǫᴜᴇꜱᴛᴇᴅ :** {next_item.requester}\n\n"
                f"🦋 ✦ᴘᴏᴡєʀєᴅ ʙʏ » ── [@R4J_81](https://t.me/R4J_81)",
                reply_markup=_control_keyboard(color),
            )
            await _add_reaction(chat_id, message.id)
            # Track this new "Now Playing" message (thread-safe)
            await _add_now_playing(chat_id, reply)
        except Exception:
            LOG.exception("Skip failed in %s", chat_id)
            reply = await message.reply_text("❌ পরের গানে যেতে সমস্যা হয়েছে।")
            await _add_reaction(chat_id, message.id)
    finally:
        try:
            lock.release()
        except Exception:
            pass


# ── /stop | /end ─────────────────────────────────────────────────────────────

@bot.on_message(filters.command(["stop", "end"]) & not_edited)
async def stop_cmd(client: Client, message: Message):
    chat_id = message.chat.id
    if not is_active(chat_id):
        reply = await message.reply_text("❌ কিছু চলছে না এখন।")
        await _add_reaction(chat_id, message.id)
        return

    # Acquire skip lock with generous timeout.  Raises RuntimeError if a
    # previous play() is still in flight — we tell the user to retry
    # rather than racing the in-flight call (which crashed pytgcalls).
    try:
        lock = await acquire_skip_lock(chat_id, timeout=15.0)
    except RuntimeError:
        await message.reply_text(
            "⏳ আগের command এখনো চলছে — একটু পরে আবার চেষ্টা করুন।"
        )
        return
    try:
        # Stop progress timer
        _stop_progress_timer(chat_id)

        # Delete previous "Now Playing" messages (thread-safe)
        old_msgs = await _pop_now_playing(chat_id)
        for old_msg in old_msgs:
            try:
                await old_msg.delete()
            except Exception:
                pass

        await leave_voice_chat(chat_id)
        reply = await message.reply_text(
            "⏹ **Stopped!**\n\n"
            "✅ Queue clear করে voice chat থেকে বের হয়ে গেছি।"
        )
        await _add_reaction(chat_id, message.id)
    finally:
        try:
            lock.release()
        except Exception:
            pass


# ── /seek <seconds> ──────────────────────────────────────────────────────────

@bot.on_message(filters.command("seek") & not_edited)
async def seek_cmd(client: Client, message: Message):
    chat_id = message.chat.id
    if not is_active(chat_id):
        reply = await message.reply_text("❌ কিছু চলছে না এখন।")
        await _add_reaction(chat_id, message.id)
        return
    if len(message.command) < 2:
        reply = await message.reply_text("**Usage:** `/seek <seconds>`")
        await _add_reaction(chat_id, message.id)
        return
    try:
        seconds = int(message.command[1])
    except ValueError:
        reply = await message.reply_text("❌ সঠিক সংখ্যা দিন। Example: `/seek 30`")
        await _add_reaction(chat_id, message.id)
        return
    ok = await seek_stream(chat_id, seconds)
    if ok:
        reply = await message.reply_text(f"⏩ **{seconds}s** এ seek করা হয়েছে।")
    else:
        reply = await message.reply_text(
            "❌ Seek এখনো এই version-এ fully supported নয়।"
        )
    await _add_reaction(chat_id, message.id)


# ── /volume <1-200> ──────────────────────────────────────────────────────────

@bot.on_message(filters.command(["volume", "vol"]) & not_edited)
async def volume_cmd(client: Client, message: Message):
    chat_id = message.chat.id
    if not is_active(chat_id):
        reply = await message.reply_text("❌ কিছু চলছে না এখন।")
        await _add_reaction(chat_id, message.id)
        return
    if len(message.command) < 2:
        reply = await message.reply_text("**Usage:** `/volume <1-200>`")
        await _add_reaction(chat_id, message.id)
        return
    try:
        vol = int(message.command[1])
    except ValueError:
        reply = await message.reply_text("❌ সঠিক সংখ্যা দিন (1-200)।")
        await _add_reaction(chat_id, message.id)
        return
    if not 1 <= vol <= 200:
        reply = await message.reply_text("❌ Volume 1 থেকে 200 এর মধ্যে হতে হবে।")
        await _add_reaction(chat_id, message.id)
        return
    ok = await set_volume(chat_id, vol)
    if ok:
        reply = await message.reply_text(f"🔊 Volume **{vol}%** সেট হয়েছে।")
    else:
        reply = await message.reply_text("❌ Volume পরিবর্তন করা যায়নি।")
    await _add_reaction(chat_id, message.id)


# ── /queue ───────────────────────────────────────────────────────────────────

@bot.on_message(filters.command("queue") & not_edited)
async def queue_cmd(client: Client, message: Message):
    chat_id = message.chat.id
    items = await get_queue(chat_id)
    if not items:
        reply = await message.reply_text("📜 Queue খালি আছে।")
        await _add_reaction(chat_id, message.id)
        return
    cq = await get_chat_queue(chat_id)
    lines = ["**📜 Current Queue:**\n"]
    for i, item in enumerate(items):
        dur = format_duration(item.duration)
        kind = "🎬" if item.stream_type == "video" else "🎵"
        if i == 0:
            # Currently playing (always index 0 now)
            lines.append(f"▶️ {kind} **{item.title}** [{dur}] — {item.requester}")
        else:
            lines.append(f"{i}. {kind} **{item.title}** [{dur}] — {item.requester}")
    loop_status = "🔁 Loop: ON" if cq.loop_mode else "🔁 Loop: OFF"
    lines.append(f"\n{loop_status}")
    reply = await message.reply_text("\n".join(lines))
    await _add_reaction(chat_id, message.id)


# ── /nowplaying | /np ────────────────────────────────────────────────────────

@bot.on_message(filters.command(["nowplaying", "np"]) & not_edited)
async def nowplaying_cmd(client: Client, message: Message):
    chat_id = message.chat.id
    current = await get_current(chat_id)
    if not current:
        reply = await message.reply_text("❌ এখন কিছু চলছে না।")
        await _add_reaction(chat_id, message.id)
        return
    dur = format_duration(current.duration)
    color = _get_next_color()
    t = _get_current_theme()
    text = (
        f"{t['header']} **ᴘʟᴀʏʙᴀᴄᴋ ᴀᴄᴛɪᴠᴀᴛᴇᴅ | ᴇɴᴊᴏʏ ᴛʜᴇ ᴍᴜꜱɪᴄ**\n\n"
        f"> {t['title_icon']}  **ᴛɪᴛʟᴇ :** [{current.title}]({current.url})\n"
        f"> {t['dur_icon']}  **ᴅᴜʀᴀᴛɪᴏɴ :** {dur}\n"
        f"> 👤  **ʀᴇǫᴜᴇꜱᴛᴇᴅ :** {current.requester}\n\n"
        f"🦋 ✦ᴘᴏᴡєʀєᴅ ʙʏ » ── [@R4J_81](https://t.me/R4J_81)"
    )
    if current.thumbnail:
        reply = await bot.send_photo(
            chat_id, photo=current.thumbnail,
            caption=text, reply_markup=_control_keyboard(color),
        )
    else:
        reply = await message.reply_text(text, reply_markup=_control_keyboard(color))
    await _add_reaction(chat_id, message.id)


# ── /loop ────────────────────────────────────────────────────────────────────

@bot.on_message(filters.command("loop") & not_edited)
async def loop_cmd(client: Client, message: Message):
    chat_id = message.chat.id
    state = await toggle_loop(chat_id)
    if state:
        reply = await message.reply_text("🔁 **Loop ON** — বর্তমান গান বারবার চলবে।")
    else:
        reply = await message.reply_text("🔁 **Loop OFF** — Queue স্বাভাবিকভাবে চলবে।")
    await _add_reaction(chat_id, message.id)


# ── /shuffle ─────────────────────────────────────────────────────────────────

@bot.on_message(filters.command("shuffle") & not_edited)
async def shuffle_cmd(client: Client, message: Message):
    chat_id = message.chat.id
    items = await get_queue(chat_id)
    if len(items) < 2:
        reply = await message.reply_text("❌ Shuffle করার জন্য queue-তে কমপক্ষে ২টা গান থাকা দরকার।")
        await _add_reaction(chat_id, message.id)
        return
    await shuffle_queue(chat_id)
    reply = await message.reply_text("🔀 **Queue shuffle হয়ে গেছে!**")
    await _add_reaction(chat_id, message.id)


# ══════════════════════════════════════════════════════════════════════════════
# Callback query handlers (inline keyboard buttons)
# ══════════════════════════════════════════════════════════════════════════════

@bot.on_callback_query(filters.regex(r"^ctl_pause$"))
async def cb_pause(client: Client, callback: CallbackQuery):
    chat_id = callback.message.chat.id
    if not await _is_admin_callback(client, callback):
        await _deny_non_admin(callback)
        return
    if not is_active(chat_id):
        try:
            await callback.answer("কিছু চলছে না!", show_alert=True)
        except Exception:
            pass
        return
    ok = await pause_stream(chat_id)
    try:
        await callback.answer("⏸ Paused!" if ok else "❌ Pause failed")
    except Exception:
        pass


@bot.on_callback_query(filters.regex(r"^ctl_resume$"))
async def cb_resume(client: Client, callback: CallbackQuery):
    chat_id = callback.message.chat.id
    if not await _is_admin_callback(client, callback):
        await _deny_non_admin(callback)
        return
    if not is_active(chat_id):
        try:
            await callback.answer("কিছু চলছে না!", show_alert=True)
        except Exception:
            pass
        return
    ok = await resume_stream(chat_id)
    try:
        await callback.answer("▶️ Resumed!" if ok else "❌ Resume failed")
    except Exception:
        pass


@bot.on_callback_query(filters.regex(r"^ctl_skip$"))
async def cb_skip(client: Client, callback: CallbackQuery):
    chat_id = callback.message.chat.id
    if not await _is_admin_callback(client, callback):
        await _deny_non_admin(callback)
        return
    if not is_active(chat_id):
        try:
            await callback.answer("কিছু চলছে না!", show_alert=True)
        except Exception:
            pass
        return

    # Answer callback IMMEDIATELY to prevent timeout
    try:
        await callback.answer("⏭ Skipping...")
    except Exception:
        pass

    # Acquire skip lock with generous timeout — refuse on collision
    # instead of force-replacing (which used to race the in-flight call
    # and crash pytgcalls).
    try:
        lock = await acquire_skip_lock(chat_id, timeout=15.0)
    except RuntimeError:
        try:
            await callback.message.reply_text(
                "⏳ আগের command এখনো চলছে — একটু পরে আবার চেষ্টা করুন।"
            )
        except Exception:
            pass
        return
    try:
        if not is_active(chat_id):
            return

        # Stop progress timer
        _stop_progress_timer(chat_id)

        # Delete previous "Now Playing" messages (thread-safe)
        old_msgs = await _pop_now_playing(chat_id)
        for old_msg in old_msgs:
            try:
                await old_msg.delete()
            except Exception:
                pass

        next_item = await skip_queue(chat_id, force=True)
        if next_item is None:
            try:
                reply = await callback.message.reply_text(
                    "✅ **Queue শেষ হয়ে গেছে!**\n\n"
                    "Voice chat থেকে বের হচ্ছি।"
                )
            except Exception:
                pass
            await leave_voice_chat(chat_id)
            return

        try:
            # NOTE: Do NOT call suppress_next_stream_end here!
            # _do_play() already handles suppression internally.

            # 5 attempts is plenty — 25 was overkill (each attempt fans
            # out to 4-5 platforms = up to 100+ blocking calls).
            played = await _try_play_chain(chat_id, next_item, max_attempts=5)

            if played is None:
                try:
                    err_reply = await callback.message.reply_text(
                        "❌ Queue শেষ — কোনো গান চালানো যায়নি।\n"
                        "Voice chat থেকে বের হচ্ছি — আবার `/play` দিন।"
                    )
                except Exception:
                    pass
                await leave_voice_chat(chat_id)
                return

            # Use the item that actually started playing for the UI message.
            next_item = played

            # Start progress timer for the new track
            await _start_progress_timer(chat_id, next_item.duration)

            dur = format_duration(next_item.duration)
            color = _get_next_color()
            t = _get_current_theme()
            reply = await callback.message.reply_text(
                f"⏭ **ꜱᴋɪᴘᴘᴇᴅ!**\n\n"
                f"> {t['title_icon']}  **ᴛɪᴛʟᴇ :** [{next_item.title}]({next_item.url})\n"
                f"> {t['dur_icon']}  **ᴅᴜʀᴀᴛɪᴏɴ :** {dur}\n"
                f"> 👤  **ʀᴇǫᴜᴇꜱᴛᴇᴅ :** {next_item.requester}\n\n"
                f"🦋 ✦ᴘᴏᴡєʀєᴅ ʙʏ » ── [@R4J_81](https://t.me/R4J_81)",
                reply_markup=_control_keyboard(color),
            )
            # Track this new "Now Playing" message (thread-safe)
            await _add_now_playing(chat_id, reply)
        except Exception:
            LOG.exception("Skip callback failed in %s", chat_id)
            try:
                err_reply = await callback.message.reply_text("❌ Skip করা যায়নি। আবার চেষ্টা করুন।")
            except Exception:
                pass
    finally:
        try:
            lock.release()
        except Exception:
            pass


@bot.on_callback_query(filters.regex(r"^ctl_stop$"))
async def cb_stop(client: Client, callback: CallbackQuery):
    """CLOSE button — only deletes the Now Playing message, does NOT stop playback."""
    chat_id = callback.message.chat.id

    if not await _is_admin_callback(client, callback):
        await _deny_non_admin(callback)
        return

    # Answer callback immediately
    try:
        await callback.answer("✖ Closed")
    except Exception:
        pass

    # Remove this message from the tracking list (thread-safe)
    msg_id = callback.message.id
    await _remove_now_playing(chat_id, msg_id)

    # Delete only this message
    try:
        await callback.message.delete()
    except Exception:
        pass


@bot.on_callback_query(filters.regex(r"^ctl_queue$"))
async def cb_queue(client: Client, callback: CallbackQuery):
    chat_id = callback.message.chat.id
    items = await get_queue(chat_id)
    if not items:
        try:
            await callback.answer("Queue খালি!", show_alert=True)
        except Exception:
            pass
        return
    cq = await get_chat_queue(chat_id)
    lines = []
    for i, item in enumerate(items):
        dur = format_duration(item.duration)
        if i == 0:
            lines.append(f"▶️ {item.title} [{dur}]")
        else:
            lines.append(f"{i}. {item.title} [{dur}]")
    text = "\n".join(lines[:15])  # limit to 15 to avoid message length issues
    if len(items) > 15:
        text += f"\n\n... এবং আরো {len(items) - 15}টি গান"
    try:
        await callback.answer(text[:200], show_alert=True)
    except Exception:
        pass


@bot.on_callback_query(filters.regex(r"^ctl_loop$"))
async def cb_loop(client: Client, callback: CallbackQuery):
    chat_id = callback.message.chat.id
    state = await toggle_loop(chat_id)
    try:
        await callback.answer(
            "🔁 Loop ON" if state else "🔁 Loop OFF",
            show_alert=False,
        )
    except Exception:
        pass
