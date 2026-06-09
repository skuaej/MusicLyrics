"""Error logging and owner notification plugin for MusicLyrics bot."""

import logging
import traceback
import time

from pyrogram import filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait

from MusicLyrics.bot import bot
from config import Config

LOG = logging.getLogger(__name__)

_BOT_START_TIME = time.time()


async def notify_owner(text: str):
    """Send a notification to the bot owner and/or LOG_GROUP_ID."""
    # Send to LOG_GROUP_ID
    if Config.LOG_GROUP_ID:
        try:
            await bot.send_message(
                chat_id=Config.LOG_GROUP_ID,
                text=text,
                disable_web_page_preview=True,
            )
        except FloodWait as e:
            import asyncio
            await asyncio.sleep(e.value + 1)
            try:
                await bot.send_message(
                    chat_id=Config.LOG_GROUP_ID, text=text,
                    disable_web_page_preview=True,
                )
            except Exception:
                pass
        except Exception:
            LOG.exception("Failed to send notification to LOG_GROUP_ID")

    # Also send to OWNER_ID personally
    if Config.OWNER_ID and Config.OWNER_ID != Config.LOG_GROUP_ID:
        try:
            await bot.send_message(
                chat_id=Config.OWNER_ID,
                text=text,
                disable_web_page_preview=True,
            )
        except Exception:
            LOG.exception("Failed to send notification to OWNER_ID")


async def log_error(context: str, error: Exception):
    """Log an error to both console and owner/log group."""
    tb = traceback.format_exc()
    LOG.error("Error in %s: %s\n%s", context, error, tb)

    # Truncate traceback for Telegram message limit
    tb_short = tb[-1500:] if len(tb) > 1500 else tb
    text = (
        f"**Error Report**\n\n"
        f"**Context:** {context}\n"
        f"**Error:** `{str(error)[:200]}`\n\n"
        f"```\n{tb_short}\n```"
    )
    await notify_owner(text)


async def log_activity(text: str):
    """Log a general activity/event to owner/log group."""
    await notify_owner(text)


@bot.on_message(filters.command("status") & filters.private)
async def status_cmd(client, message: Message):
    """Show bot status (owner/sudo only)."""
    user_id = message.from_user.id if message.from_user else 0
    if user_id != Config.OWNER_ID and user_id not in Config.SUDO_USERS:
        return

    import psutil

    ram = psutil.virtual_memory()
    cpu = psutil.cpu_percent(interval=0.5)
    disk = psutil.disk_usage('/')

    # Uptime
    uptime_s = int(time.time() - _BOT_START_TIME)
    d, rem = divmod(uptime_s, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    uptime_str = ""
    if d: uptime_str += f"{d}d "
    if h: uptime_str += f"{h}h "
    uptime_str += f"{m}m {s}s"

    # Count handlers
    handler_count = sum(len(h) for h in bot.dispatcher.groups.values())

    # Count handler groups
    groups_info = ", ".join(
        f"g{g}:{len(handlers)}" for g, handlers in sorted(bot.dispatcher.groups.items())
    )

    text = (
        f"**Bot Status Report**\n\n"
        f"**Uptime:** `{uptime_str}`\n"
        f"**Handlers:** {handler_count} ({groups_info})\n"
        f"**CPU:** {cpu}%\n"
        f"**RAM:** {ram.used // (1024**2)}MB / {ram.total // (1024**2)}MB ({ram.percent}%)\n"
        f"**Disk:** {disk.used // (1024**3)}GB / {disk.total // (1024**3)}GB ({disk.percent}%)\n"
        f"**LOG_GROUP_ID:** `{Config.LOG_GROUP_ID or 'Not set'}`\n"
        f"**OWNER_ID:** `{Config.OWNER_ID}`\n"
        f"**SUDO_USERS:** {len(Config.SUDO_USERS)}\n"
        f"**STRING_SESSION:** {'Set' if Config.STRING_SESSION else 'Not set'}\n"
        f"**PyTgCalls:** {'Available' if Config.STRING_SESSION else 'Disabled'}\n"
    )
    await message.reply_text(text)


@bot.on_message(filters.command("logs") & filters.private)
async def logs_cmd(client, message: Message):
    """Send recent log file to owner (owner/sudo only)."""
    user_id = message.from_user.id if message.from_user else 0
    if user_id != Config.OWNER_ID and user_id not in Config.SUDO_USERS:
        return

    import os
    log_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        os.pardir, os.pardir, os.pardir, "logs", "musiclyrics.log"
    )
    log_file = os.path.normpath(log_file)

    if os.path.exists(log_file):
        file_size = os.path.getsize(log_file)
        if file_size > 50 * 1024 * 1024:
            await message.reply_text("Log file too large (>50MB).")
            return
        try:
            await message.reply_document(
                document=log_file,
                caption="📋 MusicLyrics Bot Logs",
            )
        except Exception as e:
            # Send last 4000 chars as text
            with open(log_file, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
            tail = content[-3500:]
            await message.reply_text(f"```\n{tail}\n```")
    else:
        await message.reply_text("Log file not found.")
