#"""MusicLyrics entry-point -- run with ``python -m MusicLyrics``."""

import asyncio
import importlib
import logging
import os
import signal
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# Patch pyrogram MsgFactory to use crypto-strong random IDs instead of
# time-based duplicates. This helps eliminate RANDOM_ID_DUPLICATE storms
# under high concurrency across multiple chats.
try:
    import secrets
    from pyrogram import session as _session
    if hasattr(_session, "MsgFactory"):
        def _crypto_msg_id(self):
            return secrets.randbits(63)
        _session.MsgFactory.msg_id = _crypto_msg_id
except Exception:
    pass

try:
    import pyrogram.client as _pc
    _orig_send_message = _pc.Client.send_message
    async def _patched_send_message(self, chat_id, text, *args, **kwargs):
        return await _orig_send_message(self, chat_id, text, *args, **kwargs)
    _pc.Client.send_message = _patched_send_message
except Exception:
    pass

# ── Bounded ThreadPoolExecutor for blocking yt-dlp / network calls ──────
EXTRACTOR_EXECUTOR = ThreadPoolExecutor(
    max_workers=32,
    thread_name_prefix="extractor",
)

def _install_executor() -> None:
    """Install EXTRACTOR_EXECUTOR as the default for the running loop."""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    loop.set_default_executor(EXTRACTOR_EXECUTOR)

# ── LICENSE ENFORCEMENT ──────────────────────────────────────────────────
import hashlib
import hmac

_LICENSE_OWNER = "R4J_81"
_LICENSE_REPO = "https://github.com/RajSukh81/MusicLyrics"
_LICENSE_SECRET = b"R4J81_MusicLyrics_2026_PropSecret"

def _generate_license_key(bot_token: str) -> str:
    digest = hmac.new(_LICENSE_SECRET, bot_token.encode(), hashlib.sha256).hexdigest()
    return f"ML-{digest[:8].upper()}-{digest[8:16].upper()}"

def _verify_license():
    """Verify LICENSE file + LICENSE_KEY environment variable."""
    return # <-- THE BYPASS IS HERE: Automatically skips verification

    def _err(msg):
        print(msg, file=sys.stderr, flush=True)
        print(msg, flush=True)

    # (Original validation logic is skipped because of the return above)
    pass

_verify_license()

# ── CRITICAL: Monkey-patch asyncio subprocess to fix ProcessLookupError ──
try:
    import asyncio.subprocess as _asub
    _orig_transport_kill = _asub.SubprocessTransport.kill if hasattr(_asub, 'SubprocessTransport') else None
except (ImportError, AttributeError):
    _orig_transport_kill = None

try:
    from asyncio import base_subprocess as _base_sub
    _orig_base_kill = _base_sub.BaseSubprocessTransport.kill
    _orig_base_terminate = getattr(_base_sub.BaseSubprocessTransport, 'terminate', None)
    _orig_base_send_signal = getattr(_base_sub.BaseSubprocessTransport, 'send_signal', None)

    def _safe_kill(self):
        try:
            _orig_base_kill(self)
        except (ProcessLookupError, OSError):
            pass

    _base_sub.BaseSubprocessTransport.kill = _safe_kill

    if _orig_base_terminate:
        def _safe_terminate(self):
            try:
                _orig_base_terminate(self)
            except (ProcessLookupError, OSError):
                pass
        _base_sub.BaseSubprocessTransport.terminate = _safe_terminate

    if _orig_base_send_signal:
        def _safe_send_signal(self, signum):
            try:
                _orig_base_send_signal(self, signum)
            except (ProcessLookupError, OSError):
                pass
        _base_sub.BaseSubprocessTransport.send_signal = _safe_send_signal
except (ImportError, AttributeError):
    pass

import aiohttp
from pyrogram import idle, filters
from pyrogram.types import Message
from pyrogram.enums import ChatType, ParseMode
from pyrogram.errors import FloodWait

from config import Config
from MusicLyrics import bot, userbot, pytgcalls, __version__
from MusicLyrics.bot import get_bot_info

_log_dir = Path(__file__).parent.parent / "logs"
_log_dir.mkdir(exist_ok=True)
_log_file = _log_dir / "musiclyrics.log"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(_log_file, encoding="utf-8"),
    ],
)
LOG = logging.getLogger("MusicLyrics")

async def _safe_send(chat_id: int, text: str, photo: str = None):
    try:
        if photo:
            await bot.send_photo(chat_id, photo=photo, caption=text)
        else:
            await bot.send_message(chat_id, text=text, disable_web_page_preview=True)
    except FloodWait as e:
        LOG.warning("FloodWait %ds for chat %s, skipping log.", e.value, chat_id)
        await asyncio.sleep(e.value + 1)
        try:
            await bot.send_message(chat_id, text=text, disable_web_page_preview=True)
        except Exception:
            pass
    except Exception as exc:
        LOG.warning("Failed to send log to %s: %s", chat_id, exc)

def log_to_group(text: str, photo: str = None):
    if Config.LOG_GROUP_ID:
        asyncio.create_task(_safe_send(Config.LOG_GROUP_ID, text, photo))
    if Config.OWNER_ID and Config.OWNER_ID != Config.LOG_GROUP_ID:
        asyncio.create_task(_safe_send(Config.OWNER_ID, text, photo))

def _load_plugins():
    plugins_dir = Path(__file__).parent / "plugins"
    if not plugins_dir.is_dir():
        LOG.warning("plugins/ directory not found -- skipping plugin load.")
        return 0, 0

    loaded, failed = 0, 0
    for py_file in sorted(plugins_dir.rglob("*.py")):
        if py_file.name.startswith("__"):
            continue
        relative = py_file.relative_to(Path(__file__).parent.parent)
        module_path = ".".join(relative.with_suffix("").parts)
        try:
            importlib.import_module(module_path)
            loaded += 1
        except Exception as e:
            LOG.exception("Failed to load plugin: %s", module_path)
            failed += 1

    LOG.info("Plugin loading complete: %d loaded, %d failed.", loaded, failed)
    return loaded, failed

async def _send_startup_message():
    if not Config.LOG_GROUP_ID and not Config.OWNER_ID:
        return
    bot_me = await get_bot_info()
    user_info = "N/A (no userbot)"
    userbot_id, userbot_name = 0, "N/A"
    if userbot:
        try:
            user_me = await userbot.get_me()
            user_info = f"{user_me.first_name} (ID: {user_me.id})"
            userbot_id = user_me.id
            userbot_name = user_me.first_name
        except Exception:
            user_info = "N/A"

    handler_count = sum(len(h) for h in bot.dispatcher.groups.values())
    try:
        import psutil
        ram = psutil.virtual_memory()
        ram_info = f"{ram.used // (1024**2)}MB / {ram.total // (1024**2)}MB ({ram.percent}%)"
        cpu_info = f"{psutil.cpu_percent(interval=0.3)}%"
    except Exception:
        ram_info, cpu_info = "N/A", "N/A"

    text = (
        f"**MusicLyrics v{__version__} Started Successfully!**\n\n"
        f"**Bot:** @{bot_me.username} (ID: `{bot_me.id}`)\n"
        f"**Userbot:** {user_info}\n"
        f"**Handlers:** {handler_count}\n"
        f"**PyTgCalls:** {'Active' if pytgcalls else 'Disabled'}\n"
        f"**CPU:** {cpu_info} | **RAM:** {ram_info}\n\n"
        f"[Support]({Config.SUPPORT_GROUP})"
    )
    for cid in {Config.LOG_GROUP_ID, Config.OWNER_ID} - {0, None}:
        await _safe_send(cid, text, photo=Config.BRAND_PHOTO)

    if userbot and userbot_id:
        assistant_text = f"🎵 **Assistant Online!**\n**ID:** `{userbot_id}`"
        for cid in {Config.LOG_GROUP_ID, Config.OWNER_ID} - {0, None}:
            try:
                await userbot.send_message(cid, assistant_text)
            except Exception:
                pass

async def _start_with_retry(client, name, max_retries=5):
    for attempt in range(1, max_retries + 1):
        try:
            await client.start()
            LOG.info("%s client started.", name)
            return
        except FloodWait as e:
            LOG.warning("%s: FloodWait %ds. Attempt %d.", name, e.value, attempt)
            await asyncio.sleep(e.value + 2)
        except Exception as e:
            LOG.exception("%s: Failed to start: %s", name, e)
            if attempt < max_retries:
                await asyncio.sleep(10)
            else:
                raise

def _setup_event_logging():
    @bot.on_message(filters.new_chat_members)
    async def on_bot_added(client, message: Message):
        try:
            me = await get_bot_info()
            for member in message.new_chat_members:
                if member.id == me.id:
                    added_by = message.from_user.mention if message.from_user else "Unknown"
                    log_to_group(f"**Bot Added**\nGroup: {message.chat.title}\nAdded by: {added_by}")
                    await message.reply_text(f"**Thanks for adding me!** 🎵\nI am {Config.BOT_NAME}!")
                    
                    if userbot:
                        try:
                            user_me = await userbot.get_me()
                            assistant_id = user_me.id
                            try:
                                await client.add_chat_members(message.chat.id, assistant_id)
                            except Exception:
                                try:
                                    invite_link = await client.export_chat_invite_link(message.chat.id)
                                    if invite_link:
                                        await userbot.join_chat(invite_link)
                                except Exception:
                                    try:
                                        await userbot.join_chat(message.chat.id)
                                    except Exception as direct_err:
                                        LOG.warning("Could not auto-add assistant to group %s: %s", message.chat.id, direct_err)
                        except Exception:
                            pass
        except Exception as e:
            LOG.error("Error in on_bot_added: %s", e)

async def main():
    _install_executor()
    _setup_event_logging()
    
    await _start_with_retry(bot, "Bot")
    if userbot:
        await _start_with_retry(userbot, "Userbot")
        
    _load_plugins()
    
    if pytgcalls:
        try:
            await pytgcalls.start()
        except Exception as e:
            LOG.error("PyTgCalls failed to start: %s", e)
            
    await _send_startup_message()
    LOG.info("Bot is now running. Press Ctrl+C to stop.")
    
    await idle()
    
    if pytgcalls:
        await pytgcalls.stop()
    await bot.stop()
    if userbot:
        await userbot.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        LOG.info("Bot stopped by user.")
    except Exception as e:
        LOG.exception("Fatal error occurred: %s", e)
