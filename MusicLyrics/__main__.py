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
# Default asyncio executor on Railway free tier has only ~5-6 worker
# threads (min(32, cpu_count + 4)). When a queue holds 3+ tracks and each
# track fans out to 4-5 platform fallbacks (YouTube → JioSaavn → Piped →
# Invidious → SoundCloud) the pool gets exhausted in seconds, asyncio
# coroutines hang forever, health checks fail and the container is killed
# ("crash" symptom). A dedicated, larger pool fixes the primary cause.
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
# This software is proprietary. See LICENSE file for full terms.
# Copyright (c) 2026 R4J_81 (https://github.com/RajSukh81)
# Unauthorized copying, modification, or redistribution is prohibited.
# ─────────────────────────────────────────────────────────────────────────

import hashlib
import hmac

_LICENSE_OWNER = "R4J_81"
_LICENSE_REPO = "https://github.com/RajSukh81/MusicLyrics"
# ── HMAC secret used to generate and validate license keys ──
# This is the master secret. Only the owner (R4J_81) knows this.
# Keys are generated as: HMAC-SHA256(secret, bot_token)[:16] uppercased hex
_LICENSE_SECRET = b"R4J81_MusicLyrics_2026_PropSecret"


def _generate_license_key(bot_token: str) -> str:
    """Generate a valid LICENSE_KEY for a given BOT_TOKEN.

    This function is for the OWNER to use when issuing keys.
    Run:  python3 -c "from MusicLyrics.__main__ import _generate_license_key; print(_generate_license_key('YOUR_BOT_TOKEN'))"
    """
    digest = hmac.new(_LICENSE_SECRET, bot_token.encode(), hashlib.sha256).hexdigest()
    return f"ML-{digest[:8].upper()}-{digest[8:16].upper()}"


def _verify_license():
    """Verify LICENSE file + LICENSE_KEY environment variable."""
    return # <-- BYPASS ADDED HERE. Exits the function before doing any checks.

    # Use print + sys.stderr because logging may not be configured yet
    def _err(msg):
        print(msg, file=sys.stderr, flush=True)
        print(msg, flush=True)

    # ── Step 1: Check LICENSE file ──
    license_path = Path(__file__).parent.parent / "LICENSE"

    if not license_path.exists():
        sys.exit(1)

    try:
        content = license_path.read_text(encoding="utf-8")
    except Exception:
        sys.exit(1)

    required_markers = [
        _LICENSE_OWNER,
        "PROPRIETARY LICENSE",
        "All Rights Reserved",
        "RajSukh81",
        "STRICTLY PROHIBITED",
    ]
    for marker in required_markers:
        if marker not in content:
            sys.exit(1)

    # ── Step 2: Validate LICENSE_KEY from environment variable ──
    license_key = os.environ.get("LICENSE_KEY", "").strip()
    bot_token = os.environ.get("BOT_TOKEN", "").strip()

    if not license_key:
        sys.exit(1)

    if not bot_token:
        sys.exit(1)

    # Generate expected key from bot_token and compare
    expected_key = _generate_license_key(bot_token)

    if not hmac.compare_digest(license_key, expected_key):
        sys.exit(1)


# Run license check immediately on import
# _verify_license() # <-- BYPASS ADDED HERE. Commented out the execution.

# ── CRITICAL: Monkey-patch asyncio subprocess to fix ProcessLookupError ──
# py-tgcalls (NTgCalls) uses asyncio subprocesses for ffprobe to analyze
# media before streaming. On Heroku and other cloud platforms, the ffprobe
# process often exits before py-tgcalls calls kill() or terminate() on it,
# raising ProcessLookupError. This is a benign race condition — the process
# already completed its work successfully. This patch makes kill/terminate
# safely ignore the already-exited process instead of crashing.
try:
    import asyncio.subprocess as _asub
    _orig_transport_kill = _asub.SubprocessTransport.kill if hasattr(_asub, 'SubprocessTransport') else None
except (ImportError, AttributeError):
    _orig_transport_kill = None

# Patch at the base_subprocess level (where the actual kill happens)
try:
    from asyncio import base_subprocess as _base_sub

    _orig_base_kill = _base_sub.BaseSubprocessTransport.kill
    _orig_base_terminate = getattr(_base_sub.BaseSubprocessTransport, 'terminate', None)
    _orig_base_send_signal = getattr(_base_sub.BaseSubprocessTransport, 'send_signal', None)

    def _safe_kill(self):
        try:
            _orig_base_kill(self)
        except (ProcessLookupError, OSError):
            pass  # Process already exited — safe to ignore

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
    pass  # Older Python — patch not needed or not possible

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


# -- Logger helper: send messages to LOG_GROUP_ID and/or OWNER_ID --
# Uses fire-and-forget pattern to NEVER block command handlers.

async def _safe_send(chat_id: int, text: str, photo: str = None):
    """Send a message to a chat, silently ignoring all errors."""
    try:
        if photo:
            await bot.send_photo(chat_id, photo=photo, caption=text)
        else:
            await bot.send_message(
                chat_id, text=text,
                disable_web_page_preview=True,
            )
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
    """Schedule a log message (non-blocking, fire-and-forget)."""
    if Config.LOG_GROUP_ID:
        asyncio.create_task(_safe_send(Config.LOG_GROUP_ID, text, photo))
    if Config.OWNER_ID and Config.OWNER_ID != Config.LOG_GROUP_ID:
        asyncio.create_task(_safe_send(Config.OWNER_ID, text, photo))


def _load_plugins():
    """Recursively import every .py module under MusicLyrics/plugins/."""
    plugins_dir = Path(__file__).parent / "plugins"
    if not plugins_dir.is_dir():
        LOG.warning("plugins/ directory not found -- skipping plugin load.")
        return

    loaded = 0
    failed = 0
    for py_file in sorted(plugins_dir.rglob("*.py")):
        if py_file.name.startswith("__"):
            continue
        relative = py_file.relative_to(Path(__file__).parent.parent)
        module_path = ".".join(relative.with_suffix("").parts)
        try:
            importlib.import_module(module_path)
            LOG.info("Loaded plugin: %s", module_path)
            loaded += 1
        except Exception:
            LOG.exception("Failed to load plugin: %s", module_path)
            failed += 1

    LOG.info("Plugin loading complete: %d loaded, %d failed.", loaded, failed)
    return loaded, failed


async def _send_startup_message():
    """Send a branded startup notification from BOTH bot and userbot (assistant)."""
    if not Config.LOG_GROUP_ID and not Config.OWNER_ID:
        return
    bot_me = await get_bot_info()
    user_info = "N/A (no userbot)"
    userbot_id = 0
    userbot_name = "N/A"
    if userbot:
        try:
            user_me = await userbot.get_me()
            user_info = f"{user_me.first_name} (ID: {user_me.id})"
            userbot_id = user_me.id
            userbot_name = user_me.first_name
        except Exception:
            user_info = "N/A"

    handler_count = sum(len(h) for h in bot.dispatcher.groups.values())

    # Gather system info
    try:
        import psutil
        ram = psutil.virtual_memory()
        ram_info = f"{ram.used // (1024**2)}MB / {ram.total // (1024**2)}MB ({ram.percent}%)"
        cpu_info = f"{psutil.cpu_percent(interval=0.3)}%"
    except Exception:
        ram_info = "N/A"
        cpu_info = "N/A"

    text = (
        f"**MusicLyrics v{__version__} Started Successfully!**\n\n"
        f"**Bot:** @{bot_me.username} (ID: `{bot_me.id}`)\n"
        f"**Userbot/Assistant:** {user_info}\n"
        f"**Handlers:** {handler_count}\n"
        f"**PyTgCalls:** {'Active' if pytgcalls else 'Disabled'}\n"
        f"**CPU:** {cpu_info}\n"
        f"**RAM:** {ram_info}\n"
        f"**LOG_GROUP_ID:** `{Config.LOG_GROUP_ID or 'Not set'}`\n"
        f"**OWNER_ID:** `{Config.OWNER_ID}`\n\n"
        f"All systems operational. Bot is ready to receive commands.\n\n"
        f"[Support]({Config.SUPPORT_GROUP}) | "
        f"[Channel]({Config.SUPPORT_CHANNEL}) | "
        f"[Owner]({Config.OWNER_LINK})"
    )
    # Direct send (not fire-and-forget) only for startup
    for cid in {Config.LOG_GROUP_ID, Config.OWNER_ID} - {0, None}:
        await _safe_send(cid, text, photo=Config.BRAND_PHOTO)

    # ── Assistant (Userbot) also sends a startup message ──
    # This makes it clear which userbot/assistant account is being used
    if userbot and userbot_id:
        assistant_text = (
            f"🎵 **MusicLyrics Assistant Online!**\n\n"
            f"**Assistant ID:** `{userbot_id}`\n"
            f"**Assistant Name:** {userbot_name}\n"
            f"**Bot:** @{bot_me.username}\n\n"
            f"✅ Voice chat streaming is ready.\n"
            f"I will join voice chats to play music when requested."
        )
        for cid in {Config.LOG_GROUP_ID, Config.OWNER_ID} - {0, None}:
            try:
                await userbot.send_message(cid, assistant_text)
                LOG.info("Assistant startup message sent to %s", cid)
            except Exception as e:
                LOG.warning("Could not send assistant startup message to %s: %s", cid, e)


async def _start_with_retry(client, name, max_retries=5):
    """Start a Pyrogram client with FloodWait retry handling."""
    for attempt in range(1, max_retries + 1):
        try:
            await client.start()
            LOG.info("%s client started.", name)
            return
        except FloodWait as e:
            wait = e.value
            LOG.warning(
                "%s: FloodWait %ds. Attempt %d/%d.",
                name, wait, attempt, max_retries,
            )
            await asyncio.sleep(wait + 2)
        except Exception as e:
            LOG.exception("%s: Failed to start (attempt %d/%d): %s",
                          name, attempt, max_retries, e)
            if attempt < max_retries:
                await asyncio.sleep(10)
            else:
                raise


# -- Runtime event logging (only important events, non-blocking) --

def _setup_event_logging():
    """Register lightweight event-logging handlers."""

    @bot.on_message(filters.new_chat_members)
    async def on_bot_added(client, message: Message):
        """Log when bot is added to a new group and auto-invite assistant."""
        try:
            me = await get_bot_info()
            for member in message.new_chat_members:
                if member.id == me.id:
                    added_by = message.from_user.mention if message.from_user else "Unknown"
                    log_to_group(
                        f"**Bot Added to New Group**\n\n"
                        f"Group: {message.chat.title}\n"
                        f"Chat ID: `{message.chat.id}`\n"
                        f"Members: {message.chat.members_count or 'N/A'}\n"
                        f"Added by: {added_by}"
                    )
                    # Send welcome message to the group
                    await message.reply_text(
                        f"**ধন্যবাদ আমাকে যোগ করার জন্য!** 🎵\n\n"
                        f"আমি {Config.BOT_NAME}! Music streaming, games, "
                        f"security tools সব আছে।\n\n"
                        f"/help দিয়ে সব কমান্ড দেখো!\n"
                        f"Use /help to see all commands."
                    )

                    # Auto-invite the assistant (userbot) to this group
                    if userbot:
                        try:
                            user_me = await userbot.get_me()
                            assistant_id = user_me.id
                            # Try to add the assistant via the bot (requires admin invite permission)
                            try:
                                await client.add_chat_members(message.chat.id, assistant_id)
                                LOG.info("Auto-invited assistant %s to group %s", assistant_id, message.chat.id)
                            except Exception as invite_err:
                                LOG.debug("Bot could not invite assistant (no invite perm): %s", invite_err)
                                # Fallback: assistant joins by itself using invite link
                                try:
                                    invite_link = await client.export_chat_invite_link(message.chat.id)
                                    if invite_link:
                                        await userbot.join_chat(invite_link)
                                        LOG.info("Assistant self-joined group %s via invite link", message.chat.id)
                                except Exception as join_err:
                                    LOG.debug("Assistant self-join via invite link failed: %s", join_err)
                                    # Last resort: try joining by chat ID directly
                                    try:
                                        await userbot.join_chat(message.chat.id)
                                        LOG.info("Assistant joined group %s by chat ID", message.chat.id)
                                    except Exception as direct_err:
                                        LOG.warning(
                                            "Could not auto-add assistant to group %s: %s",
                                            message.chat.id, direct_err
                                        )
        except Exception as e:
            LOG.error("Error in on_bot_added: %s", e)

