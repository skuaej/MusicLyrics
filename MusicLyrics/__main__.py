"""MusicLyrics entry-point -- run with ``python -m MusicLyrics``."""

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

    # Use print + sys.stderr because logging may not be configured yet
    def _err(msg):
        print(msg, file=sys.stderr, flush=True)
        print(msg, flush=True)

    # ── Step 1: Check LICENSE file ──
    license_path = Path(__file__).parent.parent / "LICENSE"

    if not license_path.exists():
        _err(
            "\n"
            "╔══════════════════════════════════════════════════════════════╗\n"
            "║  ❌  LICENSE FILE MISSING!                                  ║\n"
            "║                                                            ║\n"
            "║  This software is proprietary and requires the LICENSE     ║\n"
            "║  file to be present. Unauthorized use is prohibited.       ║\n"
            "║                                                            ║\n"
            f"║  Owner: {_LICENSE_OWNER:<51}║\n"
            f"║  Repo:  {_LICENSE_REPO:<51}║\n"
            "║                                                            ║\n"
            "║  Please restore the LICENSE file or contact the owner.     ║\n"
            "╚══════════════════════════════════════════════════════════════╝\n"
        )
        sys.exit(1)

    try:
        content = license_path.read_text(encoding="utf-8")
    except Exception:
        _err("❌ Cannot read LICENSE file. Aborting.")
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
            _err(
                f"\n❌ LICENSE file has been tampered with!\n"
                f"   Missing required text: '{marker}'\n"
                f"   Owner: {_LICENSE_OWNER}\n"
                f"   Repo: {_LICENSE_REPO}\n"
                f"\n   Restore the original LICENSE file to continue.\n"
            )
            sys.exit(1)

    print(f"✅ License file OK — {_LICENSE_OWNER} Proprietary License", flush=True)

    # ── Step 2: Validate LICENSE_KEY from environment variable ──
    license_key = os.environ.get("LICENSE_KEY", "").strip()
    bot_token = os.environ.get("BOT_TOKEN", "").strip()

    if not license_key:
        _err(
            "\n"
            "╔══════════════════════════════════════════════════════════════╗\n"
            "║  ❌  LICENSE_KEY NOT SET!                                   ║\n"
            "║                                                            ║\n"
            "║  To deploy this bot you need a valid LICENSE_KEY.          ║\n"
            "║  Set it in your environment variables / Config Vars:       ║\n"
            "║                                                            ║\n"
            "║    LICENSE_KEY=ML-XXXXXXXX-XXXXXXXX                        ║\n"
            "║                                                            ║\n"
            "║  Get your key from the owner:                              ║\n"
            "║    Telegram: @R4J_81                                       ║\n"
            "║    GitHub:   github.com/RajSukh81                          ║\n"
            "║                                                            ║\n"
            "║  Send your BOT_TOKEN to @R4J_81 to receive your key.      ║\n"
            "╚══════════════════════════════════════════════════════════════╝\n"
        )
        sys.exit(1)

    if not bot_token:
        _err("❌ BOT_TOKEN is not set. Cannot validate license.")
        sys.exit(1)

    # Generate expected key from bot_token and compare
    expected_key = _generate_license_key(bot_token)

    if not hmac.compare_digest(license_key, expected_key):
        _err(
            "\n"
            "╔══════════════════════════════════════════════════════════════╗\n"
            "║  ❌  INVALID LICENSE_KEY!                                   ║\n"
            "║                                                            ║\n"
            "║  The LICENSE_KEY you provided does not match your          ║\n"
            "║  BOT_TOKEN. Each key is unique per bot.                    ║\n"
            "║                                                            ║\n"
            "║  Possible reasons:                                         ║\n"
            "║    • Wrong LICENSE_KEY                                      ║\n"
            "║    • BOT_TOKEN changed after key was issued                ║\n"
            "║    • Key was generated for a different bot                 ║\n"
            "║                                                            ║\n"
            "║  Contact @R4J_81 on Telegram for a new key.               ║\n"
            "╚══════════════════════════════════════════════════════════════╝\n"
        )
        sys.exit(1)

    print(
        f"✅ License key validated — {license_key}\n"
        f"   Authorized by: {_LICENSE_OWNER}\n"
        f"   Source: {_LICENSE_REPO}",
        flush=True,
    )


# Run license check immediately on import
_verify_license()

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
                                            message.chat.id, direct_err,
                                        )
                        except Exception as e:
                            LOG.debug("Assistant auto-invite error: %s", e)

                    break
        except Exception:
            LOG.exception("Error in on_bot_added")

    @bot.on_message(filters.left_chat_member)
    async def on_bot_removed(client, message: Message):
        """Log when bot is removed from a group."""
        try:
            me = await get_bot_info()
            if message.left_chat_member and message.left_chat_member.id == me.id:
                removed_by = message.from_user.mention if message.from_user else "Unknown"
                log_to_group(
                    f"**Bot Removed from Group**\n\n"
                    f"Group: {message.chat.title}\n"
                    f"Chat ID: `{message.chat.id}`\n"
                    f"Removed by: {removed_by}"
                )
        except Exception:
            LOG.exception("Error in on_bot_removed")

    @bot.on_message(filters.command([
        "play", "p", "vplay", "vp", "song", "vsong",
    ]) & filters.group, group=98)
    async def _log_music_commands(client, message: Message):
        """Log music commands in groups to owner."""
        if not message.from_user:
            return
        user = message.from_user
        cmd = message.text or ""
        log_to_group(
            f"**Music Command**\n\n"
            f"**Group:** {message.chat.title}\n"
            f"**Chat ID:** `{message.chat.id}`\n"
            f"**User:** {user.mention} (`{user.id}`)\n"
            f"**Command:** `{cmd[:200]}`"
        )

    LOG.info("Event logging handlers registered.")


def _setup_catchall_handler():
    """Register a catch-all handler so the bot always responds."""

    @bot.on_message(filters.command([
        "start", "help", "play", "p", "vplay", "vp", "pause", "resume",
        "skip", "next", "stop", "end", "seek", "volume", "vol", "queue",
        "nowplaying", "np", "loop", "shuffle", "song", "vsong", "ping",
        "alive", "ban", "unban", "mute", "unmute", "warn", "antispam",
        "antiflood", "captcha", "blacklist", "setwelcome", "tr", "tts",
        "sticker", "s", "toimg", "kang", "getsticker", "stickerid",
        "info", "chatinfo", "paste", "telegraph", "tagall", "afk",
        "react", "reactall", "emoji", "mixemoji", "randomemoji",
        "broadcast", "stats", "addsudo", "rmsudo", "sudolist",
        "status", "ttt", "quiz", "truth", "dare", "flip", "dice",
        "wordseek", "kill", "pin", "unpin", "purge",
        "filter", "filters", "clearfilter", "notes", "save", "get",
    ]) & filters.private, group=99)
    async def _log_private_commands(client, message: Message):
        """Log all private commands to owner/log group."""
        if not message.from_user:
            return
        user = message.from_user
        cmd = message.text or message.caption or ""
        log_to_group(
            f"**Private Command**\n\n"
            f"**User:** {user.mention} (`{user.id}`)\n"
            f"**Username:** @{user.username or 'N/A'}\n"
            f"**Command:** `{cmd[:200]}`"
        )

    @bot.on_message(~filters.me & ~filters.service & filters.private, group=100)
    async def _catchall_private(client, message: Message):
        """Respond to unrecognized private messages with unknown command notice."""
        if message.text and message.text.startswith("/"):
            cmd = message.text.split()[0]
            await message.reply_text(
                f"❌ **Unknown command:** `{cmd}`\n\n"
                f"কমান্ড তালিকা দেখতে /help দিন।\n"
                f"Use /help to see all available commands."
            )
        # Non-command private messages are handled by the AI chat plugin

    LOG.info("Catch-all and mention handlers registered.")


async def _delete_webhook():
    """Explicitly delete any Telegram webhook and drop pending updates."""
    url = f"https://api.telegram.org/bot{Config.BOT_TOKEN}/deleteWebhook"
    params = {"drop_pending_updates": True}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, json=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                result = await resp.json()
                if result.get("result"):
                    LOG.info("Webhook deleted + pending updates dropped.")
                else:
                    LOG.warning("deleteWebhook response: %s", result)
    except Exception as e:
        LOG.warning("Could not delete webhook: %s", e)
        # Fallback: try GET method
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{url}?drop_pending_updates=true",
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    result = await resp.json()
                    LOG.info("Webhook delete fallback response: %s", result)
        except Exception as e2:
            LOG.error("Webhook delete fallback also failed: %s", e2)


async def _check_bot_info():
    """Log bot info for diagnostics."""
    url = f"https://api.telegram.org/bot{Config.BOT_TOKEN}/getWebhookInfo"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                result = await resp.json()
                info = result.get("result", {})
                webhook_url = info.get("url", "")
                pending = info.get("pending_update_count", 0)
                LOG.info(
                    "Webhook info: url=%s, pending_updates=%d, last_error=%s",
                    webhook_url or "(none)",
                    pending,
                    info.get("last_error_message", "none"),
                )
                if webhook_url:
                    LOG.warning(
                        "WEBHOOK IS SET to '%s'! This prevents long polling. "
                        "Deleting it now...", webhook_url
                    )
                    await _delete_webhook()
    except Exception as e:
        LOG.warning("Could not check webhook info: %s", e)


async def _check_mongo():
    """Check MongoDB connectivity and log the result."""
    try:
        from MusicLyrics.mongo.db import _client
        info = await _client.server_info()
        LOG.info("MongoDB connected — version %s", info.get("version", "?"))
        return True
    except Exception as e:
        LOG.error(
            "MongoDB UNREACHABLE: %s\n"
            "  MONGO_URL: %s\n"
            "  Features needing DB (filters, warns, notes, sudo, welcome) "
            "will NOT work until MongoDB is available.",
            e,
            Config.MONGO_URL[:30] + "..." if len(Config.MONGO_URL) > 30 else Config.MONGO_URL,
        )
        return False


def _log_config_summary():
    """Log all config vars status so deployment issues are obvious."""
    LOG.info("=== CONFIG SUMMARY ===")
    LOG.info("API_ID:            %s", "SET" if Config.API_ID else "MISSING")
    LOG.info("API_HASH:          %s", "SET" if Config.API_HASH else "MISSING")
    LOG.info("BOT_TOKEN:         %s", "SET (ends ...%s)" % Config.BOT_TOKEN[-6:] if Config.BOT_TOKEN else "MISSING")
    LOG.info("STRING_SESSION:    %s", "SET" if Config.STRING_SESSION else "NOT SET (music streaming disabled)")
    LOG.info("MONGO_URL:         %s", "SET" if Config.MONGO_URL != "mongodb://localhost:27017/musiclyrics" else "DEFAULT (localhost)")
    LOG.info("OWNER_ID:          %s", Config.OWNER_ID or "NOT SET")
    LOG.info("SUDO_USERS:        %s", Config.SUDO_USERS or "NONE")
    LOG.info("LOG_GROUP_ID:      %s", Config.LOG_GROUP_ID or "NOT SET")
    LOG.info("YOUTUBE_PROXY:     %s", "SET" if Config.YOUTUBE_PROXY else "NOT SET")
    LOG.info("YOUTUBE_PROXIES:   %d proxies", len(Config.YOUTUBE_PROXIES))
    LOG.info("SPOTIFY:           %s", "SET" if Config.SPOTIFY_CLIENT_ID else "NOT SET")
    LOG.info("AI_API_KEY:        %s", "SET" if Config.AI_API_KEY else "NOT SET")
    LOG.info("======================")


def _install_global_error_handler():
    """Install a global error handler on the bot's dispatcher.

    Pyrogram silently swallows exceptions in handlers. This decorator
    wraps every registered handler so unhandled errors are logged to
    console, log file, AND sent to the owner/log group.
    """
    from pyrogram.handlers import MessageHandler, CallbackQueryHandler

    for group_id, handlers in bot.dispatcher.groups.items():
        for i, handler in enumerate(handlers):
            original_cb = handler.callback

            async def _wrapped(client, update, _cb=original_cb, _gid=group_id):
                try:
                    return await _cb(client, update)
                except FloodWait as e:
                    LOG.warning("FloodWait %ds in handler %s (group %d)",
                                e.value, _cb.__name__, _gid)
                    await asyncio.sleep(e.value + 1)
                except Exception as exc:
                    LOG.exception(
                        "UNHANDLED ERROR in handler '%s' (group %d): %s",
                        _cb.__name__, _gid, exc,
                    )
                    # Notify owner about the crash
                    try:
                        import traceback
                        tb = traceback.format_exc()[-1200:]
                        err_text = (
                            f"**Handler Crash Report**\n\n"
                            f"**Handler:** `{_cb.__name__}` (group {_gid})\n"
                            f"**Error:** `{str(exc)[:200]}`\n\n"
                            f"```\n{tb}\n```"
                        )
                        log_to_group(err_text)
                    except Exception:
                        pass

            handler.callback = _wrapped

    LOG.info("Global error handler installed on all %d handler groups.",
             len(bot.dispatcher.groups))


async def main():
    """Start the bot, userbot, and py-tgcalls, then idle."""

    # Install the bounded extractor executor BEFORE any blocking call
    # is scheduled with loop.run_in_executor(None, ...). Otherwise the
    # tiny default pool gets saturated by yt-dlp / network calls and
    # the bot hangs on multi-track queues.
    _install_executor()
    LOG.info(
        "Installed bounded ThreadPoolExecutor (max_workers=%d) as loop default.",
        EXTRACTOR_EXECUTOR._max_workers,
    )

    # Log config summary first so missing vars are immediately visible
    _log_config_summary()

    # CRITICAL: Delete webhook FIRST — if a webhook is set, Telegram sends
    # all updates there and long-polling receives NOTHING.
    LOG.info("Step 1: Checking and deleting webhook...")
    await _check_bot_info()
    await _delete_webhook()
    # Wait a moment for Telegram to process the webhook deletion
    await asyncio.sleep(2)

    # Step 1.5: Check MongoDB connectivity
    LOG.info("Step 1.5: Checking MongoDB connectivity...")
    await _check_mongo()

    LOG.info("Step 2: Loading plugins...")
    result = _load_plugins()

    handler_count = sum(len(h) for h in bot.dispatcher.groups.values())
    LOG.info("Total handlers registered: %d", handler_count)
    if handler_count == 0:
        LOG.warning("No handlers registered after plugin loading!")

    LOG.info("Step 3: Starting bot client...")
    await _start_with_retry(bot, "Bot")

    # Double-check: delete webhook again AFTER bot.start() in case
    # Pyrogram re-set it during handshake -- but do NOT drop pending
    # updates this time, as that would discard updates Pyrogram is
    # already polling for and can desync the getUpdates offset.
    _wh_url = f"https://api.telegram.org/bot{Config.BOT_TOKEN}/deleteWebhook"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                _wh_url, json={"drop_pending_updates": False},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                _wh_resp = await resp.json()
                LOG.info("Post-start webhook delete: %s", _wh_resp)
    except Exception as e:
        LOG.warning("Post-start webhook delete failed: %s", e)

    # Verify bot can receive updates (also populates the cache)
    bot_me = await get_bot_info()
    LOG.info("Bot identity: @%s (ID: %d)", bot_me.username, bot_me.id)

    from MusicLyrics.userbot import get_all_userbots, get_all_pytgcalls

    if Config.STRING_SESSION and userbot and pytgcalls:
        LOG.info("Step 4: Starting userbot pool + PyTgCalls...")
        for idx, ub in enumerate(get_all_userbots(), start=1):
            await _start_with_retry(ub, f"Userbot[{idx}]")
        for ptc in get_all_pytgcalls():
            try:
                await ptc.start()
                LOG.info("PyTgCalls instance started: %s", getattr(ptc, 'client', ptc))
            except Exception:
                LOG.exception("PyTgCalls start failed for %s", getattr(ptc, 'client', ptc))
                log_to_group(
                    "**Warning:** PyTgCalls failed to start for one assistant.\n"
                    "Music streaming may not work for some groups."
                )
    else:
        LOG.info("Step 4: Skipped userbot (STRING_SESSION not set).")

    # Setup event logging (lightweight, non-blocking)
    LOG.info("Step 5: Setting up event logging and catch-all handler...")
    _setup_event_logging()
    _setup_catchall_handler()

    # Install global error handler AFTER all handlers are registered
    LOG.info("Step 6: Installing global error handler...")
    _install_global_error_handler()

    await _send_startup_message()

    if result:
        loaded, failed = result
        if failed > 0:
            log_to_group(
                f"**Plugin Loading Report**\n\n"
                f"Loaded: {loaded}\nFailed: {failed}\n"
                "Check server logs for details."
            )

    LOG.info("MusicLyrics v%s is running. Waiting for updates...", __version__)
    await idle()

    # Graceful shutdown
    LOG.info("Shutting down MusicLyrics...")
    log_to_group("**MusicLyrics is shutting down...**")
    try:
        if Config.STRING_SESSION and pytgcalls:
            try:
                calls = pytgcalls.calls
                if asyncio.iscoroutine(calls):
                    calls = await calls
                if isinstance(calls, (dict, set, list)):
                    _leave_fn = getattr(pytgcalls, 'leave_call', None) or getattr(pytgcalls, 'leave_group_call', None)
                    if _leave_fn:
                        for chat_id in list(calls):
                            try:
                                await _leave_fn(chat_id)
                            except Exception:
                                pass
            except Exception:
                LOG.warning("Could not leave active calls during shutdown.")
        if Config.STRING_SESSION and userbot:
            await userbot.stop()
        await bot.stop()
    except Exception:
        LOG.exception("Error during shutdown.")
    try:
        EXTRACTOR_EXECUTOR.shutdown(wait=False, cancel_futures=True)
    except Exception:
        pass
    LOG.info("Goodbye.")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    # Install a process-wide asyncio exception handler so unhandled
    # exceptions in background tasks (prefetch, watchdogs, py-tgcalls
    # callbacks, etc.) are logged rather than allowed to crash the whole
    # bot.  Many of the recent "frequent crash" reports come from a
    # single exception in a background coroutine going unhandled and
    # tearing down the event loop.  This handler turns those into
    # warnings the bot can recover from.
    def _global_async_excepthook(loop, context):
        msg = context.get("message", "Unhandled async exception")
        exc = context.get("exception")
        try:
            if exc:
                LOG.warning("ASYNC EXCEPTION: %s — %r", msg, exc)
            else:
                LOG.warning("ASYNC EXCEPTION: %s — %r", msg, context)
        except Exception:
            # Last-ditch print so we never lose visibility
            print(f"ASYNC EXCEPTION: {msg}", file=sys.stderr, flush=True)

    try:
        loop.set_exception_handler(_global_async_excepthook)
    except Exception:
        pass

    loop.run_until_complete(main())
