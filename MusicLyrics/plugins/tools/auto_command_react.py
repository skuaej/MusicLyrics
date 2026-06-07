"""Global auto-reaction plugin — reacts to ALL bot commands with emoji reactions,
big emoji replies, and random sticker/dice sends.

Uses handler group 97 (very high) so it runs AFTER all real command handlers.
Does NOT use continue_propagation() — just a fire-and-forget watcher.

Compatible with ALL pyrogram versions (no ReactionTypeEmoji dependency).
"""

from __future__ import annotations

import random
import asyncio
import logging
import time

from pyrogram import Client, filters
from pyrogram.types import Message

from MusicLyrics.bot import bot
from MusicLyrics.utils import sticker_pool

LOG = logging.getLogger(__name__)

# Lazy-load guard: load the configured sticker packs on first command.
_pool_load_lock = asyncio.Lock()
_pool_load_attempted = False


async def _ensure_pool_loaded(client):
    """Load sticker packs once, on first command. Safe to call repeatedly.

    Also kicks off the background refresh loop so the pool stays warm
    and file_references don't go stale on long-running deployments.
    """
    global _pool_load_attempted
    if _pool_load_attempted:
        # Even after first load, check whether a stale flag / expiry timer
        # warrants a fresh fetch in the background.
        try:
            asyncio.create_task(sticker_pool.refresh_if_needed(client))
        except Exception:
            pass
        return
    async with _pool_load_lock:
        if _pool_load_attempted:
            return
        _pool_load_attempted = True
        try:
            await sticker_pool.load_all_packs(client)
            sticker_pool.start_background_refresh(client)
        except Exception as e:
            LOG.warning("Sticker pool load failed: %s", e)


# ── Telegram-supported reaction emojis (large pool) ────────────────────────
REACTION_POOL = [
    "\U0001f44d",  # 👍
    "\U0001f44e",  # 👎
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
    "\U0001f44c",  # 👌
    "\U0001f60e",  # 😎
    "\U0001f618",  # 😘
    "\U0001f64f",  # 🙏
    "\U0001f31a",  # 🌚
    "\U0001f37e",  # 🍾
    "\U0001f48b",  # 💋
    "\U0001f607",  # 😇
    "\U0001f92f",  # 🤯
    "\U0001f62d",  # 😭
    "\U0001f608",  # 😈
    "\U0001f440",  # 👀
    "\U0001f47b",  # 👻
    "\U0001f383",  # 🎃
    "\U0001f913",  # 🤓
    "\U0001f633",  # 😳
    "\U0001f353",  # 🍓
    "\U0001f34c",  # 🍌
    "\U0001f494",  # 💔
    "\U0001f648",  # 🙈
    "\U0001f634",  # 😴
    "\U0001f928",  # 🧐
    "\U0001f32d",  # 🌭
    "\U0001f60b",  # 😋
    "\U0001f631",  # 😱
    "\U0001f921",  # 🤡
    "\U0001f973",  # 🥳
    "\U0001f480",  # 💀
    "\u2764\ufe0f\u200d\U0001f525",  # ❤️‍🔥
    "\U0001f54a\ufe0f",  # 🕊️
    "\U0001f911",  # 🤑
    "\U0001f917",  # 🤗
    "\U0001f643",  # 🙃
]

# ── Big emoji grids for reply ───────────────────────────────────────────────
BIG_EMOJI_SETS = [
    "🔥🔥🔥🔥🔥\n🔥🔥🔥🔥🔥\n🔥🔥🔥🔥🔥",
    "⚡⚡⚡⚡⚡\n⚡⚡⚡⚡⚡\n⚡⚡⚡⚡⚡",
    "🎵🎵🎵🎵🎵\n🎵🎵🎵🎵🎵\n🎵🎵🎵🎵🎵",
    "💫💫💫💫💫\n💫💫💫💫💫\n💫💫💫💫💫",
    "🌟🌟🌟🌟🌟\n🌟🌟🌟🌟🌟\n🌟🌟🌟🌟🌟",
    "❤️❤️❤️❤️❤️\n❤️❤️❤️❤️❤️\n❤️❤️❤️❤️❤️",
    "🦋🦋🦋🦋🦋\n🦋🦋🦋🦋🦋\n🦋🦋🦋🦋🦋",
    "✨✨✨✨✨\n✨✨✨✨✨\n✨✨✨✨✨",
    "🎶🎶🎶🎶🎶\n🎶🎶🎶🎶🎶\n🎶🎶🎶🎶🎶",
    "💖💖💖💖💖\n💖💖💖💖💖\n💖💖💖💖💖",
    "🏆🏆🏆🏆🏆\n🏆🏆🏆🏆🏆\n🏆🏆🏆🏆🏆",
    "🎉🎉🎉🎉🎉\n🎉🎉🎉🎉🎉\n🎉🎉🎉🎉🎉",
    "👑👑👑👑👑\n👑👑👑👑👑\n👑👑👑👑👑",
    "🌈🌈🌈🌈🌈\n🌈🌈🌈🌈🌈\n🌈🌈🌈🌈🌈",
    "💎💎💎💎💎\n💎💎💎💎💎\n💎💎💎💎💎",
    "🎸🎸🎸🎸🎸\n🎸🎸🎸🎸🎸\n🎸🎸🎸🎸🎸",
    "🌹🌹🌹🌹🌹\n🌹🌹🌹🌹🌹\n🌹🌹🌹🌹🌹",
    "🍀🍀🍀🍀🍀\n🍀🍀🍀🍀🍀\n🍀🍀🍀🍀🍀",
    "🔮🔮🔮🔮🔮\n🔮🔮🔮🔮🔮\n🔮🔮🔮🔮🔮",
    "🎪🎪🎪🎪🎪\n🎪🎪🎪🎪🎪\n🎪🎪🎪🎪🎪",
]

DICE_EMOJIS = ["🎲", "🎯", "🏀", "⚽", "🎳", "🎰"]

# Cooldown tracker per chat
_last_react_time: dict[int, float] = {}
_REACT_COOLDOWN = 4  # seconds


async def _send_reaction_safe(chat_id: int, message_id: int):
    """Send a random emoji reaction (fire-and-forget, all pyrogram versions)."""
    emoji = random.choice(REACTION_POOL)
    for attempt in range(4):
        try:
            if attempt == 0:
                await bot.send_reaction(chat_id, message_id, emoji=emoji)
            elif attempt == 1:
                await bot.send_reaction(chat_id, message_id, emoji=[emoji])
            elif attempt == 2:
                await bot.send_reaction(chat_id, message_id, reaction=emoji)
            else:
                try:
                    from pyrogram.types import ReactionTypeEmoji
                    await bot.send_reaction(chat_id, message_id, emoji=[ReactionTypeEmoji(emoji=emoji)])
                except ImportError:
                    pass
            return
        except Exception:
            continue


async def _try_send_sticker(message: Message, command: str | None, chat_id: int) -> bool:
    """Pick + send a sticker. On stale-cache errors, refresh once and retry.

    Returns True if a sticker was sent successfully, False otherwise.
    """
    for attempt in range(2):
        file_id = sticker_pool.pick_sticker(command=command, chat_id=chat_id)
        if not file_id:
            return False
        try:
            sticker_msg = await message.reply_sticker(sticker=file_id)
            await _send_reaction_safe(chat_id, sticker_msg.id)
            return True
        except Exception as e:
            if sticker_pool.is_stale_error(e) and attempt == 0:
                LOG.info("Sticker file_reference stale (%s) — refreshing pool.", e)
                sticker_pool.mark_stale()
                try:
                    await sticker_pool.refresh_if_needed(message._client)
                except Exception as r:
                    LOG.warning("Pool refresh after stale error failed: %s", r)
                continue  # retry once with fresh file_ids
            LOG.debug("reply_sticker failed (%s) — falling back to emoji.", e)
            return False
    return False


async def _send_big_reaction_and_sticker(chat_id: int, message: Message, command: str | None = None):
    """Send a sticker / big emoji / dice as reply (with cooldown).

    Mix (when sticker pool is loaded):
      * 65% real sticker from the curated pool (mood-matched to command)
      * 25% big emoji grid
      * 10% dice
    If the pool is empty, falls back to the original 55/45 emoji/dice mix.
    """
    now = time.time()
    if chat_id in _last_react_time:
        if now - _last_react_time[chat_id] < _REACT_COOLDOWN:
            return
    _last_react_time[chat_id] = now

    roll = random.random()
    pool_ready = sticker_pool.is_ready()

    # Prefer real stickers when available
    if pool_ready and roll < 0.65:
        sent_ok = await _try_send_sticker(message, command, chat_id)
        if sent_ok:
            return
        # If sticker failed (after retry attempt), fall through to emoji/dice

    # Big emoji grid
    if roll < 0.90 or not pool_ready and roll < 0.55:
        emoji_grid = random.choice(BIG_EMOJI_SETS)
        try:
            big_msg = await message.reply_text(emoji_grid)
            await _send_reaction_safe(chat_id, big_msg.id)
            return
        except Exception:
            pass

    # Dice fallback
    dice_emoji = random.choice(DICE_EMOJIS)
    try:
        dice_msg = await message.reply_dice(emoji=dice_emoji)
        await _send_reaction_safe(chat_id, dice_msg.id)
    except Exception:
        try:
            await message.reply_text(random.choice(BIG_EMOJI_SETS))
        except Exception:
            pass


# ── Watcher handler at group=97 (runs AFTER all real handlers) ──────────────
# This does NOT use continue_propagation(). It sits in a high group number
# so pyrogram runs it after the real command handler in group 0 is done.
# It just adds reactions silently — no interference with actual commands.

@bot.on_message(filters.command([
    "start", "help", "pause", "resume",
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
    "emojirain", "emojiart", "emojistory", "emojimood",
    "rps", "guess", "emojichain", "typerace",
    "antilink", "antiraid", "slowmode", "report", "reports",
    "autoreact", "reactpoll", "reactcombo",
    "locks", "lock", "unlock", "nsfw",
    "ai", "ask",
]) & ~filters.edited, group=97)
async def _global_command_reactor(client: Client, message: Message):
    """React to commands with emoji + big emoji/sticker reply.

    Runs in group 97 — AFTER all real handlers. No continue_propagation needed.
    Play commands are excluded (they have their own reactions).
    """
    if not message.from_user:
        return

    chat_id = message.chat.id

    # First-time lazy load of the configured sticker packs
    await _ensure_pool_loaded(client)

    # Extract the command word (without leading slash, without @username)
    cmd_text = (message.text or "").lstrip().split()
    cmd_word = None
    if cmd_text and cmd_text[0].startswith("/"):
        cmd_word = cmd_text[0][1:].split("@", 1)[0].lower()

    # Add reaction to the original command message
    try:
        await _send_reaction_safe(chat_id, message.id)
    except Exception:
        pass

    # Send big emoji / sticker reply
    try:
        await _send_big_reaction_and_sticker(chat_id, message, command=cmd_word)
    except Exception:
        pass
