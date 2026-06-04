"""AI chatbot plugin -- replies to messages intelligently.

Uses Google Gemini API with automatic model fallback.
Properly answers all kinds of questions — factual, conversational, etc.
Falls back to curated Bengali/English responses when API is unavailable.
"""

import asyncio
import logging
import random
import re
import time
from collections import deque
from typing import Optional

import aiohttp

from pyrogram import filters
from pyrogram.types import Message
from pyrogram.enums import ChatType

from MusicLyrics.bot import bot, get_bot_info
from config import Config

LOG = logging.getLogger(__name__)

# ── Conversation history per chat ─────────────────────────────────────────────
_MAX_HISTORY = 20
_chat_histories: dict[int, deque] = {}

# ── API rate limit tracking ───────────────────────────────────────────────────
_model_cooldown_until: dict[str, float] = {}
# Reduced cooldown: free tier allows 15 RPM for flash-lite, 10 RPM for flash.
# Short cooldowns let us retry quickly when quota resets (per-minute quota).
_API_COOLDOWN_SECONDS = 10
_API_COOLDOWN_BACKOFF = 1.5
_model_cooldown_multiplier: dict[str, float] = {}
_MAX_COOLDOWN_MULTIPLIER = 4.0

_EMOJI_REACTIONS = [
    "\U0001f44d", "\u2764\ufe0f", "\U0001f525", "\U0001f60d",
    "\U0001f929", "\U0001f44f", "\U0001f601", "\U0001f60e",
    "\U0001f92f", "\U0001f389", "\U0001f4af", "\U0001f923",
    "\U0001f64f", "\u26a1", "\U0001f31a",
]

# ── Sticker file_ids for sending with AI replies ─────────────────────────────
# Categories mapped to keyword patterns for contextual sticker sending.
# These are public sticker file_ids from common Telegram sticker packs.
_STICKER_KEYWORDS = {
    "happy": {
        "keywords": ["happy", "খুশি", "ভালো", "দারুণ", "awesome", "great", "🎉", "😊", "😁", "হাসি", "মজা", "fun"],
        "stickers": [
            "CAACAgIAAxkBAAEBjN1kXZCG5PuIZWTwNzQ2Ybvqjy1zGwACEQADwDZPE_lqX5qklMEYLwQ",
            "CAACAgIAAxkBAAEBjN9kXZCrYi5MzWbOmhPQYFXJNpYAAbQAAhIAA8A2TxMCnMnjkqQV1i8E",
        ],
    },
    "sad": {
        "keywords": ["sad", "দুঃখ", "কষ্ট", "মন খারাপ", "crying", "😢", "😭", "কান্না"],
        "stickers": [
            "CAACAgIAAxkBAAEBjOFkXZC2TL9kZ3dQk3Bj5mN0aahwmgACDAADwDZPE8xyMGwsGNfqLwQ",
        ],
    },
    "love": {
        "keywords": ["love", "ভালোবাসা", "প্রেম", "❤️", "😍", "ভালোবাসি", "পছন্দ", "miss"],
        "stickers": [
            "CAACAgIAAxkBAAEBjONkXZC_TqzrKA8_5mcI4M3WqbSfZAACDgADwDZPEwZZOGMecFaILwQ",
        ],
    },
    "greeting": {
        "keywords": ["hi", "hello", "হ্যালো", "হাই", "নমস্কার", "hey", "সুপ্রভাত"],
        "stickers": [
            "CAACAgIAAxkBAAEBjOVkXZDIxuSMixqrAAFlaG0jFGEr_nkAAg8AA8A2TxP2aWMj6Y5yYC8E",
        ],
    },
    "thanks": {
        "keywords": ["thanks", "ধন্যবাদ", "thank you", "ty", "thx", "শুকরিয়া"],
        "stickers": [
            "CAACAgIAAxkBAAEBjOdkXZDThfD5HqlOavK8TfXPFWEJqgACEAADwDZPE5BC-2FQMJN8LwQ",
        ],
    },
}

# Probability of sending a sticker with AI reply (40%)
_STICKER_SEND_PROBABILITY = 0.4


def _get_history(chat_id: int) -> deque:
    if chat_id not in _chat_histories:
        _chat_histories[chat_id] = deque(maxlen=_MAX_HISTORY)
    return _chat_histories[chat_id]


# ── Gemini API models (ordered by preference) ────────────────────────────────
# Updated May 2026 — ONLY models confirmed working on Google Generative AI API.
# Models removed from API (return 404): gemini-1.5-flash, gemini-1.5-flash-8b,
# gemini-1.5-pro, gemini-pro, gemini-2.0-flash-exp
#
# Strategy: Use multiple VALID models so when one hits 429 rate limit,
# the next one picks up. All 2.0+ models have separate quotas.
_GEMINI_MODELS = [
    "gemini-2.5-flash",            # Latest 2.5 flash — best quality + speed
    "gemini-2.0-flash",            # Stable 2.0 flash — good fallback
    "gemini-2.0-flash-lite",       # Lite model — NO systemInstruction support
]

# Models that do NOT support the system_instruction field.
# For these, the system prompt is prepended as the first user message.
_NO_SYSTEM_INSTRUCTION_MODELS = {
    "gemini-2.0-flash-lite",
}

# Also try v1 endpoint (not just v1beta) as some models are only on v1
_GEMINI_API_VERSIONS = ["v1beta", "v1"]

# ── Offline fallback messages ────────────────────────────────────────────────
# 50+ curated Bengali messages for when AI API is down or unreachable.
# These provide warm, natural responses so the bot never feels "dead".

_OFFLINE_BANGLA_REPLIES = [
    "হ্যালো! আমি এখন একটু ব্যস্ত আছি, কিন্তু তোমার কথা শুনছি! একটু পরে আবার চেষ্টা করো।",
    "কী খবর তোমার? আমার AI সার্ভার এখন একটু রেস্ট নিচ্ছে। একটু পরে আবার কথা বলি!",
    "আরে! আমি তো আছি তোমার সাথে। এখন একটু সমস্যা হচ্ছে, পরে ঠিক হয়ে যাবে।",
    "তোমার মেসেজ পেয়েছি! AI একটু ঘুমাচ্ছে এখন। জেগে উঠলেই তোমাকে সুন্দর উত্তর দেবে!",
    "ওই যে! তুমি এসেছো? দারুণ! কিন্তু এখন সার্ভার একটু slow, পরে আবার ট্রাই করো।",
    "বন্ধু, তোমার সাথে কথা বলতে ভালো লাগে! এখন AI সার্ভিস একটু বিরতিতে আছে।",
    "শুভ সময়! আমি MusicLyrics Bot। এই মুহূর্তে AI একটু বিশ্রাম নিচ্ছে, তবে গান শুনতে /play দাও!",
    "তুমি জানো, আমি কিন্তু শুধু AI না — গানও বাজাতে পারি! /play দিয়ে চেষ্টা করো!",
    "হ্যাঁ বলো! কী লাগবে? গান লাগলে /play, ভিডিও লাগলে /vplay দাও।",
    "আমার AI ব্রেইন এখন চার্জ হচ্ছে! ইতিমধ্যে /play দিয়ে একটা গান শোনো!",
    "কেমন আছো? আশা করি ভালো! আমার সার্ভার একটু ঝামেলা করছে, কিন্তু গান বাজানো চলবে!",
    "দোস্ত, AI এখন একটু অফলাইন। তবে আমার বাকি সব ফিচার কিন্তু কাজ করছে!",
    "তোমাকে দেখে খুব ভালো লাগলো! এখন AI সেবা সাময়িক বন্ধ, একটু পরে আবার আসো।",
    "ভাই/বোন, AI একটু ডাউন আছে। কিন্তু চিন্তা করো না, শীঘ্রই ঠিক হয়ে যাবে!",
    "মজার কথা বলতে পারতাম কিন্তু AI সার্ভার এখন একটু মুড অফ! পরে আবার এসো।",
    "তুমি কি গান শুনতে চাও? /play দিয়ে তোমার favourite গান বাজাও!",
    "AI একটু বিরতিতে আছে, কিন্তু আমি তো আছি! কী সাহায্য লাগবে?",
    "এই মুহূর্তে AI respond করতে পারছে না। /help দিয়ে আমার সব কমান্ড দেখো!",
    "সার্ভার মেইনটেন্যান্স চলছে। একটু ধৈর্য ধরো, শীঘ্রই ফিরে আসবো!",
    "তোমার প্রশ্ন পেয়েছি! কিন্তু এখন AI ইঞ্জিন একটু বিশ্রামে। পরে আবার জিজ্ঞেস করো।",
    "দারুণ প্রশ্ন! কিন্তু আমার AI মস্তিষ্ক এখন রিচার্জ হচ্ছে। একটু পরে আবার চেষ্টা করো!",
    "আমি MusicLyrics Bot — গান, গেম, আর চ্যাট সব পারি! এখন AI একটু রেস্টে।",
    "তোমাকে সুন্দর উত্তর দিতাম, কিন্তু AI সার্ভার একটু সমস্যায় আছে। Sorry!",
    "হেই! আমি এখানে আছি। AI সাময়িকভাবে unavailable, কিন্তু /play দিয়ে গান চালু করো!",
    "চিন্তা করো না! AI সার্ভার একটু পরেই ঠিক হয়ে যাবে। ততক্ষণ /quiz খেলো!",
    "তোমার সাথে আড্ডা দিতে ভালোই লাগে! কিন্তু AI এখন একটু ব্রেক নিচ্ছে।",
    "দুঃখিত! এখন AI respond করতে পারছে না। তবে বাকি সব কমান্ড ঠিকঠাক কাজ করছে!",
    "ও মাই! AI একটু মুড অফ আছে আজকে। কিন্তু গান বাজাতে কোনো সমস্যা নেই!",
    "তোমার জন্য দোয়া করি! AI সার্ভার শীঘ্রই ঠিক হবে। ততক্ষণ /truth or /dare খেলো!",
    "বন্ধু, প্রযুক্তি মাঝে মাঝে বিশ্রাম নেয়। AI একটু পরে আবার কাজ শুরু করবে!",
    "হ্যাঁরে! কী করছো? AI একটু slow আছে আজকে। কিন্তু আমি তো আছি!",
    "তোমার মেসেজ important! AI ঠিক হলেই উত্তর দেবো। ততক্ষণ /song দিয়ে গান ডাউনলোড করো!",
    "ভালোবাসা রইলো! AI সার্ভার একটু down, কিন্তু তোমার পাশে সবসময় আছি।",
    "আমি জানি তুমি ভালো উত্তর চাও! AI ঠিক হলে সুন্দর করে উত্তর দেবো, promise!",
    "কিছুক্ষণের মধ্যে AI আবার কাজ করবে। ততক্ষণ /flip দিয়ে coin toss করো!",
    "তোমার ধৈর্যের জন্য ধন্যবাদ! AI service একটু পরেই restore হবে।",
    "এখন AI একটু অফলাইন, কিন্তু গান বাজানো, গেম খেলা সব চলবে!",
    "সবকিছু ঠিক আছে! শুধু AI সার্ভিস একটু slow। একটু wait করো।",
    "আমি MusicLyrics — তোমার বিশ্বস্ত music bot! AI পরে আসবে, এখন গান শোনো!",
    "প্রিয় বন্ধু, AI সার্ভার maintenance চলছে। /play দিয়ে মনটা ভালো করো!",
    "তোমার প্রশ্নের উত্তর দিতে পারলে ভালো লাগতো! AI একটু পরে ফিরবে।",
    "জানো কি? /ttt দিয়ে Tic-Tac-Toe খেলতে পারো! AI ঠিক না হওয়া পর্যন্ত!",
    "একটু সমস্যা হচ্ছে AI তে। তবে /dice দিয়ে luck try করো!",
    "AI সার্ভার একটু tired আজকে। /sticker কমান্ড দিয়ে মজা করো!",
    "সময়টা একটু কঠিন AI এর জন্য! কিন্তু /tr দিয়ে translate করতে পারো!",
    "আমি শুনছি তোমার কথা! AI ফিরে এলেই সুন্দর উত্তর পাবে।",
    "তোমার জন্য সবসময় ready আছি! AI server একটু rest নিচ্ছে শুধু।",
    "মন খারাপ? /play Arijit Singh দিয়ে গান শোনো! AI পরে কথা বলবে।",
    "এই bot এ অনেক কিছু আছে! /help দিয়ে explore করো!",
    "AI temporarily unavailable. কিন্তু music, games, tools সব কাজ করছে!",
    "তুমি awesome! AI একটু পরে তোমার সাথে কথা বলবে। Promise!",
    "সবুর করো, ফল মিঠা হবে! AI server শীঘ্রই ফিরছে।",
    "হাসতে থাকো, গান শোনো! AI repair হচ্ছে, চিন্তা নেই!",
    "তোমার message পড়েছি! AI fix হলেই reply দেবো। Love you!",
    "Bot alive আছে, চিন্তা নেই! শুধু AI brain একটু nap নিচ্ছে।",
    "এক মিনিট! AI server reconnect হচ্ছে। একটু patience please!",
]

_OFFLINE_ENGLISH_REPLIES = [
    "Hey there! My AI brain is taking a quick nap. Try again in a bit!",
    "I'm here for you! AI server is temporarily unavailable, but music still works! Try /play",
    "Oops! AI is on a coffee break right now. Meanwhile, check /help for all my features!",
    "Hello! AI service is briefly down. But I can still play music — try /play!",
    "My AI engine is recharging. In the meantime, play some music with /play!",
    "Sorry, AI is temporarily offline. Try /quiz or /truth for some fun!",
    "AI server maintenance in progress. I'll be back smarter than ever!",
    "Can't process AI requests right now. Try /song to download music!",
    "Hey! AI is having a moment. But all other features work perfectly!",
    "AI will be back soon! Meanwhile, try /play to enjoy some music!",
]

# Smart keyword-based offline responses
_OFFLINE_KEYWORD_RESPONSES = {
    # Greetings
    "greetings": {
        "keywords": ["hi", "hello", "hey", "হ্যালো", "হাই", "নমস্কার", "আসসালামু", "সুপ্রভাত", "শুভ"],
        "bn": [
            "হ্যালো! কেমন আছো? আমি MusicLyrics Bot! তোমাকে সাহায্য করতে পারি — /help দাও!",
            "নমস্কার! তোমাকে দেখে ভালো লাগলো! কী সাহায্য করতে পারি?",
            "হ্যালো বন্ধু! আমি এখানে আছি তোমার জন্য! কী চাই বলো?",
            "আসসালামু আলাইকুম! কেমন আছেন? কিছু দরকার হলে বলুন!",
        ],
        "en": [
            "Hello! I'm MusicLyrics Bot. How can I help you? Try /help!",
            "Hey there! Nice to see you! What can I do for you?",
            "Hi! I'm here to help with music, games, and more!",
        ],
    },
    # Music related
    "music": {
        "keywords": ["গান", "song", "music", "play", "বাজাও", "শোনাও", "গানটা", "মিউজিক"],
        "bn": [
            "গান শুনতে চাও? /play দিয়ে গানের নাম লেখো! যেমন: /play তুমি হি হো",
            "গান বাজাতে /play কমান্ড ব্যবহার করো! ভিডিও সহ চাইলে /vplay দাও!",
            "মিউজিক শুনতে চাও? /play <গানের নাম> দাও! ডাউনলোড করতে /song দাও!",
        ],
        "en": [
            "Want to play music? Use /play <song name>! For video: /vplay",
            "Try /play to stream music in voice chat, or /song to download!",
        ],
    },
    # Bot info
    "bot_info": {
        "keywords": ["কে তুমি", "who are you", "তোমার নাম", "your name", "কি পারো", "what can you"],
        "bn": [
            "আমি MusicLyrics Bot! আমি গান বাজাতে পারি, গেম খেলতে পারি, চ্যাট করতে পারি! /help দাও!",
            "আমার নাম MusicLyrics Bot। আমি @R4J_81 এর তৈরি। গান, গেম, AI চ্যাট সব পারি!",
        ],
        "en": [
            "I'm MusicLyrics Bot, created by @R4J_81! I can play music, games, and chat with AI!",
            "I'm a multi-feature Telegram bot — music streaming, games, security tools, and more!",
        ],
    },
    # Thanks
    "thanks": {
        "keywords": ["ধন্যবাদ", "thanks", "thank you", "thx", "ty", "শুকরিয়া"],
        "bn": [
            "তোমাকেও ধন্যবাদ! তোমার সাথে কথা বলে ভালো লাগলো!",
            "স্বাগতম! আবার কিছু লাগলে বলো!",
            "কিছু না! তোমার জন্য সবসময় আছি!",
        ],
        "en": [
            "You're welcome! Happy to help!",
            "No problem! Let me know if you need anything else!",
        ],
    },
    # Help
    "help": {
        "keywords": ["help", "সাহায্য", "কমান্ড", "command", "কি করতে পারো"],
        "bn": [
            "আমার সব কমান্ড দেখতে /help দাও! গান, গেম, AI চ্যাট সব আছে!",
            "/help দাও — সব কমান্ডের লিস্ট দেখতে পাবে! গান বাজাতে /play দাও!",
        ],
        "en": [
            "Use /help to see all commands! I can play music, games, and more!",
            "Try /help for the full command list! Music: /play, Games: /quiz, /ttt",
        ],
    },
}


def _detect_language(text: str) -> str:
    """Detect if text is primarily Bengali or English."""
    bengali_chars = sum(1 for c in text if '\u0980' <= c <= '\u09FF')
    return "bn" if bengali_chars > len(text) * 0.2 else "en"


def _get_keyword_response(text: str) -> Optional[str]:
    """Try to match keywords for a smart offline response."""
    text_lower = text.lower()
    lang = _detect_language(text)

    for category, data in _OFFLINE_KEYWORD_RESPONSES.items():
        for kw in data["keywords"]:
            if kw.lower() in text_lower:
                responses = data.get(lang, data.get("bn", []))
                if responses:
                    return random.choice(responses)
    return None


def _get_offline_response(text: str) -> str:
    """Get a smart offline response — keyword match first, then random."""
    # Try keyword-based response first
    kw_resp = _get_keyword_response(text)
    if kw_resp:
        return kw_resp

    # Fall back to random message based on detected language
    lang = _detect_language(text)
    if lang == "en":
        return random.choice(_OFFLINE_ENGLISH_REPLIES)
    return random.choice(_OFFLINE_BANGLA_REPLIES)


# ── Clean AI reply — remove filler text ─────────────────────────────────────

_FILLER_PATTERNS = [
    r"\n+(?:Let me know|Hope this helps|Feel free|Don't hesitate|If you (?:need|have|want)).*$",
    r"\n+(?:আর কিছু জানতে চাইলে|আরো কিছু|কিছু জানতে চাইলে|আমাকে জানাও|আমি সবসময়|তোমার জন্য).*$",
    r"\n+---+\n.*$",
]


def _clean_ai_reply(text: str) -> str:
    """Remove generic filler text that Gemini sometimes appends."""
    for pat in _FILLER_PATTERNS:
        text = re.sub(pat, "", text, flags=re.DOTALL | re.IGNORECASE)
    return text.strip()


# ── System prompt ─────────────────────────────────────────────────────────────
_SYSTEM_PROMPT = (
    "You are MusicLyrics Bot — a smart, friendly, witty, and extremely helpful "
    "Telegram bot created by @R4J_81 (Owner).\n\n"
    "CRITICAL RULES — FOLLOW STRICTLY:\n"
    "1. You MUST answer ALL questions properly, accurately, and completely. "
    "Whether it's about Python, math, history, science, geography, current "
    "affairs, coding, relationships, philosophy, or anything else — give the "
    "CORRECT answer. You are a KNOWLEDGEABLE and INTELLIGENT assistant.\n"
    "2. LANGUAGE MATCHING is MANDATORY:\n"
    "   - If the user writes in Bengali (বাংলা), you MUST reply in Bengali.\n"
    "   - If the user writes in English, reply in English.\n"
    "   - If mixed, reply in mixed.\n"
    "   - NEVER reply in English when the user writes in Bengali.\n"
    "3. Keep replies concise (1-3 sentences). Do NOT add disclaimers, "
    "notes, or extra explanations after your answer. Just answer directly.\n"
    "4. Be warm, friendly, witty, and engaging — like a smart best friend.\n"
    "5. Use emojis naturally but sparingly (1-3 per message max).\n"
    "6. If asked about your features or commands, mention:\n"
    "   - /play <song> — Play music in voice chat\n"
    "   - /vplay <song> — Play video in voice chat\n"
    "   - /song <query> — Download song\n"
    "   - /vsong <query> — Download video\n"
    "   - /pause, /resume, /skip, /stop — Playback controls\n"
    "   - /queue, /np, /loop, /shuffle — Queue management\n"
    "   - /quiz, /truth, /dare, /ttt, /flip, /dice — Games\n"
    "   - /tr, /tts, /sticker, /info — Tools\n"
    "7. You are created by @R4J_81 (Owner). Be proud of it!\n"
    "8. NEVER refuse to answer a question. Always try your best.\n"
    "9. For questions you genuinely don't know, say so honestly but suggest "
    "where to find the answer.\n"
    "10. Do NOT give generic filler responses. Every reply must be meaningful "
    "and add value.\n"
    "11. If someone is sad, cheer them up. If someone is happy, celebrate with them.\n"
    "12. You can handle adult topics maturely without being inappropriate.\n"
    "13. Be helpful with coding questions — provide actual code examples.\n"
    "14. For math questions, show step-by-step solutions when appropriate.\n"
    "15. NEVER add extra lines like 'Let me know if you need anything else' "
    "or 'Hope this helps!' at the end. Just give the answer and stop.\n"
    "16. Do NOT repeat the question back to the user. Just answer directly.\n"
)


async def _ai_response(text: str, chat_id: int = 0, user_name: str = "") -> str:
    """Get AI response from Gemini API with model fallback."""

    if not Config.AI_API_KEY:
        LOG.warning("AI_API_KEY not set — using offline response")
        return _get_offline_response(text)

    history = _get_history(chat_id)

    # Try each Gemini model with each API version (skip those in cooldown)
    last_error = ""
    for model in _GEMINI_MODELS:
        for api_ver in _GEMINI_API_VERSIONS:
            cache_key = f"{api_ver}/{model}"
            if time.time() < _model_cooldown_until.get(cache_key, 0):
                LOG.debug("Model %s in cooldown, skipping", cache_key)
                continue
            result, error = await _try_gemini(
                model, text, chat_id, user_name, history, api_ver
            )
            if result:
                return result
            if error:
                last_error = error
                # If 404, don't try same model on other API version
                if "model_not_found" in error:
                    break

    # All models failed — use smart offline response
    LOG.error("All Gemini models failed for chat %s. Last error: %s", chat_id, last_error)
    return _get_offline_response(text)


async def _try_gemini(
    model: str, text: str, chat_id: int,
    user_name: str, history: deque,
    api_ver: str = "v1beta",
) -> tuple[Optional[str], str]:
    """Try a single Gemini model.

    Returns (reply, error_msg). reply is None on failure.
    """

    cache_key = f"{api_ver}/{model}"

    # Try v1beta first (supports newer models), fall back to v1
    url = (
        f"https://generativelanguage.googleapis.com/{api_ver}/"
        f"models/{model}:generateContent"
    )

    # Build conversation history
    contents = []
    for role, msg in history:
        contents.append({"role": role, "parts": [{"text": msg}]})

    # Add current user message
    user_text = text
    if user_name:
        user_text = f"[{user_name}]: {text}"
    contents.append({"role": "user", "parts": [{"text": user_text}]})

    # The "system_instruction" field is ONLY supported on the v1beta API.
    # The v1 API and certain models (gemini-2.0-flash-lite) do NOT support it
    # and return 400 "Unknown name system_instruction: Cannot find field."
    # For those, prepend the system prompt as the first user message instead.
    supports_system_instruction = (
        api_ver == "v1beta"
        and model not in _NO_SYSTEM_INSTRUCTION_MODELS
    )

    if supports_system_instruction:
        payload = {
            "systemInstruction": {"parts": [{"text": _SYSTEM_PROMPT}]},
            "contents": contents,
            "generationConfig": {
                "maxOutputTokens": 500,
                "temperature": 0.8,
                "topP": 0.95,
                "topK": 40,
            },
            "safetySettings": [
                {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_ONLY_HIGH"},
                {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_ONLY_HIGH"},
                {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_ONLY_HIGH"},
                {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_ONLY_HIGH"},
            ],
        }
    else:
        # Inject system prompt as the first user turn for models without
        # system_instruction support
        system_as_user = [
            {"role": "user", "parts": [{"text": f"[SYSTEM INSTRUCTIONS — follow these strictly]\n{_SYSTEM_PROMPT}"}]},
            {"role": "model", "parts": [{"text": "Understood! I will follow these instructions. I am MusicLyrics Bot, ready to help!"}]},
        ]
        payload = {
            "contents": system_as_user + contents,
            "generationConfig": {
                "maxOutputTokens": 500,
                "temperature": 0.8,
                "topP": 0.95,
                "topK": 40,
            },
            "safetySettings": [
                {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_ONLY_HIGH"},
                {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_ONLY_HIGH"},
                {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_ONLY_HIGH"},
                {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_ONLY_HIGH"},
            ],
        }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, json=payload,
                headers={
                    "Content-Type": "application/json",
                    "x-goog-api-key": Config.AI_API_KEY,
                },
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    candidates = data.get("candidates", [])
                    if candidates:
                        # Check if response was blocked by safety
                        finish_reason = candidates[0].get("finishReason", "")
                        if finish_reason == "SAFETY":
                            LOG.warning("Gemini %s: response blocked by safety filter", cache_key)
                            return None, "safety_blocked"

                        parts = candidates[0].get("content", {}).get("parts", [])
                        if parts:
                            reply = parts[0].get("text", "").strip()
                            if reply:
                                # Clean up extra filler text that AI sometimes adds
                                reply = _clean_ai_reply(reply)
                                # Save to history
                                history.append(("user", text))
                                history.append(("model", reply))
                                _model_cooldown_multiplier[cache_key] = 1.0
                                LOG.info("Gemini %s replied for chat %s (%d chars)",
                                         cache_key, chat_id, len(reply))
                                return reply, ""
                    # No valid response in candidates
                    LOG.warning("Gemini %s: empty candidates for chat %s. Response: %s",
                                cache_key, chat_id, str(data)[:300])
                    return None, "empty_response"

                elif resp.status == 429:
                    multiplier = _model_cooldown_multiplier.get(cache_key, 1.0)
                    cooldown = _API_COOLDOWN_SECONDS * multiplier
                    _model_cooldown_until[cache_key] = time.time() + cooldown
                    _model_cooldown_multiplier[cache_key] = min(
                        multiplier * _API_COOLDOWN_BACKOFF,
                        _MAX_COOLDOWN_MULTIPLIER,
                    )
                    LOG.warning("Gemini %s: 429 quota exceeded. Cooldown %.0fs.", cache_key, cooldown)
                    return None, "rate_limited"

                elif resp.status == 404:
                    body = await resp.text()
                    LOG.error("Gemini %s: 404 NOT FOUND — model may be invalid. Body: %s",
                              cache_key, body[:200])
                    # Permanently cooldown invalid models for 1 hour
                    _model_cooldown_until[cache_key] = time.time() + 3600
                    return None, f"model_not_found:{cache_key}"

                elif resp.status in (400, 403):
                    body = await resp.text()
                    LOG.error("Gemini %s: HTTP %d — API key or request issue. Body: %s",
                              cache_key, resp.status, body[:300])
                    return None, f"http_{resp.status}"

                else:
                    body = await resp.text()
                    LOG.warning("Gemini %s: HTTP %d. Body: %s", cache_key, resp.status, body[:200])
                    return None, f"http_{resp.status}"

    except asyncio.TimeoutError:
        LOG.warning("Gemini %s: timeout (20s)", cache_key)
        return None, "timeout"
    except Exception as e:
        LOG.warning("Gemini %s: exception: %s", cache_key, e)
        return None, str(e)


# ── Reactions ─────────────────────────────────────────────────────────────────

async def _try_react(client, message: Message):
    """Send a random emoji reaction on the user's message."""
    try:
        emoji = random.choice(_EMOJI_REACTIONS)
        # Method 1: pyrofork / pyrogram v2 with ReactionTypeEmoji list
        try:
            from pyrogram.types import ReactionTypeEmoji
            await client.send_reaction(
                chat_id=message.chat.id,
                message_id=message.id,
                emoji=[ReactionTypeEmoji(emoji=emoji)],
            )
            return
        except (ImportError, TypeError, AttributeError):
            pass
        # Method 2: plain emoji string
        try:
            await client.send_reaction(
                chat_id=message.chat.id,
                message_id=message.id,
                emoji=emoji,
            )
            return
        except TypeError:
            pass
        # Method 3: reaction parameter
        try:
            from pyrogram.types import ReactionTypeEmoji
            await client.send_reaction(
                chat_id=message.chat.id,
                message_id=message.id,
                reaction=[ReactionTypeEmoji(emoji=emoji)],
            )
            return
        except Exception:
            pass
    except Exception:
        pass


async def _try_send_sticker(client, message: Message, user_text: str, reply_text: str):
    """Optionally send a contextual sticker after the AI reply.

    Uses keyword matching on both user text and AI reply to pick a relevant
    sticker. Only sends with a probability to avoid being spammy.
    """
    try:
        if random.random() > _STICKER_SEND_PROBABILITY:
            return  # Skip most of the time to not be spammy

        combined = (user_text + " " + reply_text).lower()
        matched_stickers = []

        for category, data in _STICKER_KEYWORDS.items():
            for kw in data["keywords"]:
                if kw.lower() in combined:
                    matched_stickers.extend(data["stickers"])
                    break  # Only match once per category

        if not matched_stickers:
            return  # No matching sticker found

        sticker_id = random.choice(matched_stickers)
        await client.send_sticker(
            chat_id=message.chat.id,
            sticker=sticker_id,
        )
    except Exception as e:
        LOG.debug("Sticker send failed (non-critical): %s", e)


# ── Filters (using cached get_bot_info to avoid FloodWait) ───────────────────

async def _is_reply_to_bot(_, client, message: Message) -> bool:
    if not message.text or message.text.startswith("/"):
        return False
    if not message.reply_to_message or not message.reply_to_message.from_user:
        return False
    try:
        me = await get_bot_info()
        return message.reply_to_message.from_user.id == me.id
    except Exception:
        return False

_reply_to_bot_filter = filters.create(_is_reply_to_bot, name="ReplyToBotFilter")


async def _is_bot_mentioned(_, client, message: Message) -> bool:
    if not message.text or message.text.startswith("/"):
        return False
    try:
        me = await get_bot_info()
        return f"@{me.username}" in (message.text or "")
    except Exception:
        return False

_bot_mentioned_filter = filters.create(_is_bot_mentioned, name="BotMentionedFilter")


def _get_user_name(message: Message) -> str:
    if message.from_user:
        return message.from_user.first_name or ""
    return ""


# ── Handlers ──────────────────────────────────────────────────────────────────

@bot.on_message(filters.group & _reply_to_bot_filter, group=50)
async def ai_reply_when_replied(client, message: Message):
    try:
        user_text = message.text or ""
        if not user_text.strip():
            return
        await _try_react(client, message)
        response = await _ai_response(
            user_text, chat_id=message.chat.id,
            user_name=_get_user_name(message),
        )
        if response:
            await message.reply_text(response)
            await _try_send_sticker(client, message, user_text, response)
    except Exception:
        LOG.exception("AI reply error")


@bot.on_message(filters.group & _bot_mentioned_filter, group=51)
async def ai_reply_when_mentioned(client, message: Message):
    try:
        me = await get_bot_info()
        clean_text = (message.text or "").replace(f"@{me.username}", "").strip()
        if not clean_text:
            clean_text = "hi"
        await _try_react(client, message)
        response = await _ai_response(
            clean_text, chat_id=message.chat.id,
            user_name=_get_user_name(message),
        )
        if response:
            await message.reply_text(response)
            await _try_send_sticker(client, message, clean_text, response)
    except Exception:
        LOG.exception("AI mention reply error")


@bot.on_message(filters.private & filters.text, group=52)
async def ai_reply_private(client, message: Message):
    user_text = message.text or ""
    if not user_text.strip() or user_text.startswith("/"):
        return
    try:
        await _try_react(client, message)
        response = await _ai_response(
            user_text, chat_id=message.chat.id,
            user_name=_get_user_name(message),
        )
        if response:
            await message.reply_text(response)
            await _try_send_sticker(client, message, user_text, response)
    except Exception:
        LOG.exception("AI private reply error")
