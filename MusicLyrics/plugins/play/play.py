"""Handler for /play <query|url> — audio playback in voice chat.

Uses stream URL extraction as primary method (no download needed),
with download as fallback. Based on patterns from YukkiMusicBot.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re

from pyrogram import Client, filters
from pyrogram.types import (
    Message,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)

from MusicLyrics.bot import bot
from MusicLyrics.helpers.filters import not_edited
from config import Config

from MusicLyrics.plugins.play.queue import (
    QueueItem,
    add_to_queue,
    get_chat_queue,
    clear_queue,
    format_duration,
)
from MusicLyrics.plugins.play.stream import (
    stream_audio,
    is_active,
    pre_join_vc,
    _now_playing_messages,
    _control_keyboard,
    _get_next_color,
    _get_current_theme,
    _start_progress_timer,
    _stop_progress_timer,
    _add_reaction,
    leave_voice_chat,
    _get_skip_lock,
)
from MusicLyrics.plugins.play.platforms.youtube import (
    search_youtube,
    get_audio_stream_url,
    download_audio,
    get_video_info,
    is_youtube_url,
    search_and_download_audio,
)
from MusicLyrics.plugins.play.platforms.spotify import (
    is_spotify_url,
    get_spotify_track,
    get_spotify_playlist,
)
from MusicLyrics.plugins.play.platforms.jiosaavn import (
    is_jiosaavn_url,
    get_jiosaavn_song,
    download_jiosaavn,
    search_and_download_jiosaavn,
    get_jiosaavn_stream_url,
    search_jiosaavn,
)
from MusicLyrics.plugins.play.platforms.apple_music import (
    is_apple_music_url,
    get_apple_music_track,
)
from MusicLyrics.plugins.play.platforms.soundcloud import (
    is_soundcloud_url,
    search_soundcloud,
    get_soundcloud_info,
    download_soundcloud,
    get_soundcloud_stream_url,
    search_and_download_soundcloud,
)
from MusicLyrics.utils.autodelete import (
    auto_delete_service,
    auto_delete_playing,
    auto_delete_cmd,
)

LOG = logging.getLogger(__name__)


def _detect_platform(text: str) -> str:
    """Return platform name from a URL or 'query' for plain text."""
    if is_youtube_url(text):
        return "youtube"
    if is_spotify_url(text):
        return "spotify"
    if is_jiosaavn_url(text):
        return "jiosaavn"
    if is_apple_music_url(text):
        return "apple_music"
    if is_soundcloud_url(text):
        return "soundcloud"
    if re.match(r"https?://", text):
        return "direct_url"
    return "query"


async def _get_audio_media(url: str) -> tuple[str, bool]:
    """Get media path for audio playback.

    Returns (media_path, is_stream_url).
    PRIORITY (user-requested, matches /vplay flow):
    Step 1: yt-dlp download FIRST (sequential await) — most reliable
    Step 2: ONLY if Step 1 fails: YouTube stream URL
    Step 3: ONLY if Step 2 fails: title-search + JioSaavn + SoundCloud
    """
    import asyncio as _aio

    # ── Step 1: yt-dlp direct download FIRST (strict priority) ──
    LOG.info("Trying yt-dlp direct download FIRST for: %s", url)
    try:
        fp = await download_audio(url)
        if fp and os.path.isfile(fp):
            LOG.info("yt-dlp direct download succeeded for: %s", url)
            return fp, False
    except Exception as e:
        LOG.debug("yt-dlp direct download failed for %s: %s", url, e)

    # ── Step 2: YouTube stream URL fallback ──
    LOG.info("Direct download failed, trying YouTube stream URL for: %s", url)
    try:
        su = await get_audio_stream_url(url)
        if su:
            LOG.info("Stream URL obtained for: %s", url)
            return su, True
    except Exception as e:
        LOG.debug("Stream URL extraction failed for %s: %s", url, e)

    # ── Step 3: YouTube failed — title-search + JioSaavn + SoundCloud ──
    LOG.info("YouTube methods failed, trying title-search + JioSaavn + SoundCloud for: %s", url)

    from MusicLyrics.plugins.play.platforms.youtube import get_video_info
    info = await get_video_info(url)
    title = info.get("title", "") if info else ""

    async def _try_ytdlp_search():
        if title and title not in ("YouTube Audio", "Unknown"):
            try:
                fp, _ = await search_and_download_audio(title)
                if fp and os.path.isfile(fp):
                    return fp, False
            except Exception:
                pass
        return None

    async def _try_jiosaavn():
        if not title or title in ("YouTube Audio", "Unknown"):
            return None
        try:
            jp, ji = await search_and_download_jiosaavn(title)
            if jp and os.path.isfile(jp):
                return jp, False
        except Exception:
            pass
        return None

    async def _try_soundcloud():
        if not title or title in ("YouTube Audio", "Unknown"):
            return None
        try:
            sp, si = await search_and_download_soundcloud(title)
            if sp:
                if si and si.get("_is_stream_url"):
                    return sp, True
                if os.path.isfile(sp):
                    return sp, False
        except Exception:
            pass
        return None

    fb_tasks = [
        _aio.create_task(_try_ytdlp_search()),
        _aio.create_task(_try_jiosaavn()),
        _aio.create_task(_try_soundcloud()),
    ]
    fb_pending = set(fb_tasks)
    while fb_pending:
        done, fb_pending = await _aio.wait(fb_pending, return_when=_aio.FIRST_COMPLETED)
        for task in done:
            try:
                result = task.result()
                if result:
                    for p in fb_pending:
                        p.cancel()
                    return result
            except Exception:
                pass

    return "", False


async def _resolve_query(query: str, platform: str, msg: Message):
    """Resolve the user query into (info_dict, media_path, is_stream_url) or raise."""

    # -- YouTube URL
    if platform == "youtube":
        info = await get_video_info(query)
        if not info:
            # Try searching by URL as query
            info = {"title": "YouTube Audio", "url": query,
                    "duration": 0, "thumbnail": "", "channel": ""}
        if info["duration"] > Config.DURATION_LIMIT_MIN * 60 and info["duration"] > 0:
            raise ValueError(
                f"গানটি {Config.DURATION_LIMIT_MIN} মিনিটের বেশি, "
                "play করা যাবে না।"
            )
        media_path, is_stream = await _get_audio_media(query)
        if not media_path:
            # Fallback: try search+download with title as query
            title_query = info.get("title", "")
            channel_query = info.get("channel", "")
            if title_query and title_query != "YouTube Audio":
                LOG.info("YouTube URL extraction failed, trying search+download: %s", title_query)
                filepath, dl_info = await search_and_download_audio(
                    f"{title_query} {channel_query}".strip()
                )
                if filepath:
                    return (dl_info or info), filepath, False
            raise ValueError("YouTube থেকে audio পাওয়া যায়নি। আবার চেষ্টা করুন।")
        return info, media_path, is_stream

    # -- Spotify
    if platform == "spotify":
        track = await get_spotify_track(query)
        if not track:
            raise ValueError("Spotify link parse করা যায়নি।")
        # Try JioSaavn first (often has Indian songs Spotify links point to)
        LOG.info("Spotify: trying JioSaavn for: %s", track["query"])
        js_path, js_info = await search_and_download_jiosaavn(track["query"])
        if js_path and js_info:
            import os as _os
            if _os.path.isfile(js_path):
                info = {
                    "title": js_info.get("title", track.get("title", "Unknown")),
                    "url": js_info.get("url", query),
                    "duration": js_info.get("duration", track.get("duration", 0)),
                    "thumbnail": js_info.get("thumbnail", track.get("thumbnail", "")),
                    "channel": js_info.get("artist", track.get("artist", "")),
                    "platform": "jiosaavn",
                }
                return info, js_path, False
        # Then try YouTube
        yt = await search_youtube(track["query"])
        if yt:
            media_path, is_stream = await _get_audio_media(yt["url"])
            if media_path:
                info = {**yt, "platform": "spotify"}
                return info, media_path, is_stream
        # Fallback: yt-dlp search+download
        LOG.info("Spotify -> YouTube failed, trying yt-dlp search+download: %s", track["query"])
        filepath, dl_info = await search_and_download_audio(track["query"])
        if filepath and dl_info:
            return dl_info, filepath, False
        # SoundCloud as last resort
        LOG.info("Spotify -> all failed, trying SoundCloud for: %s", track["query"])
        sc_path, sc_info = await search_and_download_soundcloud(track["query"])
        if sc_path and sc_info:
            is_stream = bool(sc_info.get("_is_stream_url"))
            info = {
                "title": sc_info.get("title", track.get("title", "Unknown")),
                "url": sc_info.get("url", query),
                "duration": sc_info.get("duration", track.get("duration", 0)),
                "thumbnail": sc_info.get("thumbnail", track.get("thumbnail", "")),
                "channel": sc_info.get("channel", track.get("artist", "")),
                "platform": "soundcloud",
            }
            return info, sc_path, is_stream
        raise ValueError("YouTube ও SoundCloud কোথাও গানটি পাওয়া যায়নি।")

    # -- JioSaavn
    if platform == "jiosaavn":
        song = await get_jiosaavn_song(query)
        if not song:
            raise ValueError("JioSaavn link থেকে তথ্য পাওয়া যায়নি।")
        # Try JioSaavn direct download first (CDN URL — fastest, most reliable)
        filepath = await download_jiosaavn(query, song_info=song)
        if filepath:
            import os as _os
            if _os.path.isfile(filepath):
                info = {
                    "title": song["title"], "url": song["url"],
                    "duration": song["duration"],
                    "thumbnail": song.get("thumbnail", ""),
                    "channel": song.get("artist", ""),
                    "platform": "jiosaavn",
                }
                return info, filepath, False
        # Try JioSaavn stream URL (no disk write)
        if song.get("download_url"):
            LOG.info("JioSaavn download failed, using stream URL for: %s", song["title"])
            info = {
                "title": song["title"], "url": song["url"],
                "duration": song["duration"],
                "thumbnail": song.get("thumbnail", ""),
                "channel": song.get("artist", ""),
                "platform": "jiosaavn",
            }
            return info, song["download_url"], True
        # Fallback to YouTube
        yt = await search_youtube(f"{song['title']} {song.get('artist','')}")
        if yt:
            media_path, is_stream = await _get_audio_media(yt["url"])
            if media_path:
                info = {
                    "title": song["title"], "url": song["url"],
                    "duration": song["duration"],
                    "thumbnail": song.get("thumbnail", ""),
                    "channel": song.get("artist", ""),
                    "platform": "jiosaavn",
                }
                return info, media_path, is_stream
        # Fallback to SoundCloud as LAST RESORT
        LOG.info("JioSaavn -> YouTube all failed, trying SoundCloud for: %s", song["title"])
        sc_query = f"{song['title']} {song.get('artist', '')}".strip()
        sc_path, sc_info = await search_and_download_soundcloud(sc_query)
        if sc_path and sc_info:
            is_stream = bool(sc_info.get("_is_stream_url"))
            info = {
                "title": song["title"], "url": song["url"],
                "duration": song["duration"],
                "thumbnail": song.get("thumbnail", ""),
                "channel": song.get("artist", ""),
                "platform": "soundcloud",
            }
            return info, sc_path, is_stream
        raise ValueError("গানটি কোথাও পাওয়া যায়নি।")

    # -- Apple Music
    if platform == "apple_music":
        track = await get_apple_music_track(query)
        if not track:
            raise ValueError("Apple Music link parse করা যায়নি।")
        # Try JioSaavn first
        LOG.info("Apple Music: trying JioSaavn for: %s", track["query"])
        js_path, js_info = await search_and_download_jiosaavn(track["query"])
        if js_path and js_info:
            import os as _os
            if _os.path.isfile(js_path):
                info = {
                    "title": js_info.get("title", track.get("title", "Unknown")),
                    "url": js_info.get("url", query),
                    "duration": js_info.get("duration", 0),
                    "thumbnail": js_info.get("thumbnail", ""),
                    "channel": js_info.get("artist", track.get("artist", "")),
                    "platform": "jiosaavn",
                }
                return info, js_path, False
        # Then YouTube
        yt = await search_youtube(track["query"])
        if yt:
            media_path, is_stream = await _get_audio_media(yt["url"])
            if media_path:
                info = {**yt, "platform": "apple_music"}
                return info, media_path, is_stream
        # yt-dlp search+download
        LOG.info("Apple Music -> YouTube failed, trying yt-dlp: %s", track["query"])
        filepath, dl_info = await search_and_download_audio(track["query"])
        if filepath and dl_info:
            return dl_info, filepath, False
        # SoundCloud last resort
        LOG.info("Apple Music -> all failed, trying SoundCloud: %s", track["query"])
        sc_path, sc_info = await search_and_download_soundcloud(track["query"])
        if sc_path and sc_info:
            is_stream = bool(sc_info.get("_is_stream_url"))
            info = {
                "title": sc_info.get("title", track.get("title", "Unknown")),
                "url": sc_info.get("url", query),
                "duration": sc_info.get("duration", 0),
                "thumbnail": sc_info.get("thumbnail", ""),
                "channel": sc_info.get("channel", track.get("artist", "")),
                "platform": "soundcloud",
            }
            return info, sc_path, is_stream
        raise ValueError("Audio stream পাওয়া যায়নি।")

    # -- SoundCloud URL
    if platform == "soundcloud":
        sc_info = await get_soundcloud_info(query)
        if not sc_info:
            sc_info = {"title": "SoundCloud Audio", "url": query,
                       "duration": 0, "thumbnail": "", "channel": "",
                       "platform": "soundcloud"}
        if sc_info.get("duration", 0) > Config.DURATION_LIMIT_MIN * 60 and sc_info["duration"] > 0:
            raise ValueError(
                f"গানটি {Config.DURATION_LIMIT_MIN} মিনিটের বেশি, "
                "play করা যাবে না।"
            )
        # Try downloading first
        filepath = await download_soundcloud(query)
        if filepath and os.path.isfile(filepath):
            return sc_info, filepath, False
        # Try stream URL
        stream_url = await get_soundcloud_stream_url(query)
        if stream_url:
            return sc_info, stream_url, True
        raise ValueError("SoundCloud থেকে audio পাওয়া যায়নি।")

    # -- Direct URL
    if platform == "direct_url":
        info = await get_video_info(query)
        if not info:
            info = {"title": "Direct Stream", "url": query,
                    "duration": 0, "thumbnail": "", "channel": ""}
        media_path, is_stream = await _get_audio_media(query)
        if not media_path:
            raise ValueError("URL থেকে audio পাওয়া যায়নি।")
        return info, media_path, is_stream

    # -- Plain text query --
    # PRIORITY (user-requested, matches /vplay flow):
    # Step 1: YouTube yt-dlp search+download FIRST (sequential await — strictly prioritised)
    # Step 2: ONLY if Step 1 fails: YouTube stream URL
    # Step 3: ONLY if Step 2 fails: JioSaavn + SoundCloud concurrent

    import asyncio as _aio

    # ── Step 1: yt-dlp search+download (STRICT FIRST PRIORITY) ──
    LOG.info("Query search: yt-dlp search+download FIRST (sequential) for: %s", query)
    try:
        fp, dl_info = await search_and_download_audio(query)
        if fp and os.path.isfile(fp):
            LOG.info("yt-dlp search+download succeeded for: %s", query)
            if dl_info:
                if dl_info.get("duration", 0) > Config.DURATION_LIMIT_MIN * 60 and dl_info["duration"] > 0:
                    raise ValueError(
                        f"গানটি {Config.DURATION_LIMIT_MIN} মিনিটের বেশি, "
                        "play করা যাবে না।"
                    )
                return dl_info, fp, False
            return ({"title": "Unknown", "url": "", "duration": 0, "thumbnail": "", "channel": ""}, fp, False)
    except ValueError:
        raise
    except Exception as e:
        LOG.info("yt-dlp search+download failed for '%s': %s", query, e)

    # ── Step 2: YouTube stream URL fallback ──
    LOG.info("Query search: search+download failed, trying YouTube stream URL for: %s", query)
    try:
        yt = await search_youtube(query)
        if yt:
            if yt["duration"] > Config.DURATION_LIMIT_MIN * 60 and yt["duration"] > 0:
                raise ValueError(
                    f"গানটি {Config.DURATION_LIMIT_MIN} মিনিটের বেশি, "
                    "play করা যাবে না।"
                )
            su = await get_audio_stream_url(yt["url"])
            if su:
                LOG.info("YouTube stream URL succeeded for: %s", query)
                return yt, su, True
    except ValueError:
        raise
    except Exception as e:
        LOG.info("YouTube stream URL failed for '%s': %s", query, e)

    # ── Step 3: YouTube fully failed — JioSaavn + SoundCloud concurrent ──
    LOG.info("Query search: YouTube failed, trying JioSaavn + SoundCloud for: %s", query)

    async def _jiosaavn_search_and_stream():
        """Search JioSaavn — CDN URL gives instant playback for Indian songs."""
        try:
            js_search = await search_jiosaavn(query)
            if js_search and js_search.get("download_url"):
                info = {
                    "title": js_search.get("title", "Unknown"),
                    "url": js_search.get("url", ""),
                    "duration": js_search.get("duration", 0),
                    "thumbnail": js_search.get("thumbnail", ""),
                    "channel": js_search.get("artist", ""),
                    "platform": "jiosaavn",
                }
                return info, js_search["download_url"], True
        except Exception:
            pass
        try:
            js_path, js_info = await search_and_download_jiosaavn(query)
            if js_path and js_info and os.path.isfile(js_path):
                info = {
                    "title": js_info.get("title", "Unknown"),
                    "url": js_info.get("url", ""),
                    "duration": js_info.get("duration", 0),
                    "thumbnail": js_info.get("thumbnail", ""),
                    "channel": js_info.get("artist", ""),
                    "platform": "jiosaavn",
                }
                return info, js_path, False
        except Exception:
            pass
        return None

    async def _soundcloud_search():
        """Search SoundCloud concurrently."""
        try:
            sc_path, sc_info = await search_and_download_soundcloud(query)
            if sc_path and sc_info:
                is_stream = bool(sc_info.get("_is_stream_url"))
                if is_stream or os.path.isfile(str(sc_path)):
                    return sc_info, sc_path, is_stream
        except Exception:
            pass
        return None

    stream_tasks = [
        _aio.create_task(_jiosaavn_search_and_stream()),
        _aio.create_task(_soundcloud_search()),
    ]

    pending = set(stream_tasks)
    while pending:
        done, pending = await _aio.wait(pending, return_when=_aio.FIRST_COMPLETED)
        for task in done:
            try:
                result = task.result()
                if result:
                    for p in pending:
                        p.cancel()
                    return result
            except Exception:
                pass

    raise ValueError("কোনো result পাওয়া যায়নি। অন্য keyword দিয়ে চেষ্টা করুন।")


@bot.on_message(filters.command(["play", "p"]) & not_edited)
async def play_command(client: Client, message: Message):
    """Handle /play <query|url>."""
    chat_id = message.chat.id
    user = message.from_user
    requester = user.mention if user else "Unknown"
    requester_id = user.id if user else 0

    # Extract query
    query = ""
    if len(message.command) > 1:
        query = " ".join(message.command[1:])
    elif message.reply_to_message and message.reply_to_message.text:
        query = message.reply_to_message.text
    elif message.reply_to_message and message.reply_to_message.audio:
        pass  # audio file reply not handled here

    if not query:
        usage_msg = await message.reply_text(
            "**Usage:** `/play <song name or URL>`\n\n"
            "Example:\n"
            "`/play Arijit Singh Tum Hi Ho`\n"
            "`/play https://youtu.be/...`"
        )
        await _add_reaction(chat_id, message.id)
        return

    status_msg = await message.reply_text(
        f"🔍 **খুঁজছি:** `{query[:80]}`\n\nঅপেক্ষা করুন..."
    )
    await _add_reaction(chat_id, message.id)

    platform = _detect_platform(query)

    try:
        # Pre-join VC concurrently while resolving media (speed optimization)
        pre_join_task = asyncio.create_task(pre_join_vc(chat_id))
        try:
            info, media_path, is_stream = await _resolve_query(query, platform, message)
        finally:
            # Ensure pre-join task completes (or is cancelled)
            try:
                await pre_join_task
            except Exception:
                pass
    except ValueError as exc:
        await status_msg.edit_text(f"❌ **Error:** {exc}")
        return
    except Exception as exc:
        LOG.exception("Unexpected error in /play for %s", chat_id)
        await status_msg.edit_text(
            f"❌ কিছু একটা সমস্যা হয়েছে। পরে আবার চেষ্টা করুন।\n"
            f"**Details:** `{type(exc).__name__}: {str(exc)[:200]}`"
        )
        return

    title = info.get("title", "Unknown")
    duration = info.get("duration", 0)
    thumbnail = info.get("thumbnail", "")
    url = info.get("url", query)
    channel = info.get("channel", "")

    item = QueueItem(
        title=title,
        url=url,
        media_path=media_path,
        duration=duration,
        requester=requester,
        requester_id=requester_id,
        thumbnail=thumbnail,
        stream_type="audio",
        platform=platform if platform != "query" else "youtube",
        is_stream_url=is_stream,
    )

    position = await add_to_queue(chat_id, item)

    # If something is already playing, just queue it
    if position > 1 and is_active(chat_id):
        dur = format_duration(duration)
        color = _get_next_color()
        await status_msg.edit_text(
            f"**🎵 Queue-তে যোগ হয়েছে #{position}**\n\n"
            f"**Title:** {title}\n"
            f"**Duration:** {dur}\n"
            f"**Requested by:** {requester}\n\n"
            f"🦋 ✦ᴘᴏᴡєʀєᴅ ʙʏ » ── [@R4J_81](https://t.me/R4J_81)",
            reply_markup=_control_keyboard(color),
        )
        await _add_reaction(chat_id, message.id)
        return

    # Start streaming
    try:
        await stream_audio(
            chat_id, media_path,
            title=title, duration=duration,
            thumbnail=thumbnail, requester=requester,
        )
    except FileNotFoundError:
        LOG.exception("Media not found for stream in %s", chat_id)
        await status_msg.edit_text(
            "❌ মিডিয়া ফাইল/URL পাওয়া যায়নি।\n"
            "আবার `/play` দিয়ে চেষ্টা করুন।"
        )
        return
    except RuntimeError as exc:
        await status_msg.edit_text(
            f"❌ {exc}\n\n"
            "STRING_SESSION সেট করা আছে কিনা চেক করুন।"
        )
        return
    except Exception as exc:
        LOG.exception("Stream start failed in %s", chat_id)
        await status_msg.edit_text(
            "❌ Voice chat-এ connect করা যাচ্ছে না।\n"
            "নিশ্চিত করুন voice chat চালু আছে এবং "
            "assistant গ্রুপে আছে।\n\n"
            f"**Error:** `{type(exc).__name__}: {str(exc)[:150]}`"
        )
        return

    # Start the progress timer for this track
    await _start_progress_timer(chat_id, duration)

    dur = format_duration(duration)
    color = _get_next_color()
    t = _get_current_theme()
    text = (
        f"{t['header']} **ᴘʟᴀʏʙᴀᴄᴋ ᴀᴄᴛɪᴠᴀᴛᴇᴅ | ᴇɴᴊᴏʏ ᴛʜᴇ ᴍᴜꜱɪᴄ**\n\n"
        f"> {t['title_icon']}  **ᴛɪᴛʟᴇ :** [{title}]({url})\n"
        f"> {t['dur_icon']}  **ᴅᴜʀᴀᴛɪᴏɴ :** {dur}\n"
        f"> 👤  **ʀᴇǫᴜᴇꜱᴛᴇᴅ :** {requester}\n\n"
        f"🦋 ✦ᴘᴏᴡєʀєᴅ ʙʏ » ── [@R4J_81](https://t.me/R4J_81)"
    )

    try:
        if thumbnail:
            await status_msg.delete()
            now_playing_msg = await bot.send_photo(
                chat_id,
                photo=thumbnail,
                caption=text,
                reply_markup=_control_keyboard(color),
            )
            # Track this message so we can delete it when track ends
            if chat_id not in _now_playing_messages:
                _now_playing_messages[chat_id] = []
            _now_playing_messages[chat_id].append(now_playing_msg)
            await _add_reaction(chat_id, message.id)
        else:
            await status_msg.edit_text(text, reply_markup=_control_keyboard(color))
            # Track this message
            if chat_id not in _now_playing_messages:
                _now_playing_messages[chat_id] = []
            _now_playing_messages[chat_id].append(status_msg)
            await _add_reaction(chat_id, message.id)
    except Exception:
        await status_msg.edit_text(text, reply_markup=_control_keyboard(color))
        if chat_id not in _now_playing_messages:
            _now_playing_messages[chat_id] = []
        _now_playing_messages[chat_id].append(status_msg)
        await _add_reaction(chat_id, message.id)

@bot.on_message(filters.command(["playforce", "pf", "forceplay"]) & not_edited)
async def playforce_command(client: Client, message: Message):
    """Handle /playforce <query|url> — stop current and play immediately."""
    chat_id = message.chat.id
    user = message.from_user
    requester = user.mention if user else "Unknown"
    requester_id = user.id if user else 0

    # Extract query
    query = ""
    if len(message.command) > 1:
        query = " ".join(message.command[1:])
    elif message.reply_to_message and message.reply_to_message.text:
        query = message.reply_to_message.text

    if not query:
        usage_msg = await message.reply_text(
            "**Usage:** `/playforce <song name or URL>`\n\n"
            "বর্তমান গান বন্ধ করে সাথে সাথে নতুন গান চালায়।\n\n"
            "Example:\n"
            "`/playforce Arijit Singh Tum Hi Ho`"
        )
        await _add_reaction(chat_id, message.id)
        return

    status_msg = await message.reply_text(
        f"⚡ **Force Play:** `{query[:80]}`\n\nবর্তমান গান বন্ধ করছি..."
    )
    await _add_reaction(chat_id, message.id)

    # Stop current playback if active
    if is_active(chat_id):
        lock = _get_skip_lock(chat_id)
        async with lock:
            _stop_progress_timer(chat_id)
            # Delete previous "Now Playing" messages
            if chat_id in _now_playing_messages:
                for old_msg in _now_playing_messages[chat_id]:
                    try:
                        await old_msg.delete()
                    except Exception:
                        pass
                _now_playing_messages[chat_id].clear()
            await leave_voice_chat(chat_id)

    platform = _detect_platform(query)

    try:
        # Pre-join VC concurrently while resolving media (speed optimization)
        pre_join_task = asyncio.create_task(pre_join_vc(chat_id))
        try:
            info, media_path, is_stream = await _resolve_query(query, platform, message)
        finally:
            try:
                await pre_join_task
            except Exception:
                pass
    except ValueError as exc:
        await status_msg.edit_text(f"❌ **Error:** {exc}")
        return
    except Exception as exc:
        LOG.exception("Unexpected error in /playforce for %s", chat_id)
        await status_msg.edit_text(
            f"❌ কিছু একটা সমস্যা হয়েছে।\n"
            f"**Details:** `{type(exc).__name__}: {str(exc)[:200]}`"
        )
        return

    title = info.get("title", "Unknown")
    duration = info.get("duration", 0)
    thumbnail = info.get("thumbnail", "")
    url = info.get("url", query)
    channel = info.get("channel", "")

    item = QueueItem(
        title=title,
        url=url,
        media_path=media_path,
        duration=duration,
        requester=requester,
        requester_id=requester_id,
        thumbnail=thumbnail,
        stream_type="audio",
        platform=platform if platform != "query" else "youtube",
        is_stream_url=is_stream,
    )

    # Clear queue and add as first item
    await clear_queue(chat_id)
    await add_to_queue(chat_id, item)

    # Start streaming
    try:
        await stream_audio(
            chat_id, media_path,
            title=title, duration=duration,
            thumbnail=thumbnail, requester=requester,
        )
    except FileNotFoundError:
        await status_msg.edit_text(
            "❌ মিডিয়া ফাইল/URL পাওয়া যায়নি।\n"
            "আবার `/playforce` দিয়ে চেষ্টা করুন।"
        )
        return
    except RuntimeError as exc:
        await status_msg.edit_text(
            f"❌ {exc}\n\nSTRING_SESSION সেট করা আছে কিনা চেক করুন।"
        )
        return
    except Exception as exc:
        LOG.exception("Stream start failed in /playforce %s", chat_id)
        await status_msg.edit_text(
            "❌ Voice chat-এ connect করা যাচ্ছে না।\n"
            "নিশ্চিত করুন voice chat চালু আছে।\n\n"
            f"**Error:** `{type(exc).__name__}: {str(exc)[:150]}`"
        )
        return

    # Start the progress timer
    await _start_progress_timer(chat_id, duration)

    dur = format_duration(duration)
    color = _get_next_color()
    t = _get_current_theme()
    text = (
        f"⚡ **ꜰᴏʀᴄᴇ ᴘʟᴀʏ | ᴇɴᴊᴏʏ ᴛʜᴇ ᴍᴜꜱɪᴄ**\n\n"
        f"> {t['title_icon']}  **ᴛɪᴛʟᴇ :** [{title}]({url})\n"
        f"> {t['dur_icon']}  **ᴅᴜʀᴀᴛɪᴏɴ :** {dur}\n"
        f"> 👤  **ʀᴇǫᴜᴇꜱᴛᴇᴅ :** {requester}\n\n"
        f"🦋 ✦ᴘᴏᴡєʀєᴅ ʙʏ » ── [@R4J_81](https://t.me/R4J_81)"
    )

    try:
        if thumbnail:
            await status_msg.delete()
            now_playing_msg = await bot.send_photo(
                chat_id,
                photo=thumbnail,
                caption=text,
                reply_markup=_control_keyboard(color),
            )
            if chat_id not in _now_playing_messages:
                _now_playing_messages[chat_id] = []
            _now_playing_messages[chat_id].append(now_playing_msg)
            await _add_reaction(chat_id, message.id)
        else:
            await status_msg.edit_text(text, reply_markup=_control_keyboard(color))
            if chat_id not in _now_playing_messages:
                _now_playing_messages[chat_id] = []
            _now_playing_messages[chat_id].append(status_msg)
            await _add_reaction(chat_id, message.id)
    except Exception:
        await status_msg.edit_text(text, reply_markup=_control_keyboard(color))
        if chat_id not in _now_playing_messages:
            _now_playing_messages[chat_id] = []
        _now_playing_messages[chat_id].append(status_msg)
        await _add_reaction(chat_id, message.id)
