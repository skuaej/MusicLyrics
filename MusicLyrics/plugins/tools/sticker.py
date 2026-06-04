"""Sticker tools plugin for MusicLyrics bot.

Commands:
    /sticker, /s   → convert replied photo/sticker/image-document to webp sticker
    /toimg         → convert replied static sticker to PNG
    /getsticker    → get replied sticker as raw .webp document
    /stickerid     → print file_id of replied sticker
    /kang [emoji] [pack_num] → steal sticker into user's personal pack (USERBOT ONLY)
"""

import os
import tempfile
import asyncio

from pyrogram import filters
from pyrogram.types import Message
from pyrogram.errors import (
    StickersetInvalid,
    PeerIdInvalid,
    BadRequest,
)

from MusicLyrics.bot import bot


# ---------------- Helpers ----------------

MAX_STICKER_SIDE = 512
MAX_STICKER_BYTES = 512 * 1024  # Telegram hard limit


def _resize_for_sticker(img):
    """Resize a PIL image so longest side == 512, preserving aspect ratio."""
    from PIL import Image
    w, h = img.size
    if w == 0 or h == 0:
        raise ValueError("Empty image")
    scale = MAX_STICKER_SIDE / max(w, h)
    new_size = (max(1, int(w * scale)), max(1, int(h * scale)))
    return img.resize(new_size, Image.LANCZOS)


def _save_webp(img, path: str) -> str:
    """Save image as Telegram-compatible WEBP. Drops quality if too large."""
    img.save(path, "WEBP", quality=100, method=6, lossless=False)
    # If over 512KB, retry with lower quality
    if os.path.getsize(path) > MAX_STICKER_BYTES:
        for q in (90, 80, 70, 60, 50):
            img.save(path, "WEBP", quality=q, method=6)
            if os.path.getsize(path) <= MAX_STICKER_BYTES:
                break
    return path


async def _download_media(reply: Message) -> str | None:
    """Download photo / sticker / image-document into temp dir. Returns path or None."""
    tmp_dir = tempfile.mkdtemp(prefix="ml_sticker_")
    if reply.photo or reply.sticker:
        return await reply.download(file_name=os.path.join(tmp_dir, ""))
    if reply.document and reply.document.mime_type and reply.document.mime_type.startswith("image/"):
        return await reply.download(file_name=os.path.join(tmp_dir, ""))
    return None


def _cleanup(*paths):
    for p in paths:
        if not p:
            continue
        try:
            if os.path.isfile(p):
                os.remove(p)
                d = os.path.dirname(p)
                if d.startswith(tempfile.gettempdir()) and not os.listdir(d):
                    os.rmdir(d)
        except Exception:
            pass


# ---------------- /sticker, /s ----------------

@bot.on_message(filters.command(["sticker", "s"]))
async def to_sticker(client, message: Message):
    """Convert a replied photo / image / static sticker to a webp sticker."""
    reply = message.reply_to_message
    if not reply:
        return await message.reply_text(
            "❌ একটি ফটো / ইমেজ / স্টিকারে রিপ্লাই দাও।\n"
            "Reply to a photo, image, or static sticker."
        )

    # Reject animated/video stickers up front
    if reply.sticker and (reply.sticker.is_animated or reply.sticker.is_video):
        return await message.reply_text(
            "❌ অ্যানিমেটেড/ভিডিও স্টিকার সাপোর্ট নেই।\n"
            "Animated/video stickers not supported."
        )

    status = await message.reply_text("🔄 স্টিকার বানাচ্ছি... / Converting...")
    src = webp = None
    try:
        src = await _download_media(reply)
        if not src:
            return await status.edit_text(
                "❌ Unsupported media. Send a photo, image, or static sticker."
            )

        from PIL import Image
        img = Image.open(src).convert("RGBA")
        img = _resize_for_sticker(img)

        webp = os.path.join(os.path.dirname(src), "sticker.webp")
        _save_webp(img, webp)

        await message.reply_sticker(sticker=webp)
        await status.delete()

    except Exception as e:
        try:
            await status.edit_text(f"❌ Error: `{type(e).__name__}: {e}`")
        except Exception:
            pass
    finally:
        _cleanup(src, webp)


# ---------------- /toimg ----------------

@bot.on_message(filters.command("toimg"))
async def sticker_to_img(client, message: Message):
    """Convert a replied static sticker to a PNG image."""
    reply = message.reply_to_message
    if not reply or not reply.sticker:
        return await message.reply_text(
            "❌ একটি স্টিকারে রিপ্লাই দাও। / Reply to a sticker."
        )
    if reply.sticker.is_animated or reply.sticker.is_video:
        return await message.reply_text(
            "❌ অ্যানিমেটেড/ভিডিও স্টিকার সাপোর্ট নেই।\n"
            "Animated/video stickers not supported."
        )

    status = await message.reply_text("🔄 ইমেজে কনভার্ট করছি... / Converting...")
    src = png = None
    try:
        src = await _download_media(reply)
        from PIL import Image
        img = Image.open(src).convert("RGBA")
        png = os.path.join(os.path.dirname(src), "sticker.png")
        img.save(png, "PNG")
        await message.reply_photo(photo=png)
        await status.delete()
    except Exception as e:
        try:
            await status.edit_text(f"❌ Error: `{type(e).__name__}: {e}`")
        except Exception:
            pass
    finally:
        _cleanup(src, png)


# ---------------- /getsticker ----------------

@bot.on_message(filters.command("getsticker"))
async def get_sticker(client, message: Message):
    """Get a replied sticker as a document file."""
    reply = message.reply_to_message
    if not reply or not reply.sticker:
        return await message.reply_text(
            "❌ একটি স্টিকারে রিপ্লাই দাও। / Reply to a sticker."
        )

    src = None
    try:
        src = await _download_media(reply)
        await message.reply_document(
            document=src,
            caption="📎 এই নাও স্টিকার ফাইল। / Here is the sticker file.",
            file_name="sticker.webp",
        )
    except Exception as e:
        await message.reply_text(f"❌ Error: `{type(e).__name__}: {e}`")
    finally:
        _cleanup(src)


# ---------------- /stickerid ----------------

@bot.on_message(filters.command("stickerid"))
async def sticker_id(_, message: Message):
    """Get the file_id of a replied sticker."""
    reply = message.reply_to_message
    if not reply or not reply.sticker:
        return await message.reply_text(
            "❌ একটি স্টিকারে রিপ্লাই দাও। / Reply to a sticker."
        )
    s = reply.sticker
    await message.reply_text(
        f"🆔 **Sticker File ID:**\n`{s.file_id}`\n\n"
        f"📐 Size: `{s.width}x{s.height}`\n"
        f"😀 Emoji: {s.emoji or '—'}\n"
        f"🎬 Animated: `{s.is_animated}` • Video: `{s.is_video}`"
    )


# ---------------- /kang (userbot-only) ----------------

@bot.on_message(filters.command("kang"))
async def kang_sticker(client, message: Message):
    """Steal a sticker into the user's pack.

    NOTE: Requires a user/string session — Telegram bot accounts cannot
    create or modify sticker packs via raw API. Use @Stickers for bots.

    Usage: /kang [emoji] [pack_index]
    """
    reply = message.reply_to_message
    if not reply:
        return await message.reply_text(
            "❌ একটি স্টিকার বা ফটোতে রিপ্লাই দাও।\n"
            "Reply to a sticker or photo."
        )

    user = message.from_user
    status = await message.reply_text("🔄 স্টিকার কাং করছি... / Kanging sticker...")

    # Parse args: /kang [emoji] [pack_num]
    args = (message.text or "").split()[1:]
    sticker_emoji = "🤩"
    pack_num = 1
    for a in args:
        if a.isdigit():
            pack_num = max(1, min(int(a), 50))
        elif len(a) <= 8:  # likely an emoji
            sticker_emoji = a

    if reply.sticker and reply.sticker.emoji and sticker_emoji == "🤩":
        sticker_emoji = reply.sticker.emoji

    # Reject animated/video — raw API path here only handles static webp
    if reply.sticker and (reply.sticker.is_animated or reply.sticker.is_video):
        return await status.edit_text(
            "❌ অ্যানিমেটেড/ভিডিও স্টিকার কাং সাপোর্ট নেই।\n"
            "Cannot kang animated/video stickers."
        )

    src = webp = None
    try:
        src = await _download_media(reply)
        if not src:
            return await status.edit_text(
                "❌ শুধু স্টিকার বা ছবি কাং করা যায়। / Only stickers or images."
            )

        from PIL import Image
        img = Image.open(src).convert("RGBA")
        img = _resize_for_sticker(img)
        webp = os.path.join(os.path.dirname(src), "kang.webp")
        _save_webp(img, webp)

        # Build a Telegram-legal pack short_name (must end with _by_<botusername>, ≤64 chars)
        me = await client.get_me()
        bot_uname = me.username or "bot"
        # Reserve suffix length
        suffix = f"_by_{bot_uname}"
        max_base = 64 - len(suffix)
        base = f"ml{pack_num}_u{user.id}"
        base = base[:max_base]
        pack_name = base + suffix
        pack_title = f"{(user.first_name or 'User')[:48]}'s Pack vol.{pack_num}"

        from pyrogram.raw.functions.messages import GetStickerSet
        from pyrogram.raw.functions.stickers import (
            CreateStickerSet,
            AddStickerToSet,
        )
        from pyrogram.raw.types import (
            InputStickerSetShortName,
            InputStickerSetItem,
        )

        # Upload file once
        try:
            saved_file = await client.save_file(webp)
        except AttributeError:
            return await status.edit_text(
                "❌ এই client `kang` সাপোর্ট করে না।\n"
                "Kang requires a user/string session (not a bot token)."
            )

        item = InputStickerSetItem(document=saved_file, emoji=sticker_emoji)

        # Check if pack exists
        pack_exists = True
        try:
            await client.invoke(
                GetStickerSet(
                    stickerset=InputStickerSetShortName(short_name=pack_name),
                    hash=0,
                )
            )
        except StickersetInvalid:
            pack_exists = False
        except Exception:
            pack_exists = False

        if not pack_exists:
            # Create new pack
            try:
                user_peer = await client.resolve_peer(user.id)
                await client.invoke(
                    CreateStickerSet(
                        user_id=user_peer,
                        title=pack_title,
                        short_name=pack_name,
                        stickers=[item],
                    )
                )
            except PeerIdInvalid:
                return await status.edit_text(
                    "❌ User peer resolve হয়নি। আগে আমাকে DM করো।\n"
                    "Couldn't resolve your peer — DM the userbot first."
                )
            except BadRequest as e:
                return await status.edit_text(
                    f"❌ Pack create failed: `{e}`\n"
                    f"(bot account দিয়ে kang কাজ করবে না)"
                )
        else:
            # Add to existing pack
            try:
                await client.invoke(
                    AddStickerToSet(
                        stickerset=InputStickerSetShortName(short_name=pack_name),
                        sticker=item,
                    )
                )
            except BadRequest as e:
                msg = str(e).lower()
                if "full" in msg or "stickerpack_too_big" in msg:
                    return await status.edit_text(
                        f"❌ Pack পূর্ণ (120 sticker limit)। নতুন pack-এ যেতে:\n"
                        f"`/kang {sticker_emoji} {pack_num + 1}`"
                    )
                return await status.edit_text(f"❌ Add failed: `{e}`")

        await status.edit_text(
            f"✅ স্টিকার কাং হয়েছে! / Sticker kanged!\n\n"
            f"📦 Pack: [Open Pack](https://t.me/addstickers/{pack_name})\n"
            f"😎 Emoji: {sticker_emoji}\n"
            f"🔢 Vol: {pack_num}",
            disable_web_page_preview=True,
        )

    except Exception as e:
        try:
            await status.edit_text(f"❌ Kang failed: `{type(e).__name__}: {e}`")
        except Exception:
            pass
    finally:
        _cleanup(src, webp)
