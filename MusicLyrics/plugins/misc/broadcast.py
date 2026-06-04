"""Broadcast plugin — send message to all users/chats (sudo only).

Supported flags (combine freely):
    -users      → only personal users
    -chats      → only groups/channels
    -pin        → pin the broadcast in target chats
    -pinloud    → pin with notification
    -forward    → forward instead of copy (shows original sender)
    -nobot      → do NOT remove dead/blocked users from DB
    -dryrun     → simulate without sending

Usage:
    /broadcast <text>
    /broadcast -chats -pin <text>
    Reply to any message with /broadcast [-flags]
"""

import asyncio
import time
from typing import Optional

from pyrogram import filters
from pyrogram.errors import (
    FloodWait,
    InputUserDeactivated,
    PeerIdInvalid,
    UserIsBlocked,
    ChatWriteForbidden,
    ChannelPrivate,
    ChatAdminRequired,
)
from pyrogram.types import Message

from MusicLyrics.bot import bot
from MusicLyrics.helpers.decorators import sudo_required
from MusicLyrics.mongo.users_db import get_all_users
from MusicLyrics.mongo.chats_db import get_all_chats

# Try to import removal helpers; fall back to no-op if not present
try:
    from MusicLyrics.mongo.users_db import remove_user  # type: ignore
except ImportError:
    async def remove_user(_uid: int) -> None:  # type: ignore
        return None

try:
    from MusicLyrics.mongo.chats_db import remove_chat  # type: ignore
except ImportError:
    async def remove_chat(_cid: int) -> None:  # type: ignore
        return None


# ---------------- Tunables ----------------
CONCURRENCY = 20          # parallel sends
PROGRESS_EVERY = 2.5      # seconds between progress edits
MAX_FLOOD_WAIT = 60       # cap auto-sleep for FloodWait (s)
# ------------------------------------------

VALID_FLAGS = {
    "-users", "-chats", "-pin", "-pinloud",
    "-forward", "-nobot", "-dryrun",
}

DEAD_ERRORS = (InputUserDeactivated, UserIsBlocked, PeerIdInvalid)


def _parse_flags(text: str) -> tuple[set[str], str]:
    """Strip flags from message text. Returns (flags, remaining_text)."""
    parts = text.split()
    flags, rest = set(), []
    for p in parts[1:]:  # skip command itself
        if p in VALID_FLAGS:
            flags.add(p)
        else:
            rest.append(p)
    return flags, " ".join(rest).strip()


async def _send_one(
    client,
    target_id: int,
    source_msg: Optional[Message],
    text: Optional[str],
    flags: set[str],
) -> tuple[bool, bool]:
    """Send to one target. Returns (success, is_dead)."""
    if "-dryrun" in flags:
        return True, False

    try:
        if source_msg is not None:
            if "-forward" in flags:
                sent = await source_msg.forward(target_id)
            else:
                sent = await source_msg.copy(target_id)
        else:
            sent = await client.send_message(target_id, text)

        if "-pin" in flags or "-pinloud" in flags:
            try:
                await sent.pin(disable_notification=("-pin" in flags))
            except (ChatAdminRequired, ChatWriteForbidden, ChannelPrivate):
                pass
            except Exception:
                pass

        return True, False

    except FloodWait as e:
        wait = min(int(getattr(e, "value", 5)), MAX_FLOOD_WAIT)
        await asyncio.sleep(wait)
        # retry once after flood wait
        return await _send_one(client, target_id, source_msg, text, flags)

    except DEAD_ERRORS:
        return False, True

    except (ChatWriteForbidden, ChannelPrivate, ChatAdminRequired):
        return False, False

    except Exception:
        return False, False


@bot.on_message(filters.command("broadcast"))
@sudo_required
async def broadcast_cmd(client, message: Message):
    """Broadcast a message to all users and/or chats."""
    flags, remaining = _parse_flags(message.text or "")

    # Determine payload
    source_msg: Optional[Message] = None
    text: Optional[str] = None
    if message.reply_to_message:
        source_msg = message.reply_to_message
    elif remaining:
        text = remaining
    else:
        return await message.reply_text(
            "❌ **ব্যবহার / Usage:**\n"
            "`/broadcast <message>` বা একটি মেসেজে রিপ্লাই দিয়ে `/broadcast`\n\n"
            "**Flags:**\n"
            "`-users` শুধু users\n"
            "`-chats` শুধু groups/channels\n"
            "`-pin` / `-pinloud` chats-এ pin করো\n"
            "`-forward` copy না করে forward করো\n"
            "`-nobot` dead users delete করো না\n"
            "`-dryrun` test mode (পাঠাবে না)"
        )

    # Build target list
    targets: list[int] = []
    target_kind: list[str] = []  # parallel array: "user" | "chat"

    want_users = "-users" in flags or not ({"-users", "-chats"} & flags)
    want_chats = "-chats" in flags or not ({"-users", "-chats"} & flags)

    if want_users:
        for u in await get_all_users():
            uid = u.get("user_id") if isinstance(u, dict) else u
            if uid:
                targets.append(int(uid))
                target_kind.append("user")

    if want_chats:
        for c in await get_all_chats():
            cid = c.get("chat_id") if isinstance(c, dict) else c
            if cid:
                targets.append(int(cid))
                target_kind.append("chat")

    total = len(targets)
    if total == 0:
        return await message.reply_text("⚠️ কোনো target পাওয়া যায়নি / No targets found.")

    status = await message.reply_text(
        f"📡 **Broadcast শুরু হচ্ছে...**\n"
        f"🎯 Targets: `{total}`\n"
        f"⚙️ Mode: `{'forward' if '-forward' in flags else 'copy'}`"
        f"{' • pin' if ('-pin' in flags or '-pinloud' in flags) else ''}"
        f"{' • DRY RUN' if '-dryrun' in flags else ''}"
    )

    sent = failed = removed = 0
    last_edit = time.monotonic()
    start = last_edit
    sem = asyncio.Semaphore(CONCURRENCY)
    lock = asyncio.Lock()
    cleanup_dead = "-nobot" not in flags and "-dryrun" not in flags

    async def worker(idx: int):
        nonlocal sent, failed, removed
        async with sem:
            ok, dead = await _send_one(
                client, targets[idx], source_msg, text, flags
            )
            async with lock:
                if ok:
                    sent += 1
                else:
                    failed += 1
                    if dead and cleanup_dead:
                        try:
                            if target_kind[idx] == "user":
                                await remove_user(targets[idx])
                            else:
                                await remove_chat(targets[idx])
                            removed += 1
                        except Exception:
                            pass

    async def progress_loop():
        nonlocal last_edit
        while True:
            await asyncio.sleep(PROGRESS_EVERY)
            done = sent + failed
            if done >= total:
                return
            now = time.monotonic()
            if now - last_edit < PROGRESS_EVERY:
                continue
            last_edit = now
            rate = done / max(now - start, 0.1)
            eta = (total - done) / max(rate, 0.1)
            try:
                await status.edit_text(
                    f"📡 **Broadcasting...**\n\n"
                    f"✅ Sent: `{sent}`\n"
                    f"❌ Failed: `{failed}`\n"
                    f"🧹 Cleaned: `{removed}`\n"
                    f"📊 Progress: `{done}/{total}` "
                    f"({done * 100 // total}%)\n"
                    f"⚡ Rate: `{rate:.1f}/s` • ETA `{int(eta)}s`"
                )
            except FloodWait as e:
                await asyncio.sleep(int(getattr(e, "value", 2)))
            except Exception:
                pass

    progress_task = asyncio.create_task(progress_loop())
    try:
        await asyncio.gather(*(worker(i) for i in range(total)))
    finally:
        progress_task.cancel()

    elapsed = time.monotonic() - start
    try:
        await status.edit_text(
            f"📡 **ব্রডকাস্ট সম্পন্ন! / Broadcast Complete!**\n\n"
            f"✅ Sent: `{sent}`\n"
            f"❌ Failed: `{failed}`\n"
            f"🧹 Cleaned: `{removed}`\n"
            f"📊 Total: `{total}`\n"
            f"⏱ Time: `{elapsed:.1f}s` "
            f"({sent / max(elapsed, 0.1):.1f}/s)"
            f"{chr(10) + '⚠️ DRY RUN — কিছু পাঠানো হয়নি' if '-dryrun' in flags else ''}"
        )
    except Exception:
        pass
