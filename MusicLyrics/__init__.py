"""MusicLyrics — Telegram music & utility bot."""

__version__ = "2.0.0"

# Avoid importing heavy optional dependencies at package import time so
# users can import the package for inspection or tooling without
# installing runtime-only requirements (e.g. during static analysis).
try:
	from .bot import bot, get_bot_info
except Exception:
	bot = None
	get_bot_info = None

try:
	from .userbot import userbot, pytgcalls
except Exception:
	userbot = None
	pytgcalls = None

__all__ = ["__version__", "bot", "get_bot_info", "userbot", "pytgcalls"]
