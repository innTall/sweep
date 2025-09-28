# core/telegram_bot_async.py
import os
import json
import logging
import aiohttp
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("sweep")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Load config.json once
try:
    CONFIG = json.load(open("config.json", encoding="utf-8"))
except Exception:
    CONFIG = {}

TELEGRAM_TIMEOUT = CONFIG.get("timeouts", {}).get("telegram", 10)  # default 10s

async def send_signal(message: str) -> None:
    """Send a message to Telegram channel or chat asynchronously."""
    if not BOT_TOKEN or not CHAT_ID:
        raise ValueError("Telegram credentials not found in .env")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}

    logger.info(f"[Telegram] Sending: {payload}")
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=TELEGRAM_TIMEOUT)) as session:
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                logger.error(f"[Telegram] Error: {await resp.text()}")
            else:
                logger.info(f"[Telegram] OK: {await resp.json()}")
            resp.raise_for_status()