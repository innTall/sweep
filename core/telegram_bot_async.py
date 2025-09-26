# core/telegram_bot_async.py
import os
import logging
import aiohttp
import asyncio
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("sweep")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


async def send_signal(message: str) -> None:
    """Send a message to Telegram channel or chat asynchronously."""
    if not BOT_TOKEN or not CHAT_ID:
        raise ValueError("Telegram credentials not found in .env")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}

    logger.info(f"[Telegram] Sending: {payload}")
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                logger.error(f"[Telegram] Error: {await resp.text()}")
            else:
                logger.info(f"[Telegram] OK: {await resp.json()}")
            resp.raise_for_status()