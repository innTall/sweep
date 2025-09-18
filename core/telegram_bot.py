# core/telegram_bot.py
import os
import requests
import logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("sweep")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def send_signal(message: str) -> None:
    """Send a message to Telegram channel or chat."""
    if not BOT_TOKEN or not CHAT_ID:
        raise ValueError("Telegram credentials not found in .env")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}

    logger.info(f"[Telegram] Sending: {payload}")
    response = requests.post(url, json=payload)

    if response.status_code != 200:
        logger.error(f"[Telegram] Error: {response.text}")
    else:
        logger.info(f"[Telegram] OK: {response.json()}")

    response.raise_for_status()