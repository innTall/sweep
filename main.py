# main.py
import json
import logging
import pytz
from datetime import datetime
import utils.bingx_api as bingx_api
from core.telegram_bot import send_signal

def setup_logger(config: dict):
    level_str = config.get("logging", {}).get("level", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("sweep")


def format_message(symbol: str, interval: str, candle: dict, tz) -> str:
    ts = datetime.fromtimestamp(bingx_api.Last_Close_Time / 1000, tz=tz)
    return (
        f"🔔 <b>Signal</b>\n"
        f"Symbol: <code>{symbol}</code>\n"
        f"Interval: {interval}\n"
        f"OHLC: O={candle['open']}, H={candle['high']}, L={candle['low']}, C={candle['close']}\n"
        f"Last_Close_Time: {ts.strftime('%Y-%m-%d %H:%M %Z')}\n"
        f"Last_Close_Price: {bingx_api.Last_Close_Price}"
    )


def main():
    with open("config.json") as f:
        config = json.load(f)

    tz = pytz.timezone(config.get("timezone", "UTC"))
    interval_map = config.get("interval_map", {})
    send_messages = config["send_messages"]

    logger = setup_logger(config)
    logger.info("Starting bot...")

    for symbol in config["symbols"]:
        for interval in config["intervals"]:
            try:
                candle = bingx_api.get_last_confirmed_candle(symbol, interval, interval_map)
                message = format_message(symbol, interval, candle, tz)
                logger.info(f"Prepared message: {message}")

                if send_messages:
                    send_signal(message)
                else:
                    logger.info("Message sending disabled (send_messages=false)")
                    
            except Exception as e:
                logger.error(f"Failed for {symbol}-{interval}: {e}")


if __name__ == "__main__":
    main()

# python main.py
# python -m main