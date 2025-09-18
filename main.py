# main.py
import json
import logging
import pytz
from datetime import datetime

import utils.bingx_api as bingx_api
from core.telegram_bot import send_signal
from modules.fractals import detect_fractals, get_candles
from modules.breakouts import check_breakouts, format_breakout_message


def setup_logger(config: dict):
    level_str = config.get("logging", {}).get("level", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)
    logging.basicConfig(level=level,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    return logging.getLogger("sweep")


def run_fractal_detection(config, tz, logger):
    interval_map = config.get("interval_map", {})
    history_limit = int(config["history_limit"])
    fractal_window = int(config["fractal_window"])
    send_messages = config["send_messages"]

    for symbol in config["symbols"]:
        for interval in config["intervals"]:
            try:
                # 1) Get the last confirmed (closed) candle used for breakout checking
                last_candle = bingx_api.get_last_confirmed_candle(symbol, interval, interval_map)
                logger.debug(
                    f"{symbol}-{interval} last_closed: ts={last_candle['timestamp']} close={last_candle['close']}"
                )

                # 2) Get history candles (may include the last closed or an in-progress candle)
                candles = get_candles(symbol, interval, history_limit, interval_map)

                # Normalize: choose a canonical close-time key for each candle
                def _close_time(c):
                    return int(c.get("close_time") or c.get("timestamp"))

                # Ensure candles sorted ascending (oldest -> newest)
                candles.sort(key=_close_time)

                # 3) Build candles BEFORE the last closed candle (strictly less)
                candles_before_last = [c for c in candles if _close_time(c) < int(last_candle["timestamp"])]
                logger.debug(
                    f"{symbol}-{interval} fetched={len(candles)} before_last={len(candles_before_last)}"
                )

                # If not enough history before the last closed candle, skip fractal detection
                if len(candles_before_last) < fractal_window:
                    logger.info(f"Not enough history (before last close) for {symbol}-{interval} (need {fractal_window})")
                    continue

                # 4) Detect active fractals using only candles BEFORE the last-close (so last-close can break them)
                H_fractals, L_fractals = detect_fractals(candles_before_last, fractal_window)

                # Logging of all active fractals (latest first)
                logger.info(f"\n{symbol}-{interval} Active HFractals: {len(H_fractals)}")
                for f in H_fractals:
                    ts = datetime.fromtimestamp(int(f["time"]) / 1000, tz=tz)
                    logger.info(f"  H @ {ts.strftime('%Y-%m-%d %H:%M %Z')} | high={f['high']}")

                logger.info(f"{symbol}-{interval} Active LFractals: {len(L_fractals)}")
                for f in L_fractals:
                    ts = datetime.fromtimestamp(int(f["time"]) / 1000, tz=tz)
                    logger.info(f"  L @ {ts.strftime('%Y-%m-%d %H:%M %Z')} | low={f['low']}")

                # 5) Check breakouts of the last closed candle against those active fractals
                breakout = check_breakouts(symbol, interval, H_fractals, L_fractals, last_candle, tz, interval_map)
                if breakout:
                    message = format_breakout_message(breakout, tz)
                    logger.info(f"Breakout detected: {message}")
                    if send_messages:
                        send_signal(message)
                    else:
                        logger.info("Message sending disabled (send_messages=false)")
                else:
                    logger.info(f"No breakout for {symbol}-{interval}")

            except Exception as e:
                logger.error(f"Fractal detection failed for {symbol}-{interval}: {e}")

def main():
    with open("config.json") as f:
        config = json.load(f)

    tz = pytz.timezone(config.get("timezone", "UTC"))
    logger = setup_logger(config)

    logger.info("Starting bot (Stage 2: fractals & breakouts)...")
    run_fractal_detection(config, tz, logger)


if __name__ == "__main__":
    main()

# python main.py
# python -m main