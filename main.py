# main.py
import json
import logging
import pytz
from datetime import datetime

import utils.bingx_api as bingx_api
from core.telegram_bot import send_signal
from modules.fractals import detect_fractals, get_candles
from modules.breakouts import check_breakouts, format_breakout_message
from core.fractal_storage import load_storage, save_storage, update_storage


def setup_logger(config: dict):
    level_str = config.get("logging", {}).get("level", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)
    logging.basicConfig(level=level,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    return logging.getLogger("sweep")


def run_fractal_detection(config, tz, logger, storage):
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

                # 2) Get history candles
                candles = get_candles(symbol, interval, history_limit, interval_map)

                def _close_time(c):
                    return int(c.get("close_time") or c.get("timestamp"))

                candles.sort(key=_close_time)
                candles_before_last = [c for c in candles if _close_time(c) < int(last_candle["timestamp"])]
                logger.debug(
                    f"{symbol}-{interval} fetched={len(candles)} before_last={len(candles_before_last)}"
                )

                if len(candles_before_last) < fractal_window:
                    logger.info(f"Not enough history (before last close) for {symbol}-{interval} (need {fractal_window})")
                    continue

                # 3) Detect active fractals
                H_fractals, L_fractals = detect_fractals(candles_before_last, fractal_window)

                logger.info(f"\n{symbol}-{interval} Active HFractals: {len(H_fractals)}")
                for f in H_fractals:
                    ts = datetime.fromtimestamp(int(f["time"]) / 1000, tz=tz)
                    logger.info(f"  H @ {ts.strftime('%Y-%m-%d %H:%M %Z')} | high={f['high']}")

                logger.info(f"{symbol}-{interval} Active LFractals: {len(L_fractals)}")
                for f in L_fractals:
                    ts = datetime.fromtimestamp(int(f["time"]) / 1000, tz=tz)
                    logger.info(f"  L @ {ts.strftime('%Y-%m-%d %H:%M %Z')} | low={f['low']}")

                # 4) Check breakouts
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

                # 5) Update storage at the very end
                storage = update_storage(storage, symbol, interval, candles_before_last, fractal_window)
                save_storage(storage, last_candle=last_candle)
                logger.info(
                    f"Storage updated and saved at {storage['metadata']['last_update_time']} "
                    f"(candle close {storage['metadata'].get('last_candle_close_time')})"
                )

            except Exception as e:
                logger.error(f"Fractal detection failed for {symbol}-{interval}: {e}")

    return storage


def main():
    with open("config.json") as f:
        config = json.load(f)

    # 1) Setup timezone and logger
    tz = pytz.timezone(config.get("timezone", "UTC"))
    logger = setup_logger(config)

    logger.info("Starting bot (Stage 2: fractals & breakouts)...")

    # 2) Load storage at start
    storage = load_storage()

    # 3) Run detection and update storage
    storage = run_fractal_detection(config, tz, logger, storage)

    # 4) Log storage save info (testing only)
    if "last_candle_close_time" in storage.get("metadata", {}):
        close_dt = datetime.fromtimestamp(
            storage["metadata"]["last_candle_close_time"]/1000, tz=tz
        )
        logger.debug(
            f"Saving storage at {storage['metadata']['last_update_time']} UTC "
            f"(candle close {close_dt.strftime('%Y-%m-%d %H:%M:%S %Z')})"
        )

    logger.info("Cycle finished.")

if __name__ == "__main__":
    main()

# python main.py
# python -m main

'''
def main():
    with open("config.json") as f:
        config = json.load(f)

    tz = pytz.timezone(config.get("timezone", "UTC"))
    logger = setup_logger(config)
    
    logger.info("Starting bot (Stage 2: fractals & breakouts)...")

    # Load storage at start
    storage = load_storage()

    # Run detection and update storage
    storage = run_fractal_detection(config, tz, logger, storage)

    # Save step not needed here anymore (already done per symbol/interval)
    logger.info("Cycle finished.")
'''