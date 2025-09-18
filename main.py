# main.py
import json
import logging
import pytz
from datetime import datetime
from modules import fractals
from core.telegram_bot import send_signal
import utils.bingx_api as bingx_api

def setup_logger(config: dict):
    level_str = config.get("logging", {}).get("level", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)
    logging.basicConfig(level=level,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    return logging.getLogger("sweep")

def run_fractal_detection(config, tz, logger):
    interval_map = config.get("interval_map", {})
    history_limit = config.get("history_limit", 200)
    fractal_window = config.get("fractal_window", 5)
    send_messages = config["send_messages"]

    for symbol in config["symbols"]:
        for interval in config["intervals"]:
            try:
                candles = fractals.get_candles(symbol, interval, history_limit, interval_map)
                H_fractals, L_fractals = fractals.detect_fractals(candles, fractal_window)

                # Print all active fractals
                logger.info(f"\n{symbol}-{interval} Active HFractals:")
                for f in H_fractals:
                    ts = datetime.fromtimestamp(f["time"]/1000, tz=tz)
                    logger.info(f"  H @ {ts.strftime('%Y-%m-%d %H:%M %Z')} | high={f['high']}")

                logger.info(f"{symbol}-{interval} Active LFractals:")
                for f in L_fractals:
                    ts = datetime.fromtimestamp(f["time"]/1000, tz=tz)
                    logger.info(f"  L @ {ts.strftime('%Y-%m-%d %H:%M %Z')} | low={f['low']}")

                # Send latest fractals to Telegram
                if send_messages:
                    if H_fractals:
                        last_H = H_fractals[0]
                        ts = datetime.fromtimestamp(last_H["time"]/1000, tz=tz)
                        msg = f"🔹 <b>Fractal detected</b>\nSymbol: {symbol}, Interval: {interval}\nType: HFractal\nTime: {ts.strftime('%Y-%m-%d %H:%M %Z')}\nHigh: {last_H['high']}"
                        send_signal(msg)

                    if L_fractals:
                        last_L = L_fractals[0]
                        ts = datetime.fromtimestamp(last_L["time"]/1000, tz=tz)
                        msg = f"🔹 <b>Fractal detected</b>\nSymbol: {symbol}, Interval: {interval}\nType: LFractal\nTime: {ts.strftime('%Y-%m-%d %H:%M %Z')}\nLow: {last_L['low']}"
                        send_signal(msg)

            except Exception as e:
                logger.error(f"Fractal detection failed for {symbol}-{interval}: {e}")

def main():
    with open("config.json") as f:
        config = json.load(f)

    tz = pytz.timezone(config.get("timezone", "UTC"))
    logger = setup_logger(config)

    logger.info("Starting bot (Stage 2: fractals)...")
    run_fractal_detection(config, tz, logger)

if __name__ == "__main__":
    main()

# python main.py
# python -m main