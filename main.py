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

def detect_fractals(candles: list[dict], fractal_window: int) -> tuple[list[dict], list[dict]]:
    """Detect active HFractals and LFractals."""
    n = (fractal_window - 1) // 2
    H_fractals, L_fractals = [], []

    for i in range(n, len(candles) - n):
        mid = candles[i]
        left = candles[i-n:i]
        right = candles[i+1:i+n+1]

        # HFractal: mid.high > all highs left/right
        if all(mid["high"] > c["high"] for c in left+right):
            H_fractals.append({
                "type": "HFractal",
                "time": mid["close_time"],
                "high": mid["high"],
            })

        # LFractal: mid.low < all lows left/right
        if all(mid["low"] < c["low"] for c in left+right):
            L_fractals.append({
                "type": "LFractal",
                "time": mid["close_time"],
                "low": mid["low"],
            })

    # --- Keep only active fractals ---
    active_H = []
    for f in H_fractals:
        broken = any(c["high"] > f["high"] for c in candles if c["close_time"] > f["time"])
        if not broken:
            active_H.append(f)

    active_L = []
    for f in L_fractals:
        broken = any(c["low"] < f["low"] for c in candles if c["close_time"] > f["time"])
        if not broken:
            active_L.append(f)

    # Sort (latest first, wedge order)
    active_H.sort(key=lambda x: (x["time"], x["high"]), reverse=True)
    active_L.sort(key=lambda x: (x["time"], -x["low"]), reverse=True)

    return active_H, active_L

def run_fractal_detection(config, tz, logger):
    interval_map = config.get("interval_map", {})
    history_limit = config.get("history_limit", 200)
    fractal_window = config.get("fractal_window", 5)
    send_messages = config["send_messages"]

    for symbol in config["symbols"]:
        for interval in config["intervals"]:
            try:
                candles = bingx_api.get_candles(symbol, interval, history_limit, interval_map)
                H_fractals, L_fractals = detect_fractals(candles, fractal_window)

                # Print ALL active fractals in the terminal
                logger.info(f"\n{symbol}-{interval} Active HFractals:")
                for f in H_fractals:
                    ts = datetime.fromtimestamp(f["time"]/1000, tz=tz)
                    logger.info(f"  H @ {ts.strftime('%Y-%m-%d %H:%M %Z')} | high={f['high']}")

                logger.info(f"{symbol}-{interval} Active LFractals:")
                for f in L_fractals:
                    ts = datetime.fromtimestamp(f["time"]/1000, tz=tz)
                    logger.info(f"  L @ {ts.strftime('%Y-%m-%d %H:%M %Z')} | low={f['low']}")

                # --- Send SEPARATE Telegram messages for last H and L ---
                if send_messages:
                    if H_fractals:
                        last_H = H_fractals[0]
                        ts = datetime.fromtimestamp(last_H["time"]/1000, tz=tz)
                        msg = (
                            f"🔹 <b>Fractal detected</b>\n"
                            f"Symbol: {symbol}, Interval: {interval}\n"
                            f"Type: HFractal\n"
                            f"Time: {ts.strftime('%Y-%m-%d %H:%M %Z')}\n"
                            f"High: {last_H['high']}"
                        )
                        send_signal(msg)

                    if L_fractals:
                        last_L = L_fractals[0]
                        ts = datetime.fromtimestamp(last_L["time"]/1000, tz=tz)
                        msg = (
                            f"🔹 <b>Fractal detected</b>\n"
                            f"Symbol: {symbol}, Interval: {interval}\n"
                            f"Type: LFractal\n"
                            f"Time: {ts.strftime('%Y-%m-%d %H:%M %Z')}\n"
                            f"Low: {last_L['low']}"
                        )
                        send_signal(msg)

            except Exception as e:
                logger.error(f"Fractal detection failed for {symbol}-{interval}: {e}")

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
    run_fractal_detection(config, tz, logger)

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