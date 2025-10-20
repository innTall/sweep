# main.py
import logging
from core.telegram_bot_async import send_signal
from modules.fractals import detect_fractals
from modules.breakouts import check_breakouts, format_breakout_message

def setup_logger(config: dict):
    level_str = config.get("logging", {}).get("level", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)
    logging.basicConfig(level=level,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    return logging.getLogger("sweep")

async def run_fractal_detection(config, tz, logger, storage_mgr, bingx_api):
    base_interval = config["base_interval"]
    interval_map = config["interval_map"]
    history_limit = int(config["history_limit"])
    fractal_window = int(config["fractal_window"])
    send_messages = config["send_messages"]

    def normalize_candles(candles):
        for c in candles:
            if "close_time" not in c and "timestamp" in c:
                c["close_time"] = c["timestamp"]
        return candles

    # async with BingxApiAsync() as bingx_api:
    for symbol in config["top_symbols"]:
        try:
            last_candle = await bingx_api.get_last_confirmed_candle(symbol, base_interval, interval_map)

            candles = normalize_candles(
                await bingx_api.get_candles(symbol, base_interval, history_limit, interval_map)
            )
            candles.sort(key=lambda c: int(c["close_time"]))
            candles_before_last = [c for c in candles if int(c["close_time"]) < int(last_candle["timestamp"])]

            # ✅ Get all currently active fractals from storage (not limited history)
            H_fractals, L_fractals = await storage_mgr.get_active_fractals(symbol, base_interval)
            breakout = check_breakouts(symbol, base_interval, H_fractals, L_fractals, last_candle, tz, interval_map)
            logger.info(f"{symbol}-{base_interval} {history_limit}: H={len(H_fractals)} L={len(L_fractals)}")

            if breakout:
                from core.fractal_storage import handle_htf_match
                storage_mgr.storage, matched_htfs = handle_htf_match(
                    storage_mgr.storage, symbol, breakout, config["higher_intervals"]
                )

                # --- Only send if breakout matches any HTF fractal ---
                has_htf_match = len(matched_htfs) > 0

                message = format_breakout_message(
                    breakout, tz,
                    H_fractals=H_fractals, L_fractals=L_fractals,
                    storage=storage_mgr.storage,
                    higher_intervals=config["higher_intervals"],
                    matched_htfs=matched_htfs,
                )

                if has_htf_match:
                    logger.info(f"✅ HTF breakout detected: {message}")
                    if send_messages:
                        await send_signal(message)
                else:
                    logger.info(f"⚙️ 15m breakout ignored (no HTF match).")

                # continue updating live data
                await storage_mgr.update_live([symbol])

        except Exception as e:
            logger.error(f"Detection failed for {symbol}: {e}")

    return storage_mgr

async def main(config, tz, logger, storage_mgr, bingx_api):
    """
    Main entrypoint for fractal detection and breakout handling.
    config, tz, logger, storage must be passed in from runner.py
    """
    logger.info("Starting bot (Stage 2: fractals & breakouts)...")

    # storage = await ensure_storage(config, tz, logger)
    storage_mgr = await run_fractal_detection(config, tz, logger, storage_mgr, bingx_api)

    logger.info("Cycle finished.")
    return storage_mgr

if __name__ == "__main__":
    # Prevent accidental direct execution
    print("⚠️ Please run the bot using runner.py, not main.py")
    
# python main.py
# python -m main
