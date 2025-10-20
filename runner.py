# runner.py
import os
import json
import logging
import asyncio
from datetime import datetime, timedelta, timezone
import pytz
import main
from modules.candles import CandleFetcher
from utils.bingx_api_async import BingxApiAsync
from core.telegram_bot_async import send_signal
from core.storage_manager import StorageManager

# --- log cleanup defaults ---
LOG_PATH = "logs/runner.log"
CLEAN_INTERVAL = timedelta(hours=1)
_last_cleanup = datetime.now(timezone.utc)
_error_detected = False


def get_next_run_time(tz, interval_minutes, delay_seconds):
    """Calculate next run time aligned to interval + delay."""
    now = datetime.now(tz)
    minutes = (now.minute // interval_minutes) * interval_minutes
    base = now.replace(second=0, microsecond=0, minute=minutes)
    next_close = base + timedelta(minutes=interval_minutes)
    run_time = next_close + timedelta(seconds=delay_seconds)
    return run_time


async def clean_log_if_needed(logger):
    """Periodically clean runner.log unless errors detected."""
    global _last_cleanup, _error_detected
    now = datetime.now(timezone.utc)
    if now - _last_cleanup >= CLEAN_INTERVAL:
        if _error_detected:
            logger.warning("Skipped log cleanup due to errors.")
            try:
                await send_signal("⚠️ Bot error detected - log cleanup skipped, check runner.log!")
            except Exception as e:
                logger.error(f"Failed to send Telegram alert: {e}")
            _error_detected = False
        else:
            try:
                open(LOG_PATH, "w").close()
                logger.info(f"runner.log cleaned at {now.isoformat()}")
            except Exception as e:
                logger.error(f"Failed to clean runner.log: {e}")
        _last_cleanup = now


async def runner_loop(tz, interval_minutes, delay_seconds):
    logger = setup_runner_logger()
    global _error_detected

    # Load config
    with open("config.json", "r", encoding="utf-8") as f:
        config = json.load(f)

    async with BingxApiAsync(timeout=config.get("timeouts", {}).get("http", 15)) as bingx_api:
        # Initialize CandleFetcher (only config + interval_map)
        candle_fetcher = CandleFetcher(config, config["interval_map"])
        # StorageManager handles scans and live updates
        storage_mgr = StorageManager(config, config["interval_map"], tz)

        # Compute downtime since last candle
        last_candle_ts = storage_mgr.storage.get("metadata", {}).get("last_candle_close_time")
        if last_candle_ts:
            last_dt = datetime.fromtimestamp(int(last_candle_ts)/1000, tz=tz)
            downtime = int((datetime.now(tz) - last_dt).total_seconds() / 60)
        else:
            downtime = None  # force full scan

        # Check if config.json updated
        try:
            config_mtime = datetime.fromtimestamp(os.path.getmtime("config.json"), tz=tz)
        except Exception:
            config_mtime = None

        last_full_iso = storage_mgr.storage.get("metadata", {}).get("last_full_scan")
        last_full_dt = (datetime.fromisoformat(last_full_iso.replace("Z", "+00:00")).astimezone(tz)
                        if last_full_iso else None)

        force_full = last_full_dt is None or (config_mtime and config_mtime > last_full_dt) \
                     or downtime is None or downtime > int(config.get("history_limit", 0))

        scan_limit = int(config.get("full_scan_limit", config.get("history_limit", 0)))

        # Initial storage startup
        await storage_mgr.startup(config["top_symbols"], downtime, force_full=force_full, scan_limit=scan_limit)

        prev_symbols = None

        # Main scheduled loop
        while True:
            next_run = get_next_run_time(tz, interval_minutes, delay_seconds)
            now = datetime.now(tz)
            wait_seconds = (next_run - now).total_seconds()
            if wait_seconds > 0:
                logger.info(f"Next run at {next_run.strftime('%Y-%m-%d %H:%M:%S %Z')} "
                            f"(waiting {int(wait_seconds)}s)")
                await asyncio.sleep(wait_seconds)

            # Reload config for dynamic top_symbols
            try:
                with open("config.json", "r", encoding="utf-8") as f:
                    config = json.load(f)
                active_symbols = config.get("top_symbols", [])
                display_symbols = sorted([s.replace("USDT", "") for s in active_symbols])

                if prev_symbols is None or set(prev_symbols) != set(active_symbols):
                    logger.info(f"Active top_symbols updated: {display_symbols[:10]}... "
                                f"({len(active_symbols)} total)")
                    try:
                        await send_signal(f"🔄 Active symbols updated:\n{', '.join(display_symbols)}")
                    except Exception as e:
                        logger.error(f"Failed to send Telegram alert: {e}")
                    prev_symbols = active_symbols

            except Exception as e:
                logger.error(f"Failed to reload config.json: {e}")
                await asyncio.sleep(5)
                continue

            # Run main bot cycle
            logger.info(f"Running main.main() at {datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")
            try:
                await main.main(config, tz, logger, storage_mgr, bingx_api)
            except Exception as e:
                logger.exception(f"[runner] Error in main: {e}")
                _error_detected = True
                try:
                    await send_signal(f"❌ Bot crashed with error: {e}")
                except Exception as te:
                    logger.error(f"Failed to send Telegram alert: {te}")

            # Periodic log cleanup
            await clean_log_if_needed(logger)


def setup_runner_logger():
    os.makedirs("logs", exist_ok=True)
    log_file = LOG_PATH

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logging.getLogger("").addHandler(console)

    return logging.getLogger("runner")


if __name__ == "__main__":
    with open("config.json") as f:
        config = json.load(f)

    tz = pytz.timezone(config.get("timezone", "UTC"))
    cleanup_minutes = config["runner_log_cleanup_minutes"]
    CLEAN_INTERVAL = timedelta(minutes=cleanup_minutes)
    interval_minutes = config["runner_interval_minutes"]
    delay_seconds = config["runner_delay_seconds"]

    print(f"[runner] Starting runner loop ({interval_minutes}m interval, +{delay_seconds}s delay), "
          f"timezone={tz}, cleanup_interval={CLEAN_INTERVAL}")

    asyncio.run(runner_loop(tz, interval_minutes, delay_seconds))

# venv\Scripts\activate.bat
# python runner.py