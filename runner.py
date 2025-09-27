# runner.py
import os
import json
import logging
import asyncio
from datetime import datetime, timedelta, timezone
import pytz
import main
from core.telegram_bot_async import send_signal  # for Telegram alerts

# --- log cleanup settings (defaults, can be overridden by config.json) ---
LOG_PATH = "logs/runner.log"
CLEAN_INTERVAL = timedelta(hours=1)   # default
_last_cleanup = datetime.now(timezone.utc)
_error_detected = False


def get_next_run_time(tz, interval_minutes, delay_seconds):
    """
    Calculate the next run time aligned to the interval + delay.
    Example: if interval=5m, and now is 10:02, next run is 10:06 (close at 10:05 + 60s).
    """
    now = datetime.now(tz)
    # Round down to nearest interval
    minutes = (now.minute // interval_minutes) * interval_minutes
    base = now.replace(second=0, microsecond=0, minute=minutes)

    # Next close = base + interval
    next_close = base + timedelta(minutes=interval_minutes)

    # Add delay
    run_time = next_close + timedelta(seconds=delay_seconds)
    return run_time


async def clean_log_if_needed(logger):
    """Clear runner.log periodically unless errors detected."""
    global _last_cleanup, _error_detected

    now = datetime.now(timezone.utc)
    if now - _last_cleanup >= CLEAN_INTERVAL:
        if _error_detected:
            logger.warning("Skipped log cleanup due to errors.")
            try:
                await send_signal("⚠️ Bot error detected - log cleanup skipped, check runner.log!")
            except Exception as e:
                logger.error(f"Failed to send Telegram alert: {e}")
            _error_detected = False  # reset after alert
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

    storage = main.load_storage()  # pre-load storage once
    prev_symbols = None            # track last used symbol set

    while True:
        # ⏳ Wait until next scheduled run
        next_run = get_next_run_time(tz, interval_minutes=interval_minutes, delay_seconds=delay_seconds)
        now = datetime.now(tz)
        wait_seconds = (next_run - now).total_seconds()
        if wait_seconds > 0:
            logger.info(f"Next run scheduled at {next_run.strftime('%Y-%m-%d %H:%M:%S %Z')} "
                        f"(waiting {int(wait_seconds)}s)")
            await asyncio.sleep(wait_seconds)

        # 🔄 Reload config.json each cycle
        try:
            with open("config.json", "r", encoding="utf-8") as f:
                config = json.load(f)

            # direct list from config
            active_symbols = config.get("top_symbols", [])
            if not isinstance(active_symbols, list):
                logger.error("top_symbols in config.json is not a list. Skipping run.")
                await asyncio.sleep(5)
                continue

            # 🅰️ Display symbols sorted alphabetically without USDT
            display_symbols = sorted([s.replace("USDT", "") for s in active_symbols])

            # 📢 Send Telegram alert if symbols changed
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

        # ▶️ Run bot cycle
        logger.info(f"Running main.main() at {datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")
        try:
            storage = await main.main(config, tz, logger, storage)
        except Exception as e:
            logger.exception(f"[runner] Error while running main: {e}")
            _error_detected = True
            try:
                await send_signal(f"❌ Bot crashed with error: {e}")
            except Exception as te:
                logger.error(f"Failed to send Telegram alert: {te}")

        # 🧹 Periodic log cleanup
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

    # Also print to console
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
# python runner.py