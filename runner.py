# runner.py
import os
import time
import json
import logging
from datetime import datetime, timedelta, timezone
import pytz
import main
from core.telegram_bot import send_signal  # for Telegram alerts

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


def clean_log_if_needed(logger):
    """Clear runner.log periodically unless errors detected."""
    global _last_cleanup, _error_detected

    now = datetime.now(timezone.utc)
    if now - _last_cleanup >= CLEAN_INTERVAL:
        if _error_detected:
            logger.warning("Skipped log cleanup due to errors.")
            try:
                send_signal("⚠️ Bot error detected – log cleanup skipped, check runner.log!")
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


def runner_loop(tz):
    """Run main.main() at strict schedule."""
    logger = setup_runner_logger()
    global _error_detected

    while True:
        next_run = get_next_run_time(tz, interval_minutes=interval_minutes, delay_seconds=delay_seconds)
        now = datetime.now(tz)

        wait_seconds = (next_run - now).total_seconds()
        if wait_seconds > 0:
            logger.info(f"Next run scheduled at {next_run.strftime('%Y-%m-%d %H:%M:%S %Z')} "
                        f"(waiting {int(wait_seconds)}s)")
            time.sleep(wait_seconds)

        # Run main.py once (fractal detection + breakouts + storage update)
        logger.info(f"Running main.main() at {datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")
        try:
            main.main()
        except Exception as e:
            logger.exception(f"[runner] Error while running main: {e}")
            _error_detected = True
            try:
                send_signal(f"❌ Bot crashed with error: {e}")
            except Exception as te:
                logger.error(f"Failed to send Telegram alert: {te}")

        # cleanup step
        clean_log_if_needed(logger)


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
    # Load timezone and cleanup interval from config.json
    import json
    with open("config.json") as f:
        config = json.load(f)

    tz = pytz.timezone(config.get("timezone", "UTC"))

    cleanup_minutes = config["runner_log_cleanup_minutes"]
    CLEAN_INTERVAL = timedelta(minutes=cleanup_minutes)

    interval_minutes = config["runner_interval_minutes"]
    delay_seconds = config["runner_delay_seconds"]

    print(f"[runner] Starting runner loop (5m interval, +60s delay), "
          f"timezone={tz}, cleanup_interval={CLEAN_INTERVAL}")
    runner_loop(tz)

# python runner.py