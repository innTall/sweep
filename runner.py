# runner.py
import os
import time
import logging
from datetime import datetime, timedelta
import pytz
import main


def get_next_run_time(tz, interval_minutes=5, delay_seconds=60):
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


def runner_loop(tz):
    """Run main.main() at strict schedule."""
    logger = setup_runner_logger()
    while True:
        next_run = get_next_run_time(tz, interval_minutes=5, delay_seconds=60)
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
            print(f"[runner] Error while running main: {e}")

def setup_runner_logger():
    os.makedirs("logs", exist_ok=True)
    log_file = "logs/runner.log"

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
    # Load timezone from config.json
    import json
    with open("config.json") as f:
        config = json.load(f)

    tz = pytz.timezone(config.get("timezone", "UTC"))

    print(f"[runner] Starting runner loop (5m interval, +60s delay), timezone={tz}")
    runner_loop(tz)

# python runner.py