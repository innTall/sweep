# main.py
import json
import logging
import pytz
import asyncio
from datetime import datetime, timedelta
from utils.bingx_api_async import BingxApiAsync
from core.telegram_bot import send_signal
from modules.fractals import detect_fractals
from modules.breakouts import check_breakouts, format_breakout_message
from core.fractal_storage import load_storage, save_storage, update_storage, init_full_scan


def setup_logger(config: dict):
    level_str = config.get("logging", {}).get("level", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)
    logging.basicConfig(level=level,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    return logging.getLogger("sweep")


async def run_fractal_detection(config, tz, logger, storage):
    interval_map = config.get("interval_map", {})
    history_limit = int(config["history_limit"])
    fractal_window = int(config["fractal_window"])
    send_messages = config["send_messages"]
    base_interval = config["base_interval"]

    def normalize_candles(candles: list[dict]) -> list[dict]:
        """Ensure every candle has a close_time key for consistency."""
        for c in candles:
            if "close_time" not in c and "timestamp" in c:
                c["close_time"] = c["timestamp"]
        return candles

    async with BingxApiAsync() as bingx_api:
        for symbol in config["top_symbols"]:
            for interval in [base_interval]:  # base interval only, e.g. "15m"
                try:
                    # 1) Get the last confirmed (closed) candle
                    last_candle = await bingx_api.get_last_confirmed_candle(
                        symbol, interval, interval_map
                    )
                    logger.debug(
                        f"{symbol}-{interval} last_closed: ts={last_candle['timestamp']} close={last_candle['close']}"
                    )

                    # 2) Get history candles
                    candles = normalize_candles(
                        await bingx_api.get_candles(
                            symbol, interval, history_limit, interval_map
                        )
                    )
                    candles.sort(key=lambda c: int(c["close_time"]))
                    candles_before_last = [
                        c for c in candles if int(c["close_time"]) < int(last_candle["timestamp"])
                    ]

                    logger.debug(
                        f"{symbol}-{interval} fetched={len(candles)} before_last={len(candles_before_last)}"
                    )

                    if len(candles_before_last) < fractal_window:
                        logger.info(
                            f"Not enough history (before last close) for {symbol}-{interval} (need {fractal_window})"
                        )
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
                    breakout = check_breakouts(
                        symbol,
                        interval,
                        H_fractals,
                        L_fractals,
                        last_candle,
                        tz,
                        interval_map,
                    )
                    if breakout:
                        from core.fractal_storage import handle_htf_match

                        storage, matched_htfs = handle_htf_match(
                            storage, symbol, breakout, config["higher_intervals"]
                        )

                        message = format_breakout_message(
                            breakout,
                            tz,
                            H_fractals=H_fractals,
                            L_fractals=L_fractals,
                            storage=storage,
                            higher_intervals=config["higher_intervals"],
                            matched_htfs=matched_htfs,
                        )

                        logger.info(f"Breakout detected: {message}")
                        if send_messages:
                            send_signal(message)
                        else:
                            logger.info("Message sending disabled (send_messages=false)")
                    else:
                        logger.info(f"No breakout for {symbol}-{interval}")

                    # 5) Update storage for base + higher intervals
                    all_intervals = [interval] + config["higher_intervals"]
                    for iv in all_intervals:
                        candles_iv = normalize_candles(
                            await bingx_api.get_candles(symbol, iv, history_limit, interval_map)
                        )
                        candles_iv.sort(key=lambda c: int(c["close_time"]))
                        candles_before_last_iv = [
                            c for c in candles_iv if int(c["close_time"]) < int(last_candle["timestamp"])
                        ]

                        storage = await update_storage(   # ✅ now awaited
                            storage, symbol, iv, candles_before_last_iv, fractal_window
                        )

                    # Save storage once after all intervals updated
                    save_storage(storage, last_candle=last_candle)
                    logger.info(
                        f"Storage updated and saved at {storage['metadata']['last_update_time']} "
                        f"(candle close {storage['metadata'].get('last_candle_close_time')})"
                    )

                except Exception as e:
                    logger.error(f"Fractal detection failed for {symbol}-{interval}: {e}")

    return storage

async def ensure_storage(config, tz, logger):
    """Decide whether to run a full scan or continue from storage."""
    interval_map = config.get("interval_map", {})
    history_limit = int(config["history_limit"])
    fractal_window = int(config["fractal_window"])
    base_interval = config["base_interval"]
    higher_intervals = config["higher_intervals"]

    storage = load_storage()
    meta = storage.get("metadata", {})

    # 🔑 prune symbols not in config
    current_symbols = set(config["top_symbols"])
    stored_symbols = set(storage.keys()) - {"metadata"}
    removed = stored_symbols - current_symbols
    if removed:
        for sym in removed:
            del storage[sym]
        logger.info(f"Pruned {len(removed)} symbols from storage: {sorted(list(removed))}")
        save_storage(storage)

    now = datetime.now(tz)
    last_full_scan_str = meta.get("last_full_scan")
    last_candle_ts = meta.get("last_candle_close_time")

    force_full = False

    if not storage or not meta.get("last_full_scan"):
        logger.info("No valid storage found → running full scan")
        force_full = True
    elif last_full_scan_str:
        try:
            last_full_scan = datetime.fromisoformat(
                last_full_scan_str.replace("Z", "+00:00")
            )
            if last_full_scan.tzinfo is None:
                last_full_scan = last_full_scan.replace(tzinfo=tz)

            if now - last_full_scan > timedelta(hours=24):
                logger.info("Last full scan >24h ago → running full scan")
                force_full = True
        except Exception as e:
            logger.warning(f"Could not parse last_full_scan='{last_full_scan_str}': {e}")
            force_full = True

    if not force_full and last_candle_ts:
        last_candle_dt = datetime.fromtimestamp(int(last_candle_ts) / 1000, tz=tz)
        gap_minutes = (now - last_candle_dt).total_seconds() / 60
        base_interval_minutes = (
            int(base_interval.rstrip("m")) if "m" in base_interval else 15
        )

        if gap_minutes > history_limit * base_interval_minutes:
            logger.info(f"Gap {gap_minutes:.1f}m > history_limit × {base_interval} → full scan")
            force_full = True

    if force_full:
        async with BingxApiAsync() as bingx_api:
            storage = await init_full_scan(  # ✅ properly awaited
                config["top_symbols"],
                base_interval,
                higher_intervals,
                fractal_window,
                history_limit,
                interval_map,
                tz,
                bingx_api.get_candles
            )
        save_storage(storage)
        logger.info("Full scan complete and storage initialized.")
    else:
        logger.info("Continuing with existing storage.")

    return storage


async def main(config, tz, logger, storage):
    """
    Main entrypoint for fractal detection and breakout handling.
    config, tz, logger, storage must be passed in from runner.py
    """
    logger.info("Starting bot (Stage 2: fractals & breakouts)...")

    storage = await ensure_storage(config, tz, logger)
    storage = await run_fractal_detection(config, tz, logger, storage)

    logger.info("Cycle finished.")
    return storage


if __name__ == "__main__":
    with open("config.json") as f:
        config = json.load(f)

    tz = pytz.timezone(config.get("timezone", "UTC"))
    logger = setup_logger(config)
    storage = load_storage()

    asyncio.run(main(config, tz, logger, storage))
# python main.py
# python -m main
