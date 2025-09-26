# core/fractal_storage.py
import json
import os
import logging
from datetime import datetime, timezone
from typing import Callable, Awaitable
from modules.fractals import detect_fractals

logger = logging.getLogger("sweep")


def load_storage(path: str = "storage.json") -> dict:
    """Load fractal storage from file (or return empty structure)."""
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load storage from {path}: {e}")
    # Always return with metadata keys present
    return {
        "metadata": {
            "last_full_scan": None,
            "last_update_time": None,
            "last_candle_close_time": None,
        }
    }


def save_storage(storage: dict, path: str = "storage.json", last_candle: dict | None = None):
    """Save fractal storage to file with tz-aware ISO8601 timestamps."""
    try:
        storage.setdefault("metadata", {})
        now = datetime.now(timezone.utc).isoformat()  # ✅ tz-aware
        storage["metadata"]["last_update_time"] = now

        if last_candle is not None:
            storage["metadata"]["last_candle_close_time"] = int(last_candle["timestamp"])

        with open(path, "w") as f:
            json.dump(storage, f, indent=2)

        logger.info(
            f"Storage saved to {path} at {storage['metadata']['last_update_time']} "
            f"(candle close {storage['metadata'].get('last_candle_close_time')})"
        )

    except Exception as e:
        logger.error(f"Failed to save storage to {path}: {e}")


def normalize_candles(candles: list[dict]) -> list[dict]:
    """Ensure every candle has a close_time key for consistency."""
    for c in candles:
        if "close_time" not in c and "timestamp" in c:
            c["close_time"] = c["timestamp"]
    return candles


async def init_full_scan(
    symbols,
    base_interval,
    higher_intervals,
    fractal_window,
    history_limit,
    interval_map,
    tz,
    get_candles_fn: Callable[[str, str, int, dict], Awaitable[list[dict]]],
) -> dict:
    """
    Run full market scan asynchronously, detect all active fractals, return storage dict.
    get_candles_fn must be async: (symbol, interval, limit, interval_map) -> list[dict]
    """
    storage = {}
    all_intervals = [base_interval] + list(higher_intervals)

    for symbol in symbols:
        storage[symbol] = {}
        for interval in all_intervals:
            try:
                candles = normalize_candles(
                    await get_candles_fn(symbol, interval, history_limit, interval_map)
                )
                candles.sort(key=lambda c: int(c["close_time"]))
                H_fractals, L_fractals = detect_fractals(candles, fractal_window)

                storage[symbol][interval] = {
                    "H": H_fractals,
                    "L": L_fractals,
                }

                logger.info(
                    f"{symbol}-{interval} full scan: H={len(H_fractals)} L={len(L_fractals)}"
                )
            except Exception as e:
                logger.error(f"Full scan failed for {symbol}-{interval}: {e}")

    now = datetime.now(timezone.utc).isoformat()  # ✅ tz-aware
    storage["metadata"] = {
        "last_full_scan": now,
        "last_update_time": now,
        "last_candle_close_time": None,
    }
    return storage


async def update_storage(
    storage: dict,
    symbol: str,
    interval: str,
    candles: list,
    fractal_window: int,
) -> dict:
    """
    Update storage incrementally:
      - remove broken fractals
      - add new fractals
      - keep still-active fractals
    """
    H_new, L_new = detect_fractals(candles, fractal_window)

    # Ensure symbol/interval structure exists
    if symbol not in storage:
        storage[symbol] = {}
    if interval not in storage[symbol]:
        storage[symbol][interval] = {"H": [], "L": []}

    # Remove broken H fractals
    storage[symbol][interval]["H"] = [
        f for f in storage[symbol][interval]["H"]
        if not any(c["high"] > f["high"] for c in candles if c["close_time"] > f["time"])
    ]

    # Remove broken L fractals
    storage[symbol][interval]["L"] = [
        f for f in storage[symbol][interval]["L"]
        if not any(c["low"] < f["low"] for c in candles if c["close_time"] > f["time"])
    ]

    # Add new H fractals (avoid duplicates)
    for f in H_new:
        if not any(existing["time"] == f["time"] and existing["high"] == f["high"]
                   for existing in storage[symbol][interval]["H"]):
            storage[symbol][interval]["H"].append(f)

    # Add new L fractals (avoid duplicates)
    for f in L_new:
        if not any(existing["time"] == f["time"] and existing["low"] == f["low"]
                   for existing in storage[symbol][interval]["L"]):
            storage[symbol][interval]["L"].append(f)

    # Sort newest first
    storage[symbol][interval]["H"].sort(key=lambda x: (x["time"], x["high"]), reverse=True)
    storage[symbol][interval]["L"].sort(key=lambda x: (x["time"], -x["low"]), reverse=True)

    return storage


def handle_htf_match(storage, symbol, breakout, higher_intervals):
    """Check if breakout fractal matches HTF fractals and clean up."""
    fractal_value = breakout["fractal_value"]
    ftype = breakout["fractal_side"]

    matched_htfs = []
    for interval in higher_intervals:
        if interval not in storage.get(symbol, {}):
            continue
        if ftype not in storage[symbol][interval]:
            continue

        before = len(storage[symbol][interval][ftype])
        storage[symbol][interval][ftype] = [
            f for f in storage[symbol][interval][ftype]
            if f.get("high") != fractal_value and f.get("low") != fractal_value
        ]
        after = len(storage[symbol][interval][ftype])

        if before != after:
            matched_htfs.append(interval)

    return storage, matched_htfs

