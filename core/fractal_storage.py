# core/fractal_storage.py
import json
import os
import logging
from datetime import datetime

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
    return {"metadata": {"last_full_scan": None}}


def save_storage(storage: dict, path: str = "storage.json", last_candle: dict | None = None):
    """Save fractal storage to file."""
    try:
        # Add test-only metadata field
        storage.setdefault("metadata", {})
        storage["metadata"]["last_update_time"] = datetime.now().isoformat() + "Z"
        
        if last_candle is not None:
            storage["metadata"]["last_candle_close_time"] = int(last_candle["timestamp"])

        with open(path, "w") as f:
            json.dump(storage, f, indent=2)
        
        logger.info(
            f"Storage saved to {path} at {storage['metadata']['last_update_time']}"
            f"(candle close {storage['metadata'].get('last_candle_close_time')})"
        )

    except Exception as e:
        logger.error(f"Failed to save storage to {path}: {e}")

def init_full_scan(symbols, intervals, fractal_window, history_limit, interval_map, tz, get_candles_fn) -> dict:
    """
    Run full market scan, detect all active fractals, return storage dict.
    get_candles_fn: function(symbol, interval, limit, interval_map)
    """
    storage = {}

    for symbol in symbols:
        storage[symbol] = {}
        for interval in intervals:
            try:
                candles = get_candles_fn(symbol, interval, history_limit, interval_map)
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

    storage["metadata"] = {"last_full_scan": datetime.utcnow().isoformat() + "Z"}
    return storage


def update_storage(storage: dict, symbol: str, interval: str, candles: list, fractal_window: int) -> dict:
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

