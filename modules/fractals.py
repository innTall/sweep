# modules/fractals.py
import logging
from utils.bingx_api_async import BingxApiAsync

logger = logging.getLogger("sweep")

def detect_fractals(candles: list[dict], fractal_window: int):
    """Detect active HFractals and LFractals."""
    n = (fractal_window - 1) // 2
    H_fractals, L_fractals = [], []

    for i in range(n, len(candles) - n):
        mid = candles[i]
        left = candles[i-n:i]
        right = candles[i+1:i+n+1]

        # HFractal: mid.high > all highs left/right
        if all(mid["high"] > c["high"] for c in left+right):
            H_fractals.append({"type": "HFractal", "time": mid["close_time"], "high": mid["high"]})

        # LFractal: mid.low < all lows left/right
        if all(mid["low"] < c["low"] for c in left+right):
            L_fractals.append({"type": "LFractal", "time": mid["close_time"], "low": mid["low"]})

    # Keep only active fractals
    active_H = [f for f in H_fractals if not any(c["high"] > f["high"] for c in candles if c["close_time"] > f["time"])]
    active_L = [f for f in L_fractals if not any(c["low"] < f["low"] for c in candles if c["close_time"] > f["time"])]

    # Sort (latest first, wedge order)
    active_H.sort(key=lambda x: (x["time"], x["high"]), reverse=True)
    active_L.sort(key=lambda x: (x["time"], -x["low"]), reverse=True)

    return active_H, active_L

async def get_fractal_candles(symbol: str, interval: str, limit: int, interval_map: dict):
    async with BingxApiAsync() as bingx_api:
        return await bingx_api.get_candles(symbol, interval, limit, interval_map)
