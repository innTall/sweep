# modules/breakouts.py
from datetime import datetime

def _format_ts(ts_ms: int, tz) -> str:
    """Format timestamp (ms) into human-readable string."""
    return datetime.fromtimestamp(ts_ms / 1000, tz=tz).strftime("%Y-%m-%d %H:%M")

def check_breakouts(symbol, interval, H_fractals, L_fractals, candle, tz, interval_map):
    """
    Check if the last confirmed candle breaks any active fractals.
    Returns a breakout dict or None.
    """
    candle_high = candle["high"]
    candle_low = candle["low"]
    candle_close = candle["close"]
    candle_time = candle["timestamp"]

    breakout = None
    interval_seconds = interval_map[interval]  # already in seconds

    # 1. Check HFractals (bearish fractals at highs)
    broken_h = [f for f in H_fractals if candle_high > f["high"]]
    if broken_h:
        # pick the "furthest" = highest high (most recent to break)
        target_fractal = max(broken_h, key=lambda f: f["high"])
        breakout_type = "HConfirm" if candle_close > target_fractal["high"] else "HSweep"

        distance = int((candle_time - target_fractal["time"]) / (interval_seconds * 1000))

        breakout = {
            "symbol": symbol,
            "interval": interval,
            "type": breakout_type,
            "fractal_value": target_fractal["high"],
            "fractal_time": target_fractal["time"],
            "candle_high": candle_high,
            "candle_low": candle_low,
            "candle_close": candle_close,
            "candle_time": candle_time,
            "distance": distance,
        }

    # 2. Check LFractals (bullish fractals at lows) if no H breakout
    if breakout is None:
        broken_l = [f for f in L_fractals if candle_low < f["low"]]
        if broken_l:
            # pick the "furthest" = lowest low (most recent to break)
            target_fractal = min(broken_l, key=lambda f: f["low"])
            breakout_type = "LConfirm" if candle_close < target_fractal["low"] else "LSweep"

            distance = int((candle_time - target_fractal["time"]) / (interval_seconds * 1000))

            breakout = {
                "symbol": symbol,
                "interval": interval,
                "type": breakout_type,
                "fractal_value": target_fractal["low"],
                "fractal_time": target_fractal["time"],
                "candle_high": candle_high,
                "candle_low": candle_low,
                "candle_close": candle_close,
                "candle_time": candle_time,
                "distance": distance,
            }

    return breakout

def format_breakout_message(breakout: dict, tz) -> str:
    """Format breakout signal for Telegram."""

    # Choose icon + label based on type
    if breakout["type"] in ("HSweep", "HConfirm"):
        icon = "🟩"
    else:
        icon = "🟥"

    # Distance in candles between fractal and breakout
    distance = breakout.get("distance", "?")

    # Format fractal time
    ftime = datetime.fromtimestamp(breakout["fractal_time"] / 1000, tz=tz)
    ftime_str = ftime.strftime("%d%b %H:%M")

    # Layout message
    if breakout["type"].startswith("H"):  # High breakout
        msg = (
            f"{icon} {breakout['type']} ({distance})\n"
            f"Symbol: {breakout['symbol']}, {breakout['interval']}\n"
            f"HFractal High={breakout['fractal_value']} | {ftime_str}\n"
            f"BreakCandle High={breakout['candle_high']}"
        )
    else:  # Low breakout
        msg = (
            f"{icon} {breakout['type']} ({distance})\n"
            f"Symbol: {breakout['symbol']}, {breakout['interval']}\n"
            f"LFractal Low={breakout['fractal_value']} | {ftime_str}\n"
            f"BreakCandle Low={breakout['candle_low']}"
        )

    return msg
