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
        target_fractal = max(broken_h, key=lambda f: f["high"])
        breakout_type = "HConfirm" if candle_close > target_fractal["high"] else "HSweep"

        distance = int((candle_time - target_fractal["time"]) / (interval_seconds * 1000))

        breakout = {
            "symbol": symbol,
            "interval": interval,
            "type": breakout_type,
            "fractal_side": "H",   # ✅ added
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
            target_fractal = min(broken_l, key=lambda f: f["low"])
            breakout_type = "LConfirm" if candle_close < target_fractal["low"] else "LSweep"

            distance = int((candle_time - target_fractal["time"]) / (interval_seconds * 1000))

            breakout = {
                "symbol": symbol,
                "interval": interval,
                "type": breakout_type,
                "fractal_side": "L",   # ✅ added
                "fractal_value": target_fractal["low"],
                "fractal_time": target_fractal["time"],
                "candle_high": candle_high,
                "candle_low": candle_low,
                "candle_close": candle_close,
                "candle_time": candle_time,
                "distance": distance,
            }

    return breakout

def format_breakout_message(
    breakout,
    tz,
    H_fractals=None,
    L_fractals=None,
    storage=None,
    higher_intervals=None,
    matched_htfs=None,
):
    """
    Format the Telegram breakout message.
    Example:
    ⬇️ LConfirm (12) [1h] [4h]
    BTCUSDT, 15m
    LFractal Low=111861.0 | 22Sep 21:45
    BreakCandle Low=111600.0
    Active HF-LF: 10-3/15m/ * 6-2/1h * 4-4/4h
    """

    icon = "⬆️" if breakout["type"].startswith("H") else "⬇️"
    distance = breakout.get("distance", "?")

    # --- HTF tags (if this breakout matched higher intervals) ---
    htf_tags = ""
    if matched_htfs:
        htf_tags = " " + " ".join(f"[{htf}]" for htf in matched_htfs)

    # Format fractal time
    ftime = datetime.fromtimestamp(int(breakout["fractal_time"]) / 1000, tz=tz)
    ftime_str = ftime.strftime("%d%b %H:%M")

    # --- Core breakout header line ---
    msg_lines = [
        f"{icon} {breakout['type']} ({distance}){htf_tags}",
        f"{breakout['symbol']} - {breakout['interval']}",
    ]

    # --- Fractal details ---
    if breakout["type"].startswith("H"):
        msg_lines.append(f"HFractal High={breakout['fractal_value']} | {ftime_str}")
        msg_lines.append(f"BreakCandle High={breakout['candle_high']}")
    else:
        msg_lines.append(f"LFractal Low={breakout['fractal_value']} | {ftime_str}")
        msg_lines.append(f"BreakCandle Low={breakout['candle_low']}")

    # --- Active fractals summary ---
    if storage is not None:
        parts = []

        # always include base interval (15m)
        if H_fractals is not None and L_fractals is not None:
            parts.append(f"{len(H_fractals)}-{len(L_fractals)}/15m")

        # add higher TFs
        if higher_intervals:
            for htf in higher_intervals:
                if htf in storage.get(breakout["symbol"], {}):
                    h_count = len(storage[breakout["symbol"]][htf].get("H", []))
                    l_count = len(storage[breakout["symbol"]][htf].get("L", []))
                    parts.append(f"{h_count}-{l_count}/{htf}")

        if parts:
            msg_lines.append("Active HF-LF: " + " * ".join(parts))

    return "\n".join(msg_lines)

'''
def format_breakout_message(
    breakout,
    tz,
    H_fractals=None,
    L_fractals=None,
    storage=None,
    higher_intervals=None,
    matched_htfs=None,
):
    """
    Format the Telegram breakout message.
    Example:
    ⬇️ LConfirm (12) [1h] [4h]
    Symbol: BTCUSDT, 15m
    LFractal Low=111861.0 | 22Sep 21:45
    BreakCandle Low=111600.0
    Active fractals: H=10 L=3 [15m] * H=6 L=2 [1h] * H=4 L=4 [4h]
    """

    icon = "⬆️" if breakout["type"].startswith("H") else "⬇️"
    distance = breakout.get("distance", "?")

    # --- HTF tags (if this breakout matched higher intervals) ---
    htf_tags = ""
    if matched_htfs:
        htf_tags = " " + " ".join(f"[{htf}]" for htf in matched_htfs)

    # Format fractal time
    ftime = datetime.fromtimestamp(int(breakout["fractal_time"]) / 1000, tz=tz)
    ftime_str = ftime.strftime("%d%b %H:%M")

    # --- Core breakout header line ---
    msg_lines = [
        f"{icon} {breakout['type']} ({distance}){htf_tags}",
        f"Symbol: {breakout['symbol']}, {breakout['interval']}",
    ]

    # --- Fractal details ---
    if breakout["type"].startswith("H"):
        msg_lines.append(f"HFractal High={breakout['fractal_value']} | {ftime_str}")
        msg_lines.append(f"BreakCandle High={breakout['candle_high']}")
    else:
        msg_lines.append(f"LFractal Low={breakout['fractal_value']} | {ftime_str}")
        msg_lines.append(f"BreakCandle Low={breakout['candle_low']}")

    # --- Active fractals summary ---
    if storage is not None:
        parts = []

        # always include base interval (15m)
        if H_fractals is not None and L_fractals is not None:
            parts.append(f"H={len(H_fractals)} L={len(L_fractals)} [15m]")

        # add higher TFs
        if higher_intervals:
            for htf in higher_intervals:
                if htf in storage.get(breakout["symbol"], {}):
                    h_count = len(storage[breakout["symbol"]][htf].get("H", []))
                    l_count = len(storage[breakout["symbol"]][htf].get("L", []))
                    parts.append(f"H={h_count} L={l_count} [{htf}]")

        if parts:
            msg_lines.append("Active fractals: " + " * ".join(parts))

    return "\n".join(msg_lines)
'''