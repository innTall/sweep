# utils/bingx_api.py
import requests
import logging
from typing import Any

APIURL = "https://open-api.bingx.com/openApi/swap/v3/quote/klines"
logger = logging.getLogger("sweep")

# Global variables
Last_Closed_Candle = None
Last_Close_Price = None
Last_Close_Time = None


def _normalize_symbol(symbol: str) -> str:
    return symbol.replace("USDT", "-USDT")


def get_last_confirmed_candle(symbol: str, interval: str, interval_map: dict) -> dict[str, Any]:
    """Fetch the last confirmed candle (penultimate) for a symbol/interval."""
    global Last_Closed_Candle, Last_Close_Price, Last_Close_Time

    if interval not in interval_map:
        raise ValueError(f"Interval {interval} not defined in interval_map")

    params = {"symbol": _normalize_symbol(symbol), "interval": interval, "limit": 3}
    logger.debug(f"Fetching last confirmed candle: {params}")

    response = requests.get(APIURL, params=params)
    response.raise_for_status()
    data = response.json()

    if isinstance(data, dict):
        candles = data.get("data", [])
    elif isinstance(data, list):
        candles = data
    else:
        raise ValueError("Unexpected response format")

    if len(candles) < 2:
        raise ValueError("Not enough candles returned")

    c = candles[-2]  # last closed
    Last_Closed_Candle = c

    # Determine open timestamp
    if isinstance(c, dict):
        open_ts = c.get("time") or c.get("openTime")
        if open_ts is None:
            raise ValueError("Candle missing time field")
        o, h, l, cl = float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])
        Last_Close_Price = cl
    else:  # list format
        open_ts = c[0]
        o, h, l, cl = float(c[1]), float(c[2]), float(c[3]), float(c[4])
        Last_Close_Price = cl

    # Compute close time: open time + interval
    Last_Close_Time = int(open_ts) + interval_map[interval] * 1000  # milliseconds

    return {
        "symbol": symbol,
        "interval": interval,
        "timestamp": Last_Close_Time,
        "open": o,
        "high": h,
        "low": l,
        "close": cl,
    }
