# utils/bingx_api_async.py
import json
import logging
from typing import Any, Optional
import aiohttp

APIURL = "https://open-api.bingx.com/openApi/swap/v3/quote/klines"
logger = logging.getLogger("sweep")

# Load config.json for timeout
try:
    CONFIG = json.load(open("config.json", encoding="utf-8"))
except Exception:
    CONFIG = {}

HTTP_TIMEOUT = CONFIG.get("timeouts", {}).get("http", 15)

def _normalize_symbol(symbol: str) -> str:
    return symbol.replace("USDT", "-USDT")

class BingxApiAsync:
    """Asynchronous client for BingX USDT-M Futures API."""

    def __init__(self, timeout: int = HTTP_TIMEOUT):
        self._session: Optional[aiohttp.ClientSession] = None
        self._timeout = aiohttp.ClientTimeout(total=timeout)

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._session:
            await self._session.close()

    async def _get(self, url: str, params: dict) -> Any:
        if not self._session:
            raise RuntimeError("Session not initialized. Use 'async with BingxApiAsync()'.")

        async with self._session.get(url, params=params, timeout=self._timeout) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_last_confirmed_candle(
        self, symbol: str, interval: str, interval_map: dict
    ) -> dict[str, Any]:
        """Fetch the last confirmed (closed) candle for a symbol/interval."""
        params = {"symbol": _normalize_symbol(symbol), "interval": interval, "limit": 3}
        data = await self._get(APIURL, params)

        if isinstance(data, dict):
            candles = data.get("data", [])
        elif isinstance(data, list):
            candles = data
        else:
            raise ValueError("Unexpected response format")

        if len(candles) < 2:
            raise ValueError("Not enough candles returned")

        c = candles[-2]  # penultimate = last closed candle
        if isinstance(c, dict):
            open_ts = c.get("time") or c.get("openTime")
            if open_ts is None:
                raise ValueError("Candle missing time field")
            o, h, low, cl = float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])
        else:  # list format
            open_ts = c[0]
            o, h, low, cl = float(c[1]), float(c[2]), float(c[3]), float(c[4])

        close_ts = int(open_ts) + interval_map[interval] * 1000
        return {
            "symbol": symbol,
            "interval": interval,
            "timestamp": close_ts,
            "open": o,
            "high": h,
            "low": low,
            "close": cl,
        }

    async def get_candles(
        self, symbol: str, interval: str, limit: int, interval_map: dict
    ) -> list[dict[str, Any]]:
        """Fetch candles with unified structure (close times)."""
        params = {"symbol": _normalize_symbol(symbol), "interval": interval, "limit": limit}
        data = await self._get(APIURL, params)

        if isinstance(data, dict):
            candles = data.get("data", [])
        elif isinstance(data, list):
            candles = data
        else:
            raise ValueError("Unexpected response format")

        results = []
        for c in candles:
            if isinstance(c, dict):
                open_ts = c.get("time") or c.get("openTime")
                if open_ts is None:
                    continue
                close_ts = int(open_ts) + interval_map[interval] * 1000
                o, h, low, cl = float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])
            else:
                open_ts = int(c[0])
                close_ts = open_ts + interval_map[interval] * 1000
                o, h, low, cl = float(c[1]), float(c[2]), float(c[3]), float(c[4])

            results.append({
                "close_time": close_ts,
                "open": o, "high": h, "low": low, "close": cl,
            })

        return results