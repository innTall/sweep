# modules/candles.py
import logging
from utils.bingx_api_async import BingxApiAsync

logger = logging.getLogger("sweep")


def normalize_candles(candles: list[dict]) -> list[dict]:
    """Ensure every candle has a close_time field and sort ascending."""
    for c in candles:
        if "close_time" not in c and "timestamp" in c:
            c["close_time"] = c["timestamp"]
    candles.sort(key=lambda c: int(c["close_time"]))
    return candles


class CandleFetcher:
    def __init__(self, config: dict, interval_map: dict):
        self.interval_map = interval_map
        self._cache: dict[tuple[str, str, int], list[dict]] = {}
        self.api = BingxApiAsync(timeout=config.get("timeouts", {}).get("http", 15))

    async def get(self, symbol: str, interval: str, limit: int) -> list[dict]:
        """Fetch candles with caching per cycle."""
        key = (symbol, interval, limit)
        if key in self._cache:
            return self._cache[key]

        # ✅ call BingxApiAsync.get_candles
        async with self.api as client:
            candles = await client.get_candles(symbol, interval, limit, self.interval_map)

        candles = normalize_candles(candles)
        self._cache[key] = candles
        return candles

    async def full_scan(self, symbol: str, interval: str, limit: int) -> list[dict]:
        """Get full history for a symbol/interval (used in init_full_scan)."""
        return await self.get(symbol, interval, limit)

    async def recovery(self, symbol: str, interval: str, downtime: int, history_limit: int) -> list[dict]:
        """Fetch enough candles to cover downtime, capped by history_limit."""
        needed = min(downtime // self.interval_map[interval] + 5, history_limit)
        return await self.get(symbol, interval, needed)

    async def live(self, symbol: str, interval: str, lookback: int = 3) -> list[dict]:
        """Only get last few candles (default=3)."""
        return await self.get(symbol, interval, lookback)

    def clear_cache(self):
        """Clear cache at end of cycle."""
        self._cache.clear()