# core/storage_manager.py
import logging
from datetime import datetime, timezone

from core.fractal_storage import (
    load_storage, save_storage, init_full_scan, update_storage
)

logger = logging.getLogger("sweep")

class StorageManager:
    def __init__(self, config, interval_map, get_candles_fn, tz):
        self.history_limit = config["history_limit"]
        self.base_interval = config["base_interval"]
        self.fractal_window = config["fractal_window"]
        self.higher_intervals = config["higher_intervals"]
        self.tz = tz

        self.interval_map = interval_map
        self.get_candles_fn = get_candles_fn
        self.storage = load_storage()
    
    # ---- decision on startup ----
    async def startup(self, symbols, downtime: int | None):
        """
        Decide how to rebuild storage based on downtime.
        """
        if downtime is None or downtime > self.history_limit:
            logger.info("⏳ Running full scan...")
            self.storage = await init_full_scan(
                symbols,
                self.base_interval,
                self.higher_intervals,
                self.fractal_window,
                self.history_limit,
                self.interval_map,
                self.tz,
                self.get_candles_fn,
            )
        elif downtime > self.base_interval:
            logger.info("🔄 Running recovery scan...")
            await self.recover_from_timestamp(symbols, downtime)
        else:
            logger.info("✅ Downtime < base_interval → skip recovery, use existing storage.")
    
    # ---- recovery scan ----
    async def recover_from_timestamp(self, symbols, downtime: int):
        """
        Catch-up update since last timestamp.
        """
        last_ts = self.storage.get("metadata", {}).get("last_candle_close_time")
        if not last_ts:
            logger.warning("No last timestamp found → fallback to full scan")
            return await self.startup(symbols, downtime=self.history_limit+1)

        for sym in symbols:
            for interval in [self.base_interval] + list(self.higher_intervals):
                # compute how many candles we need to fetch
                limit = min(downtime // self.interval_map[interval] + 5, self.history_limit)
                candles = await self.get_candles_fn(sym, interval, limit, self.interval_map)
                await update_storage(self.storage, sym, interval, candles, self.fractal_window)

        # update metadata
        self.storage["metadata"]["last_candle_close_time"] = int(datetime.now(timezone.utc).timestamp() * 1000)
        save_storage(self.storage)

    # ---- live update per cycle ----
    async def update_live(self, symbols):
        """
        Lightweight update with the most recent candle(s).
        """
        for sym in symbols:
            for interval in [self.base_interval] + list(self.higher_intervals):
                candles = await self.get_candles_fn(sym, interval, 3, self.interval_map)  # only a few latest
                await update_storage(self.storage, sym, interval, candles, self.fractal_window)

        save_storage(self.storage)
