# core/storage_manager.py
import logging
from datetime import datetime, timezone
from modules.candles import CandleFetcher
from core.fractal_storage import (
    load_storage, save_storage, init_full_scan, update_storage
)

logger = logging.getLogger("sweep")


class StorageManager:
    def __init__(self, config, interval_map, tz):
        """
        config: dictionary from config.json
        interval_map: mapping interval -> seconds (used for downtime -> candle count)
        tz: timezone (pytz timezone instance)
        """
        self.history_limit = int(config["history_limit"])
        self.base_interval = config["base_interval"]
        self.fractal_window = int(config["fractal_window"])
        self.higher_intervals = config["higher_intervals"]
        self.tz = tz

        self.interval_map = interval_map
        self.candles = CandleFetcher(config, interval_map)
        self.storage = load_storage()

    # ============================================================
    # INITIALIZATION
    # ============================================================
    async def startup(self, symbols, downtime: int | None, force_full: bool = False, scan_limit: int | None = None):
        """
        Decide how to rebuild storage based on downtime or forced full scan.
        - If force_full True → run full scan with scan_limit (or history_limit fallback)
        - Else if downtime is None or downtime > history_limit → full scan
        - Else if downtime > base_interval -> recover
        - Else skip (live updates only)
        """
        scan_limit = int(scan_limit) if scan_limit is not None else self.history_limit

        if force_full or downtime is None or downtime > self.history_limit:
            logger.info("⏳ Running full scan (limit=%s)...", scan_limit)
            self.storage = await init_full_scan(
                symbols,
                self.base_interval,
                self.higher_intervals,
                self.fractal_window,
                scan_limit,
                self.candles,
            )
            save_storage(self.storage)
        elif downtime > int(self.base_interval.rstrip("m")):  # e.g. base_interval "15m"
            logger.info("🔄 Running recovery scan...")
            await self.recover_from_timestamp(symbols, downtime)
        else:
            logger.info("✅ Downtime < base_interval → skip recovery, use existing storage.")

    # ============================================================
    # RECOVERY
    # ============================================================
    async def recover_from_timestamp(self, symbols, downtime: int):
        """Catch-up update since last timestamp."""
        last_ts = self.storage.get("metadata", {}).get("last_candle_close_time")
        if not last_ts:
            logger.warning("No last timestamp found → fallback to full scan")
            return await self.startup(symbols, downtime=self.history_limit + 1)

        for sym in symbols:
            for interval in [self.base_interval] + list(self.higher_intervals):
                candles = await self.candles.recovery(sym, interval, downtime, self.history_limit)
                await update_storage(self.storage, sym, interval, candles, self.fractal_window)

        self.storage["metadata"]["last_candle_close_time"] = int(datetime.now(timezone.utc).timestamp() * 1000)
        save_storage(self.storage)

    # ============================================================
    # LIVE UPDATE
    # ============================================================
    async def update_live(self, symbols):
        """Lightweight update with the most recent candle(s)."""
        for sym in symbols:
            for interval in [self.base_interval] + list(self.higher_intervals):
                candles = await self.candles.live(sym, interval)
                await update_storage(self.storage, sym, interval, candles, self.fractal_window)
        save_storage(self.storage)

    # ============================================================
    # FRACTAL ACCESSORS
    # ============================================================
    async def get_active_fractals(self, symbol: str, interval: str):
        """
        Async accessor for currently active fractals of given symbol/interval.
        Returns (H_fractals, L_fractals) lists.
        """
        s = self.storage.get(symbol, {}).get(interval, {})
        return s.get("H", []), s.get("L", [])

    async def get_all_active_fractals(self, symbol: str):
        """
        Async accessor returning all intervals for a symbol:
        { interval: {"H": [...], "L": [...]} }
        """
        return self.storage.get(symbol, {})

    async def purge_broken_fractals(self):
        """
        Optional async method to remove broken fractals after a cycle.
        (Can be called at end of runner.py if desired.)
        """
        removed = 0
        for sym, iv_data in self.storage.items():
            if sym == "metadata":
                continue
            for interval, data in iv_data.items():
                before_h = len(data.get("H", []))
                before_l = len(data.get("L", []))
                data["H"] = [f for f in data.get("H", []) if "high" in f]
                data["L"] = [f for f in data.get("L", []) if "low" in f]
                removed += (before_h - len(data["H"])) + (before_l - len(data["L"]))
        if removed > 0:
            logger.info(f"🧹 Purged {removed} invalid fractals from storage.")
            save_storage(self.storage)
