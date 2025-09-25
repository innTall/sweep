# utils/get_symbols.py
import argparse
import json
import logging
import sys
from pathlib import Path

import requests

# Constants
CONTRACTS_URL = "https://open-api.bingx.com/openApi/swap/v2/quote/contracts"
SYMBOLS_FILE = Path("symbols.json")
CONFIG_FILE = Path("config.json")

# Logger setup
logger = logging.getLogger("get_symbols")
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def get_all_usdtm_symbols() -> list[str]:
    """Fetch all active BingX USDT-M perpetual futures symbols (normalized)."""
    try:
        resp = requests.get(CONTRACTS_URL, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error(f"Failed to fetch contracts from BingX: {e}")
        return []

    if not isinstance(data, dict) or "data" not in data:
        logger.error(f"Unexpected contracts response: {data}")
        return []

    contracts = data["data"]
    logger.info(f"Fetched {len(contracts)} contracts from BingX")
    logger.debug(f"Sample contracts: {contracts[:3]}")

    symbols: list[str] = []
    for item in contracts:
        try:
            if item.get("currency") == "USDT" and item.get("status") == 1:
                symbols.append(item["symbol"].replace("-", ""))
        except Exception as e:
            logger.warning(f"Skipping malformed contract: {item} ({e})")

    logger.info(f"Filtered {len(symbols)} USDT-M perpetual symbols")
    return symbols


def save_symbols(symbols: list[str], path: Path, force: bool = False):
    """Save symbols.json safely."""
    if path.exists() and not force:
        logger.warning(f"{path} already exists. Use --force to overwrite.")
        return
    try:
        path.write_text(json.dumps(symbols, indent=2), encoding="utf-8")
        logger.info(f"Saved {len(symbols)} symbols to {path}")
    except Exception as e:
        logger.error(f"Failed to save {path}: {e}")


def update_top_symbols(add_symbols: int):
    """Update top_symbols in config.json based on add_symbols and symbols.json."""
    if not SYMBOLS_FILE.exists():
        logger.error(f"{SYMBOLS_FILE} not found. Run fetcher first.")
        return

    try:
        all_symbols = json.loads(SYMBOLS_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"Failed to read {SYMBOLS_FILE}: {e}")
        return

    try:
        config = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"Failed to read {CONFIG_FILE}: {e}")
        return

    n = max(0, add_symbols)
    top_symbols = all_symbols[:n]
    if not top_symbols:
        logger.warning("No symbols selected for top_symbols. Skipping update.")
        return

    config["top_symbols"] = top_symbols
    try:
        CONFIG_FILE.write_text(json.dumps(config, indent=2), encoding="utf-8")
        logger.info(f"Updated config.json with top {len(top_symbols)} symbols")
    except Exception as e:
        logger.error(f"Failed to write config.json: {e}")


def main():
    parser = argparse.ArgumentParser(description="Fetch and manage BingX USDT-M symbols")
    parser.add_argument("--force", action="store_true", help="Overwrite symbols.json")
    parser.add_argument("--limit", type=int, default=None, help="Fetch only N symbols (test)")
    parser.add_argument("--update-top", action="store_true", help="Update top_symbols from symbols.json")
    args = parser.parse_args()

    symbols = get_all_usdtm_symbols()
    if not symbols:
        logger.error("No symbols retrieved. Aborting.")
        return

    if args.limit:
        symbols = symbols[: args.limit]
        logger.info(f"Using top {len(symbols)} symbols (test mode)")

    save_symbols(symbols, SYMBOLS_FILE, force=args.force)

    if args.update_top:
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                config = json.load(f)
            add_symbols = int(config.get("add_symbols", 0))
        except Exception as e:
            logger.error(f"Failed to read add_symbols from config.json: {e}")
            return
        update_top_symbols(add_symbols)


if __name__ == "__main__":
    main()