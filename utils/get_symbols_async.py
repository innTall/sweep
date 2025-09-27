import argparse
import json
import logging
import sys
from pathlib import Path
import aiohttp
import asyncio

CONTRACTS_URL = "https://open-api.bingx.com/openApi/swap/v2/quote/contracts"
SYMBOLS_FILE = Path("symbols.json")
COINS_FILE = Path("coins.txt")
CONFIG_FILE = Path("config.json")

logger = logging.getLogger("get_symbols")
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


async def get_all_usdtm_symbols() -> list[str]:
    """Fetch all active BingX USDT-M perpetual futures symbols (normalized)."""
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.get(CONTRACTS_URL) as resp:
                resp.raise_for_status()
                data = await resp.json()
    except Exception as e:
        logger.error(f"Failed to fetch contracts from BingX: {e}")
        return []

    if not isinstance(data, dict) or "data" not in data:
        logger.error(f"Unexpected contracts response: {data}")
        return []

    contracts = data["data"]
    logger.info(f"Fetched {len(contracts)} contracts from BingX")

    symbols: list[str] = []
    for item in contracts:
        try:
            if item.get("currency") == "USDT" and item.get("status") == 1:
                symbols.append(item["symbol"].replace("-", ""))
        except Exception as e:
            logger.warning(f"Skipping malformed contract: {item} ({e})")

    logger.info(f"Filtered {len(symbols)} USDT-M perpetual symbols")
    return symbols


def save_symbols(symbols: list[str], force: bool = False):
    """Save symbols.json and coins.txt safely."""
    # --- Save JSON ---
    if SYMBOLS_FILE.exists() and not force:
        logger.warning(f"{SYMBOLS_FILE} already exists. Use --force to overwrite.")
    else:
        try:
            SYMBOLS_FILE.write_text(json.dumps(symbols, indent=2), encoding="utf-8")
            logger.info(f"Saved {len(symbols)} symbols to {SYMBOLS_FILE}")
        except Exception as e:
            logger.error(f"Failed to save {SYMBOLS_FILE}: {e}")

    # --- Save TXT (alphabetical, no USDT) ---
    coins = sorted(sym.replace("USDT", "") for sym in symbols)
    try:
        with COINS_FILE.open("w", encoding="utf-8") as f:
            f.write("# List of coins (alphabetical, without USDT)\n")
            f.write("# You can add/remove/edit coins manually; comments allowed with '#'\n")
            f.write("# Example: keep only coins you want to prioritize\n\n")
            for coin in coins:
                f.write(f"{coin}\n")
        logger.info(f"Saved {len(coins)} coins to {COINS_FILE}")
    except Exception as e:
        logger.error(f"Failed to save {COINS_FILE}: {e}")


def update_config_top_symbols(symbols: list[str], force: bool = False):
    """Update config.json → top_symbols with the full list (alphabetical)."""
    if not CONFIG_FILE.exists():
        logger.error(f"{CONFIG_FILE} not found. Skipping top_symbols update.")
        return

    try:
        config = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"Failed to read {CONFIG_FILE}: {e}")
        return

    sorted_symbols = sorted(symbols)
    config["top_symbols"] = sorted_symbols

    try:
        CONFIG_FILE.write_text(json.dumps(config, indent=2), encoding="utf-8")
        logger.info(f"Updated config.json with {len(sorted_symbols)} top_symbols (alphabetical)")
    except Exception as e:
        logger.error(f"Failed to write {CONFIG_FILE}: {e}")


async def main():
    parser = argparse.ArgumentParser(description="Fetch and manage BingX USDT-M symbols (async)")
    parser.add_argument("--force", action="store_true", help="Overwrite symbols.json & coins.txt and reset top_symbols")
    parser.add_argument("--limit", type=int, default=None, help="Fetch only N symbols (test)")
    args = parser.parse_args()

    symbols = await get_all_usdtm_symbols()
    if not symbols:
        logger.error("No symbols retrieved. Aborting.")
        return

    if args.limit:
        symbols = symbols[: args.limit]
        logger.info(f"Using top {len(symbols)} symbols (test mode)")

    save_symbols(symbols, force=args.force)

    # 🔄 If --force, also update config.json → top_symbols
    if args.force:
        update_config_top_symbols(symbols, force=True)


if __name__ == "__main__":
    asyncio.run(main())