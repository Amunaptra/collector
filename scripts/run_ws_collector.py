#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.atomic import atomic_write_json
from services.bootstrap import bootstrap_ohlcv_history
from services.market_cache import JsonlCandleWriter, MarketCache
from services.universe_manager import manager_from_env
from services.ws_collector import BinanceKlineWSCollector


def env_bool(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).lower() in {"1", "true", "yes", "on"}


def setup_logging(root: Path) -> None:
    (root / "logs").mkdir(parents=True, exist_ok=True)
    (root / "runtime").mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(root / "logs" / "ws_collector.log"),
            logging.StreamHandler(),
        ],
    )


def load_cached_bootstrap_symbols(root: Path, min_symbols: int = 80) -> list[str]:
    candidates = [
        root / "runtime" / "last_good_universe.json",
        root / "runtime" / "ws_collector_status.json",
    ]
    for path in candidates:
        try:
            if not path.exists():
                continue
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload.get("status"), dict):
                payload = payload["status"]
            universe = payload.get("universe") if isinstance(payload.get("universe"), dict) else payload
            symbols = [str(symbol).upper().strip() for symbol in (universe.get("selected_symbols") or []) if str(symbol).strip()]
            if len(symbols) >= min_symbols:
                return symbols
        except Exception:
            continue
    return []


async def main() -> None:
    parser = argparse.ArgumentParser(description="Independent Binance Futures dynamic universe WS collector")
    parser.add_argument("--timeframes", default=os.getenv("WS_COLLECTOR_TIMEFRAMES", "5m,15m,1h,4h"))
    parser.add_argument("--chunk-size", type=int, default=int(os.getenv("WS_COLLECTOR_CHUNK_SIZE", "120") or 120))
    parser.add_argument("--quote-chunk-size", type=int, default=int(os.getenv("WS_COLLECTOR_QUOTE_CHUNK_SIZE", "25") or 25))
    parser.add_argument("--max-bars", type=int, default=int(os.getenv("WS_COLLECTOR_MAX_BARS", "500") or 500))
    parser.add_argument("--universe-refresh-seconds", type=int, default=int(os.getenv("WS_COLLECTOR_UNIVERSE_REFRESH_SECONDS", "60") or 60))
    parser.add_argument("--snapshot-interval", type=int, default=int(os.getenv("WS_COLLECTOR_SNAPSHOT_INTERVAL", "10") or 10))
    parser.add_argument("--stale-after-seconds", type=int, default=int(os.getenv("WS_COLLECTOR_STALE_AFTER_SECONDS", "20") or 20))
    parser.add_argument("--chunk-message-timeout-seconds", type=int, default=int(os.getenv("WS_COLLECTOR_CHUNK_MESSAGE_TIMEOUT_SECONDS", "45") or 45))
    parser.add_argument("--api-host", default=os.getenv("WS_COLLECTOR_API_HOST", "127.0.0.1"))
    parser.add_argument("--api-port", type=int, default=int(os.getenv("WS_COLLECTOR_API_PORT", "8000") or 8000))
    parser.add_argument("--disable-api", action="store_true", default=env_bool("WS_COLLECTOR_DISABLE_API", "0"))
    parser.add_argument("--no-raw-jsonl", action="store_true")

    # REST bootstrap: fills historical bars before WebSocket starts.
    parser.add_argument("--bootstrap", action="store_true", default=env_bool("WS_COLLECTOR_BOOTSTRAP_ENABLED", "1"))
    parser.add_argument("--no-bootstrap", action="store_false", dest="bootstrap")
    parser.add_argument("--bootstrap-limit", type=int, default=int(os.getenv("WS_COLLECTOR_BOOTSTRAP_LIMIT", "240") or 240))
    parser.add_argument("--bootstrap-concurrency", type=int, default=int(os.getenv("WS_COLLECTOR_BOOTSTRAP_CONCURRENCY", "12") or 12))
    parser.add_argument("--bootstrap-timeout", type=float, default=float(os.getenv("WS_COLLECTOR_BOOTSTRAP_TIMEOUT", "12") or 12))
    args = parser.parse_args()

    setup_logging(ROOT)
    timeframes = [item.strip() for item in args.timeframes.split(",") if item.strip()]
    raw_writer = None if args.no_raw_jsonl else JsonlCandleWriter(ROOT)
    cache = MarketCache(max_bars=args.max_bars, raw_writer=raw_writer)
    universe = manager_from_env()

    if args.bootstrap:
        symbols = await universe.symbols()
        if len(symbols) < 80:
            cached_symbols = load_cached_bootstrap_symbols(ROOT, min_symbols=80)
            if cached_symbols:
                logging.warning(
                    "bootstrap_universe_fallback current_symbols=%s cached_symbols=%s",
                    len(symbols),
                    len(cached_symbols),
                )
                symbols = cached_symbols
        logging.info("bootstrap_start symbols=%s timeframes=%s limit=%s", len(symbols), timeframes, args.bootstrap_limit)
        stats = await bootstrap_ohlcv_history(
            cache=cache,
            symbols=symbols,
            timeframes=timeframes,
            limit=args.bootstrap_limit,
            concurrency=args.bootstrap_concurrency,
            timeout_seconds=args.bootstrap_timeout,
            rest_base_url=os.getenv("WS_COLLECTOR_BINANCE_FUTURES_REST", "https://fapi.binance.com"),
            fallback_rest_base_url=None,
            jsonl_root=ROOT,
            jsonl_namespace=os.getenv("WS_COLLECTOR_DATA_NAMESPACE", "klines"),
            snapshot_path=ROOT / "runtime" / "market_cache_snapshot.json",
            min_contiguous_bars=int(os.getenv("WS_COLLECTOR_REST_BOOTSTRAP_MIN_UNIQUE_BARS", "81") or 81),
        )
        atomic_write_json(ROOT / "runtime" / "bootstrap_status.json", stats)
    else:
        logging.info("bootstrap_disabled")

    collector = BinanceKlineWSCollector(
        timeframes=timeframes,
        cache=cache,
        universe=universe,
        root=ROOT,
        chunk_size=args.chunk_size,
        quote_chunk_size=args.quote_chunk_size,
        universe_refresh_seconds=args.universe_refresh_seconds,
        snapshot_interval=args.snapshot_interval,
        stale_after_seconds=args.stale_after_seconds,
        chunk_message_timeout_seconds=args.chunk_message_timeout_seconds,
        api_enabled=not args.disable_api,
        api_host=args.api_host,
        api_port=args.api_port,
    )
    logging.info("starting dynamic ws collector root=%s timeframes=%s", ROOT, timeframes)
    await collector.run()


if __name__ == "__main__":
    asyncio.run(main())
