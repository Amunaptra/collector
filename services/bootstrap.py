from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import aiohttp

from services.market_cache import MarketCache

BINANCE_FUTURES_REST = os.getenv("WS_COLLECTOR_BINANCE_FUTURES_REST", "https://fapi.binance.com")


class BootstrapStats(dict):
    pass


async def bootstrap_ohlcv_history(
    *,
    cache: MarketCache,
    symbols: list[str],
    timeframes: list[str],
    limit: int = 240,
    concurrency: int = 12,
    timeout_seconds: float = 12.0,
    rest_base_url: str = BINANCE_FUTURES_REST,
    fallback_rest_base_url: str | None = None,
    jsonl_root: str | Path | None = None,
    jsonl_namespace: str = "klines",
    snapshot_path: str | Path | None = None,
    min_contiguous_bars: int = 81,
) -> dict[str, Any]:
    """Load initial OHLCV history into MarketCache before WebSocket starts.

    This solves the "collector has only a few bars after restart" problem.
    WebSocket keeps the bars fresh after this bootstrap.
    """
    started = time.time()
    symbols = [str(s).upper().replace("/", "").replace("-", "") for s in symbols if s]
    timeframes = [str(tf).strip() for tf in timeframes if str(tf).strip()]
    limit = max(2, min(int(limit), 1500))
    concurrency = max(1, int(concurrency))
    min_contiguous_bars = max(2, min(int(min_contiguous_bars), limit))

    stats: dict[str, Any] = {
        "enabled": True,
        "started_at": started,
        "symbols": len(symbols),
        "timeframes": timeframes,
        "series_target": len(symbols) * len(timeframes),
        "series_ok": 0,
        "series_error": 0,
        "bars_loaded": 0,
        "limit": limit,
        "concurrency": concurrency,
        "rest_base_url": str(rest_base_url).rstrip("/"),
        "fallback_rest_base_url": str(fallback_rest_base_url or "").rstrip("/") or None,
        "source_counts": {},
        "min_contiguous_bars": min_contiguous_bars,
        "series_repair_needed": 0,
        "series_repaired": 0,
        "series_capacity_limited": 0,
        "errors": [],
        "jsonl_root": str(jsonl_root) if jsonl_root else None,
        "jsonl_namespace": jsonl_namespace,
        "legacy_jsonl_bootstrap_disabled": True,
        "snapshot_path": str(snapshot_path) if snapshot_path else None,
        "snapshot_created_at": None,
    }

    if not symbols or not timeframes:
        stats["duration_seconds"] = round(time.time() - started, 3)
        return stats

    missing_series: list[tuple[str, str]] = []
    snapshot_symbols: dict[str, Any] = {}
    if snapshot_path:
        try:
            snapshot_payload = json.loads(Path(snapshot_path).read_text(encoding="utf-8"))
            snapshot_symbols = snapshot_payload.get("symbols") or {}
            stats["snapshot_created_at"] = snapshot_payload.get("created_at")
        except FileNotFoundError:
            pass
        except Exception as exc:
            stats["errors"].append({"snapshot_error": str(exc)})
            logging.warning("bootstrap_snapshot_error path=%s error=%s", snapshot_path, exc)

    for symbol in symbols:
        for timeframe in timeframes:
            loaded = 0
            now = time.time()
            snapshot_rows = (snapshot_symbols.get(symbol) or {}).get(timeframe) or []
            for row in snapshot_rows[-limit:]:
                row = dict(row)
                row["received_at"] = float(row.get("received_at") or now)
                cache.add_candle(symbol, timeframe, row, persist=False)
                loaded += 1
            if loaded > 0:
                stats["bars_loaded"] += loaded
            health = cache.series_health(
                symbol,
                timeframe,
                min_contiguous_bars=min_contiguous_bars,
            )
            if loaded > 0 and health.get("ok"):
                stats["series_ok"] += 1
                stats["source_counts"]["snapshot"] = int(stats["source_counts"].get("snapshot", 0)) + 1
                logging.info("bootstrap_snapshot_ok symbol=%s tf=%s bars=%s", symbol, timeframe, loaded)
            else:
                missing_series.append((symbol, timeframe))
                if loaded > 0:
                    stats["series_repair_needed"] += 1
                    stats["source_counts"]["snapshot_repair_needed"] = int(
                        stats["source_counts"].get("snapshot_repair_needed", 0)
                    ) + 1
                    logging.warning(
                        "bootstrap_snapshot_repair_needed symbol=%s tf=%s bars=%s health=%s",
                        symbol,
                        timeframe,
                        loaded,
                        health,
                    )

    if not missing_series:
        stats["duration_seconds"] = round(time.time() - started, 3)
        stats["finished_at"] = time.time()
        logging.info(
            "bootstrap_done source=jsonl_only symbols=%s series_ok=%s series_error=%s bars=%s duration=%ss",
            stats["symbols"], stats["series_ok"], stats["series_error"], stats["bars_loaded"], stats["duration_seconds"],
        )
        return stats

    sem = asyncio.Semaphore(concurrency)
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    async def fetch_one(session: aiohttp.ClientSession, symbol: str, timeframe: str) -> None:
        base_urls = [str(rest_base_url).rstrip("/")]
        fallback = str(fallback_rest_base_url or "").rstrip("/")
        if fallback and fallback not in base_urls:
            base_urls.append(fallback)
        params = {"symbol": symbol, "interval": timeframe, "limit": limit}
        async with sem:
            try:
                rows = None
                source_url = None
                last_error = None
                for base_url in base_urls:
                    url = f"{base_url}/fapi/v1/klines"
                    try:
                        async with session.get(url, params=params) as resp:
                            resp.raise_for_status()
                            rows = await resp.json()
                            source_url = base_url
                            break
                    except Exception as exc:
                        last_error = exc
                        continue
                if rows is None:
                    raise last_error or RuntimeError("bootstrap_fetch_failed")
                now = time.time()
                loaded = 0
                for row in rows or []:
                    # Binance kline array:
                    # [open_time, open, high, low, close, volume, close_time, ...]
                    try:
                        open_time = int(row[0])
                        close_time = int(row[6])
                        candle = {
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "open_time": open_time,
                            "close_time": close_time,
                            "open": float(row[1]),
                            "high": float(row[2]),
                            "low": float(row[3]),
                            "close": float(row[4]),
                            "volume": float(row[5]),
                            "closed": close_time < int(now * 1000) - 500,
                            "event_time": close_time,
                            "received_at": now,
                            "latency_ms": None,
                            "source": "rest_bootstrap" if source_url == str(rest_base_url).rstrip("/") else "rest_bootstrap_fallback",
                        }
                        cache.add_candle(symbol, timeframe, candle, persist=False)
                        loaded += 1
                    except Exception as exc:  # keep one bad row from killing the series
                        if len(stats["errors"]) < 30:
                            stats["errors"].append({"symbol": symbol, "timeframe": timeframe, "row_error": str(exc)})
                stats["bars_loaded"] += loaded
                health = cache.series_health(
                    symbol,
                    timeframe,
                    min_contiguous_bars=min_contiguous_bars,
                )
                health_reasons = set(health.get("reasons") or [])
                capacity_limited = (
                    loaded > 0
                    and health_reasons == {"contiguous_tail_below_minimum"}
                    and int(health.get("unique_bars") or 0) == int(health.get("contiguous_tail_bars") or 0)
                )
                if health.get("ok") or capacity_limited:
                    stats["series_ok"] += 1
                    if loaded > 0:
                        stats["series_repaired"] += 1
                    if capacity_limited:
                        stats["series_capacity_limited"] += 1
                    source_key = "primary" if source_url == str(rest_base_url).rstrip("/") else "fallback"
                    stats["source_counts"][source_key] = int(stats["source_counts"].get(source_key, 0)) + 1
                    logging.info(
                        "bootstrap_ok symbol=%s tf=%s bars=%s capacity_limited=%s",
                        symbol,
                        timeframe,
                        loaded,
                        capacity_limited,
                    )
                else:
                    stats["series_error"] += 1
                    if len(stats["errors"]) < 50:
                        stats["errors"].append(
                            {
                                "symbol": symbol,
                                "timeframe": timeframe,
                                "error": "rest_backfill_series_unhealthy",
                                "health": health,
                            }
                        )
                    logging.warning(
                        "bootstrap_rest_unhealthy symbol=%s tf=%s bars=%s health=%s",
                        symbol,
                        timeframe,
                        loaded,
                        health,
                    )
            except Exception as exc:
                stats["series_error"] += 1
                if len(stats["errors"]) < 50:
                    stats["errors"].append({"symbol": symbol, "timeframe": timeframe, "error": str(exc)})
                logging.warning("bootstrap_error symbol=%s tf=%s error=%s", symbol, timeframe, exc)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [fetch_one(session, symbol, timeframe) for symbol, timeframe in missing_series]
        await asyncio.gather(*tasks)

    stats["duration_seconds"] = round(time.time() - started, 3)
    stats["finished_at"] = time.time()
    logging.info(
        "bootstrap_done symbols=%s series_ok=%s series_error=%s bars=%s duration=%ss",
        stats["symbols"], stats["series_ok"], stats["series_error"], stats["bars_loaded"], stats["duration_seconds"],
    )
    return stats
