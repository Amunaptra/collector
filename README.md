# WS Collector

A real-time WebSocket market-data collector for Binance Futures, built as a
standalone async service independent of any trading strategy. It has run
continuously for months and is the data backbone that made offline
strategy research possible in the first place (hundreds of thousands of
backtested trades across multiple indicator families, all sourced from
this collector's output plus its historical REST backfill counterpart).

## What it does

- **`services/ws_collector.py`** — maintains WebSocket subscriptions to
  Binance Futures kline streams across the active symbol universe,
  writes closed candles atomically (`services/atomic.py`,
  write-to-temp-then-replace — never leaves a partially-written file on
  disk for a reader to pick up).
- **`services/universe_manager.py`** — `DynamicUniverseManager`: decides
  which symbols are in-scope (allowed/tradable-execution symbol lists,
  top-volume ranking from the exchange ticker, an "early hotset" path
  that confirms and promotes fast-moving symbols into the watched universe
  ahead of the next full refresh cycle).
- **`services/market_cache.py`** — in-memory rolling cache of recent bars
  per symbol/timeframe (dedupes on `(symbol, timeframe, close_time)`,
  tracks last-closed-candle time per timeframe for freshness checks),
  backed by append-only JSONL under `data/<namespace>/<symbol>/<timeframe>.jsonl`.
- **`services/api_server.py`** — `ReadOnlyCollectorAPI`: an aiohttp HTTP
  server exposing the cache read-only — `/health`, `/status`, `/freshness`,
  `/symbols`, `/snapshot`, `/bars`, `/latest`, `/quote`, `/quotes` — so
  downstream consumers (execution engines, dashboards, research scripts)
  never touch the WebSocket connection or the on-disk files directly.
- **`services/bootstrap.py`** — REST-based cold-start/backfill so the
  cache isn't empty on process start before the WebSocket stream catches
  up.
- **`scripts/run_ws_collector.py`** — process entrypoint.
- **`scripts/test_ws_collector_health.py`** — standalone health check
  script (separate from the pytest suite under `tests/`), meant to be run
  by a monitor/cron, not as part of `pytest`.

## Status

- Months of continuous uptime feeding a multi-timeframe (5m/15m/1h/4h)
  historical archive used for offline backtesting.
- Read-only HTTP API keeps every downstream consumer decoupled from the
  collection mechanism itself — the collector can be restarted, reworked,
  or have its universe-selection logic changed without any consumer
  noticing anything beyond a brief data gap.
- 3 pytest files under `tests/` (closed-bar capacity contract, early
  hotset universe promotion, market-cache continuity).

## Not included

This repository is the real-time collection service only. It does not
include:
- The historical REST backfill script that seeds/extends the parquet
  archive used for backtesting (lives alongside a separate data-lake
  module in the source monorepo).
- Any execution or strategy code that consumes this data.

## Disclaimer

Built and operated against Binance Futures market data. No credentials
or secrets are required to run this service — it only reads public
market data endpoints.
