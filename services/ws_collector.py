from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import websockets
import aiohttp

from services.atomic import atomic_write_json
from services.market_cache import MarketCache
from services.universe_manager import DynamicUniverseManager
from services.api_server import ReadOnlyCollectorAPI


BINANCE_FUTURES_WS = os.getenv(
    "WS_COLLECTOR_BINANCE_FUTURES_WS",
    "wss://fstream.binance.com/market/stream?streams=",
)

BINANCE_FUTURES_PUBLIC_WS = os.getenv(
    "WS_COLLECTOR_BINANCE_FUTURES_PUBLIC_WS",
    "wss://fstream.binance.com/public/stream?streams=",
)

BINANCE_FUTURES_REST = os.getenv(
    "WS_COLLECTOR_BINANCE_FUTURES_REST",
    "https://fapi.binance.com",
).rstrip("/")

BINANCE_FUTURES_PUBLIC_REST_FALLBACK = os.getenv(
    "WS_COLLECTOR_BINANCE_FUTURES_PUBLIC_REST_FALLBACK",
    "",
).rstrip("/")

COLLECTOR_TRANSPORT = os.getenv("WS_COLLECTOR_TRANSPORT", "websocket").strip().lower()


def chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def round_robin_chunked(items: list[str], size: int) -> list[list[str]]:
    """Distribute ordered symbols across balanced quote chunks.

    `size` is treated as the maximum desired chunk size.
    We first derive the number of chunks, then spread symbols
    round-robin so high-volume front-loaded symbols do not pile
    into the same WebSocket connection.
    """
    if not items:
        return []
    safe_size = max(1, int(size))
    chunk_count = max(1, (len(items) + safe_size - 1) // safe_size)
    chunks: list[list[str]] = [[] for _ in range(chunk_count)]
    for idx, item in enumerate(items):
        chunks[idx % chunk_count].append(item)
    return [chunk for chunk in chunks if chunk]


class BinanceKlineWSCollector:
    def __init__(
        self,
        *,
        timeframes: list[str],
        cache: MarketCache,
        universe: DynamicUniverseManager,
        root: str | Path,
        chunk_size: int = 120,
        quote_chunk_size: int = 25,
        universe_refresh_seconds: int = 60,
        snapshot_interval: int = 10,
        stale_after_seconds: int = 20,
        chunk_message_timeout_seconds: int = 45,
        api_enabled: bool = True,
        api_host: str = "127.0.0.1",
        api_port: int = 8000,
    ):
        self.timeframes = timeframes
        self.cache = cache
        self.universe = universe
        self.root = Path(root)
        self.runtime_dir = self.root / "runtime"
        self.chunk_size = chunk_size
        self.quote_chunk_size = quote_chunk_size
        self.universe_refresh_seconds = universe_refresh_seconds
        self.snapshot_interval = snapshot_interval
        self.stale_after_seconds = stale_after_seconds
        self.chunk_message_timeout_seconds = chunk_message_timeout_seconds
        self.api_enabled = api_enabled
        self.api_host = api_host
        self.api_port = int(api_port)
        self.api_server: ReadOnlyCollectorAPI | None = None
        self.transport = COLLECTOR_TRANSPORT
        self.rest_base_url = BINANCE_FUTURES_REST
        self.public_rest_fallback_url = BINANCE_FUTURES_PUBLIC_REST_FALLBACK
        configured_rest_poll = float(os.getenv("WS_COLLECTOR_REST_POLL_SECONDS", "15") or 15)
        self.rest_poll_seconds = max(2.0, configured_rest_poll)
        self.rest_quote_poll_seconds = max(0.5, float(os.getenv("WS_COLLECTOR_REST_QUOTE_POLL_SECONDS", "2") or 2))
        configured_rest_concurrency = max(1, int(os.getenv("WS_COLLECTOR_REST_CONCURRENCY", "16") or 16))
        self.rest_concurrency = configured_rest_concurrency
        self.rest_kline_request_timeout_seconds = max(
            3.0,
            float(os.getenv("WS_COLLECTOR_REST_KLINE_REQUEST_TIMEOUT_SECONDS", "8") or 8),
        )
        self.rest_kline_connect_timeout_seconds = max(
            1.0,
            min(
                self.rest_kline_request_timeout_seconds,
                float(os.getenv("WS_COLLECTOR_REST_KLINE_CONNECT_TIMEOUT_SECONDS", "5") or 5),
            ),
        )
        self.rest_kline_read_timeout_seconds = max(
            1.0,
            min(
                self.rest_kline_request_timeout_seconds,
                float(os.getenv("WS_COLLECTOR_REST_KLINE_READ_TIMEOUT_SECONDS", "5") or 5),
            ),
        )
        self.rest_incremental_kline_limit = max(2, int(os.getenv("WS_COLLECTOR_REST_INCREMENTAL_KLINE_LIMIT", "2") or 2))
        self.rest_bootstrap_kline_limit = max(
            self.rest_incremental_kline_limit,
            int(os.getenv("WS_COLLECTOR_REST_BOOTSTRAP_KLINE_LIMIT", "220") or 220),
        )
        self.rest_bootstrap_min_unique_bars = max(
            1,
            int(os.getenv("WS_COLLECTOR_REST_BOOTSTRAP_MIN_UNIQUE_BARS", "81") or 81),
        )
        self.rest_repair_cooldown_seconds = max(
            self.rest_poll_seconds,
            float(os.getenv("WS_COLLECTOR_REST_REPAIR_COOLDOWN_SECONDS", "60") or 60),
        )
        self._rest_bootstrapped_series: set[tuple[str, str]] = set()
        self._rest_repair_last_attempt_at: dict[tuple[str, str], float] = {}
        self.start_time = time.time()
        self.current_symbols: list[str] = []
        self._chunk_tasks: list[asyncio.Task] = []
        self._quote_chunk_tasks: list[asyncio.Task] = []
        self._stop = asyncio.Event()
        self.status: dict[str, Any] = {
            "connected": False,
            "started_at": datetime.now(UTC).isoformat(),
            "symbols_count": 0,
            "timeframes": self.timeframes,
            "last_message_at": None,
            "last_message_age_seconds": None,
            "reconnect_count": 0,
            "error_count": 0,
            "uptime_seconds": 0,
            "cache_size": {},
            "chunks": {},
            "streams_count": 0,
            "quote_connected": False,
            "quote_last_message_at": None,
            "quote_last_message_age_seconds": None,
            "quote_reconnect_count": 0,
            "quote_error_count": 0,
            "quote_chunks": {},
            "quote_streams_count": 0,
            "quote_cache_size": {},
            "universe": {},
            "transport": self.transport,
            "rest_base_url": self.rest_base_url,
            "public_rest_fallback_url": self.public_rest_fallback_url,
            "market_environment": os.getenv("WS_COLLECTOR_MARKET_ENVIRONMENT", "live"),
            "rest_poll_seconds": self.rest_poll_seconds,
            "rest_quote_poll_seconds": self.rest_quote_poll_seconds,
            "rest_concurrency": self.rest_concurrency,
            "rest_kline_request_timeout_seconds": self.rest_kline_request_timeout_seconds,
            "rest_kline_connect_timeout_seconds": self.rest_kline_connect_timeout_seconds,
            "rest_kline_read_timeout_seconds": self.rest_kline_read_timeout_seconds,
            "rest_repair_cooldown_seconds": self.rest_repair_cooldown_seconds,
        }

    def _rest_urls(self, path: str) -> list[str]:
        urls = [f"{self.rest_base_url}{path}"]
        fallback = f"{self.public_rest_fallback_url}{path}" if self.public_rest_fallback_url else ""
        cross_environment_fallback = (
            self.transport == "demo_rest_polling"
            and self.public_rest_fallback_url
            and self.public_rest_fallback_url != self.rest_base_url
        )
        if fallback and not cross_environment_fallback and fallback not in urls:
            urls.append(fallback)
        return urls

    async def _get_public_json(
        self,
        session: aiohttp.ClientSession,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> tuple[Any, str, int]:
        last_status = 0
        last_body = ""
        for idx, url in enumerate(self._rest_urls(path)):
            async with session.get(url, params=params) as resp:
                last_status = int(resp.status)
                if resp.status == 200:
                    return await resp.json(), url, last_status
                last_body = (await resp.text())[:240]
                if idx == 0 and self.transport == "demo_rest_polling" and resp.status in {403, 418, 429}:
                    continue
                break
        raise aiohttp.ClientResponseError(
            request_info=resp.request_info,
            history=resp.history,
            status=last_status,
            message=last_body or f"HTTP {last_status}",
            headers=resp.headers,
        )

    async def _rest_poll_quotes(self, session: aiohttp.ClientSession) -> None:
        request_started = time.time()
        rows, source_url, _ = await self._get_public_json(session, "/fapi/v1/ticker/bookTicker")
        wanted = set(self.current_symbols)
        now = time.time()
        count = 0
        for data in rows or []:
            symbol = str(data.get("symbol") or "")
            if symbol not in wanted:
                continue
            bid = float(data.get("bidPrice") or 0)
            ask = float(data.get("askPrice") or 0)
            if bid <= 0 or ask <= 0:
                continue
            self.cache.add_quote(symbol, {
                "symbol": symbol,
                "best_bid": bid,
                "best_bid_qty": float(data.get("bidQty") or 0),
                "best_ask": ask,
                "best_ask_qty": float(data.get("askQty") or 0),
                "update_id": int(data.get("lastUpdateId") or 0),
                "event_time": int(data.get("time") or now * 1000),
                "transaction_time": int(data.get("time") or now * 1000),
                "received_at": now,
                "latency_ms": round((now - request_started) * 1000, 3),
                "source": "demo_rest_book_ticker" if source_url.startswith(self.rest_base_url) else "public_rest_book_ticker_fallback",
            })
            count += 1
        self.status["quote_last_message_at"] = now
        self.status["last_message_at"] = now
        self.status["quote_connected"] = count > 0
        self.status["connected"] = count > 0
        self.status["quote_streams_count"] = count

    async def _rest_poll_klines(self, session: aiohttp.ClientSession) -> None:
        sem = asyncio.Semaphore(self.rest_concurrency)
        cycle_started_at = time.time()
        attempted_count = len(self.current_symbols) * len(self.timeframes)
        success_count = 0
        timeout_count = 0
        error_count = 0
        fallback_count = 0
        repair_request_count = 0
        repair_cooldown_count = 0
        repair_reasons: dict[str, int] = {}
        success_latencies_ms: list[float] = []
        cycle_errors: list[str] = []

        def request_limit(symbol: str, timeframe: str) -> int:
            nonlocal repair_request_count, repair_cooldown_count
            key = (symbol, timeframe)
            first_check = key not in self._rest_bootstrapped_series
            min_contiguous = self.rest_bootstrap_min_unique_bars if first_check else 20
            health = self.cache.series_health(
                symbol,
                timeframe,
                min_contiguous_bars=min_contiguous,
                max_lag_intervals=1,
            )
            self._rest_bootstrapped_series.add(key)
            health_reasons = set(health.get("reasons") or [])
            capacity_limited = (
                not first_check
                and health_reasons == {"contiguous_tail_below_minimum"}
                and int(health.get("unique_bars") or 0) == int(health.get("contiguous_tail_bars") or 0)
            )
            if not health.get("ok") and not capacity_limited:
                now = time.time()
                last_attempt = float(self._rest_repair_last_attempt_at.get(key) or 0.0)
                if now - last_attempt < self.rest_repair_cooldown_seconds:
                    repair_cooldown_count += 1
                    return self.rest_incremental_kline_limit
                self._rest_repair_last_attempt_at[key] = now
                repair_request_count += 1
                for reason in health.get("reasons") or ["unknown"]:
                    repair_reasons[str(reason)] = int(repair_reasons.get(str(reason), 0)) + 1
                return self.rest_bootstrap_kline_limit
            return self.rest_incremental_kline_limit

        async def fetch(symbol: str, timeframe: str) -> None:
            nonlocal success_count, timeout_count, error_count, fallback_count
            limit = request_limit(symbol, timeframe)
            params = {"symbol": symbol, "interval": timeframe, "limit": limit}
            async with sem:
                request_started_at = time.perf_counter()
                try:
                    rows, source_url, _ = await self._get_public_json(session, "/fapi/v1/klines", params=params)
                except asyncio.TimeoutError as exc:
                    timeout_count += 1
                    message = f"{symbol} {timeframe}: {type(exc).__name__}"
                    if len(cycle_errors) < 20:
                        cycle_errors.append(message)
                    return
                except Exception as exc:
                    error_count += 1
                    detail = str(exc).strip()
                    message = f"{symbol} {timeframe}: {type(exc).__name__}{':' + detail if detail else ''}"
                    if len(cycle_errors) < 20:
                        cycle_errors.append(message)
                    return
            success_count += 1
            success_latencies_ms.append((time.perf_counter() - request_started_at) * 1000.0)
            if not source_url.startswith(self.rest_base_url):
                fallback_count += 1
            received_at = time.time()
            for row in rows or []:
                close_time = int(row[6])
                candle = {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "open_time": int(row[0]),
                    "close_time": close_time,
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                    "closed": close_time < int(received_at * 1000) - 500,
                    "event_time": int(row[0]),
                    "received_at": received_at,
                    "latency_ms": None,
                    "source": "demo_rest_poll" if source_url.startswith(self.rest_base_url) else "public_rest_kline_fallback",
                }
                self.cache.add_candle(
                    symbol,
                    timeframe,
                    candle,
                    persist=limit <= self.rest_incremental_kline_limit,
                )
        await asyncio.gather(*(fetch(symbol, tf) for symbol in self.current_symbols for tf in self.timeframes))
        cycle_finished_at = time.time()
        ordered_latencies = sorted(success_latencies_ms)

        def percentile(fraction: float) -> float | None:
            if not ordered_latencies:
                return None
            index = min(len(ordered_latencies) - 1, int(fraction * (len(ordered_latencies) - 1)))
            return round(ordered_latencies[index], 3)

        self.status["last_message_at"] = cycle_finished_at
        self.status["connected"] = success_count > 0
        self.status["symbols_count"] = len(self.current_symbols)
        self.status["streams_count"] = len(self.current_symbols) * len(self.timeframes)
        self.status["rest_kline_cycle_started_at"] = cycle_started_at
        self.status["rest_kline_cycle_finished_at"] = cycle_finished_at
        self.status["rest_kline_cycle_seconds"] = round(cycle_finished_at - cycle_started_at, 3)
        self.status["rest_kline_cycle_attempted_count"] = attempted_count
        self.status["rest_kline_cycle_success_count"] = success_count
        self.status["rest_kline_cycle_timeout_count"] = timeout_count
        self.status["rest_kline_cycle_error_count"] = error_count
        self.status["rest_kline_cycle_fallback_count"] = fallback_count
        self.status["rest_kline_cycle_repair_request_count"] = repair_request_count
        self.status["rest_kline_cycle_repair_cooldown_count"] = repair_cooldown_count
        self.status["rest_kline_cycle_repair_reasons"] = repair_reasons
        self.status["rest_kline_cycle_success_latency_p50_ms"] = percentile(0.50)
        self.status["rest_kline_cycle_success_latency_p95_ms"] = percentile(0.95)
        self.status["rest_kline_cycle_success_latency_max_ms"] = round(ordered_latencies[-1], 3) if ordered_latencies else None
        self.status["rest_kline_cycle_errors"] = cycle_errors
        self.status["rest_kline_last_error"] = cycle_errors[-1] if cycle_errors else None

    async def _rest_poll_loop(self) -> None:
        timeout = aiohttp.ClientTimeout(total=30)
        last_kline = 0.0
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while not self._stop.is_set():
                started = time.time()
                try:
                    await self._rest_poll_quotes(session)
                    if started - last_kline >= self.rest_poll_seconds:
                        await self._rest_poll_klines(session)
                        last_kline = time.time()
                    self.status["rest_last_success_at"] = time.time()
                    self.status["rest_last_error"] = None
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.status["error_count"] += 1
                    self.status["rest_last_error"] = str(exc)
                    logging.exception("rest polling error: %s", exc)
                await asyncio.sleep(max(0.1, self.rest_quote_poll_seconds - (time.time() - started)))

    async def _rest_quote_loop(self) -> None:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while not self._stop.is_set():
                started = time.time()
                try:
                    await self._rest_poll_quotes(session)
                    self.status["rest_quote_last_success_at"] = time.time()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.status["quote_error_count"] += 1
                    self.status["rest_quote_last_error"] = str(exc)
                    logging.exception("REST quote polling error: %s", exc)
                await asyncio.sleep(max(0.1, self.rest_quote_poll_seconds - (time.time() - started)))

    async def _rest_kline_loop(self) -> None:
        timeout = aiohttp.ClientTimeout(
            total=self.rest_kline_request_timeout_seconds,
            connect=self.rest_kline_connect_timeout_seconds,
            sock_connect=self.rest_kline_connect_timeout_seconds,
            sock_read=self.rest_kline_read_timeout_seconds,
        )
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while not self._stop.is_set():
                started = time.time()
                try:
                    await self._rest_poll_klines(session)
                    self.status["rest_kline_last_success_at"] = time.time()
                    self.status["rest_last_success_at"] = time.time()
                    self.status["rest_last_error"] = None
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.status["error_count"] += 1
                    self.status["rest_last_error"] = str(exc)
                    logging.exception("REST kline polling error: %s", exc)
                await asyncio.sleep(max(0.1, self.rest_poll_seconds - (time.time() - started)))

    def _streams_for_symbols(self, symbols: list[str]) -> str:
        streams: list[str] = []
        for symbol in symbols:
            lower_symbol = symbol.lower()
            for timeframe in self.timeframes:
                streams.append(f"{lower_symbol}@kline_{timeframe}")
        return "/".join(streams)

    def _quote_streams_for_symbols(self, symbols: list[str]) -> str:
        streams: list[str] = []
        for symbol in symbols:
            lower_symbol = symbol.lower()
            streams.append(f"{lower_symbol}@bookTicker")
        return "/".join(streams)

    async def _consume_chunk(self, symbols_chunk: list[str], chunk_id: int) -> None:
        streams = self._streams_for_symbols(symbols_chunk)
        url = BINANCE_FUTURES_WS + streams
        chunk_key = str(chunk_id)
        self.status["chunks"][chunk_key] = {
            "connected": False,
            "symbols_count": len(symbols_chunk),
            "streams_count": len(symbols_chunk) * len(self.timeframes),
            "last_message_at": None,
            "last_message_age_seconds": None,
            "reconnect_count": 0,
            "error_count": 0,
        }
        while not self._stop.is_set():
            try:
                logging.info("connecting chunk=%s symbols=%s streams=%s", chunk_id, len(symbols_chunk), len(symbols_chunk) * len(self.timeframes))
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=1, max_queue=4096) as ws:
                    self.status["chunks"][chunk_key]["connected"] = True
                    logging.info("connected chunk=%s", chunk_id)
                    while not self._stop.is_set():
                        raw = await asyncio.wait_for(ws.recv(), timeout=self.chunk_message_timeout_seconds)
                        self._handle_message(raw, chunk_key)
            except asyncio.CancelledError:
                self.status["chunks"].get(chunk_key, {})["connected"] = False
                raise
            except Exception as exc:
                chunk = self.status["chunks"].setdefault(chunk_key, {})
                chunk["connected"] = False
                chunk["reconnect_count"] = int(chunk.get("reconnect_count") or 0) + 1
                chunk["error_count"] = int(chunk.get("error_count") or 0) + 1
                self.status["reconnect_count"] += 1
                self.status["error_count"] += 1
                logging.exception("ws error chunk=%s: %s", chunk_id, exc)
                await asyncio.sleep(min(30, 3 + int(chunk.get("reconnect_count") or 0)))

    async def _consume_quote_chunk(self, symbols_chunk: list[str], chunk_id: int) -> None:
        streams = self._quote_streams_for_symbols(symbols_chunk)
        url = BINANCE_FUTURES_PUBLIC_WS + streams
        chunk_key = str(chunk_id)
        self.status["quote_chunks"][chunk_key] = {
            "connected": False,
            "symbols_count": len(symbols_chunk),
            "streams_count": len(symbols_chunk),
            "last_message_at": None,
            "last_message_age_seconds": None,
            "reconnect_count": 0,
            "error_count": 0,
        }
        while not self._stop.is_set():
            try:
                logging.info(
                    "quote_connecting chunk=%s symbols=%s streams=%s",
                    chunk_id,
                    len(symbols_chunk),
                    len(symbols_chunk),
                )
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=1,
                    max_queue=4096,
                ) as ws:
                    self.status["quote_chunks"][chunk_key]["connected"] = True
                    logging.info("quote_connected chunk=%s", chunk_id)
                    while not self._stop.is_set():
                        raw = await asyncio.wait_for(
                            ws.recv(),
                            timeout=self.chunk_message_timeout_seconds,
                        )
                        self._handle_quote_message(raw, chunk_key)
            except asyncio.CancelledError:
                self.status["quote_chunks"].get(chunk_key, {})["connected"] = False
                raise
            except Exception as exc:
                chunk = self.status["quote_chunks"].setdefault(chunk_key, {})
                chunk["connected"] = False
                chunk["reconnect_count"] = int(chunk.get("reconnect_count") or 0) + 1
                chunk["error_count"] = int(chunk.get("error_count") or 0) + 1
                self.status["quote_reconnect_count"] += 1
                self.status["quote_error_count"] += 1
                logging.exception("quote ws error chunk=%s: %s", chunk_id, exc)
                await asyncio.sleep(min(30, 3 + int(chunk.get("reconnect_count") or 0)))

    def _handle_quote_message(self, raw: str, chunk_key: str) -> None:
        try:
            msg = json.loads(raw)
            data = msg.get("data", {})
            if data.get("e") != "bookTicker":
                return

            symbol = data.get("s")
            if not symbol:
                return

            event_time = int(data.get("E", 0))
            transaction_time = int(data.get("T", 0))
            received_at = time.time()

            quote = {
                "symbol": str(symbol),
                "best_bid": float(data["b"]),
                "best_bid_qty": float(data["B"]),
                "best_ask": float(data["a"]),
                "best_ask_qty": float(data["A"]),
                "update_id": int(data.get("u") or 0),
                "event_time": event_time,
                "transaction_time": transaction_time,
                "received_at": received_at,
                "latency_ms": round(received_at * 1000 - event_time, 3) if event_time else None,
            }

            self.cache.add_quote(str(symbol), quote)
            self.status["quote_last_message_at"] = received_at
            chunk = self.status["quote_chunks"].setdefault(chunk_key, {})
            chunk["last_message_at"] = received_at
        except Exception as exc:
            self.status["quote_error_count"] += 1
            logging.exception("handle_quote_message error: %s", exc)

    def _handle_message(self, raw: str, chunk_key: str) -> None:
        try:
            msg = json.loads(raw)
            data = msg.get("data", {})
            if data.get("e") != "kline":
                return
            kline = data.get("k", {})
            symbol = kline.get("s")
            timeframe = kline.get("i")
            if not symbol or not timeframe:
                return
            event_time = int(data.get("E", 0))
            received_at = time.time()
            candle = {
                "symbol": str(symbol),
                "timeframe": str(timeframe),
                "open_time": int(kline["t"]),
                "close_time": int(kline["T"]),
                "open": float(kline["o"]),
                "high": float(kline["h"]),
                "low": float(kline["l"]),
                "close": float(kline["c"]),
                "volume": float(kline["v"]),
                "closed": bool(kline["x"]),
                "event_time": event_time,
                "received_at": received_at,
                "latency_ms": round(received_at * 1000 - event_time, 3) if event_time else None,
            }
            stored = self.cache.add_candle(str(symbol), str(timeframe), candle)
            self.status["last_message_at"] = received_at
            chunk = self.status["chunks"].setdefault(chunk_key, {})
            chunk["last_message_at"] = received_at
            if stored.get("closed"):
                logging.info("candle_closed symbol=%s tf=%s close=%s volume=%s", symbol, timeframe, stored["close"], stored["volume"])
        except Exception as exc:
            self.status["error_count"] += 1
            logging.exception("handle_message error: %s", exc)

    async def _replace_streams(self, symbols: list[str]) -> None:
        for task in self._chunk_tasks:
            task.cancel()
        for task in self._quote_chunk_tasks:
            task.cancel()

        if self._chunk_tasks:
            await asyncio.gather(*self._chunk_tasks, return_exceptions=True)
        if self._quote_chunk_tasks:
            await asyncio.gather(*self._quote_chunk_tasks, return_exceptions=True)

        self.status["chunks"] = {}
        self.status["quote_chunks"] = {}
        self.current_symbols = symbols
        if self.transport == "demo_rest_polling":
            self.status["chunks"] = {}
            self.status["quote_chunks"] = {}
            self.status["symbols_count"] = len(symbols)
            self.status["streams_count"] = len(symbols) * len(self.timeframes)
            self.status["quote_streams_count"] = len(symbols)
            self.status["universe"] = self.universe.last_report
            logging.info("REST polling universe replaced symbols=%s", len(symbols))
            return
        kline_chunks = chunked(symbols, self.chunk_size)
        quote_chunks = round_robin_chunked(symbols, self.quote_chunk_size)

        self._chunk_tasks = [
            asyncio.create_task(self._consume_chunk(symbols_chunk, idx))
            for idx, symbols_chunk in enumerate(kline_chunks, start=1)
        ]
        self._quote_chunk_tasks = [
            asyncio.create_task(self._consume_quote_chunk(symbols_chunk, idx))
            for idx, symbols_chunk in enumerate(quote_chunks, start=1)
        ]

        self.status["symbols_count"] = len(symbols)
        self.status["streams_count"] = len(symbols) * len(self.timeframes)
        self.status["quote_streams_count"] = len(symbols)
        self.status["universe"] = self.universe.last_report

        logging.info(
            "stream universe replaced symbols=%s kline_chunks=%s quote_chunks=%s quote_streams=%s",
            len(symbols),
            len(kline_chunks),
            len(quote_chunks),
            len(symbols),
        )

    async def _universe_supervisor(self) -> None:
        while not self._stop.is_set():
            symbols = await self.universe.symbols()
            if symbols and set(symbols) != set(self.current_symbols):
                await self._replace_streams(symbols)
            await asyncio.sleep(self.universe_refresh_seconds)

    async def _status_writer(self) -> None:
        while not self._stop.is_set():
            now = time.time()
            last = self.status.get("last_message_at")
            quote_last = self.status.get("quote_last_message_at")

            self.status["uptime_seconds"] = int(now - self.start_time)
            self.status["last_message_age_seconds"] = None if not last else round(now - float(last), 3)
            self.status["quote_last_message_age_seconds"] = None if not quote_last else round(now - float(quote_last), 3)

            for chunk in self.status.get("chunks", {}).values():
                chunk_last = chunk.get("last_message_at")
                chunk["last_message_age_seconds"] = None if not chunk_last else round(now - float(chunk_last), 3)

            for chunk in self.status.get("quote_chunks", {}).values():
                chunk_last = chunk.get("last_message_at")
                chunk["last_message_age_seconds"] = None if not chunk_last else round(now - float(chunk_last), 3)

            chunks = list((self.status.get("chunks") or {}).values())
            quote_chunks = list((self.status.get("quote_chunks") or {}).values())

            if self.transport != "demo_rest_polling":
                self.status["connected"] = bool(chunks) and all(bool(item.get("connected")) for item in chunks)
                self.status["quote_connected"] = bool(quote_chunks) and all(bool(item.get("connected")) for item in quote_chunks)
            self.status["cache_size"] = self.cache.cache_size()
            self.status["quote_cache_size"] = self.cache.quote_cache_size()
            self.status["universe"] = self.universe.last_report
            if int((self.universe.last_report or {}).get("selected_count") or 0) >= 40:
                atomic_write_json(self.runtime_dir / "last_good_universe.json", self.universe.last_report)
            atomic_write_json(self.runtime_dir / "ws_collector_status.json", self.status)
            await asyncio.sleep(2)

    async def _snapshot_writer(self) -> None:
        while not self._stop.is_set():
            universe = self.status.get("universe") or {}
            active_symbols = universe.get("selected_symbols") or self.current_symbols or None
            atomic_write_json(self.runtime_dir / "market_cache_snapshot.json", self.cache.snapshot(active_symbols=active_symbols))
            atomic_write_json(self.runtime_dir / "quote_cache_snapshot.json", self.cache.quote_snapshot(active_symbols=active_symbols))
            atomic_write_json(
                self.runtime_dir / "ws_freshness_report.json",
                self.cache.freshness_report(self.stale_after_seconds, active_symbols=active_symbols),
            )
            await asyncio.sleep(self.snapshot_interval)

    async def run(self) -> None:
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        if self.api_enabled:
            self.api_server = ReadOnlyCollectorAPI(
                cache=self.cache,
                status_ref=self.status,
                stale_after_seconds=self.stale_after_seconds,
                host=self.api_host,
                port=self.api_port,
            )
            await self.api_server.start()

        tasks = [
            asyncio.create_task(self._universe_supervisor()),
            asyncio.create_task(self._status_writer()),
            asyncio.create_task(self._snapshot_writer()),
        ]
        if self.transport == "demo_rest_polling":
            tasks.append(asyncio.create_task(self._rest_quote_loop()))
            tasks.append(asyncio.create_task(self._rest_kline_loop()))
        try:
            await asyncio.gather(*tasks)
        finally:
            self._stop.set()
            for task in self._chunk_tasks:
                task.cancel()
            for task in self._quote_chunk_tasks:
                task.cancel()
            await asyncio.gather(*self._chunk_tasks, return_exceptions=True)
            await asyncio.gather(*self._quote_chunk_tasks, return_exceptions=True)
            if self.api_server:
                await self.api_server.stop()
