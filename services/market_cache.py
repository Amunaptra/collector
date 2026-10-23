from __future__ import annotations

import json
import os
import time
from collections import defaultdict, deque
from pathlib import Path
from threading import RLock
from typing import Any


def timeframe_seconds(timeframe: str) -> int:
    value = str(timeframe).strip().lower()
    if value.endswith("m"):
        return int(value[:-1]) * 60
    if value.endswith("h"):
        return int(value[:-1]) * 3600
    if value.endswith("d"):
        return int(value[:-1]) * 86400
    return 0


class JsonlCandleWriter:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.data_namespace = str(os.getenv("WS_COLLECTOR_DATA_NAMESPACE", "klines")).strip() or "klines"
        self._seen_closed: set[tuple[str, str, int]] = set()
        self._lock = RLock()

    def mark_seen(self, candle: dict[str, Any]) -> None:
        if not bool(candle.get("closed")):
            return
        symbol = str(candle.get("symbol") or "").upper()
        timeframe = str(candle.get("timeframe") or "")
        close_time = int(candle.get("close_time") or 0)
        if symbol and timeframe and close_time:
            with self._lock:
                self._seen_closed.add((symbol, timeframe, close_time))

    def write_closed_once(self, candle: dict[str, Any]) -> None:
        if not bool(candle.get("closed")):
            return
        symbol = str(candle.get("symbol") or "").upper()
        timeframe = str(candle.get("timeframe") or "")
        close_time = int(candle.get("close_time") or 0)
        if not symbol or not timeframe or not close_time:
            return
        key = (symbol, timeframe, close_time)
        with self._lock:
            if key in self._seen_closed:
                return
            self._seen_closed.add(key)
            path = self.root / "data" / self.data_namespace / symbol / f"{timeframe}.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(candle, ensure_ascii=False, separators=(",", ":")) + "\n")


class MarketCache:
    def __init__(self, max_bars: int = 500, raw_writer: JsonlCandleWriter | None = None):
        self.max_bars = max_bars
        self.raw_writer = raw_writer
        self._lock = RLock()
        self._data: dict[str, dict[str, deque[dict[str, Any]]]] = defaultdict(
            lambda: defaultdict(lambda: deque(maxlen=max_bars))
        )
        self._quotes: dict[str, dict[str, Any]] = {}
        self._first_update_seen: set[tuple[str, str, int]] = set()
        self.last_candle_close_by_timeframe: dict[str, float] = {}

    def add_candle(
        self,
        symbol: str,
        timeframe: str,
        candle: dict[str, Any],
        *,
        persist: bool = True,
    ) -> dict[str, Any]:
        received_at = float(candle.get("received_at") or time.time())
        symbol = symbol.upper().replace("/", "").replace("-", "")
        timeframe = str(timeframe)
        candle = dict(candle)
        candle["symbol"] = symbol
        candle["timeframe"] = timeframe
        candle["cached_at"] = received_at
        candle["is_fresh"] = True

        open_time = int(candle.get("open_time") or 0)
        first_key = (symbol, timeframe, open_time)
        if first_key not in self._first_update_seen and open_time:
            self._first_update_seen.add(first_key)
            candle["first_update_for_bar"] = True
            candle["bar_first_update_latency_seconds"] = round(received_at - (open_time / 1000.0), 3)
        else:
            candle["first_update_for_bar"] = False

        tf_seconds = timeframe_seconds(timeframe)
        if tf_seconds and open_time:
            candle["bar_elapsed_seconds"] = round(received_at - (open_time / 1000.0), 3)
            candle["bar_elapsed_pct"] = round(max(0.0, min(100.0, candle["bar_elapsed_seconds"] / tf_seconds * 100.0)), 3)

        with self._lock:
            bars = self._data[symbol][timeframe]
            existing_index = None
            for index, previous_bar in enumerate(bars):
                if previous_bar.get("open_time") == candle.get("open_time"):
                    existing_index = index
                    break
            if existing_index is not None:
                previous = dict(bars[existing_index])
                if "bar_first_update_latency_seconds" in previous and "bar_first_update_latency_seconds" not in candle:
                    candle["bar_first_update_latency_seconds"] = previous["bar_first_update_latency_seconds"]
                bars[existing_index] = candle
            else:
                latest_open_time = int(bars[-1].get("open_time") or 0) if bars else 0
                if latest_open_time and open_time < latest_open_time:
                    merged = list(bars)
                    merged.append(candle)
                    merged.sort(key=lambda row: int(row.get("open_time") or 0))
                    bars.clear()
                    bars.extend(merged[-self.max_bars :])
                else:
                    bars.append(candle)

            if candle.get("closed"):
                self.last_candle_close_by_timeframe[timeframe] = received_at

        if self.raw_writer:
            if persist:
                self.raw_writer.write_closed_once(candle)
            else:
                self.raw_writer.mark_seen(candle)
        return candle

    def get_bars(self, symbol: str, timeframe: str, limit: int | None = None) -> list[dict[str, Any]]:
        symbol = symbol.upper().replace("/", "").replace("-", "")
        with self._lock:
            bars = list(self._data.get(symbol, {}).get(timeframe, []))
        return bars[-limit:] if limit else bars

    def series_health(
        self,
        symbol: str,
        timeframe: str,
        *,
        min_contiguous_bars: int = 20,
        now_ms: int | None = None,
        max_lag_intervals: int = 1,
    ) -> dict[str, Any]:
        tf_ms = timeframe_seconds(timeframe) * 1000
        observed_ms = int(now_ms if now_ms is not None else time.time() * 1000)
        bars = self.get_bars(symbol, timeframe)
        open_times = sorted(
            {
                int(row.get("open_time") or 0)
                for row in bars
                if int(row.get("open_time") or 0) > 0
            }
        )
        contiguous_tail = 0
        if open_times:
            contiguous_tail = 1
            for previous, current in zip(reversed(open_times[:-1]), reversed(open_times[1:])):
                if tf_ms <= 0 or current - previous != tf_ms:
                    break
                contiguous_tail += 1
        latest_open_time = open_times[-1] if open_times else None
        expected_open_time = (observed_ms // tf_ms) * tf_ms if tf_ms > 0 else None
        lag_intervals = None
        if latest_open_time is not None and expected_open_time is not None:
            lag_intervals = max(0, (expected_open_time - latest_open_time) // tf_ms)
        reasons: list[str] = []
        if tf_ms <= 0:
            reasons.append("unsupported_timeframe")
        if not open_times:
            reasons.append("empty_series")
        if contiguous_tail < max(1, int(min_contiguous_bars)):
            reasons.append("contiguous_tail_below_minimum")
        if lag_intervals is not None and lag_intervals > max(0, int(max_lag_intervals)):
            reasons.append("latest_bar_stale")
        return {
            "ok": not reasons,
            "symbol": str(symbol).upper(),
            "timeframe": str(timeframe),
            "unique_bars": len(open_times),
            "contiguous_tail_bars": contiguous_tail,
            "min_contiguous_bars": max(1, int(min_contiguous_bars)),
            "latest_open_time": latest_open_time,
            "expected_open_time": expected_open_time,
            "lag_intervals": lag_intervals,
            "max_lag_intervals": max(0, int(max_lag_intervals)),
            "reasons": reasons,
        }

    def latest(self, symbol: str, timeframe: str) -> dict[str, Any] | None:
        bars = self.get_bars(symbol, timeframe, limit=1)
        return bars[-1] if bars else None

    def add_quote(self, symbol: str, quote: dict[str, Any]) -> dict[str, Any]:
        received_at = float(quote.get("received_at") or time.time())
        symbol = symbol.upper().replace("/", "").replace("-", "")
        quote = dict(quote)
        quote["symbol"] = symbol
        quote["cached_at"] = received_at
        quote["is_fresh"] = True
        with self._lock:
            self._quotes[symbol] = quote
        return quote

    def latest_quote(self, symbol: str) -> dict[str, Any] | None:
        symbol = symbol.upper().replace("/", "").replace("-", "")
        with self._lock:
            quote = self._quotes.get(symbol)
            return dict(quote) if quote else None

    def quote_symbols(self) -> list[str]:
        with self._lock:
            return sorted(self._quotes.keys())

    def quote_cache_size(self) -> dict[str, Any]:
        with self._lock:
            return {"symbols": len(self._quotes)}

    def quote_snapshot_for_symbol(self, symbol: str) -> dict[str, Any]:
        symbol = symbol.upper().replace("/", "").replace("-", "")
        quote = self.latest_quote(symbol)
        return {
            "created_at": time.time(),
            "quotes_count": 1 if quote else 0,
            "quote_cache_size": self.quote_cache_size(),
            "quotes": {symbol: quote} if quote else {},
        }

    def _normalize_active_symbols(self, active_symbols: list[str] | set[str] | None) -> set[str] | None:
        if not active_symbols:
            return None
        return {
            str(symbol).upper().replace("/", "").replace("-", "")
            for symbol in active_symbols
            if str(symbol or "").strip()
        }

    def quote_snapshot(self, max_symbols: int = 120, active_symbols: list[str] | set[str] | None = None) -> dict[str, Any]:
        active_set = self._normalize_active_symbols(active_symbols)
        with self._lock:
            symbols = sorted(self._quotes.keys())
            if active_set is not None:
                symbols = [symbol for symbol in symbols if symbol in active_set]
            symbols = symbols[:max_symbols]
            quotes = {symbol: dict(self._quotes[symbol]) for symbol in symbols}
        return {
            "created_at": time.time(),
            "quotes_count": len(quotes),
            "quote_cache_size": self.quote_cache_size(),
            "active_symbols_filter": active_set is not None,
            "active_symbols_count": len(active_set) if active_set is not None else None,
            "quotes": quotes,
        }


    def symbols(self) -> list[str]:
        with self._lock:
            return sorted(self._data.keys())

    def snapshot_for_symbol(self, symbol: str, bars_per_tf: int = 81) -> dict[str, Any]:
        symbol = symbol.upper().replace("/", "").replace("-", "")
        with self._lock:
            tf_map = self._data.get(symbol, {})
            out: dict[str, Any] = {
                "created_at": time.time(),
                "symbols_count": 1 if tf_map else 0,
                "cache_size": self.cache_size(),
                "last_candle_close_by_timeframe": dict(self.last_candle_close_by_timeframe),
                "symbols": {symbol: {}},
            }
            for timeframe, bars in tf_map.items():
                out["symbols"][symbol][timeframe] = list(bars)[-bars_per_tf:]
            return out

    def cache_size(self) -> dict[str, Any]:
        with self._lock:
            series = 0
            bars = 0
            for symbol_data in self._data.values():
                for tf_bars in symbol_data.values():
                    series += 1
                    bars += len(tf_bars)
            return {"symbols": len(self._data), "series": series, "bars": bars}

    def freshness_report(self, stale_after_seconds: int = 20, active_symbols: list[str] | set[str] | None = None) -> dict[str, Any]:
        now = time.time()
        rows: list[dict[str, Any]] = []
        active_set: set[str] | None = None
        if active_symbols:
            active_set = {
                str(symbol).upper().replace("/", "").replace("-", "")
                for symbol in active_symbols
                if str(symbol or "").strip()
            }

        with self._lock:
            for symbol, tf_map in self._data.items():
                if active_set is not None and symbol not in active_set:
                    continue
                for timeframe, bars in tf_map.items():
                    if not bars:
                        rows.append(
                            {
                                "symbol": symbol,
                                "timeframe": timeframe,
                                "last_message_age_seconds": None,
                                "stale": True,
                                "status": "empty",
                                "stale_reason": "empty_series",
                                "stale_note": "Bu symbol/timeframe icin cache icinde hic bar yok.",
                                "open_time": None,
                                "close_time": None,
                                "closed": None,
                                "bar_elapsed_seconds": None,
                                "bar_elapsed_pct": None,
                                "first_update_latency_seconds": None,
                                "close": None,
                                "volume": None,
                            }
                        )
                        continue

                    latest = dict(bars[-1])
                    age = round(now - float(latest.get("received_at") or latest.get("cached_at") or now), 3)

                    tf_sec = timeframe_seconds(str(timeframe)) or 60

                    # Open/current bar updates should be monitored with a bounded freshness contract.
                    # Timeframe-based relaxed tolerances only make sense once the bar is closed.
                    if latest.get("closed") is False:
                        quiet_after = max(int(stale_after_seconds), 20)
                        stale_after_effective = max(int(stale_after_seconds * 2), 45)
                    else:
                        quiet_after = max(int(stale_after_seconds), int(tf_sec * 1.5))
                        stale_after_effective = max(int(stale_after_seconds), int(tf_sec * 4))

                    if age > stale_after_effective:
                        status = "stale"
                        is_stale = True
                        reason = "open_bar_stale" if latest.get("closed") is False else "series_stale_by_timeframe_threshold"
                        note = "Acik bar beklenen collector yenileme ritmini asti; transport veya upstream fetch kontrol edilmeli." if latest.get("closed") is False else "Bu seri timeframe toleransini asti; stream veya sembol aktivitesi kontrol edilmeli."
                    elif age > quiet_after:
                        status = "quiet"
                        is_stale = False
                        reason = "open_bar_quiet" if latest.get("closed") is False else "quiet_low_activity_series"
                        note = "Acik bar update ritmi yavasladi; seri henuz stale degil ama takip edilmeli." if latest.get("closed") is False else "Bar var ama son kline update eski. Dusuk hacimli coinlerde bu normal olabilir."
                    else:
                        status = "fresh"
                        is_stale = False
                        reason = ""
                        note = ""

                    rows.append(
                        {
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "last_message_age_seconds": age,
                            "stale": is_stale,
                            "status": status,
                            "quiet_after_seconds": quiet_after,
                            "stale_after_seconds_effective": stale_after_effective,
                            "stale_reason": reason,
                            "stale_note": note,
                            "open_time": latest.get("open_time"),
                            "close_time": latest.get("close_time"),
                            "closed": latest.get("closed"),
                            "bar_elapsed_seconds": latest.get("bar_elapsed_seconds"),
                            "bar_elapsed_pct": latest.get("bar_elapsed_pct"),
                            "first_update_latency_seconds": latest.get("bar_first_update_latency_seconds"),
                            "close": latest.get("close"),
                            "volume": latest.get("volume"),
                        }
                    )

        stale = [item for item in rows if item.get("status") == "stale"]
        quiet = [item for item in rows if item.get("status") == "quiet"]
        empty = [item for item in rows if item.get("status") == "empty"]
        fresh = [item for item in rows if item.get("status") == "fresh"]

        by_timeframe: dict[str, dict[str, int]] = {}
        for item in rows:
            bucket = by_timeframe.setdefault(
                str(item["timeframe"]),
                {"series": 0, "fresh_series": 0, "quiet_series": 0, "stale_series": 0, "empty_series": 0},
            )
            bucket["series"] += 1
            status = item.get("status")
            if status == "fresh":
                bucket["fresh_series"] += 1
            elif status == "quiet":
                bucket["quiet_series"] += 1
            elif status == "stale":
                bucket["stale_series"] += 1
            elif status == "empty":
                bucket["empty_series"] += 1

        return {
            "created_at": now,
            "rows": sorted(rows, key=lambda item: (item["timeframe"], item["symbol"])),
            "summary": {
                "series": len(rows),
                "fresh_series": len(fresh),
                "quiet_series": len(quiet),
                "stale_series": len(stale),
                "empty_series": len(empty),
                "stale_after_seconds": stale_after_seconds,
                "active_symbols_filter": active_set is not None,
                "active_symbols_count": len(active_set) if active_set is not None else None,
                "by_timeframe": by_timeframe,
                "auto_intervention": "chunk watchdog reconnects when a whole websocket chunk receives no messages",
            },
        }

    def snapshot(
        self,
        max_symbols: int = 120,
        bars_per_tf: int = 81,
        active_symbols: list[str] | set[str] | None = None,
    ) -> dict[str, Any]:
        active_set = self._normalize_active_symbols(active_symbols)
        with self._lock:
            symbols = sorted(self._data.keys())
            if active_set is not None:
                symbols = [symbol for symbol in symbols if symbol in active_set]
            out: dict[str, Any] = {
                "created_at": time.time(),
                "symbols_count": len(symbols),
                "cache_size": self.cache_size(),
                "last_candle_close_by_timeframe": dict(self.last_candle_close_by_timeframe),
                "active_symbols_filter": active_set is not None,
                "active_symbols_count": len(active_set) if active_set is not None else None,
                "symbols": {},
            }
            for symbol in symbols[:max_symbols]:
                out["symbols"][symbol] = {}
                for timeframe, bars in self._data[symbol].items():
                    out["symbols"][symbol][timeframe] = list(bars)[-bars_per_tf:]
            return out
