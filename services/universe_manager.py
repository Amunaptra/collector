from __future__ import annotations

import asyncio
import json
import math
import os
import time
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any

import aiohttp


def normalize_symbol(value: Any) -> str:
    text = str(value or "").upper().strip()
    return text.replace("/", "").replace("-", "").replace("_", "")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        if path.exists():
            raw = json.loads(path.read_text(encoding="utf-8"))
            return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}
    return {}


def _read_symbol_file(path: Path) -> set[str]:
    try:
        if not path.exists():
            return set()
        out: set[str] = set()
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            symbol = normalize_symbol(line)
            if symbol.endswith("USDT"):
                out.add(symbol)
        return out
    except Exception:
        return set()


class DynamicUniverseManager:
    def __init__(
        self,
        crypto_root: str | Path,
        *,
        max_symbols: int = 150,
        top_volume_symbols: int = 100,
        recent_minutes: int = 90,
        include_top_volume: bool = True,
        rest_base_url: str = "https://fapi.binance.com",
        allowlist_path: str | Path | None = None,
        execution_exchange_info_path: str | Path | None = None,
    ):
        self.crypto_root = Path(crypto_root)
        self.state_dir = self.crypto_root / "data" / "state"
        self.max_symbols = max(1, max_symbols)
        self.top_volume_symbols = max(0, top_volume_symbols)
        self.recent_minutes = max(1, recent_minutes)
        self.include_top_volume = include_top_volume
        self.rest_base_url = str(rest_base_url).rstrip("/")
        self.allowlist_path = Path(allowlist_path) if allowlist_path else Path("/srv/ai-server/trading/ws_collector/config/allowed_symbols.txt")
        self.execution_exchange_info_path = (
            Path(execution_exchange_info_path)
            if execution_exchange_info_path
            else Path("/srv/ai-server/trading/execution_layer_v4/runtime/exchange_info.json")
        )
        self._last_top_fetch = 0.0
        self._top_symbols: list[str] = []
        self._core_engine_symbols: list[str] = []
        self._impulse_symbols: list[str] = []
        self._engine_symbol_sources: dict[str, list[str]] = {}
        self._ticker_history: list[tuple[float, dict[str, tuple[float, float]]]] = []
        self._early_hotset_expiry: dict[str, float] = {}
        self._early_hotset_metrics: dict[str, dict[str, Any]] = {}
        self._early_admitted_at: dict[str, str] = {}
        self.last_report: dict[str, Any] = {}

    def _allowed_symbols(self) -> set[str]:
        return _read_symbol_file(self.allowlist_path)

    def _execution_tradable_symbols(self) -> tuple[set[str], dict[str, int]]:
        payload = _read_json(self.execution_exchange_info_path)
        out: set[str] = set()
        skipped: Counter[str] = Counter()
        for row in payload.get("symbols") or []:
            if not isinstance(row, dict):
                continue
            symbol = normalize_symbol(row.get("symbol"))
            if not symbol.endswith("USDT"):
                continue
            status = str(row.get("status") or "")
            contract_type = str(row.get("contractType") or "")
            if status == "TRADING" and contract_type == "PERPETUAL":
                out.add(symbol)
            else:
                skipped[f"{contract_type or 'unknown'}:{status or 'unknown'}"] += 1
        return out, dict(skipped)

    def _symbol_allowed(self, symbol: str, allowed: set[str], tradable: set[str]) -> bool:
        symbol = normalize_symbol(symbol)
        if allowed and symbol not in allowed:
            return False
        if tradable and symbol not in tradable:
            return False
        return symbol.endswith("USDT")

    def _cached_top_volume_symbols(self) -> list[str]:
        candidates = [
            Path("/srv/ai-server/trading/ws_collector/runtime/last_good_universe.json"),
            Path("/srv/ai-server/trading/ws_collector/runtime/ws_collector_status.json"),
        ]
        for path in candidates:
            payload = _read_json(path)
            if not payload:
                continue
            if "status" in payload and isinstance(payload["status"], dict):
                payload = payload["status"]
            universe = payload.get("universe") if isinstance(payload.get("universe"), dict) else payload
            engine_symbols = universe.get("engine_symbols") or []
            selected_symbols = universe.get("selected_symbols") or []
            picked = [normalize_symbol(symbol) for symbol in (engine_symbols or selected_symbols) if normalize_symbol(symbol).endswith("USDT")]
            if picked:
                return list(dict.fromkeys(picked))[: self.top_volume_symbols]
        return []

    @staticmethod
    def _ticker_float(item: dict[str, Any], key: str) -> float:
        try:
            return float(item.get(key) or 0.0)
        except Exception:
            return 0.0

    @staticmethod
    def _env_int(name: str, default: int) -> int:
        try:
            return int(os.getenv(name, str(default)) or default)
        except Exception:
            return default

    @staticmethod
    def _env_float(name: str, default: float) -> float:
        try:
            return float(os.getenv(name, str(default)) or default)
        except Exception:
            return default

    def _history_snapshot(
        self,
        now: float,
        age_seconds: float,
    ) -> tuple[float, dict[str, tuple[float, float]]] | None:
        eligible = [
            row
            for row in self._ticker_history
            if row[0] <= now - age_seconds
        ]
        return max(eligible, key=lambda row: row[0]) if eligible else None

    @staticmethod
    def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        payload,
                        ensure_ascii=True,
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                    + "\n"
                )
        except Exception:
            return

    def _early_ledger_event(
        self,
        event: str,
        symbol: str,
        now: float,
        metrics: dict[str, Any] | None = None,
        reason: str | None = None,
    ) -> None:
        path = Path(
            os.getenv(
                "WS_COLLECTOR_EARLY_HOTSET_LEDGER_PATH",
                "/srv/ai-server/trading/ws_collector/runtime/"
                "early_hotset_admissions.jsonl",
            )
        )
        self._append_jsonl(
            path,
            {
                "schema_version": "early_hotset_admission_event_v1",
                "event": event,
                "symbol": symbol,
                "event_time": datetime.fromtimestamp(now, UTC).isoformat(),
                "source": "binance_public_closed_1m_aggregate",
                "metrics": metrics or {},
                "reason": reason,
            },
        )

    @staticmethod
    def _closed_1m_evidence(
        rows: list[list[Any]],
        now_ms: int,
    ) -> dict[str, Any] | None:
        closed = [row for row in rows if len(row) > 7 and int(row[6]) <= now_ms]
        if len(closed) < 31:
            return None

        def window_metrics(size: int) -> dict[str, float]:
            current = closed[-size:]
            previous = closed[-2 * size : -size]
            baseline_windows = [
                closed[index - size : index]
                for index in range(len(closed) - size, size - 1, -size)
            ][:4]
            current_return = (
                float(current[-1][4]) / float(current[0][1]) - 1.0
            ) * 100.0
            previous_return = (
                float(previous[-1][4]) / float(previous[0][1]) - 1.0
            ) * 100.0
            current_quote_volume = sum(float(row[7]) for row in current)
            baseline_quote_volumes = [
                sum(float(row[7]) for row in window)
                for window in baseline_windows
                if len(window) == size
            ]
            volume_baseline = (
                median(baseline_quote_volumes)
                if baseline_quote_volumes
                else 0.0
            )
            return {
                "move_pct": current_return,
                "previous_move_pct": previous_return,
                "acceleration_pct": abs(current_return) - abs(previous_return),
                "quote_volume": current_quote_volume,
                "volume_ratio": (
                    current_quote_volume / volume_baseline
                    if volume_baseline > 0
                    else 0.0
                ),
            }

        return {
            "5m": window_metrics(5),
            "15m": window_metrics(15),
            "latest_close_time": int(closed[-1][6]),
        }

    async def _confirm_early_movers(
        self,
        candidates: list[str],
        now: float,
    ) -> dict[str, dict[str, Any]]:
        if not candidates:
            return {}
        base_url = os.getenv(
            "WS_COLLECTOR_UNIVERSE_TICKER_BASE",
            "https://fapi.binance.com",
        ).rstrip("/")
        concurrency = max(
            1,
            self._env_int("WS_COLLECTOR_EARLY_CONFIRM_CONCURRENCY", 8),
        )
        semaphore = asyncio.Semaphore(concurrency)
        timeout = aiohttp.ClientTimeout(total=10)

        async def fetch_one(
            session: aiohttp.ClientSession,
            symbol: str,
        ) -> tuple[str, dict[str, Any] | None]:
            async with semaphore:
                try:
                    async with session.get(
                        f"{base_url}/fapi/v1/klines",
                        params={
                            "symbol": symbol,
                            "interval": "1m",
                            "limit": 46,
                        },
                    ) as response:
                        response.raise_for_status()
                        rows = await response.json()
                    return (
                        symbol,
                        self._closed_1m_evidence(
                            rows,
                            int(now * 1000),
                        ),
                    )
                except Exception:
                    return symbol, None

        async with aiohttp.ClientSession(timeout=timeout) as session:
            fetched = await asyncio.gather(
                *(fetch_one(session, symbol) for symbol in candidates)
            )
        return {
            symbol: evidence
            for symbol, evidence in fetched
            if evidence is not None
        }

    async def _update_early_hotset(
        self,
        rows: list[dict[str, Any]],
        now: float,
        allowed: set[str],
        tradable: set[str],
    ) -> list[str]:
        current: dict[str, tuple[float, float]] = {}
        mover_min_volume = self._env_float(
            "WS_COLLECTOR_MOVER_MIN_VOLUME_LIMIT",
            5_000_000.0,
        )
        for item in rows:
            symbol = normalize_symbol(item.get("symbol"))
            price = self._ticker_float(item, "lastPrice")
            quote_volume = self._ticker_float(item, "quoteVolume")
            if (
                self._symbol_allowed(symbol, allowed, tradable)
                and price > 0
                and quote_volume >= mover_min_volume
            ):
                current[symbol] = (price, quote_volume)

        windows = (
            (
                "5m",
                self._env_float("WS_COLLECTOR_EARLY_5M_WINDOW_SECONDS", 240.0),
                self._env_float("WS_COLLECTOR_EARLY_5M_MIN_MOVE_PCT", 1.25),
            ),
            (
                "15m",
                self._env_float("WS_COLLECTOR_EARLY_15M_WINDOW_SECONDS", 840.0),
                self._env_float("WS_COLLECTOR_EARLY_15M_MIN_MOVE_PCT", 2.0),
            ),
        )
        ttl_seconds = max(
            60.0,
            self._env_float("WS_COLLECTOR_EARLY_HOTSET_TTL_SECONDS", 1800.0),
        )
        displacement: dict[str, dict[str, Any]] = {}
        for label, age_seconds, min_move_pct in windows:
            historical = self._history_snapshot(now, age_seconds)
            if historical is None:
                continue
            historical_at, previous = historical
            elapsed = max(1.0, now - historical_at)
            for symbol, (price, quote_volume) in current.items():
                prior = previous.get(symbol)
                if prior is None or prior[0] <= 0:
                    continue
                move_pct = (price / prior[0] - 1.0) * 100.0
                if abs(move_pct) + 1e-12 < min_move_pct:
                    continue
                candidate = {
                    "window": label,
                    "window_seconds": round(elapsed, 3),
                    "ticker_move_pct": round(move_pct, 6),
                    "quote_volume_24h": round(quote_volume, 3),
                }
                existing = displacement.get(symbol)
                if existing is None or abs(move_pct) > abs(
                    float(existing.get("ticker_move_pct") or 0.0)
                ):
                    displacement[symbol] = candidate

        prefilter_limit = max(
            1,
            self._env_int("WS_COLLECTOR_EARLY_PREFILTER_SYMBOLS", 40),
        )
        candidates = sorted(
            displacement,
            key=lambda symbol: (
                -abs(
                    float(
                        displacement[symbol].get("ticker_move_pct") or 0.0
                    )
                ),
                symbol,
            ),
        )[:prefilter_limit]
        confirmed = await self._confirm_early_movers(candidates, now)
        fresh_metrics: dict[str, dict[str, Any]] = {}
        for symbol, evidence in confirmed.items():
            qualifying: list[tuple[str, dict[str, float]]] = []
            for label, _age_seconds, min_move_pct in windows:
                metrics = evidence.get(label) or {}
                min_volume_ratio = self._env_float(
                    f"WS_COLLECTOR_EARLY_{label.upper()}_MIN_VOLUME_RATIO",
                    1.5 if label == "5m" else 1.35,
                )
                min_acceleration = self._env_float(
                    f"WS_COLLECTOR_EARLY_{label.upper()}_MIN_ACCELERATION_PCT",
                    0.35 if label == "5m" else 0.50,
                )
                if (
                    abs(float(metrics.get("move_pct") or 0.0))
                    >= min_move_pct
                    and float(metrics.get("volume_ratio") or 0.0)
                    >= min_volume_ratio
                    and float(metrics.get("acceleration_pct") or 0.0)
                    >= min_acceleration
                ):
                    qualifying.append((label, metrics))
            if not qualifying:
                continue
            label, metrics = max(
                qualifying,
                key=lambda item: (
                    abs(float(item[1].get("move_pct") or 0.0))
                    * float(item[1].get("volume_ratio") or 0.0)
                ),
            )
            score = (
                abs(float(metrics["move_pct"]))
                * math.log1p(float(metrics["volume_ratio"]))
                * (1.0 + max(0.0, float(metrics["acceleration_pct"])))
            )
            fresh_metrics[symbol] = {
                **displacement[symbol],
                "window": label,
                "move_pct": round(float(metrics["move_pct"]), 6),
                "previous_move_pct": round(
                    float(metrics["previous_move_pct"]),
                    6,
                ),
                "acceleration_pct": round(
                    float(metrics["acceleration_pct"]),
                    6,
                ),
                "quote_volume": round(float(metrics["quote_volume"]), 3),
                "volume_ratio": round(float(metrics["volume_ratio"]), 6),
                "score": round(score, 6),
                "detected_at": datetime.fromtimestamp(now, UTC).isoformat(),
                "source": "binance_public_closed_1m_aggregate",
            }

        for symbol, metrics in fresh_metrics.items():
            newly_admitted = symbol not in self._early_hotset_expiry
            self._early_hotset_expiry[symbol] = now + ttl_seconds
            self._early_hotset_metrics[symbol] = metrics
            if newly_admitted:
                admitted_at = datetime.fromtimestamp(now, UTC).isoformat()
                self._early_admitted_at[symbol] = admitted_at
                self._early_ledger_event(
                    "promoted",
                    symbol,
                    now,
                    metrics,
                    "closed_1m_acceleration_and_volume_expansion",
                )
        for symbol in list(self._early_hotset_expiry):
            if (
                self._early_hotset_expiry[symbol] <= now
                or symbol not in current
            ):
                reason = (
                    "ttl_elapsed"
                    if self._early_hotset_expiry[symbol] <= now
                    else "liquidity_or_tradability_lost"
                )
                self._early_ledger_event(
                    "demoted",
                    symbol,
                    now,
                    self._early_hotset_metrics.get(symbol),
                    reason,
                )
                self._early_hotset_expiry.pop(symbol, None)
                self._early_hotset_metrics.pop(symbol, None)
                self._early_admitted_at.pop(symbol, None)

        self._ticker_history.append((now, current))
        history_retention = max(
            1_200.0,
            self._env_float(
                "WS_COLLECTOR_EARLY_HISTORY_RETENTION_SECONDS",
                2_400.0,
            ),
        )
        self._ticker_history = [
            row for row in self._ticker_history
            if row[0] >= now - history_retention
        ][-60:]
        target = max(
            0,
            self._env_int("WS_COLLECTOR_EARLY_HOTSET_SYMBOLS", 20),
        )
        ranked = sorted(
            self._early_hotset_expiry,
            key=lambda symbol: (
                -float(
                    (self._early_hotset_metrics.get(symbol) or {}).get("score")
                    or 0.0
                ),
                symbol,
            ),
        )
        return ranked[:target]

    async def _fetch_top_volume_symbols(self) -> list[str]:
        if not self.include_top_volume or self.top_volume_symbols <= 0:
            return []
        now = time.time()
        fetch_seconds = max(
            30.0,
            self._env_float("WS_COLLECTOR_UNIVERSE_FETCH_SECONDS", 60.0),
        )
        if self._top_symbols and now - self._last_top_fetch < fetch_seconds:
            return self._top_symbols[: self.top_volume_symbols]
        # Universe ranking uses live public liquidity. Klines, quotes and all
        # strategy inputs remain owned by the configured demo collector.
        live_ticker_base = os.getenv(
            "WS_COLLECTOR_UNIVERSE_TICKER_BASE",
            "https://fapi.binance.com",
        )
        base_urls = [live_ticker_base]
        try:
            rows = None
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as session:
                for base_url in base_urls:
                    url = f"{base_url}/fapi/v1/ticker/24hr"
                    try:
                        async with session.get(url) as resp:
                            resp.raise_for_status()
                            rows = await resp.json()
                            break
                    except Exception:
                        continue
            if rows is not None:
                allowed = self._allowed_symbols()
                tradable, _ = self._execution_tradable_symbols()
                min_volume = float(os.getenv("WS_COLLECTOR_MIN_VOLUME_LIMIT", "15000000.0") or 15000000.0)
                filtered = [
                    item for item in rows
                    if self._symbol_allowed(str(item.get("symbol") or ""), allowed, tradable)
                    and self._ticker_float(item, "quoteVolume") >= min_volume
                ]
                mover_min_volume = self._env_float(
                    "WS_COLLECTOR_MOVER_MIN_VOLUME_LIMIT",
                    5_000_000.0,
                )
                mover_min_abs_change = self._env_float(
                    "WS_COLLECTOR_MOVER_MIN_ABS_CHANGE_PCT",
                    5.0,
                )
                mover_extra = [
                    item for item in rows
                    if self._symbol_allowed(
                        str(item.get("symbol") or ""),
                        allowed,
                        tradable,
                    )
                    and mover_min_volume
                    <= self._ticker_float(item, "quoteVolume")
                    < min_volume
                    and abs(
                        self._ticker_float(item, "priceChangePercent")
                    )
                    >= mover_min_abs_change
                ]
                mover_pool = filtered + mover_extra
                early_hotset = await self._update_early_hotset(
                    rows,
                    now,
                    allowed,
                    tradable,
                )
                # OPERATIONAL CONTRACT:
                # The collector universe is an opportunity mix, not pure top-volume.
                # Do not revert to volume-only without proving missed-move impact and
                # updating the changelog. Open positions remain forced in symbols().
                volume_target = self._env_int("WS_COLLECTOR_TOP_VOLUME_CORE_SYMBOLS", 50)
                gainer_target = self._env_int("WS_COLLECTOR_TOP_GAINER_SYMBOLS", 20)
                loser_target = self._env_int("WS_COLLECTOR_TOP_LOSER_SYMBOLS", 20)
                abs_mover_target = self._env_int("WS_COLLECTOR_TOP_ABS_MOVER_SYMBOLS", 10)
                volume_target = max(0, min(volume_target, self.top_volume_symbols))
                remaining = max(0, self.top_volume_symbols - volume_target)
                gainer_target = max(0, min(gainer_target, remaining))
                remaining = max(0, remaining - gainer_target)
                loser_target = max(0, min(loser_target, remaining))
                remaining = max(0, remaining - loser_target)
                abs_mover_target = max(0, min(abs_mover_target, remaining))

                by_volume = sorted(filtered, key=lambda item: self._ticker_float(item, "quoteVolume"), reverse=True)
                by_gainer = sorted(mover_pool, key=lambda item: self._ticker_float(item, "priceChangePercent"), reverse=True)
                by_loser = sorted(mover_pool, key=lambda item: self._ticker_float(item, "priceChangePercent"))
                by_abs_mover = sorted(
                    mover_pool,
                    key=lambda item: abs(self._ticker_float(item, "priceChangePercent")),
                    reverse=True,
                )

                ordered: list[str] = []
                source_map: dict[str, list[str]] = defaultdict(list)
                bucket_counts: Counter[str] = Counter()

                def add_bucket(bucket: list[dict[str, Any]], limit: int, source: str) -> None:
                    for item in bucket:
                        if bucket_counts[source] >= limit:
                            break
                        symbol = normalize_symbol(item.get("symbol"))
                        if not symbol:
                            continue
                        if source not in source_map[symbol]:
                            source_map[symbol].append(source)
                            bucket_counts[source] += 1
                        if symbol not in ordered:
                            ordered.append(symbol)

                add_bucket(by_volume, volume_target, "top_volume")
                add_bucket(by_gainer, gainer_target, "top_gainer")
                add_bucket(by_loser, loser_target, "top_loser")
                add_bucket(by_abs_mover, abs_mover_target, "top_abs_mover")
                early_target = max(
                    0,
                    self._env_int("WS_COLLECTOR_EARLY_HOTSET_SYMBOLS", 20),
                )
                core_limit = max(
                    1,
                    min(
                        self.top_volume_symbols,
                        self.max_symbols - min(early_target, self.max_symbols - 1),
                    ),
                )
                for item in by_volume:
                    if len(ordered) >= core_limit:
                        break
                    symbol = normalize_symbol(item.get("symbol"))
                    if symbol and symbol not in ordered:
                        ordered.append(symbol)
                        source_map[symbol].append("volume_fill")

                self._core_engine_symbols = ordered[:core_limit]
                self._impulse_symbols = [
                    symbol
                    for symbol in early_hotset
                    if symbol not in self._core_engine_symbols
                ][:early_target]
                for symbol in early_hotset:
                    if "early_hotset" not in source_map[symbol]:
                        source_map[symbol].append("early_hotset")
                self._top_symbols = (
                    self._core_engine_symbols + self._impulse_symbols
                )[: self.max_symbols]
                self._engine_symbol_sources = {symbol: list(labels) for symbol, labels in source_map.items()}
                self._last_top_fetch = now
        except Exception:
            pass
        if not self._top_symbols:
            cached = self._cached_top_volume_symbols()
            if cached:
                self._top_symbols = cached
                self._core_engine_symbols = list(cached)
                self._impulse_symbols = []
                self._engine_symbol_sources = {symbol: ["cached_universe"] for symbol in cached}
                self._last_top_fetch = now
        return self._top_symbols[: self.top_volume_symbols]

    def _shadow_active_positions(self) -> set[str]:
        paths = [
            Path("/srv/ai-server/trading/signal_layer/runtime/shadow_summary.json"),
            Path("/srv/ai-server/trading/signal_layer/runtime/experimental_summary.json"),
        ]
        out: set[str] = set()
        for path in paths:
            payload = _read_json(path)
            for row in payload.get("active", []) or []:
                if not isinstance(row, dict):
                    continue
                sym = str(row.get("symbol") or "").upper().strip()
                status = str(row.get("status") or "").lower().strip()
                if sym and status in {"active", "tp1_hit", "break_even"}:
                    out.add(sym)
        return out

    def _llm_paper_active_positions(self) -> set[str]:
        include_llm_paper = str(
            os.getenv("WS_COLLECTOR_INCLUDE_LLM_PAPER_ACTIVE", "1")
        ).strip().lower() in {"1", "true", "yes", "on"}
        if not include_llm_paper:
            return set()
        payload = _read_json(
            Path("/srv/ai-server/trading/research_lab/rooms/llm_signal_trader_lab/runtime/paper_positions.json")
        )
        out: set[str] = set()
        rows = payload.get("positions") or []
        if not isinstance(rows, list):
            return out
        for row in rows:
            if not isinstance(row, dict):
                continue
            status = str(row.get("status") or "").lower().strip()
            if status and status != "open":
                continue
            sym = normalize_symbol(row.get("symbol"))
            if sym.endswith("USDT"):
                out.add(sym)
        return out

    async def symbols(self) -> list[str]:
        allowed = self._allowed_symbols()
        tradable, tradable_skips = self._execution_tradable_symbols()
        top_volume = list(dict.fromkeys(await self._fetch_top_volume_symbols()))
        dynamic_sources: dict[str, set[str]] = {
            "shadow_active_positions": self._shadow_active_positions(),
            "llm_paper_active_positions": self._llm_paper_active_positions(),
        }
        sources: dict[str, set[str]] = {
            **dynamic_sources,
            "top_volume": set(top_volume),
            "top_gainer": {symbol for symbol, labels in self._engine_symbol_sources.items() if "top_gainer" in labels},
            "top_loser": {symbol for symbol, labels in self._engine_symbol_sources.items() if "top_loser" in labels},
            "top_abs_mover": {symbol for symbol, labels in self._engine_symbol_sources.items() if "top_abs_mover" in labels},
            "early_hotset": {
                symbol
                for symbol, labels in self._engine_symbol_sources.items()
                if "early_hotset" in labels
            },
        }

        weights = {
            "shadow_active_positions": 120,
            "llm_paper_active_positions": 110,
        }
        dynamic_scores: Counter[str] = Counter()
        by_symbol_sources: dict[str, list[str]] = defaultdict(list)

        for symbol in top_volume:
            if self._symbol_allowed(symbol, allowed, tradable):
                labels = self._engine_symbol_sources.get(symbol) or ["top_volume"]
                for label in labels:
                    if label not in by_symbol_sources[symbol]:
                        by_symbol_sources[symbol].append(label)

        for source, symbols in dynamic_sources.items():
            for symbol in symbols:
                if self._symbol_allowed(symbol, allowed, tradable):
                    dynamic_scores[symbol] += weights[source]
                    by_symbol_sources[symbol].append(source)

        dynamic_ordered = [symbol for symbol, _ in dynamic_scores.most_common()]
        selected: list[str] = []
        seen: set[str] = set()

        for symbol in top_volume:
            if symbol not in seen:
                selected.append(symbol)
                seen.add(symbol)

        for symbol in dynamic_ordered:
            if symbol not in seen:
                selected.append(symbol)
                seen.add(symbol)
            if len(selected) >= self.max_symbols:
                break

        selected = selected[: self.max_symbols]
        forced_dynamic = sorted(
            {
                sym for sym in (
                    set(sources.get("shadow_active_positions") or [])
                    | set(sources.get("llm_paper_active_positions") or [])
                )
                if self._symbol_allowed(sym, allowed, tradable)
            }
        )
        for sym in forced_dynamic:
            if len(selected) >= self.max_symbols:
                break
            if sym and sym not in selected:
                selected.append(sym)

        dynamic_extra = [s for s in dynamic_ordered if s not in set(top_volume)]
        source_counts = Counter()
        for symbol in selected:
            for source in by_symbol_sources.get(symbol, []):
                source_counts[source] += 1
        self.last_report = {
            "updated_at": datetime.now(UTC).isoformat(),
            "mode": "opportunity_mix_plus_dynamic",
            "selection_mix": {
                "top_volume_core_target": self._env_int("WS_COLLECTOR_TOP_VOLUME_CORE_SYMBOLS", 50),
                "top_gainer_target": self._env_int("WS_COLLECTOR_TOP_GAINER_SYMBOLS", 20),
                "top_loser_target": self._env_int("WS_COLLECTOR_TOP_LOSER_SYMBOLS", 20),
                "top_abs_mover_target": self._env_int("WS_COLLECTOR_TOP_ABS_MOVER_SYMBOLS", 10),
                "early_hotset_target": self._env_int(
                    "WS_COLLECTOR_EARLY_HOTSET_SYMBOLS",
                    20,
                ),
            },
            "universe_filter": {
                "allowlist_path": str(self.allowlist_path),
                "allowlist_count": len(allowed),
                "execution_exchange_info_path": str(self.execution_exchange_info_path),
                "execution_tradable_count": len(tradable),
                "execution_skip_reasons": tradable_skips,
                "effective_allowed_count": len((allowed or tradable) & tradable) if allowed and tradable else len(allowed or tradable),
            },
            "max_symbols": self.max_symbols,
            "top_volume_target": self.top_volume_symbols,
            "selected_count": len(selected),
            "selected_symbols": selected,
            "engine_symbols": list(self._core_engine_symbols),
            "impulse_symbols": list(self._impulse_symbols),
            "engine_symbols_source": "volume_early_gainer_loser_abs_mover_mix",
            "early_hotset": {
                "active_count": len(sources["early_hotset"]),
                "symbols": sorted(sources["early_hotset"]),
                "metrics": {
                    symbol: {
                        **self._early_hotset_metrics[symbol],
                        "admitted_at": self._early_admitted_at.get(symbol),
                        "demo_data_ready_at": None,
                    }
                    for symbol in sorted(sources["early_hotset"])
                    if symbol in self._early_hotset_metrics
                },
            },
            "dynamic_source_policy": {
                "shadow_active_positions_enabled": True,
                "llm_paper_active_positions_enabled": str(
                    os.getenv("WS_COLLECTOR_INCLUDE_LLM_PAPER_ACTIVE", "1")
                ).strip().lower() in {"1", "true", "yes", "on"},
            },
            "source_counts": dict(source_counts),
            "dynamic_extra_count": len(dynamic_extra),
            "dynamic_extra_symbols": dynamic_extra,
            "sources": {name: sorted(values) for name, values in sources.items()},
            "symbol_sources": {symbol: by_symbol_sources[symbol] for symbol in selected},
        }
        return selected


def manager_from_env() -> DynamicUniverseManager:
    return DynamicUniverseManager(
        os.getenv("CRYPTO_STACK_ROOT", "/srv/ai-server/trading/crypto"),
        max_symbols=int(os.getenv("WS_COLLECTOR_MAX_SYMBOLS", "150") or 150),
        top_volume_symbols=int(os.getenv("WS_COLLECTOR_TOP_VOLUME_SYMBOLS", "100") or 100),
        recent_minutes=int(os.getenv("WS_COLLECTOR_RECENT_MINUTES", "90") or 90),
        include_top_volume=str(os.getenv("WS_COLLECTOR_INCLUDE_TOP_VOLUME", "1")).strip().lower()
        in {"1", "true", "yes", "on"},
        rest_base_url=os.getenv("WS_COLLECTOR_BINANCE_FUTURES_REST", "https://fapi.binance.com"),
    )
