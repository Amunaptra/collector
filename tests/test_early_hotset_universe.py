from __future__ import annotations

import asyncio
from pathlib import Path

from ws_collector.services.universe_manager import DynamicUniverseManager


def ticker(
    symbol: str,
    price: float,
    quote_volume: float,
) -> dict[str, str]:
    return {
        "symbol": symbol,
        "lastPrice": str(price),
        "quoteVolume": str(quote_volume),
        "priceChangePercent": "0",
    }


def manager() -> DynamicUniverseManager:
    return DynamicUniverseManager(
        Path("."),
        max_symbols=150,
        top_volume_symbols=150,
        allowlist_path=Path("missing-allowlist.txt"),
        execution_exchange_info_path=Path("missing-exchange-info.json"),
    )


def confirmed_evidence(
    *,
    move_pct: float = 2.0,
    previous_move_pct: float = 0.2,
    volume_ratio: float = 2.0,
) -> dict:
    return {
        "5m": {
            "move_pct": move_pct,
            "previous_move_pct": previous_move_pct,
            "acceleration_pct": abs(move_pct) - abs(previous_move_pct),
            "quote_volume": 100_000.0,
            "volume_ratio": volume_ratio,
        },
        "15m": {
            "move_pct": move_pct,
            "previous_move_pct": previous_move_pct,
            "acceleration_pct": abs(move_pct) - abs(previous_move_pct),
            "quote_volume": 100_000.0,
            "volume_ratio": volume_ratio,
        },
        "latest_close_time": 1_300_000,
    }


def test_fast_move_with_volume_expansion_enters_early_hotset(
    monkeypatch,
) -> None:
    monkeypatch.setenv("WS_COLLECTOR_EARLY_HOTSET_LEDGER_PATH", "NUL")
    monkeypatch.setenv("WS_COLLECTOR_EARLY_5M_MIN_MOVE_PCT", "1.25")
    monkeypatch.setenv("WS_COLLECTOR_EARLY_5M_MIN_VOLUME_RATE", "2.5")
    subject = manager()
    async def confirm(candidates, _now):
        return {
            symbol: confirmed_evidence()
            for symbol in candidates
        }

    monkeypatch.setattr(subject, "_confirm_early_movers", confirm)
    asyncio.run(subject._update_early_hotset(
        [ticker("FASTUSDT", 100.0, 10_000_000.0)],
        1000.0,
        set(),
        set(),
    ))
    selected = asyncio.run(subject._update_early_hotset(
        [ticker("FASTUSDT", 102.0, 10_100_000.0)],
        1300.0,
        set(),
        set(),
    ))
    assert selected == ["FASTUSDT"]
    assert subject._early_hotset_metrics["FASTUSDT"]["window"] == "5m"


def test_price_move_without_volume_expansion_is_rejected(
    monkeypatch,
) -> None:
    monkeypatch.setenv("WS_COLLECTOR_EARLY_HOTSET_LEDGER_PATH", "NUL")
    monkeypatch.setenv("WS_COLLECTOR_EARLY_5M_MIN_MOVE_PCT", "1.25")
    monkeypatch.setenv("WS_COLLECTOR_EARLY_5M_MIN_VOLUME_RATE", "2.5")
    subject = manager()
    async def confirm(candidates, _now):
        return {
            symbol: confirmed_evidence(volume_ratio=1.0)
            for symbol in candidates
        }

    monkeypatch.setattr(subject, "_confirm_early_movers", confirm)
    asyncio.run(subject._update_early_hotset(
        [ticker("THINUSDT", 100.0, 10_000_000.0)],
        1000.0,
        set(),
        set(),
    ))
    selected = asyncio.run(subject._update_early_hotset(
        [ticker("THINUSDT", 102.0, 10_005_000.0)],
        1300.0,
        set(),
        set(),
    ))
    assert selected == []


def test_hotset_uses_ttl_to_avoid_one_cycle_churn(monkeypatch) -> None:
    monkeypatch.setenv("WS_COLLECTOR_EARLY_HOTSET_LEDGER_PATH", "NUL")
    monkeypatch.setenv("WS_COLLECTOR_EARLY_HOTSET_TTL_SECONDS", "1800")
    subject = manager()
    async def confirm(candidates, _now):
        return {
            symbol: confirmed_evidence()
            for symbol in candidates
        }

    monkeypatch.setattr(subject, "_confirm_early_movers", confirm)
    asyncio.run(subject._update_early_hotset(
        [ticker("FASTUSDT", 100.0, 10_000_000.0)],
        1000.0,
        set(),
        set(),
    ))
    assert asyncio.run(subject._update_early_hotset(
        [ticker("FASTUSDT", 102.0, 10_100_000.0)],
        1300.0,
        set(),
        set(),
    )) == ["FASTUSDT"]
    assert asyncio.run(subject._update_early_hotset(
        [ticker("FASTUSDT", 102.1, 10_101_000.0)],
        1600.0,
        set(),
        set(),
    )) == ["FASTUSDT"]


def test_closed_1m_evidence_uses_real_window_volume_and_acceleration() -> None:
    rows = []
    price = 100.0
    for index in range(46):
        open_price = price
        if index >= 41:
            price *= 1.004
            quote_volume = 50_000.0
        else:
            price *= 1.0001
            quote_volume = 5_000.0
        open_time = index * 60_000
        rows.append(
            [
                open_time,
                str(open_price),
                str(max(open_price, price)),
                str(min(open_price, price)),
                str(price),
                "1",
                open_time + 59_999,
                str(quote_volume),
            ]
        )
    evidence = DynamicUniverseManager._closed_1m_evidence(
        rows,
        rows[-1][6],
    )
    assert evidence is not None
    assert evidence["5m"]["move_pct"] > 1.25
    assert evidence["5m"]["acceleration_pct"] > 0.35
    assert evidence["5m"]["volume_ratio"] > 1.5
