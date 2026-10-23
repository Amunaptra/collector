from __future__ import annotations

from ws_collector.services.market_cache import MarketCache


def test_default_snapshot_keeps_80_closed_bars_plus_current_bar() -> None:
    cache = MarketCache(max_bars=500)
    for index in range(90):
        cache.add_candle(
            "BTCUSDT",
            "4h",
            {
                "open_time": index * 14_400_000,
                "close_time": (index + 1) * 14_400_000 - 1,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1_000.0,
                "closed": index < 89,
            },
            persist=False,
        )

    rows = cache.snapshot()["symbols"]["BTCUSDT"]["4h"]

    assert len(rows) == 81
    assert sum(row["closed"] is True for row in rows) == 80
