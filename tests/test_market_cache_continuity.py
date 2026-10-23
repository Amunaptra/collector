from __future__ import annotations

import json
import tempfile
from pathlib import Path

from ws_collector.services.market_cache import JsonlCandleWriter, MarketCache


TF_MS = 3_600_000
NOW_MS = 1_800_000_000_000
CURRENT_OPEN = (NOW_MS // TF_MS) * TF_MS


def candle(open_time: int, close: float = 100.0) -> dict:
    return {
        "open_time": open_time,
        "close_time": open_time + TF_MS - 1,
        "open": close,
        "high": close + 1,
        "low": close - 1,
        "close": close,
        "volume": 1000,
        "closed": open_time < CURRENT_OPEN,
        "received_at": NOW_MS / 1000,
    }


def test_out_of_order_backfill_keeps_series_sorted() -> None:
    cache = MarketCache(max_bars=10)
    for offset in (-4, -3, 0):
        cache.add_candle("TESTUSDT", "1h", candle(CURRENT_OPEN + offset * TF_MS))
    for offset in (-2, -1):
        cache.add_candle("TESTUSDT", "1h", candle(CURRENT_OPEN + offset * TF_MS))

    open_times = [row["open_time"] for row in cache.get_bars("TESTUSDT", "1h")]
    assert open_times == sorted(open_times)
    assert open_times[-1] == CURRENT_OPEN


def test_gap_is_detected_from_contiguous_tail_not_total_bar_count() -> None:
    cache = MarketCache(max_bars=100)
    for offset in range(-90, -10):
        cache.add_candle("TESTUSDT", "1h", candle(CURRENT_OPEN + offset * TF_MS))
    for offset in (-1, 0):
        cache.add_candle("TESTUSDT", "1h", candle(CURRENT_OPEN + offset * TF_MS))

    health = cache.series_health(
        "TESTUSDT",
        "1h",
        min_contiguous_bars=20,
        now_ms=NOW_MS,
    )
    assert health["unique_bars"] == 82
    assert health["contiguous_tail_bars"] == 2
    assert health["ok"] is False
    assert "contiguous_tail_below_minimum" in health["reasons"]


def test_full_backfill_repairs_contiguous_tail() -> None:
    cache = MarketCache(max_bars=100)
    for offset in range(-90, -10):
        cache.add_candle("TESTUSDT", "1h", candle(CURRENT_OPEN + offset * TF_MS))
    for offset in (-1, 0):
        cache.add_candle("TESTUSDT", "1h", candle(CURRENT_OPEN + offset * TF_MS))
    for offset in range(-30, 1):
        cache.add_candle("TESTUSDT", "1h", candle(CURRENT_OPEN + offset * TF_MS))

    health = cache.series_health(
        "TESTUSDT",
        "1h",
        min_contiguous_bars=20,
        now_ms=NOW_MS,
    )
    assert health["ok"] is True
    assert health["contiguous_tail_bars"] >= 31


def test_bootstrap_load_marks_seen_without_reappending_jsonl() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        writer = JsonlCandleWriter(tmp)
        cache = MarketCache(max_bars=100, raw_writer=writer)
        existing = candle(CURRENT_OPEN - 2 * TF_MS)
        cache.add_candle("TESTUSDT", "1h", existing, persist=False)
        cache.add_candle("TESTUSDT", "1h", existing)

        path = Path(tmp) / "data" / "klines" / "TESTUSDT" / "1h.jsonl"
        assert not path.exists()

        missing = candle(CURRENT_OPEN - TF_MS)
        cache.add_candle("TESTUSDT", "1h", missing)
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        assert len(rows) == 1
        assert rows[0]["open_time"] == missing["open_time"]


if __name__ == "__main__":
    test_out_of_order_backfill_keeps_series_sorted()
    test_gap_is_detected_from_contiguous_tail_not_total_bar_count()
    test_full_backfill_repairs_contiguous_tail()
    test_bootstrap_load_marks_seen_without_reappending_jsonl()
    print("manual_market_cache_continuity_tests=4_pass")
