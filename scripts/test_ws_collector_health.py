#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATUS_PATH = ROOT / "runtime" / "ws_collector_status.json"
FRESHNESS_PATH = ROOT / "runtime" / "ws_freshness_report.json"


def main() -> int:
    if not STATUS_PATH.exists():
        print(f"FAIL: status file yok: {STATUS_PATH}")
        return 1
    status = json.loads(STATUS_PATH.read_text(encoding="utf-8"))
    now = time.time()
    last = status.get("last_message_at")
    age = None if not last else now - float(last)
    print(json.dumps(status, ensure_ascii=False, indent=2))
    if not status.get("connected"):
        print("FAIL: websocket connected=false")
        return 2
    if age is None or age > 30:
        print(f"FAIL: son mesaj yasi yuksek: {age}")
        return 3
    cache_size = status.get("cache_size", {})
    if int(cache_size.get("bars") or 0) <= 0:
        print("FAIL: cache icinde bar yok")
        return 4
    if FRESHNESS_PATH.exists():
        freshness = json.loads(FRESHNESS_PATH.read_text(encoding="utf-8"))
        stale_series = int((freshness.get("summary") or {}).get("stale_series") or 0)
        print(f"freshness stale_series={stale_series}")
    print("OK: collector healthy")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
