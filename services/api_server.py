from __future__ import annotations

import copy
import logging
import time
from typing import Any

from aiohttp import web

@web.middleware
async def cors_middleware(request, handler):
    if request.method == "OPTIONS":
        return web.Response(
            status=204,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                "Access-Control-Allow-Headers": "*",
                "Access-Control-Max-Age": "86400",
            },
        )

    response = await handler(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "*"
    return response



from services.market_cache import MarketCache


COLLECTOR_DASHBOARD_HTML = """<!doctype html>
<html lang="tr">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>WS Collector</title>
  <style>
    :root{--bg:#0b0f14;--panel:#121821;--panel2:#17202c;--line:#2a3544;--text:#eef3f8;--muted:#94a3b8;--ok:#39d98a;--warn:#f7c948;--bad:#ff6b6b;--blue:#62a8ff}
    *{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--text);font-family:Segoe UI,Arial,sans-serif}.wrap{max-width:1500px;margin:0 auto;padding:22px}
    .top{display:flex;justify-content:space-between;gap:16px;align-items:flex-start;margin-bottom:14px}h1{margin:0 0 6px;font-size:24px}p{margin:0;color:var(--muted)}
    .pills{display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end}.pill,.btn{border:1px solid var(--line);background:#111923;border-radius:8px;padding:8px 10px;font-size:12px;font-weight:700;color:var(--text)}.btn{cursor:pointer}.btn:hover{border-color:var(--blue)}
    .grid{display:grid;grid-template-columns:repeat(6,minmax(130px,1fr));gap:10px;margin-bottom:14px}.card{border:1px solid var(--line);background:var(--panel);border-radius:8px;padding:12px}.label{font-size:11px;color:var(--muted);text-transform:uppercase}.value{font-size:22px;font-weight:800;margin-top:6px}.ok{color:var(--ok)}.warn{color:var(--warn)}.bad{color:var(--bad)}.muted{color:var(--muted)}.blue{color:var(--blue)}
    .section{border:1px solid var(--line);background:var(--panel);border-radius:8px;padding:14px;margin-bottom:14px}.section-head{display:flex;justify-content:space-between;gap:10px;align-items:center;margin-bottom:10px}h2{margin:0;font-size:17px}.mini{font-size:12px;color:var(--muted)}
    .table-wrap{overflow:auto;border:1px solid var(--line);border-radius:8px}table{width:100%;border-collapse:collapse;min-width:980px}th,td{padding:8px 10px;border-bottom:1px solid rgba(42,53,68,.8);font-size:12px;text-align:left;white-space:nowrap}th{position:sticky;top:0;background:#18212d;color:#b9c6d4}.tag{display:inline-flex;border:1px solid #38506b;background:#172231;border-radius:999px;padding:4px 8px;font-weight:800}
    .controls{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px}.controls input,.controls select{background:#101722;color:var(--text);border:1px solid #35465c;border-radius:7px;padding:8px 9px;font-size:12px}
    @media(max-width:1000px){.grid{grid-template-columns:repeat(2,1fr)}.top{flex-direction:column}.pills{justify-content:flex-start}}
  </style>
</head>
<body>
<div class="wrap">
  <div class="top">
    <div><h1>WS Collector</h1><p>Binance Futures kline + quote collector sagligi ve tazelik gorunumu.</p></div>
    <div class="pills"><div class="pill" id="updated">Yukleniyor</div><button class="btn" onclick="load()">Yenile</button></div>
  </div>
  <div class="grid" id="cards"></div>
  <section class="section">
    <div class="section-head"><h2>Freshness</h2><span class="mini" id="freshInfo"></span></div>
    <div class="controls">
      <input id="search" placeholder="Sembol ara: BTC, ZEC, FIL..." />
      <select id="tf"><option value="">TF: Hepsi</option><option>5m</option><option>15m</option><option>1h</option><option>4h</option></select>
      <select id="only"><option value="">Durum: Hepsi</option><option value="fresh">Fresh</option><option value="stale">Stale</option></select>
    </div>
    <div class="table-wrap"><table><thead><tr><th>Symbol</th><th>TF</th><th>Status</th><th>Msg Age</th><th>Bar Elapsed</th><th>Close</th><th>Volume</th><th>Open Time</th></tr></thead><tbody id="rows"></tbody></table></div>
  </section>
  <section class="section">
    <div class="section-head"><h2>Quote Chunks</h2><span class="mini" id="chunkInfo"></span></div>
    <div class="table-wrap"><table><thead><tr><th>Chunk</th><th>Connected</th><th>Symbols</th><th>Streams</th><th>Last Msg Age</th><th>Reconnect</th><th>Error</th></tr></thead><tbody id="chunks"></tbody></table></div>
  </section>
</div>
<script>
const $=id=>document.getElementById(id);
const esc=v=>String(v??"-").replace(/[&<>"']/g,ch=>({"&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;","'":"&#039;"}[ch]));
const num=v=>{const n=Number(v);return Number.isFinite(n)?n:null};
const fmt=(v,d=2)=>{const n=num(v);return n===null?esc(v):n.toFixed(d).replace(/\\.?0+$/,"")};
const cls=v=>v===true||v==="fresh"?"ok":v===false||v==="stale"?"bad":"muted";
const dtms=v=>{const n=num(v);if(n===null)return "-";return new Date(n).toLocaleString("tr-TR")};
let LAST=null;
function card(label,value,klass=""){return `<div class="card"><div class="label">${esc(label)}</div><div class="value ${klass}">${esc(value)}</div></div>`}
function filters(){return {q:($("search").value||"").toUpperCase().trim(),tf:$("tf").value,only:$("only").value}}
function pass(r,f){if(f.q&&!String(r.symbol||"").includes(f.q))return false;if(f.tf&&r.timeframe!==f.tf)return false;if(f.only==="fresh"&&r.stale)return false;if(f.only==="stale"&&!r.stale)return false;return true}
function render(){
  const s=LAST.status||{}, fr=(LAST.freshness||{}).freshness||{}, rows=fr.rows||[], f=filters();
  const shown=rows.filter(r=>pass(r,f)).slice(0,500);
  $("cards").innerHTML=[
    card("Kline",s.connected?"CONNECTED":"DOWN",s.connected?"ok":"bad"),
    card("Quote",s.quote_connected?"CONNECTED":"DOWN",s.quote_connected?"ok":"bad"),
    card("Symbols",s.symbols_count??"-"),
    card("Streams",s.streams_count??"-"),
    card("Msg Age",fmt(s.last_message_age_seconds,3)+"s",num(s.last_message_age_seconds)<5?"ok":"bad"),
    card("Quote Age",fmt(s.quote_last_message_age_seconds,3)+"s",num(s.quote_last_message_age_seconds)<5?"ok":"bad")
  ].join("");
  $("freshInfo").textContent=`Gosterilen: ${shown.length} / ${rows.length}`;
  $("rows").innerHTML=shown.map(r=>`<tr><td><span class="tag">${esc(r.symbol)}</span></td><td>${esc(r.timeframe)}</td><td class="${cls(r.status)}">${esc(r.status)}</td><td>${fmt(r.last_message_age_seconds,3)}s</td><td>${fmt(r.bar_elapsed_seconds,1)}s / ${fmt(r.bar_elapsed_pct,1)}%</td><td>${fmt(r.close,8)}</td><td>${fmt(r.volume,2)}</td><td>${dtms(r.open_time)}</td></tr>`).join("")||`<tr><td colspan="8" class="muted">Kayit yok.</td></tr>`;
  const chunks=s.quote_chunks||{};
  const chunkRows=Object.entries(chunks).map(([id,c])=>`<tr><td>${esc(id)}</td><td class="${c.connected?"ok":"bad"}">${esc(c.connected)}</td><td>${esc(c.symbols_count)}</td><td>${esc(c.streams_count)}</td><td>${fmt(c.last_message_age_seconds,3)}s</td><td>${esc(c.reconnect_count)}</td><td>${esc(c.error_count)}</td></tr>`);
  $("chunkInfo").textContent=`${chunkRows.length} quote chunk`;
  $("chunks").innerHTML=chunkRows.join("")||`<tr><td colspan="7" class="muted">Chunk yok.</td></tr>`;
}
async function load(){
  const [status,freshness]=await Promise.all([
    fetch("/ws-collector/status?_ts="+Date.now(),{cache:"no-store"}).then(r=>r.json()),
    fetch("/ws-collector/freshness?_ts="+Date.now(),{cache:"no-store"}).then(r=>r.json())
  ]);
  LAST={status:status.status||{},freshness};
  $("updated").textContent="Guncel "+new Date().toLocaleTimeString("tr-TR");
  render();
}
["search","tf","only"].forEach(id=>setTimeout(() => $(id).addEventListener("input",()=>LAST&&render()),0));
["tf","only"].forEach(id=>setTimeout(() => $(id).addEventListener("change",()=>LAST&&render()),0));
load(); setInterval(load,5000);
</script>
</body>
</html>"""


class ReadOnlyCollectorAPI:
    """Read-only HTTP API for the WebSocket collector.

    This server does not mutate collector state and does not place orders.
    It exposes the in-memory cache/status to dashboards and signal engines.
    """

    def __init__(
        self,
        *,
        cache: MarketCache,
        status_ref: dict[str, Any],
        stale_after_seconds: int,
        host: str = "127.0.0.1",
        port: int = 8000,
    ):
        self.cache = cache
        self.status_ref = status_ref
        self.stale_after_seconds = stale_after_seconds
        self.host = host
        self.port = int(port)
        self.app = web.Application(middlewares=[cors_middleware])
        self.runner: web.AppRunner | None = None
        self.site: web.TCPSite | None = None
        self._setup_routes()

    def _setup_routes(self) -> None:
        self.app.router.add_get("/", self.handle_root)
        self.app.router.add_get("/dashboard", self.handle_dashboard)
        self.app.router.add_get("/ws-collector/dashboard", self.handle_dashboard)
        self.app.router.add_get("/health", self.handle_health)
        self.app.router.add_get("/ws-collector/health", self.handle_health)
        self.app.router.add_get("/ws-collector/status", self.handle_status)
        self.app.router.add_get("/ws-collector/freshness", self.handle_freshness)
        self.app.router.add_get("/ws-collector/snapshot", self.handle_snapshot)
        self.app.router.add_get("/ws-collector/symbols", self.handle_symbols)
        self.app.router.add_get("/ws-collector/bars", self.handle_bars)
        self.app.router.add_get("/ws-collector/quote", self.handle_quote)
        self.app.router.add_get("/ws-collector/quotes", self.handle_quotes)
        self.app.router.add_get("/market-data/latest", self.handle_latest)
        self.app.router.add_get("/market-data/bars", self.handle_bars)
        self.app.router.add_get("/market-data/quote", self.handle_quote)
        self.app.router.add_get("/market-data/status", self.handle_status)
        self.app.router.add_get("/market-data/freshness", self.handle_freshness)

    @staticmethod
    def _json(payload: dict[str, Any], status: int = 200) -> web.Response:
        return web.json_response(
            payload,
            status=status,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                "Access-Control-Allow-Headers": "*",
            },
        )

    @staticmethod
    def _limit(request: web.Request, default: int = 80, maximum: int = 1000) -> int:
        try:
            value = int(request.query.get("limit", str(default)))
        except Exception:
            value = default
        return max(1, min(maximum, value))

    async def handle_root(self, request: web.Request) -> web.Response:
        return self._json(
            {
                "ok": True,
                "service": "ws_collector_read_only_api",
                "endpoints": [
                    "/dashboard",
                    "/ws-collector/status",
                    "/ws-collector/freshness",
                    "/ws-collector/snapshot?symbol=BTCUSDT&timeframe=5m&limit=100",
                    "/ws-collector/bars?symbol=BTCUSDT&timeframe=5m&limit=100",
                    "/ws-collector/symbols",
                ],
            }
        )

    async def handle_dashboard(self, request: web.Request) -> web.Response:
        return web.Response(
            text=COLLECTOR_DASHBOARD_HTML,
            content_type="text/html",
            headers={
                "Cache-Control": "no-store",
                "Access-Control-Allow-Origin": "*",
            },
        )

    async def handle_health(self, request: web.Request) -> web.Response:
        status = copy.deepcopy(self.status_ref)
        return self._json(
            {
                "ok": True,
                "connected": bool(status.get("connected")),
                "symbols_count": status.get("symbols_count"),
                "streams_count": status.get("streams_count"),
                "last_message_age_seconds": status.get("last_message_age_seconds"),
                "uptime_seconds": status.get("uptime_seconds"),
                "created_at": time.time(),
            }
        )

    async def handle_status(self, request: web.Request) -> web.Response:
        return self._json({"status": copy.deepcopy(self.status_ref), "created_at": time.time()})

    async def handle_freshness(self, request: web.Request) -> web.Response:
        stale_after = request.query.get("stale_after_seconds")
        try:
            stale_after_seconds = int(stale_after) if stale_after else self.stale_after_seconds
        except Exception:
            stale_after_seconds = self.stale_after_seconds
        status = copy.deepcopy(self.status_ref)
        universe = status.get("universe") or {}
        symbols = universe.get("selected_symbols") or []
        return self._json({"freshness": self.cache.freshness_report(stale_after_seconds, active_symbols=symbols or None)})

    async def handle_symbols(self, request: web.Request) -> web.Response:
        status = copy.deepcopy(self.status_ref)
        universe = status.get("universe") or {}
        symbols = universe.get("selected_symbols") or []
        if not symbols:
            symbols = self.cache.symbols()
        return self._json(
            {
                "symbols": symbols,
                "count": len(symbols),
                "universe": universe,
                "created_at": time.time(),
            }
        )

    async def handle_snapshot(self, request: web.Request) -> web.Response:
        symbol = (request.query.get("symbol") or "").upper().replace("/", "").replace("-", "")
        timeframe = request.query.get("timeframe") or request.query.get("tf") or ""
        limit = self._limit(request, default=80, maximum=1000)
        max_symbols = self._limit(request, default=120, maximum=1000) if not symbol else 1

        if symbol and timeframe:
            bars = self.cache.get_bars(symbol, timeframe, limit=limit)
            payload = {
                "created_at": time.time(),
                "symbols_count": 1 if bars else 0,
                "cache_size": self.cache.cache_size(),
                "last_candle_close_by_timeframe": dict(self.cache.last_candle_close_by_timeframe),
                "symbols": {symbol: {timeframe: bars}},
            }
        elif symbol:
            payload = self.cache.snapshot_for_symbol(symbol, bars_per_tf=limit)
        else:
            payload = self.cache.snapshot(max_symbols=max_symbols, bars_per_tf=limit)
        return self._json({"snapshot": payload})

    async def handle_bars(self, request: web.Request) -> web.Response:
        symbol = (request.query.get("symbol") or "").upper().replace("/", "").replace("-", "")
        timeframe = request.query.get("timeframe") or request.query.get("tf") or "5m"
        limit = self._limit(request, default=100, maximum=1000)
        if not symbol:
            return self._json({"error": "symbol query parameter is required"}, status=400)
        bars = self.cache.get_bars(symbol, timeframe, limit=limit)
        return self._json(
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "limit": limit,
                "count": len(bars),
                "bars": bars,
                "created_at": time.time(),
            }
        )

    async def handle_latest(self, request: web.Request) -> web.Response:
        symbol = (request.query.get("symbol") or "").upper().replace("/", "").replace("-", "")
        timeframe = request.query.get("timeframe") or request.query.get("tf") or "5m"
        if not symbol:
            return self._json({"error": "symbol query parameter is required"}, status=400)
        latest = self.cache.latest(symbol, timeframe)
        return self._json(
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "latest": latest,
                "created_at": time.time(),
            }
        )

    async def handle_quote(self, request: web.Request) -> web.Response:
        symbol = (request.query.get("symbol") or "").upper().replace("/", "").replace("-", "")
        if not symbol:
            return self._json({"error": "symbol query parameter is required"}, status=400)
        quote = self.cache.latest_quote(symbol)
        return self._json(
            {
                "symbol": symbol,
                "quote": quote,
                "created_at": time.time(),
            }
        )

    async def handle_quotes(self, request: web.Request) -> web.Response:
        limit = self._limit(request, default=120, maximum=1000)
        return self._json({"quotes": self.cache.quote_snapshot(max_symbols=limit)})

    async def start(self) -> None:
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.host, self.port)
        await self.site.start()
        logging.info("read-only collector api listening on http://%s:%s", self.host, self.port)

    async def stop(self) -> None:
        if self.runner:
            await self.runner.cleanup()
            self.runner = None
