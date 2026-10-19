"""
BTCUSDT collector: Binance -> Neon Postgres (for Grafana).  v3 (self-healing)

Tables / views
  candles     : 1-minute candles, live (last BACKFILL_DAYS days, grows forever)
  candles_1h  : 1-hour candles, full history from HISTORY_START (default 2021-01-01)
  btc_price   : VIEW = hourly candles for old history + 1-minute candles for recent time
                (this is the one Grafana should query)

What it does
  * Creates tables/view once at startup (no race between connections).
  * Back-fills missing candles at start-up and after every reconnect:
      - continues from the last stored candle
      - fills any missing history BEFORE the first stored candle (e.g. after raising BACKFILL_DAYS)
      - repairs holes in the middle if more than 2% of candles are missing
  * Streams the live 1-minute candle and UPSERTS it (no duplicates possible).
  * Keeps the hourly table current in a background task.
  * Retries on Binance rate limits / network errors and reconnects the DB and websocket.
  * Serves a public live chart page at the Render URL (/), JSON at /api/*, and row counts at /status.

Required env var:  DATABASE_URL
Optional env vars: SYMBOL (BTCUSDT), BACKFILL_DAYS (30), HISTORY_START (2021-01-01),
                   WRITE_EVERY_SECONDS (2), BINANCE_REST, BINANCE_WS
"""
import asyncio
import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

import psycopg2
import psycopg2.extras
import requests
import websockets


def parse_start(s):
    s = s.strip().replace("Z", "")
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


DB_URL = os.environ["DATABASE_URL"]
SYMBOL = os.environ.get("SYMBOL", "BTCUSDT").upper()
BACKFILL_DAYS = int(os.environ.get("BACKFILL_DAYS", "30"))
HISTORY_START = parse_start(os.environ.get("HISTORY_START", "2021-01-01"))
WRITE_EVERY = float(os.environ.get("WRITE_EVERY_SECONDS", "2"))
# Market-data-only endpoints; they work in regions where api.binance.com is blocked.
REST_BASE = os.environ.get("BINANCE_REST", "https://data-api.binance.vision")
WS_BASE = os.environ.get("BINANCE_WS", "wss://data-stream.binance.vision")
PORT = os.environ.get("PORT")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("collector")

# interval -> (table, candle length in ms)
INTERVALS = {"1m": ("candles", 60_000), "1h": ("candles_1h", 3_600_000)}

SCHEMA = """
CREATE TABLE IF NOT EXISTS candles (
    symbol TEXT             NOT NULL,
    ts     TIMESTAMPTZ      NOT NULL,
    open   DOUBLE PRECISION,
    high   DOUBLE PRECISION,
    low    DOUBLE PRECISION,
    close  DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    PRIMARY KEY (symbol, ts)
);
CREATE TABLE IF NOT EXISTS candles_1h (
    symbol TEXT             NOT NULL,
    ts     TIMESTAMPTZ      NOT NULL,
    open   DOUBLE PRECISION,
    high   DOUBLE PRECISION,
    low    DOUBLE PRECISION,
    close  DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    PRIMARY KEY (symbol, ts)
);
CREATE OR REPLACE VIEW btc_price AS
    SELECT symbol, ts, close
    FROM candles_1h
    WHERE ts < COALESCE((SELECT MIN(ts) FROM candles), 'infinity'::timestamptz)
    UNION ALL
    SELECT symbol, ts, close FROM candles;
"""

UPSERT = """
INSERT INTO {table} (symbol, ts, open, high, low, close, volume)
VALUES %s
ON CONFLICT (symbol, ts) DO UPDATE SET
    high   = EXCLUDED.high,
    low    = EXCLUDED.low,
    close  = EXCLUDED.close,
    volume = EXCLUDED.volume;
"""

STATUS = {"text": "starting..."}


def to_dt(ms):
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)


def init_schema():
    """Create tables/view once at startup, before any other DB connection exists."""
    last_err = None
    for _ in range(6):
        try:
            conn = psycopg2.connect(DB_URL, connect_timeout=15)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(SCHEMA)
            conn.close()
            log.info("Database tables and view ready.")
            return
        except Exception as e:
            last_err = e
            log.warning("Schema init failed (%s), retrying...", e)
            time.sleep(3)
    raise RuntimeError(f"Could not initialise database: {last_err}")


class DB:
    """Lazy connection that reconnects itself."""

    def __init__(self):
        self.conn = None

    def _connect(self):
        self.conn = psycopg2.connect(
            DB_URL,
            connect_timeout=15,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )
        self.conn.autocommit = True

    def run(self, fn):
        last_err = None
        for attempt in range(4):
            try:
                if self.conn is None or self.conn.closed:
                    self._connect()
                return fn(self.conn)
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                last_err = e
                log.warning("DB error (%s), reconnecting...", e)
                try:
                    if self.conn:
                        self.conn.close()
                except Exception:
                    pass
                self.conn = None
                time.sleep(2 * (attempt + 1))
        raise RuntimeError(f"Database unreachable: {last_err}")

    def upsert(self, rows, table="candles"):
        def fn(conn):
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur, UPSERT.format(table=table), rows, page_size=1000
                )
        self.run(fn)

    def bounds(self, table):
        """Returns (first_ts, last_ts, row_count) for the symbol."""
        def fn(conn):
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT MIN(ts), MAX(ts), COUNT(*) FROM {table} WHERE symbol = %s;",
                    (SYMBOL,),
                )
                return cur.fetchone()
        return self.run(fn)

    def query(self, sql, params=()):
        def fn(conn):
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
        return self.run(fn)


def fetch_klines(interval, start_ms, end_ms=None):
    params = {"symbol": SYMBOL, "interval": interval, "startTime": start_ms, "limit": 1000}
    if end_ms:
        params["endTime"] = end_ms
    for attempt in range(8):
        try:
            r = requests.get(f"{REST_BASE}/api/v3/klines", params=params, timeout=20)
            if r.status_code in (418, 429):
                wait = min(int(r.headers.get("Retry-After", 10)), 120)
                log.warning("Binance rate limit (HTTP %s), waiting %ss", r.status_code, wait)
                time.sleep(wait)
                continue
            if r.status_code in (403, 451):
                raise RuntimeError(
                    f"Binance blocks this server's region (HTTP {r.status_code}). "
                    "Change the Render region (Frankfurt/Singapore) or set BINANCE_REST."
                )
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            log.warning("Binance request failed (%s), retry %d/8", e, attempt + 1)
            time.sleep(min(2 * (attempt + 1), 30))
    raise RuntimeError("Binance requests kept failing")


def backfill_range(db, interval, start, end=None):
    """Download candles from start (to end, if given) and upsert them. Safe to repeat."""
    table, step_ms = INTERVALS[interval]
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000) if end else None
    total = 0
    while True:
        klines = fetch_klines(interval, start_ms, end_ms)
        if not klines:
            break
        rows = [
            (SYMBOL, to_dt(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5]))
            for k in klines
        ]
        db.upsert(rows, table)
        total += len(rows)
        start_ms = klines[-1][0] + step_ms
        if len(klines) < 1000 or (end_ms and start_ms > end_ms):
            break
        time.sleep(0.2)
    return total


def sync(db, interval, history_start, deep=False):
    """Bring the table up to date. deep=True also repairs old history and holes."""
    table, step_ms = INTERVALS[interval]
    first, last, count = db.bounds(table)
    total = 0
    if first is None:
        total += backfill_range(db, interval, history_start)
    else:
        if deep:
            # 1) history missing before the first stored candle
            if first - history_start > timedelta(milliseconds=step_ms * 3):
                log.info("%s: filling history before %s", table, first)
                total += backfill_range(db, interval, history_start, first)
            # 2) holes in the middle
            expected = int((last - first).total_seconds() * 1000 // step_ms) + 1
            if count < expected * 0.98:
                log.info("%s: %d of ~%d candles present, repairing holes", table, count, expected)
                total += backfill_range(db, interval, first, last)
        # 3) everything after the last stored candle (re-fetches the last one, refreshing it)
        total += backfill_range(db, interval, last)
    if total > 1:
        log.info("Sync %s done: %d candles written/refreshed.", interval, total)
    return total


async def hourly_loop():
    """Keeps the hourly table (full history) filled and current. Uses its own DB connection."""
    db = DB()
    deep = True
    while True:
        try:
            await asyncio.to_thread(sync, db, "1h", HISTORY_START, deep)
            deep = False
        except Exception as e:
            log.warning("Hourly update failed: %s", e)
        await asyncio.sleep(60)


async def status_loop():
    """Builds the status text shown on the web page and printed to the logs."""
    db = DB()
    while True:
        try:
            lines = []
            for name, (table, _) in INTERVALS.items():
                first, last, n = await asyncio.to_thread(db.bounds, table)
                lines.append(f"{table}: rows={n} first={first} last={last}")
            STATUS["text"] = f"updated {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC\n" + "\n".join(lines)
            log.info("STATUS | " + " | ".join(lines))
        except Exception as e:
            STATUS["text"] = f"status error: {e}"
        await asyncio.sleep(300)


async def stream(db):
    start_1m = datetime.now(timezone.utc) - timedelta(days=BACKFILL_DAYS)
    await asyncio.to_thread(sync, db, "1m", start_1m, True)
    url = f"{WS_BASE}/ws/{SYMBOL.lower()}@kline_1m"
    log.info("Connecting to %s", url)
    async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
        log.info("Connected. Streaming %s 1m candles to Neon.", SYMBOL)
        last_write = 0.0
        async for raw in ws:
            k = json.loads(raw)["k"]
            now = time.monotonic()
            # Always write a finished candle; otherwise throttle to protect the free DB.
            if k["x"] or now - last_write >= WRITE_EVERY:
                row = (
                    SYMBOL, to_dt(k["t"]),
                    float(k["o"]), float(k["h"]), float(k["l"]), float(k["c"]), float(k["v"]),
                )
                await asyncio.to_thread(db.upsert, [row], "candles")
                last_write = now


# ----------------------------------------------------------------------------
# Public web page + JSON API (served from the same Render URL)
# ----------------------------------------------------------------------------
WEB_DB = DB()
WEB_LOCK = threading.Lock()
CACHE = {}

# range -> (table, look-back, bucket size or None)
RANGES = {
    "1h": ("candles", timedelta(hours=1), None),
    "1d": ("candles", timedelta(days=1), None),
    "7d": ("candles", timedelta(days=7), "5 minutes"),
    "30d": ("candles_1h", timedelta(days=30), None),
    "1y": ("candles_1h", timedelta(days=365), None),
    "all": ("candles_1h", None, "4 hours"),
}
CACHE_TTL = {"1h": 10, "1d": 20, "7d": 60, "30d": 300, "1y": 600, "all": 900}


def cached(key, ttl, producer):
    """Tiny in-memory cache so many viewers never hammer the free database."""
    now = time.time()
    hit = CACHE.get(key)
    if hit and hit[0] > now:
        return hit[1]
    with WEB_LOCK:
        hit = CACHE.get(key)
        if hit and hit[0] > time.time():
            return hit[1]
        body = producer()
        CACHE[key] = (time.time() + ttl, body)
        return body


def api_prices(rng):
    def produce():
        table, lookback, bucket = RANGES[rng]
        where, params = "symbol = %s", [SYMBOL]
        if lookback:
            where += " AND ts >= %s"
            params.append(datetime.now(timezone.utc) - lookback)
        if bucket:
            sql = (
                f"SELECT EXTRACT(EPOCH FROM date_bin(INTERVAL '{bucket}', ts, "
                f"TIMESTAMPTZ '2000-01-01'))::bigint AS t, "
                f"(array_agg(close ORDER BY ts DESC))[1] "
                f"FROM {table} WHERE {where} GROUP BY t ORDER BY t"
            )
        else:
            sql = f"SELECT EXTRACT(EPOCH FROM ts)::bigint, close FROM {table} WHERE {where} ORDER BY ts"
        rows = WEB_DB.query(sql, params)
        return json.dumps([[int(t), float(c)] for t, c in rows]).encode()
    return cached("prices:" + rng, CACHE_TTL[rng], produce)


def api_latest():
    def produce():
        row = WEB_DB.query(
            "SELECT ts, close FROM candles WHERE symbol = %s ORDER BY ts DESC LIMIT 1;", (SYMBOL,)
        )
        if not row:
            return json.dumps({"ts": None, "price": None, "change24h": None}).encode()
        ts, price = row[0]
        old = WEB_DB.query(
            "SELECT close FROM candles WHERE symbol = %s AND ts <= %s ORDER BY ts DESC LIMIT 1;",
            (SYMBOL, datetime.now(timezone.utc) - timedelta(hours=24)),
        )
        change = (price / old[0][0] - 1) * 100 if old and old[0][0] else None
        return json.dumps(
            {"ts": int(ts.timestamp()), "price": float(price), "change24h": change}
        ).encode()
    return cached("latest", 3, produce)


PAGE = """<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__SYMBOL__ Live Chart</title>
<style>
  *{box-sizing:border-box}
  html,body{margin:0;height:100%;background:#0b0e11;color:#e6e9ee;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
  body{display:flex;flex-direction:column}
  header{display:flex;flex-wrap:wrap;gap:12px;align-items:center;justify-content:space-between;padding:14px 18px}
  .sym{font-size:14px;color:#8b94a3;letter-spacing:.04em}
  #price{font-size:30px;font-weight:700;line-height:1.1}
  #chg{font-size:14px;margin-left:8px}
  .up{color:#2ebd85}.down{color:#f6465d}
  #btns button{background:#1a1f27;color:#c7cdd6;border:1px solid #2a3039;border-radius:6px;padding:7px 12px;margin-right:6px;font-size:13px;cursor:pointer}
  #btns button.on{background:#f0b90b;color:#111;border-color:#f0b90b;font-weight:600}
  #chart{flex:1;min-height:300px;margin:0 8px}
  footer{padding:8px 18px 14px;font-size:12px;color:#6b7482}
</style></head>
<body>
<header>
  <div><div class="sym">__SYMBOL__ &middot; LIVE</div><div><span id="price">--</span><span id="chg"></span></div></div>
  <div id="btns">
    <button data-r="1h">1H</button><button data-r="1d" class="on">1D</button><button data-r="7d">7D</button>
    <button data-r="30d">30D</button><button data-r="1y">1Y</button><button data-r="all">ALL</button>
  </div>
</header>
<div id="chart"></div>
<footer id="foot">Loading...</footer>
<script src="https://cdn.jsdelivr.net/npm/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
<script>
const off = -new Date().getTimezoneOffset() * 60;   // show times in the viewer's local time
const box = document.getElementById('chart');
const chart = LightweightCharts.createChart(box, {
  width: box.clientWidth, height: box.clientHeight,
  layout: {background: {color: '#0b0e11'}, textColor: '#c7cdd6'},
  grid: {vertLines: {color: '#161b22'}, horzLines: {color: '#161b22'}},
  timeScale: {timeVisible: true, secondsVisible: false, borderColor: '#2a3039'},
  rightPriceScale: {borderColor: '#2a3039'}
});
const series = chart.addLineSeries({color: '#f0b90b', lineWidth: 2});
new ResizeObserver(() => chart.applyOptions({width: box.clientWidth, height: box.clientHeight})).observe(box);

const LIVE = ['1h', '1d', '7d'];
let range = '1d', lastTime = 0;

async function load(r) {
  range = r;
  document.querySelectorAll('#btns button').forEach(b => b.classList.toggle('on', b.dataset.r === r));
  try {
    const res = await fetch('/api/prices?range=' + r);
    const d = await res.json();
    const pts = d.map(p => ({time: p[0] + off, value: p[1]}));
    series.setData(pts);
    lastTime = pts.length ? pts[pts.length - 1].time : 0;
    chart.timeScale().fitContent();
    document.getElementById('foot').textContent = pts.length + ' points \u00b7 data: Binance \u00b7 updates automatically';
  } catch (e) { document.getElementById('foot').textContent = 'Could not load data, retrying...'; }
}

async function tick() {
  try {
    const d = await (await fetch('/api/latest')).json();
    if (d.price == null) return;
    document.getElementById('price').textContent = '$' + d.price.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
    const c = document.getElementById('chg');
    if (d.change24h != null) {
      c.textContent = (d.change24h >= 0 ? '+' : '') + d.change24h.toFixed(2) + '% (24h)';
      c.className = d.change24h >= 0 ? 'up' : 'down';
    }
    const t = d.ts + off;
    if (LIVE.includes(range) && t >= lastTime) { series.update({time: t, value: d.price}); lastTime = t; }
  } catch (e) {}
}

document.querySelectorAll('#btns button').forEach(b => b.onclick = () => load(b.dataset.r));
load(range); tick();
setInterval(tick, 5000);
setInterval(() => { if (!LIVE.includes(range)) load(range); }, 300000);
</script>
</body></html>
""".replace("__SYMBOL__", SYMBOL)


async def http_handler(reader, writer):
    try:
        data = await asyncio.wait_for(reader.read(4096), timeout=10)
        parts = data.split(b"\r\n", 1)[0].decode("latin1").split(" ")
        method = parts[0] if parts else "GET"
        target = parts[1] if len(parts) > 1 else "/"
        url = urlparse(target)
        q = parse_qs(url.query)
        status, ctype, body = "200 OK", "text/plain; charset=utf-8", b""
        cache_ctl = "no-store"
        try:
            if url.path in ("/", "/index.html"):
                ctype, body, cache_ctl = "text/html; charset=utf-8", PAGE.encode(), "public, max-age=60"
            elif url.path == "/api/prices":
                rng = q.get("range", ["1d"])[0]
                if rng not in RANGES:
                    status, body = "400 Bad Request", b"unknown range"
                else:
                    ctype = "application/json"
                    body = await asyncio.to_thread(api_prices, rng)
            elif url.path == "/api/latest":
                ctype = "application/json"
                body = await asyncio.to_thread(api_latest)
            elif url.path in ("/status", "/healthz"):
                body = (STATUS["text"] + "\nok").encode()
            else:
                status, body = "404 Not Found", b"not found"
        except Exception as e:
            log.warning("Web request failed: %s", e)
            status, ctype, body = "503 Service Unavailable", "text/plain; charset=utf-8", b"temporarily unavailable"
        head = (
            f"HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {len(body)}\r\n"
            f"Cache-Control: {cache_ctl}\r\nConnection: close\r\n\r\n"
        ).encode()
        writer.write(head if method == "HEAD" else head + body)
        await writer.drain()
    except Exception:
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def main():
    if PORT:
        await asyncio.start_server(http_handler, "0.0.0.0", int(PORT))
        log.info("Web page + API listening on port %s", PORT)
    await asyncio.to_thread(init_schema)
    tasks = [asyncio.create_task(hourly_loop()), asyncio.create_task(status_loop())]  # keep refs
    db = DB()
    delay = 1
    while True:
        try:
            await stream(db)
            delay = 1
            await asyncio.sleep(1)  # Binance closes streams every 24h; just reconnect
        except Exception as e:
            log.error("Stream error: %s (retrying in %ss)", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)


if __name__ == "__main__":
    asyncio.run(main())
