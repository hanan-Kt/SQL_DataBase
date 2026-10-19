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
WEB_LOCK = threading.RLock()  # re-entrant: the analysis endpoint uses nested cached() calls
CACHE = {}

# "Auto" mode: range -> (table, look-back, bucket size or None for raw candles)
RANGES = {
    "1h": ("candles", timedelta(hours=1), None),
    "1d": ("candles", timedelta(days=1), None),
    "7d": ("candles", timedelta(days=7), "5 minutes"),
    "30d": ("candles_1h", timedelta(days=30), None),
    "1y": ("candles_1h", timedelta(days=365), None),
    "all": ("candles_1h", None, "4 hours"),
}
CACHE_TTL = {"1h": 10, "1d": 20, "7d": 60, "30d": 300, "1y": 600, "all": 900}

# Manual candle sizes. They come from the 1-minute table, so they only exist for shorter ranges
# and are limited to a sane number of points (MIN_POINTS..MAX_POINTS) per chart.
INTERVAL_SECONDS = {"1m": 60, "5m": 300, "30m": 1800}
INTERVAL_BUCKET = {"1m": None, "5m": "5 minutes", "30m": "30 minutes"}
RANGE_SECONDS = {"1h": 3600, "1d": 86400, "7d": 7 * 86400, "30d": 30 * 86400}
MIN_POINTS, MAX_POINTS = 10, 12000


def combo_ok(rng, interval):
    if rng not in RANGES:
        return False
    if interval == "auto":
        return True
    if interval not in INTERVAL_SECONDS or rng not in RANGE_SECONDS:
        return False
    points = RANGE_SECONDS[rng] / INTERVAL_SECONDS[interval]
    return MIN_POINTS <= points <= MAX_POINTS


def cached(key, ttl, producer):
    """Tiny in-memory cache so many viewers never hammer the free database."""
    hit = CACHE.get(key)
    if hit and hit[0] > time.time():
        return hit[1]
    with WEB_LOCK:
        hit = CACHE.get(key)
        if hit and hit[0] > time.time():
            return hit[1]
        value = producer()
        CACHE[key] = (time.time() + ttl, value)
        if len(CACHE) > 300:  # prune expired entries
            now = time.time()
            for k in [k for k, v in CACHE.items() if v[0] <= now]:
                CACHE.pop(k, None)
        return value


def api_prices(rng, interval="auto"):
    def produce():
        if interval == "auto":
            table, lookback, bucket = RANGES[rng]
        else:
            table = "candles"
            lookback = timedelta(seconds=RANGE_SECONDS[rng])
            bucket = INTERVAL_BUCKET[interval]
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
    return cached(f"prices:{rng}:{interval}", CACHE_TTL[rng], produce)


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


# ---------------------------- analysis panel --------------------------------
def price_at(target):
    """Price at a past moment: 1-minute table first, hourly table for older moments."""
    for table in ("candles", "candles_1h"):
        rows = WEB_DB.query(
            f"SELECT close FROM {table} WHERE symbol = %s AND ts <= %s ORDER BY ts DESC LIMIT 1;",
            (SYMBOL, target),
        )
        if rows:
            return float(rows[0][0])
    return None


def rsi_wilder(closes, period=14):
    """Classic Wilder RSI. closes must be ordered oldest -> newest."""
    if len(closes) < period + 1:
        return None
    gains = losses = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        gains += max(d, 0.0)
        losses += max(-d, 0.0)
    avg_g, avg_l = gains / period, losses / period
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        avg_g = (avg_g * (period - 1) + max(d, 0.0)) / period
        avg_l = (avg_l * (period - 1) + max(-d, 0.0)) / period
    if avg_l == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + avg_g / avg_l)


def slow_stats(today_utc):
    """Numbers that barely move (history extremes, daily closes, old prices). Cached 60 s."""
    def produce():
        now = datetime.now(timezone.utc)
        hi = WEB_DB.query(
            "SELECT ts, high FROM candles_1h WHERE symbol = %s ORDER BY high DESC LIMIT 1;", (SYMBOL,)
        )
        lo = WEB_DB.query(
            "SELECT ts, low FROM candles_1h WHERE symbol = %s ORDER BY low ASC LIMIT 1;", (SYMBOL,)
        )
        daily = WEB_DB.query(
            "SELECT (ts AT TIME ZONE 'UTC')::date AS d, (array_agg(close ORDER BY ts DESC))[1] "
            "FROM candles_1h WHERE symbol = %s AND ts >= %s GROUP BY d ORDER BY d DESC LIMIT 262;",
            (SYMBOL, now - timedelta(days=264)),
        )
        return {
            "high": {"price": float(hi[0][1]), "ts": int(hi[0][0].timestamp())} if hi else None,
            "low": {"price": float(lo[0][1]), "ts": int(lo[0][0].timestamp())} if lo else None,
            # daily closes (UTC days), newest first, EXCLUDING today (today uses the live price)
            "prev": [float(c) for d, c in daily if d != today_utc],
            "p7": price_at(now - timedelta(days=7)),
            "p30": price_at(now - timedelta(days=30)),
            "p365": price_at(now - timedelta(days=365)),
        }
    return cached(f"slow:{today_utc}", 60, produce)


def pct_change(now_value, then_value):
    return (now_value / then_value - 1) * 100 if then_value else None


def extreme(rec, live_values, is_high):
    """History extreme from the hourly table, updated with today's live values."""
    if rec is None:
        return None
    best, ts = rec["price"], rec["ts"]
    for v in live_values:
        if v is not None and ((v > best) if is_high else (v < best)):
            best, ts = v, None  # a new extreme set today
    return {"price": best, "ts": ts, "today": ts is None}


def api_analysis(since_raw):
    now = datetime.now(timezone.utc)
    today_utc = now.date()
    # "Today" = from the viewer's local midnight. Only accept quarter-hour aligned times from the
    # last 27 hours (every real time zone qualifies); otherwise fall back to UTC midnight.
    since = None
    try:
        s = int(since_raw)
        cand = datetime.fromtimestamp(s, tz=timezone.utc)
        if s % 900 == 0 and cand <= now and now - cand <= timedelta(hours=27):
            since = cand
    except (TypeError, ValueError, OverflowError, OSError):
        pass
    if since is None:
        since = now.replace(hour=0, minute=0, second=0, microsecond=0)
    since_ts = int(since.timestamp())

    def produce():
        row = WEB_DB.query(
            "SELECT ts, close FROM candles WHERE symbol = %s ORDER BY ts DESC LIMIT 1;", (SYMBOL,)
        )
        if not row:
            return json.dumps({"price": None}).encode()
        ts, price = row[0]
        price = float(price)

        # --- today only: candles since `since` ---
        t = WEB_DB.query(
            "SELECT MAX(high), MIN(low), SUM(volume), COUNT(*), (array_agg(open ORDER BY ts ASC))[1] "
            "FROM candles WHERE symbol = %s AND ts >= %s;",
            (SYMBOL, since),
        )
        hi_t, lo_t, vol_t, n_t, open_t = t[0]
        today = None
        if n_t and open_t:
            o = float(open_t)
            h = max(float(hi_t), price)
            lw = min(float(lo_t), price)
            today = {
                "open": o, "high": h, "low": lw,
                "change": price - o, "pct": pct_change(price, o),
                "volume": float(vol_t or 0), "range_pct": (h - lw) / o * 100,
            }

        s = slow_stats(today_utc)

        # --- highest / lowest price in the stored history ---
        high = extreme(s["high"], [price, today["high"] if today else None], True)
        low = extreme(s["low"], [price, today["low"] if today else None], False)
        if high:
            high["from_pct"] = pct_change(price, high["price"])
        if low:
            low["from_pct"] = pct_change(price, low["price"])

        # --- moving averages and RSI on daily closes (today = live price) ---
        closes_new_first = [price] + s["prev"]
        ma = {}
        for n in (7, 50, 200):
            if len(closes_new_first) >= n:
                v = sum(closes_new_first[:n]) / n
                ma[str(n)] = {"value": v, "diff_pct": pct_change(price, v)}
            else:
                ma[str(n)] = None
        rsi = rsi_wilder(list(reversed(closes_new_first[:250])))

        return json.dumps({
            "price": price, "ts": int(ts.timestamp()), "since": since_ts, "today": today,
            "periods": {
                "7d": pct_change(price, s["p7"]),
                "30d": pct_change(price, s["p30"]),
                "1y": pct_change(price, s["p365"]),
            },
            "high": high, "low": low, "ma": ma, "rsi": rsi,
            "history_from": HISTORY_START.strftime("%Y-%m-%d"),
        }).encode()
    return cached(f"analysis:{since_ts}", 3, produce)


PAGE = """<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__SYMBOL__ Live Chart</title>
<style>
  *{box-sizing:border-box}
  html,body{margin:0;height:100%;background:#0b0e11;color:#e6e9ee;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
  body{display:flex;flex-direction:column}
  header{display:flex;flex-wrap:wrap;gap:10px 18px;align-items:flex-end;justify-content:space-between;padding:14px 18px 8px}
  .sym{font-size:14px;color:#8b94a3;letter-spacing:.04em}
  #price{font-size:30px;font-weight:700;line-height:1.1}
  #chg{font-size:14px;margin-left:8px}
  .up{color:#2ebd85}.down{color:#f6465d}
  #ctrls{display:flex;flex-wrap:wrap;gap:8px 16px;align-items:center}
  .grp{display:flex;align-items:center;gap:6px;flex-wrap:wrap}
  .lbl{font-size:11px;color:#6b7482;text-transform:uppercase;letter-spacing:.06em;margin-right:2px}
  .grp button{background:#1a1f27;color:#c7cdd6;border:1px solid #2a3039;border-radius:6px;padding:6px 11px;font-size:13px;cursor:pointer}
  .grp button.on{background:#f0b90b;color:#111;border-color:#f0b90b;font-weight:600}
  .grp button.dim{opacity:.4}
  #abtn{border-color:#f0b90b;color:#f0b90b}
  #abtn.on{background:#f0b90b;color:#111}
  #wrap{position:relative;flex:1;min-height:320px;margin:0 8px}
  #chart{position:absolute;top:0;left:0;right:0;bottom:0}
  #panel{position:absolute;top:10px;left:10px;z-index:5;width:300px;max-width:calc(100% - 20px);max-height:calc(100% - 20px);overflow:auto;
    background:rgba(15,19,25,.95);border:1px solid #2a3039;border-radius:10px;padding:10px 14px 12px;font-size:13px;box-shadow:0 6px 24px rgba(0,0,0,.5)}
  #panel[hidden]{display:none}
  .ph{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:4px}
  .ph b{font-size:14px}
  #pupd{font-size:11px;color:#6b7482;flex:1;text-align:right}
  #pclose{background:none;border:none;color:#8b94a3;font-size:18px;cursor:pointer;line-height:1;padding:0 2px}
  .sec{margin-top:10px;padding-top:8px;border-top:1px solid #222831}
  .sec h4{margin:0 0 5px;font-size:11px;color:#8b94a3;text-transform:uppercase;letter-spacing:.06em;font-weight:600}
  .big{font-size:20px;font-weight:700;margin-bottom:3px}
  .r{display:flex;justify-content:space-between;gap:10px;padding:2px 0}
  .r span:first-child{color:#9aa3b2}
  .r span:last-child{text-align:right}
  .note{margin-top:10px;font-size:11px;color:#6b7482;line-height:1.35}
  footer{padding:8px 18px 14px;font-size:12px;color:#6b7482}
</style></head>
<body>
<header>
  <div><div class="sym">__SYMBOL__ &middot; LIVE</div><div><span id="price">--</span><span id="chg"></span></div></div>
  <div id="ctrls">
    <div class="grp" id="rng"><span class="lbl">Range</span>
      <button data-r="1h">1H</button><button data-r="1d">1D</button><button data-r="7d">7D</button>
      <button data-r="30d">30D</button><button data-r="1y">1Y</button><button data-r="all">ALL</button>
    </div>
    <div class="grp" id="ivl"><span class="lbl">Interval</span>
      <button data-i="auto">Auto</button><button data-i="1m">1m</button><button data-i="5m">5m</button><button data-i="30m">30m</button>
    </div>
    <div class="grp"><button id="abtn">Analysis</button></div>
  </div>
</header>
<div id="wrap">
  <div id="chart"></div>
  <div id="panel" hidden>
    <div class="ph"><b>Analysis</b><span id="pupd"></span><button id="pclose" title="Close">&times;</button></div>
    <div id="pbody">Loading...</div>
  </div>
</div>
<footer id="foot">Loading...</footer>
<script src="https://cdn.jsdelivr.net/npm/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
<script>
const off = -new Date().getTimezoneOffset() * 60;   // show times in the viewer's local time
const box = document.getElementById('chart');
const foot = document.getElementById('foot');
const chart = LightweightCharts.createChart(box, {
  width: box.clientWidth, height: box.clientHeight,
  layout: {background: {color: '#0b0e11'}, textColor: '#c7cdd6'},
  grid: {vertLines: {color: '#161b22'}, horzLines: {color: '#161b22'}},
  timeScale: {timeVisible: true, secondsVisible: false, borderColor: '#2a3039'},
  rightPriceScale: {borderColor: '#2a3039'}
});
const series = chart.addLineSeries({color: '#f0b90b', lineWidth: 2});
new ResizeObserver(() => chart.applyOptions({width: box.clientWidth, height: box.clientHeight})).observe(box);

// ---- range / interval rules (must match the server) ----
const RANGE_ORDER = ['1h', '1d', '7d', '30d', '1y', 'all'];
const RANGE_SEC = {'1h': 3600, '1d': 86400, '7d': 604800, '30d': 2592000};
const IVL_SEC = {'1m': 60, '5m': 300, '30m': 1800};
const AUTO_SEC = {'1h': 60, '1d': 60, '7d': 300, '30d': 3600, '1y': 3600, 'all': 14400};
const MIN_PTS = 10, MAX_PTS = 12000;
function comboOk(r, i) {
  if (i === 'auto') return true;
  if (!(r in RANGE_SEC)) return false;
  const n = RANGE_SEC[r] / IVL_SEC[i];
  return n >= MIN_PTS && n <= MAX_PTS;
}
function effSec(r, i) { return i === 'auto' ? AUTO_SEC[r] : IVL_SEC[i]; }
function candleLabel(sec) { return sec < 3600 ? (sec / 60) + '-minute' : (sec / 3600) + '-hour'; }

let range = '1d', interval = 'auto', notice = '', loadId = 0, lastTime = 0;

function refreshUI() {
  document.querySelectorAll('#rng button').forEach(b => {
    b.classList.toggle('on', b.dataset.r === range);
    b.classList.toggle('dim', !comboOk(b.dataset.r, interval));
  });
  document.querySelectorAll('#ivl button').forEach(b => {
    b.classList.toggle('on', b.dataset.i === interval);
    b.classList.toggle('dim', !comboOk(range, b.dataset.i));
  });
}

function pickRange(r) {
  notice = '';
  if (!comboOk(r, interval)) {
    notice = interval + ' candles are not available for ' + r.toUpperCase() + ', switched to Auto';
    interval = 'auto';
  }
  range = r; refreshUI(); load(false);
}

function pickInterval(i) {
  notice = '';
  if (!comboOk(range, i)) {
    const valid = RANGE_ORDER.filter(r => comboOk(r, i));
    const cur = RANGE_ORDER.indexOf(range);
    let best = valid[0];
    valid.forEach(r => { if (Math.abs(RANGE_ORDER.indexOf(r) - cur) < Math.abs(RANGE_ORDER.indexOf(best) - cur)) best = r; });
    notice = i + ' candles are not available for ' + range.toUpperCase() + ', range changed to ' + best.toUpperCase();
    range = best;
  }
  interval = i; refreshUI(); load(false);
}

async function load(silent) {
  const my = ++loadId, r = range, i = interval;
  if (!silent) foot.textContent = 'Loading...';
  try {
    const res = await fetch('/api/prices?range=' + r + '&interval=' + i);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const d = await res.json();
    if (my !== loadId) return;
    const keep = silent ? chart.timeScale().getVisibleLogicalRange() : null;
    const pts = d.map(p => ({time: p[0] + off, value: p[1]}));
    series.setData(pts);
    lastTime = pts.length ? pts[pts.length - 1].time : 0;
    if (keep) chart.timeScale().setVisibleLogicalRange(keep); else chart.timeScale().fitContent();
    foot.textContent = (notice ? notice + ' \u00b7 ' : '') + pts.length + ' points \u00b7 ' +
      candleLabel(effSec(r, i)) + ' candles \u00b7 data: Binance \u00b7 updates every 5 seconds';
  } catch (e) {
    if (my === loadId) { foot.textContent = 'Could not load data, retrying...'; setTimeout(() => load(silent), 5000); }
  }
}

// ---- live ticker (header price + last candle) ----
async function tick() {
  try {
    const d = await (await fetch('/api/latest')).json();
    if (d.price != null) {
      document.getElementById('price').textContent = '$' + d.price.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
      const c = document.getElementById('chg');
      if (d.change24h != null) {
        c.textContent = (d.change24h >= 0 ? '+' : '') + d.change24h.toFixed(2) + '% (24h)';
        c.className = d.change24h >= 0 ? 'up' : 'down';
      }
      const sec = effSec(range, interval);
      const b = Math.floor(d.ts / sec) * sec + off;   // same bucket alignment as the server
      if (lastTime && b >= lastTime) { series.update({time: b, value: d.price}); lastTime = b; }
    }
  } catch (e) {}
  if (panelOpen) loadAnalysis();
}

// ---- analysis panel ----
const panel = document.getElementById('panel'), pbody = document.getElementById('pbody'), pupd = document.getElementById('pupd');
const abtn = document.getElementById('abtn');
let panelOpen = false, aId = 0;
const usd = v => v == null ? '--' : '$' + v.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
const pc = v => v == null ? '--' : (v >= 0 ? '+' : '') + v.toFixed(2) + '%';
const cl = v => v == null ? '' : (v >= 0 ? 'up' : 'down');
const dstr = ts => new Date(ts * 1000).toLocaleDateString(undefined, {year: 'numeric', month: 'short', day: 'numeric'});
const row = (a, b, c) => '<div class="r"><span>' + a + '</span><span class="' + (c || '') + '">' + b + '</span></div>';

function renderAnalysis(a) {
  if (a.price == null) { pbody.textContent = 'No data yet.'; return; }
  let h = '';
  // Today (only candles since the start of today)
  const since = new Date(a.since * 1000).toLocaleTimeString([], {hour: 'numeric', minute: '2-digit'});
  h += '<div class="sec" style="border-top:none;margin-top:4px;padding-top:0"><h4>Today (since ' + since + ')</h4>';
  if (a.today) {
    const t = a.today;
    h += '<div class="big ' + cl(t.pct) + '">' + (t.pct >= 0 ? '\u25B2 ' : '\u25BC ') + pc(t.pct) +
         ' <span style="font-size:13px;font-weight:500">(' + (t.change >= 0 ? '+' : '-') + usd(Math.abs(t.change)) + ')</span></div>';
    h += row('Open', usd(t.open)) + row('High', usd(t.high)) + row('Low', usd(t.low)) +
         row('Day range', t.range_pct.toFixed(2) + '%') + row('Volume', t.volume.toLocaleString(undefined, {maximumFractionDigits: 0}) + ' BTC');
  } else { h += '<div>No candles yet today.</div>'; }
  h += '</div>';
  // Longer trend
  h += '<div class="sec"><h4>Change over time</h4>' + row('7 days', pc(a.periods['7d']), cl(a.periods['7d'])) +
       row('30 days', pc(a.periods['30d']), cl(a.periods['30d'])) + row('1 year', pc(a.periods['1y']), cl(a.periods['1y'])) + '</div>';
  // Highest / lowest
  h += '<div class="sec"><h4>Since ' + dstr(Date.parse(a.history_from) / 1000) + '</h4>';
  if (a.high) h += row('Highest', usd(a.high.price)) + row('&nbsp;', (a.high.today ? 'set today' : dstr(a.high.ts)) + ' \u00b7 now ' + pc(a.high.from_pct), cl(a.high.from_pct));
  if (a.low) h += row('Lowest', usd(a.low.price)) + row('&nbsp;', (a.low.today ? 'set today' : dstr(a.low.ts)) + ' \u00b7 now ' + pc(a.low.from_pct), 'up');
  h += '</div>';
  // Moving averages + RSI
  h += '<div class="sec"><h4>Moving averages (daily closes)</h4>';
  [['7', '7-day'], ['50', '50-day'], ['200', '200-day']].forEach(m => {
    const v = a.ma[m[0]];
    h += v ? row(m[1] + ' MA', usd(v.value) + ' <small>' + (v.diff_pct >= 0 ? '\u25B2 ' : '\u25BC ') + Math.abs(v.diff_pct).toFixed(2) + '% ' + (v.diff_pct >= 0 ? 'above' : 'below') + '</small>', cl(v.diff_pct))
           : row(m[1] + ' MA', 'not enough data');
  });
  if (a.rsi != null) {
    const lab = a.rsi >= 70 ? 'overbought' : (a.rsi <= 30 ? 'oversold' : 'neutral');
    h += row('RSI (14, daily)', a.rsi.toFixed(1) + ' <small>' + lab + '</small>', a.rsi >= 70 ? 'down' : (a.rsi <= 30 ? 'up' : ''));
  }
  h += '</div>';
  h += '<div class="note">Today = since your local midnight. Moving averages and RSI use daily closes (UTC days) with the live price as today\\u2019s close. Not financial advice.</div>';
  pbody.innerHTML = h;
  pupd.textContent = 'Updated ' + new Date().toLocaleTimeString();
}

async function loadAnalysis() {
  const my = ++aId;
  const d0 = new Date(); d0.setHours(0, 0, 0, 0);       // viewer's local midnight
  try {
    const res = await fetch('/api/analysis?since=' + Math.floor(d0.getTime() / 1000));
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const a = await res.json();
    if (my !== aId || !panelOpen) return;
    renderAnalysis(a);
  } catch (e) { if (panelOpen) pupd.textContent = 'update failed, retrying...'; }
}

function togglePanel(open) {
  panelOpen = open; panel.hidden = !open; abtn.classList.toggle('on', open);
  if (open) { pbody.textContent = 'Loading...'; pupd.textContent = ''; loadAnalysis(); }
}
abtn.onclick = () => togglePanel(!panelOpen);
document.getElementById('pclose').onclick = () => togglePanel(false);

document.querySelectorAll('#rng button').forEach(b => b.onclick = () => pickRange(b.dataset.r));
document.querySelectorAll('#ivl button').forEach(b => b.onclick = () => pickInterval(b.dataset.i));
refreshUI(); load(false); tick();
setInterval(tick, 5000);
setInterval(() => load(true), 900000);   // quiet refresh every 15 min, keeps your zoom
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
                interval = q.get("interval", ["auto"])[0]
                if not combo_ok(rng, interval):
                    status, body = "400 Bad Request", b"unsupported range/interval"
                else:
                    ctype = "application/json"
                    body = await asyncio.to_thread(api_prices, rng, interval)
            elif url.path == "/api/latest":
                ctype = "application/json"
                body = await asyncio.to_thread(api_latest)
            elif url.path == "/api/analysis":
                ctype = "application/json"
                body = await asyncio.to_thread(api_analysis, q.get("since", [None])[0])
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
