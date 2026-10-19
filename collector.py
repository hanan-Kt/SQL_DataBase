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
  * Opens the Render URL in a browser and you see a live status page with row counts.

Required env var:  DATABASE_URL
Optional env vars: SYMBOL (BTCUSDT), BACKFILL_DAYS (30), HISTORY_START (2021-01-01),
                   WRITE_EVERY_SECONDS (2), BINANCE_REST, BINANCE_WS
"""
import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

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


async def health(reader, writer):
    try:
        await reader.read(1024)
        body = (STATUS["text"] + "\nok").encode()
        writer.write(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: "
            + str(len(body)).encode() + b"\r\n\r\n" + body
        )
        await writer.drain()
    finally:
        writer.close()


async def main():
    if PORT:
        await asyncio.start_server(health, "0.0.0.0", int(PORT))
        log.info("Health/status server listening on port %s", PORT)
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
