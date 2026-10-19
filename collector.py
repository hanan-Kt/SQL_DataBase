"""
BTCUSDT collector: Binance -> Neon Postgres (for Grafana).

Two tables:
  candles     : 1-minute candles, live (recent history, BACKFILL_DAYS)
  candles_1h  : 1-hour candles, full history from HISTORY_START (default 2021-01-01)

- On start (and after every reconnect) it back-fills any missing candles.
- It streams the live 1-minute candle and UPSERTS it (no duplicates).
- A background task keeps the hourly table up to date.
- Reconnects automatically if Binance or the database drops the connection.
- If a PORT env var exists (Render web service), it serves a tiny "ok" page for UptimeRobot.

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

DB_URL = os.environ["DATABASE_URL"]
SYMBOL = os.environ.get("SYMBOL", "BTCUSDT").upper()
BACKFILL_DAYS = int(os.environ.get("BACKFILL_DAYS", "30"))
HISTORY_START = datetime.fromisoformat(os.environ.get("HISTORY_START", "2021-01-01")).replace(
    tzinfo=timezone.utc
)
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


def to_dt(ms):
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)


def init_schema():
    """Create the tables once at startup, before any other DB connection exists."""
    last_err = None
    for _ in range(5):
        try:
            conn = psycopg2.connect(DB_URL, connect_timeout=15)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(SCHEMA)
            conn.close()
            log.info("Database tables ready.")
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

    def last_ts(self, table="candles"):
        def fn(conn):
            with conn.cursor() as cur:
                cur.execute(f"SELECT MAX(ts) FROM {table} WHERE symbol = %s;", (SYMBOL,))
                return cur.fetchone()[0]
        return self.run(fn)


def backfill(db, interval, default_start):
    """Fill every missing candle from the last stored one (or default_start) until now."""
    table, step_ms = INTERVALS[interval]
    last = db.last_ts(table)
    start = last if last else default_start
    start_ms = int(start.timestamp() * 1000)
    total = 0
    while True:
        r = requests.get(
            f"{REST_BASE}/api/v3/klines",
            params={"symbol": SYMBOL, "interval": interval, "startTime": start_ms, "limit": 1000},
            timeout=20,
        )
        r.raise_for_status()
        klines = r.json()
        if not klines:
            break
        rows = [
            (SYMBOL, to_dt(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5]))
            for k in klines
        ]
        db.upsert(rows, table)
        total += len(rows)
        start_ms = klines[-1][0] + step_ms
        if len(klines) < 1000:
            break
        time.sleep(0.2)
    if total > 1:
        log.info("Backfill %s done: %d candles written/refreshed.", interval, total)


async def hourly_loop():
    """Keeps the hourly table (full history) filled and current. Uses its own DB connection."""
    db = DB()
    while True:
        try:
            await asyncio.to_thread(backfill, db, "1h", HISTORY_START)
        except Exception as e:
            log.warning("Hourly update failed: %s", e)
        await asyncio.sleep(60)


async def stream(db):
    default_1m = datetime.now(timezone.utc) - timedelta(days=BACKFILL_DAYS)
    await asyncio.to_thread(backfill, db, "1m", default_1m)
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
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\nok")
        await writer.drain()
    finally:
        writer.close()


async def main():
    if PORT:
        await asyncio.start_server(health, "0.0.0.0", int(PORT))
        log.info("Health server listening on port %s", PORT)
    await asyncio.to_thread(init_schema)
    asyncio.create_task(hourly_loop())
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
