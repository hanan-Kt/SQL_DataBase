"""
BTCUSDT collector: Binance -> Neon Postgres (for Grafana).

- On start (and after every reconnect) it back-fills any missing 1-minute candles.
- Then it streams the live 1-minute candle and UPSERTS it (one row per minute, no duplicates).
- Reconnects automatically if Binance or the database drops the connection.
- If a PORT env var exists (Render web service), it serves a tiny "ok" page for UptimeRobot.

Required env var:  DATABASE_URL
Optional env vars: SYMBOL (BTCUSDT), BACKFILL_DAYS (30), WRITE_EVERY_SECONDS (2),
                   BINANCE_REST, BINANCE_WS
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
WRITE_EVERY = float(os.environ.get("WRITE_EVERY_SECONDS", "2"))
# Market-data-only endpoints; they work in regions where api.binance.com is blocked.
REST_BASE = os.environ.get("BINANCE_REST", "https://data-api.binance.vision")
WS_BASE = os.environ.get("BINANCE_WS", "wss://data-stream.binance.vision")
PORT = os.environ.get("PORT")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("collector")

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
"""

UPSERT = """
INSERT INTO candles (symbol, ts, open, high, low, close, volume)
VALUES %s
ON CONFLICT (symbol, ts) DO UPDATE SET
    high   = EXCLUDED.high,
    low    = EXCLUDED.low,
    close  = EXCLUDED.close,
    volume = EXCLUDED.volume;
"""


def to_dt(ms):
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)


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
        with self.conn.cursor() as cur:
            cur.execute(SCHEMA)

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

    def upsert(self, rows):
        def fn(conn):
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, UPSERT, rows, page_size=1000)
        self.run(fn)

    def last_ts(self):
        def fn(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(ts) FROM candles WHERE symbol = %s;", (SYMBOL,))
                return cur.fetchone()[0]
        return self.run(fn)


def backfill(db):
    """Fill every missing 1m candle from the last stored one (or BACKFILL_DAYS ago) until now."""
    last = db.last_ts()
    start = last if last else datetime.now(timezone.utc) - timedelta(days=BACKFILL_DAYS)
    start_ms = int(start.timestamp() * 1000)
    total = 0
    while True:
        r = requests.get(
            f"{REST_BASE}/api/v3/klines",
            params={"symbol": SYMBOL, "interval": "1m", "startTime": start_ms, "limit": 1000},
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
        db.upsert(rows)
        total += len(rows)
        start_ms = klines[-1][0] + 60_000
        if len(klines) < 1000:
            break
        time.sleep(0.2)
    log.info("Backfill done: %d candles written/refreshed.", total)


async def stream(db):
    await asyncio.to_thread(backfill, db)
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
                await asyncio.to_thread(db.upsert, [row])
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
