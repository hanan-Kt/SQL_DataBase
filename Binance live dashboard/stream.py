import asyncio
import json
import os
import psycopg2
import websockets

# Read the database URL from an environment variable
DB_URL = os.environ.get("DATABASE_URL", "postgresql://neondb_owner:npg_3uIFkZBgVb8v@ep-small-recipe-b53xutlw-pooler.c-7.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require")


def init_db():
  conn = psycopg2.connect(DB_URL)
  cursor = conn.cursor()
  cursor.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id SERIAL PRIMARY KEY,
            timestamp BIGINT,
            symbol VARCHAR(20),
            price DOUBLE PRECISION
        );
    """)
  conn.commit()
  cursor.close()
  conn.close()


async def listen_and_store():
  init_db()
  conn = psycopg2.connect(DB_URL)
  cursor = conn.cursor()

  uri = "wss://stream.binance.com:9443/ws/btcusdt@trade"

  async with websockets.connect(uri) as websocket:
    print("Connected to Binance! Streaming to Neon Postgres...")
    while True:
      raw_data = await websocket.recv()
      data = json.loads(raw_data)

      symbol = data.get("s")
      price = float(data.get("p"))
      timestamp = data.get("T")

      cursor.execute(
          "INSERT INTO trades (timestamp, symbol, price) VALUES (%s, %s, %s)",
          (timestamp, symbol, price),
      )
      conn.commit()


if __name__ == "__main__":
  asyncio.run(listen_and_store())