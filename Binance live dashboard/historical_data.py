import psycopg2
from binance.client import Client

# Replace with your actual Neon connection string
DB_URL = "postgresql://neondb_owner:npg_3uIFkZBgVb8v@ep-small-recipe-b53xutlw-pooler.c-7.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

# Initialize Binance Client (No API keys needed for public historical klines)
client = Client("", "")

def backfill_history():
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()

    # Ensure table exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id SERIAL PRIMARY KEY,
            timestamp BIGINT,
            symbol VARCHAR(20),
            price DOUBLE PRECISION
        );
    """)
    conn.commit()

    print("Fetching historical data from Binance (2021 to now)...")
    
    symbol = "BTCUSDT"
    start_str = "1 Jan, 2021"

    # Fetch historical klines (1-hour candles)
    klines = client.get_historical_klines(symbol, Client.KLINE_INTERVAL_1HOUR, start_str)

    print(f"Downloaded {len(klines)} historical records. Inserting into cloud database...")

    for kline in klines:
        open_time = kline[0]  # Timestamp in milliseconds
        close_price = float(kline[4])  # Close price

        cursor.execute(
            "INSERT INTO trades (timestamp, symbol, price) VALUES (%s, %s, %s)",
            (open_time, symbol, close_price)
        )

    conn.commit()
    cursor.close()
    conn.close()
    print("Backfill complete! Your cloud database is now fully populated with history.")

if __name__ == "__main__":
    backfill_history()