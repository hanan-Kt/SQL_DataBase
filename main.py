import os
import time
import random
import threading
import psycopg2
from flask import Flask

app = Flask(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://neondb_owner:npg_3t5hyXVfivjr@ep-damp-mud-b5db2bd4-pooler.c-7.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require")

def insert_live_data():
    while True:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            current_time = int(time.time())
            dummy_price = random.randint(40000, 90000)

            cur.execute(
                "INSERT INTO trades (timestamp, price) VALUES (%s, %s)",
                (current_time, dummy_price)
            )
            conn.commit()
            cur.close()
            conn.close()
            print(f"Inserted: {current_time}, {dummy_price}")
        except Exception as e:
            print("Database insertion error:", e)

        time.sleep(5)  # Pause between inserts

# Start the background thread
thread = threading.Thread(target=insert_live_data, daemon=True)
thread.start()

@app.route("/")
def home():
    return "Live Data Streamer is Running!"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
