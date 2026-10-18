import os
import time
import random
import threading
import psycopg2
from flask import Flask

app = Flask(__name__)
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://neondb_owner:npg_3uIFkZBgVb8v@ep-small-recipe-b53xutlw-pooler.c-7.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require")

def insert_live_data():
    # Start at a baseline price
    current_price = 60000.0
    
    while True:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            
            # Step the price up or down slightly (e.g., between -200 and +200)
            price_change = random.uniform(-200, 200)
            current_price += price_change
            
            # Keep it within reasonable bounds
            current_price = max(10000, min(150000, current_price))
            
            current_time = int(time.time())
            
            cur.execute(
                "INSERT INTO trades (timestamp, price) VALUES (%s, %s)",
                (current_time, round(current_price, 2))
            )
            conn.commit()
            cur.close()
            conn.close()
            print(f"Inserted: {current_time}, {current_price}")
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
