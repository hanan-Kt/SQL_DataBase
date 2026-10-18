import os
import random
import time
import psycopg2
from flask import Flask

app = Flask(__name__)
DATABASE_URL = os.getenv("DATABASE_URL")


@app.route("/")
def home():
  try:
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    current_price = round(random.uniform(50000, 70000), 2)
    current_time = int(time.time())

    cur.execute(
        "INSERT INTO trades (timestamp, price) VALUES (%s, %s)",
        (current_time, current_price),
    )
    conn.commit()
    cur.close()
    conn.close()
    return (
        f"Successfully inserted tick: {current_time}, {current_price}",
        200,
    )
  except Exception as e:
    return f"Error: {e}", 500


if __name__ == "__main__":
  port = int(os.environ.get("PORT", 10000))
  app.run(host="0.0.0.0", port=port)
