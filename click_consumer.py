
from kafka import KafkaConsumer
import json
import psycopg2
from datetime import datetime

# ---------- PostgreSQL Config ----------
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB   = "sales_db"      # change if your DB name is different
PG_USER = "postgres"       # change if needed
PG_PASS = "sunny1554"  # <-- put your real password here

print("Connecting to PostgreSQL...")

try:
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS
    )
    conn.autocommit = True
    cursor = conn.cursor()
    print("PostgreSQL connected \n")
except Exception as e:
    print("Error connecting to PostgreSQL:", e)
    raise

# ---------- Kafka Consumer ----------
consumer = KafkaConsumer(
    "user_clicks",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=None,   # no stored offsets → good for testing; always reads from earliest
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("Listening to 'user_clicks' and saving to database...\n")

try:
    for msg in consumer:
        try:
            event = msg.value  # dict with keys: user, page, timestamp

            user = event.get("user")
            page = event.get("page")
            ts_str = event.get("timestamp")  # e.g. "2025-07-15 12:34:56"

            # Convert string timestamp to Python datetime for proper TIMESTAMP column
            click_time = None
            if ts_str:
                click_time = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")

            print(f"User: {user} | Page: {page} | Timestamp: {ts_str}")

            insert_query = """
                INSERT INTO kafka_user_clicks (user_name, page, click_time)
                VALUES (%s, %s, %s);
            """

            cursor.execute(insert_query, (user, page, click_time))

            print("→ Click saved to database\n")

        except Exception as e:
            print("Error processing or saving message:", e)
            # you can log and continue

except KeyboardInterrupt:
    print("\nStopping consumer...")

finally:
    print("Closing DB connection and consumer...")
    try:
        cursor.close()
        conn.close()
    except Exception:
        pass
    consumer.close()
    print("Shutdown complete.")

# from kafka import KafkaConsumer
# import json
# from collections import defaultdict

# consumer = KafkaConsumer(
#     "user_clicks",
#     bootstrap_servers="localhost:9092",
#     auto_offset_reset="earliest",
#     group_id="click-analytics-group",
#     value_deserializer=lambda v: json.loads(v.decode("utf-8"))
# )

# page_counter = defaultdict(int)

# print("Listening to user clicks...\n")

# try:
#     for msg in consumer:
#         data = msg.value
#         page = data["page"]

#         page_counter[page] += 1

#         print(f"User {data['user']} visited {page}")
#         print("Current Page Stats:", dict(page_counter))
#         print("-" * 40)

# except KeyboardInterrupt:
#     print("\nStopping click consumer...")

# finally:
#     consumer.close()
#     print("Consumer closed.")