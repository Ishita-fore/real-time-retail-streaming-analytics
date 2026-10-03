import csv
import json
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer

KAFKA_SERVER = "localhost:9092"
TOPIC = "sales-events"
CSV_FILE = "data/sales_events.csv"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

print("====================================")
print("LIVE SALES PRODUCER")
print("Topic:", TOPIC)
print("Sending one event every 3 seconds")
print("====================================")

try:
    while True:

        with open(CSV_FILE, mode="r", newline="", encoding="utf-8") as file:

            reader = csv.DictReader(file)

            for row in reader:

                # Create a new unique event ID
                row["event_id"] = "LIVE-SALE-" + uuid.uuid4().hex[:10]

                # Keep original order_id so your existing
                # return/sales relationships remain meaningful

                # Use current time
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                row["timestamp"] = now
                row["streamed_at"] = now

                # Make sure numeric fields are correct
                row["quantity"] = int(row["quantity"])
                row["unit_price"] = float(row["unit_price"])
                row["discount_pct"] = float(row["discount_pct"])
                row["order_value"] = float(row["order_value"])

                producer.send(TOPIC, value=row)
                producer.flush()

                print(
                    f"[SALES] "
                    f"{row['event_id']} | "
                    f"{row['order_id']} | "
                    f"₹{row['order_value']} | "
                    f"{now}"
                )

                time.sleep(3)

except KeyboardInterrupt:
    print("\nSales producer stopped.")

finally:
    producer.close()