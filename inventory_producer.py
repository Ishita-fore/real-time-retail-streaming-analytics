import csv
import json
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer

KAFKA_SERVER = "localhost:9092"
TOPIC = "inventory-events"
CSV_FILE = "data/inventory_events.csv"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

print("====================================")
print("LIVE INVENTORY PRODUCER")
print("Topic:", TOPIC)
print("Sending one event every 3 seconds")
print("====================================")

try:
    while True:

        with open(CSV_FILE, mode="r", newline="", encoding="utf-8") as file:

            reader = csv.DictReader(file)

            for row in reader:

                # New unique event ID
                row["inventory_event_id"] = (
                    "LIVE-INV-" + uuid.uuid4().hex[:10]
                )

                # Current time
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                row["timestamp"] = now
                row["streamed_at"] = now

                # Numeric fields
                row["stock_before"] = int(row["stock_before"])
                row["quantity_change"] = int(row["quantity_change"])
                row["stock_after"] = int(row["stock_after"])
                row["reorder_level"] = int(row["reorder_level"])

                producer.send(TOPIC, value=row)
                producer.flush()

                print(
                    f"[INVENTORY] "
                    f"{row['inventory_event_id']} | "
                    f"{row['product_id']} | "
                    f"stock={row['stock_after']} | "
                    f"{now}"
                )

                time.sleep(3)

except KeyboardInterrupt:
    print("\nInventory producer stopped.")

finally:
    producer.close()