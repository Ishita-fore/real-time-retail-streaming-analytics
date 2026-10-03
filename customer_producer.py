import csv
import json
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer

KAFKA_SERVER = "localhost:9092"
TOPIC = "customer-events"
CSV_FILE = "data/customer_activity.csv"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

print("====================================")
print("LIVE CUSTOMER PRODUCER")
print("Topic:", TOPIC)
print("Sending one event every 3 seconds")
print("====================================")

try:
    while True:

        with open(CSV_FILE, mode="r", newline="", encoding="utf-8") as file:

            reader = csv.DictReader(file)

            for row in reader:

                # New unique activity ID
                row["activity_id"] = (
                    "LIVE-ACT-" + uuid.uuid4().hex[:10]
                )

                # Current time
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                row["timestamp"] = now
                row["streamed_at"] = now

                row["duration_seconds"] = int(
                    row["duration_seconds"]
                )

                producer.send(TOPIC, value=row)
                producer.flush()

                print(
                    f"[CUSTOMER] "
                    f"{row['activity_id']} | "
                    f"{row['activity_type']} | "
                    f"{row['device_type']} | "
                    f"{now}"
                )

                time.sleep(3)

except KeyboardInterrupt:
    print("\nCustomer producer stopped.")

finally:
    producer.close()