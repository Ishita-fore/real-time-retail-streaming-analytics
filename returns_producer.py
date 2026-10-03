import csv
import json
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer

KAFKA_SERVER = "localhost:9092"
TOPIC = "return-events"
CSV_FILE = "data/returns_events.csv"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

print("====================================")
print("LIVE RETURNS PRODUCER")
print("Topic:", TOPIC)
print("Sending one event every 3 seconds")
print("====================================")

try:
    while True:

        with open(CSV_FILE, mode="r", newline="", encoding="utf-8") as file:

            reader = csv.DictReader(file)

            for row in reader:

                # New unique return event ID
                row["return_event_id"] = (
                    "LIVE-RETURN-" + uuid.uuid4().hex[:10]
                )

                # Current time
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                row["timestamp"] = now
                row["streamed_at"] = now

                row["quantity_returned"] = int(
                    row["quantity_returned"]
                )

                row["refund_amount"] = float(
                    row["refund_amount"]
                )

                # Keep original order_id
                # so return -> sales joins continue working

                producer.send(TOPIC, value=row)
                producer.flush()

                print(
                    f"[RETURN] "
                    f"{row['return_event_id']} | "
                    f"{row['return_reason']} | "
                    f"₹{row['refund_amount']} | "
                    f"{now}"
                )

                time.sleep(3)

except KeyboardInterrupt:
    print("\nReturns producer stopped.")

finally:
    producer.close()