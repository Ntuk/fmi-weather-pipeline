import json
from datetime import datetime
from confluent_kafka import Consumer
from google.cloud import bigquery

PROJECT_ID = "oamk-476515"
DATASET = "raw"
TABLE = "fmi_observations"

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "fmi_observations"
GROUP_ID = "bq_raw_loader"


def to_bq_row(record: dict) -> dict:
    """
    Convert the Kafka JSON record to a BigQuery row.
    BigQuery accepts ISO8601 strings for TIMESTAMP fields.
    """
    return {
        "source": record["source"],
        "ingested_at": record["ingested_at"],
        "station_id": record["station_id"],
        "timestamp": record["timestamp"],
        "temperature": record.get("temperature"),
        "humidity": record.get("humidity"),
        "wind_speed": record.get("wind_speed"),
        "pressure": record.get("pressure"),
    }


def main():
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    print(f"[OK] Consuming from {TOPIC} and inserting into {table_id}")

    batch = []
    batch_size = 100

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if batch:
                    errors = client.insert_rows_json(table_id, batch)
                    if errors:
                        print("[ERROR] BigQuery insert errors:", errors)
                    else:
                        consumer.commit()
                        print(f"[OK] Inserted {len(batch)} rows")
                    batch.clear()
                continue

            if msg.error():
                print("[WARN] Kafka message error:", msg.error())
                continue

            try:
                record = json.loads(msg.value().decode("utf-8"))
                batch.append(to_bq_row(record))
            except Exception as e:
                print("[WARN] Bad JSON message, skipping:", e)
                continue

            if len(batch) >= batch_size:
                errors = client.insert_rows_json(table_id, batch)
                if errors:
                    print("[ERROR] BigQuery insert errors:", errors)
                else:
                    consumer.commit()
                    print(f"[OK] Inserted {len(batch)} rows")
                batch.clear()

    except KeyboardInterrupt:
        print("\n[OK] Stopping consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
