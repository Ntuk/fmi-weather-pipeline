import os
import json
from confluent_kafka import Consumer
from google.cloud import bigquery

PROJECT_ID = "oamk-476515"
DATASET = "raw"
TABLE = "fmi_observations"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "fmi_observations"
GROUP_ID = "bq_raw_loader"
BATCH_SIZE = 100
IDLE_POLLS_BEFORE_EXIT = 10


def to_bq_row(record: dict) -> dict:
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


def flush_batch(client: bigquery.Client, table_id: str, consumer: Consumer, batch: list) -> None:
    if not batch:
        return

    errors = client.insert_rows_json(table_id, batch)
    if errors:
        print("[ERROR] BigQuery insert errors:", errors)
        # Do NOT commit offsets if insert failed
        return

    consumer.commit()
    print(f"[OK] Inserted {len(batch)} rows")
    batch.clear()


def main() -> None:
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
    print(f"[OK] Kafka bootstrap: {KAFKA_BOOTSTRAP}")

    batch: list[dict] = []
    idle_polls = 0

    try:
        while idle_polls < IDLE_POLLS_BEFORE_EXIT:
            msg = consumer.poll(1.0)

            if msg is None:
                idle_polls += 1
                flush_batch(client, table_id, consumer, batch)
                continue

            idle_polls = 0

            if msg.error():
                print("[WARN] Kafka message error:", msg.error())
                continue

            try:
                record = json.loads(msg.value().decode("utf-8"))
                batch.append(to_bq_row(record))
            except Exception as e:
                print("[WARN] Bad JSON message, skipping:", e)
                continue

            if len(batch) >= BATCH_SIZE:
                flush_batch(client, table_id, consumer, batch)

        # Final flush before exit
        flush_batch(client, table_id, consumer, batch)
        print("[OK] No new messages, exiting consumer.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
