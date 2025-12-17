import json
import math
import yaml
import os
from datetime import datetime, timezone
from confluent_kafka import Producer
import fmi_weather_client as fmi


CONFIG_PATH = "config/settings.yaml"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


# Helpers
def load_station_ids():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg["stations"]


def normalize_value(value):
    if value is None:
        return None

    if isinstance(value, dict):
        value = value.get("value")
    elif isinstance(value, (list, tuple)) and len(value) > 0:
        value = value[0]

    if isinstance(value, float) and math.isnan(value):
        return None

    return value


# Main producer
def main():
    station_ids = load_station_ids()

    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "linger.ms": 50,
        }
    )

    produced = 0

    for station_id in station_ids:
        station_id = str(station_id)

        try:
            # FMI API call
            observations = fmi.observation_by_station_id(station_id)
        except Exception as e:
            print(f"[WARN] Failed to fetch station {station_id}: {e}")
            continue

        for obs in observations:
            try:
                timestamp = getattr(obs, "time", None)
                if isinstance(timestamp, datetime):
                    timestamp = timestamp.isoformat()

                if not timestamp:
                    continue

                ingested_at = datetime.now(timezone.utc).isoformat()

                record = {
                    "source": "fmi",
                    "ingested_at": ingested_at,
                    "station_id": station_id,
                    "timestamp": timestamp,
                    "temperature": normalize_value(getattr(obs, "temperature", None)),
                    "humidity": normalize_value(getattr(obs, "humidity", None)),
                    "wind_speed": normalize_value(getattr(obs, "wind_speed", None)),
                    "pressure": normalize_value(getattr(obs, "pressure", None)),
                }

                # Skip empty measurements
                if all(
                    record[k] is None
                    for k in ("temperature", "humidity", "wind_speed", "pressure")
                ):
                    continue

                key = f"{record['station_id']}:{record['timestamp']}"

                producer.produce(
                    "fmi_observations",
                    key=key,
                    value=json.dumps(record, ensure_ascii=False),
                )
                producer.poll(0)
                produced += 1

            except Exception as e:
                print(f"[WARN] Bad observation for station {station_id}: {e}")
                continue

    producer.flush(10)
    print(f"[OK] Produced {produced} records to Kafka topic 'fmi_observations'")


if __name__ == "__main__":
    main()
