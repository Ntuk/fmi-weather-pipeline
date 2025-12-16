from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "oamk-476515"
LOCATION = "EU"
CURATED_VIEW = f"{PROJECT_ID}.curated.fmi_observations_latest"
PROCESSED_TABLE = f"{PROJECT_ID}.processed.fmi_observations_daily"
STATIONS = ["100971", "101939", "101632", "101786", "101311"]
REPO_ROOT = "/opt/airflow/repo"
PYTHON_BIN = "/home/airflow/.local/bin/python"

default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="fmi_weather_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["demo", "fmi", "bigquery"],
) as dag:
    common_env = {
        "GOOGLE_CLOUD_PROJECT": PROJECT_ID,
        "PYTHONPATH": REPO_ROOT,
        "KAFKA_BOOTSTRAP_SERVERS": "redpanda:9092",
        "GOOGLE_APPLICATION_CREDENTIALS": "/home/airflow/.config/gcloud/application_default_credentials.json",
    }

    # 1) Producer (FMI -> Kafka)
    run_producer = BashOperator(
        task_id="run_producer",
        bash_command=(
            "set -euo pipefail; "
            f"cd {REPO_ROOT}; "
            f"{PYTHON_BIN} -u kafka/producer_fmi.py"
        ),
        env=common_env,
    )

    # 2) Consumer (Kafka -> BigQuery raw). Exits automatically after idle period
    run_consumer = BashOperator(
        task_id="run_consumer_to_bigquery",
        bash_command=(
            "set -euo pipefail; "
            f"cd {REPO_ROOT}; "
            f"{PYTHON_BIN} -u kafka/consumer_to_bigquery.py"
        ),
        env=common_env,
    )

    # 3) Processed daily (rebuild ds partition from curated latest)
    build_processed_daily = BigQueryInsertJobOperator(
        task_id="build_processed_daily",
        location=LOCATION,
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": f"""
                DECLARE run_date DATE DEFAULT @run_date;

                -- Idempotent: rebuild this day's partition
                DELETE FROM `{PROCESSED_TABLE}`
                WHERE DATE(timestamp) = run_date;

                INSERT INTO `{PROCESSED_TABLE}` (
                  station_id,
                  timestamp,
                  temperature,
                  humidity,
                  pressure,
                  wind_speed,
                  wind_direction,
                  precipitation_1h,
                  source,
                  ingested_at,
                  missing_temperature,
                  missing_humidity,
                  outlier_temperature,
                  outlier_humidity
                )
                SELECT
                  station_id,
                  timestamp,
                  temperature,
                  humidity,
                  pressure,
                  wind_speed,
                  wind_direction,
                  precipitation_1h,
                  source,
                  ingested_at,
                  (temperature IS NULL) AS missing_temperature,
                  (humidity IS NULL) AS missing_humidity,
                  (temperature IS NOT NULL AND (temperature < -60 OR temperature > 60)) AS outlier_temperature,
                  (humidity IS NOT NULL AND (humidity < 0 OR humidity > 100)) AS outlier_humidity
                FROM `{CURATED_VIEW}`
                WHERE DATE(timestamp) = run_date;
                """,
                "useLegacySql": False,
                "queryParameters": [
                    {
                        "name": "run_date",
                        "parameterType": {"type": "DATE"},
                        "parameterValue": {"value": "{{ ds }}"},
                    }
                ],
            }
        },
    )

    # 4) Longterm per-station updates
    longterm_tasks = []
    for station_id in STATIONS:
        longterm_table = f"{PROJECT_ID}.longterm.station_{station_id}"

        t = BigQueryInsertJobOperator(
            task_id=f"update_longterm_station_{station_id}",
            location=LOCATION,
            gcp_conn_id="google_cloud_default",
            configuration={
                "query": {
                    "query": f"""
                    DECLARE run_date DATE DEFAULT @run_date;
                    DECLARE sid STRING DEFAULT @station_id;

                    -- Idempotent: rebuild this station's partition for the day
                    DELETE FROM `{longterm_table}`
                    WHERE DATE(timestamp) = run_date;

                    INSERT INTO `{longterm_table}`
                    SELECT *
                    FROM `{PROCESSED_TABLE}`
                    WHERE DATE(timestamp) = run_date
                      AND station_id = sid;
                    """,
                    "useLegacySql": False,
                    "queryParameters": [
                        {
                            "name": "run_date",
                            "parameterType": {"type": "DATE"},
                            "parameterValue": {"value": "{{ ds }}"},
                        },
                        {
                            "name": "station_id",
                            "parameterType": {"type": "STRING"},
                            "parameterValue": {"value": station_id},
                        },
                    ],
                }
            },
        )
        longterm_tasks.append(t)

    run_producer >> run_consumer >> build_processed_daily >> longterm_tasks
