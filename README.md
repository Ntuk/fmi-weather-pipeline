# FMI Weather Data Pipeline

This project implements a small but complete ELT data pipeline for Finnish Meteorological Institute (FMI) weather observations.

The goal is to demonstrate how near real-time weather data can be ingested, streamed, stored, processed, and visualized using modern data engineering tools.

The pipeline is designed for clarity and robustness rather than scale, and it can be run locally using Docker.

## What the pipeline does

1. Fetches recent weather observations from the FMI API
2. Streams the observations as raw JSON events to Kafka (Redpanda)
3. Loads raw events into Google BigQuery without transformation
4. Processes daily and long-term datasets in BigQuery using SQL
5. Orchestrates all steps with Apache Airflow
6. Visualizes results using Streamlit

## Data ingestion (FMI → Kafka)

Weather observations are retrieved from the Finnish Meteorological Institute API and published to a Kafka topic called fmi_observations.

Each Kafka message represents one station observation at one timestamp

Messages are encoded as JSON

Missing measurements are represented as null

No validation or transformation is applied at this stage

The raw event structure is documented in: schemas/fmi_observations_raw_schema.json

Kafka (Redpanda) acts as a buffer between the external API and downstream systems, allowing ingestion and processing to run independently.

## Data loading (Kafka → BigQuery raw)

A Kafka consumer reads messages from the fmi_observations topic and batches them into a BigQuery raw table: raw.fmi_observations

#### Key characteristics:
* Data is loaded in batches of 100 rows
* Offsets are committed only after successful BigQuery inserts
* No deduplication or filtering happens at this stage
* Consumer exits automatically after idle period

This keeps the raw dataset auditable and easy to reprocess if needed.

## Data processing in BigQuery (ELT)

All transformations are performed after loading, directly in BigQuery.

### Curated layer

A curated view: 

curated.fmi_observations_latest

* Deduplicates observations
* Selects the latest record per station and timestamp

### Processed daily layer

A daily processed table:

processed.fmi_observations_daily

* Rebuilt idempotently for each run date
* Adds data quality flags:
  * `missing_temperature`, `missing_humidity` - flags NULL values
  * `outlier_temperature` - flags temperature < -60°C or > 60°C
  * `outlier_humidity` - flags humidity < 0% or > 100%
* Contains: station_id, timestamp, temperature, humidity, pressure, wind_speed, ingested_at, and quality flags

### Long-term station tables

For five selected stations, daily data is appended into station-specific long-term tables:
* longterm.station_100971 (Helsinki Kaisaniemi)
* longterm.station_101939 (Sodankylä Luosto)
* longterm.station_101632 (Joensuu Linnunlahti)
* longterm.station_101786 (Oulu Lentoasema)
* longterm.station_101311 (Tampere Siilinkari)

Each station has its own time-series table to simplify analysis and visualization.
Tables are rebuilt idempotently per station per day.

## Orchestration with Airflow

Apache Airflow orchestrates the pipeline steps but is not part of the data path.

The DAG (`fmi_weather_pipeline`) runs on a **@daily** schedule and performs:
1. **run_producer** (BashOperator) - Kafka producer run (FMI → Kafka)
2. **run_consumer_to_bigquery** (BashOperator) - Kafka consumer run (Kafka → BigQuery raw)
3. **build_processed_daily** (BigQueryInsertJobOperator) - Daily processing in BigQuery
4. **update_longterm_station_*** (5x BigQueryInsertJobOperator) - Long-term table updates per station (run in parallel)

**Authentication:** Uses Application Default Credentials (ADC) mounted from the host system into Docker containers.

## Visualization

A Streamlit app queries BigQuery to visualize:
* Latest observations
* Daily processed data
* Long-term time series for selected stations

The visualizations are intentionally simple and serve to demonstrate that the pipeline works end-to-end.

## Architecture overview

The diagram below shows the full ELT pipeline.

Weather data is fetched from the FMI API and published as raw JSON events to Kafka (Redpanda). A Kafka consumer loads the events into BigQuery without transformation. BigQuery handles all transformations and quality checks. Airflow orchestrates execution, and Streamlit reads from BigQuery for visualization.

![Architecture diagram](./assets/diagram.svg)
