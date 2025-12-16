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
* Data is loaded as-is
* Offsets are committed only after successful BigQuery inserts
* No deduplication or filtering happens at this stage

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
* Adds data quality flags (missing values, outliers)

### Long-term station tables

For selected stations, daily data is appended into station-specific long-term tables:

longterm.station_<station_id>

Each station has its own time-series table to simplify analysis and visualization.

## Orchestration with Airflow

Apache Airflow orchestrates the pipeline steps but is not part of the data path.

The DAG performs:

* Kafka producer run (FMI → Kafka)
* Kafka consumer run (Kafka → BigQuery raw)
* Daily processing in BigQuery
* Long-term table updates per station

The DAG is designed to be idempotent and demo-friendly.

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
