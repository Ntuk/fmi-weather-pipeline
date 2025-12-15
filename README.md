# FMI Weather Data Pipeline

This project implements an ELT data pipeline for Finnish Meteorological Institute (FMI) weather data.

The pipeline collects near real-time weather observations via the FMI API, streams the data using Kafka, stores raw observations in Google BigQuery, processes daily and long-term aggregates using BigQuery SQL, and visualizes the results.

## Data ingestion

Weather observations are ingested from the Finnish Meteorological Institute (FMI) and published to a Kafka topic (`fmi_observations`) in raw JSON form.

Each message represents a single station observation at a given timestamp. Missing measurements are represented as `null`.

The exact raw message structure is documented in: `./schemas/fmi_observations_raw.schema.json`

## Architecture Diagram

The diagram below illustrates my ELT-based data pipeline for Finnish Meteorological Institute (FMI) weather observations.

Weather data is fetched from the FMI API and published as raw JSON events to a Kafka (Redpanda) topic. Each event represents a single station observation at a specific timestamp. Kafka acts as a buffer that decouples ingestion from downstream processing.

A Kafka consumer loads the raw events into a BigQuery raw table without modifying the data. Transformations are intentionally deferred until after loading. In BigQuery, an ELT step deduplicates observations and selects the latest record per station and timestamp, exposing the result as a curated view.

Apache Airflow orchestrates the pipeline by scheduling and triggering ingestion, loading, and transformation tasks, but it is not part of the data path. The curated data is finally queried by a Streamlit application for visualization and analysis.

![Architecture diagram](./assets/diagram.svg)
