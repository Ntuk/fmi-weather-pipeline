# FMI Weather Data Pipeline

This project implements an ELT data pipeline for Finnish Meteorological Institute (FMI) weather data.

The pipeline collects near real-time weather observations via the FMI API, streams the data using Kafka, stores raw observations in Google BigQuery, processes daily and long-term aggregates using BigQuery SQL, and visualizes the results.

## Data ingestion

Weather observations are ingested from the Finnish Meteorological Institute (FMI) and published to a Kafka topic (`fmi_observations`) in raw JSON form.

Each message represents a single station observation at a given timestamp. Missing measurements are represented as `null`.

The exact raw message structure is documented in: `./schemas/fmi_observations_raw.schema.json`
