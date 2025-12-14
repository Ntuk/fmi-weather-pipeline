# FMI Weather Data Pipeline

This project implements an ELT data pipeline for Finnish Meteorological Institute (FMI) weather data.

The pipeline collects near real-time weather observations via the FMI API, streams the data using Kafka, stores raw observations in Google BigQuery, processes daily and long-term aggregates using BigQuery SQL, and visualizes the results.
