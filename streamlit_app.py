import pandas as pd
import streamlit as st
from google.cloud import bigquery

PROJECT_ID = "oamk-476515"
VIEW_ID = f"{PROJECT_ID}.curated.fmi_observations_latest"

st.set_page_config(page_title="FMI Weather Dashboard", layout="wide")
st.title("FMI Weather Dashboard")
st.caption("Data source: FMI → Kafka → BigQuery (raw) → BigQuery (curated)")

client = bigquery.Client(project=PROJECT_ID)

@st.cache_data(ttl=60)
def load_latest():
    query = f"""
    SELECT
      station_id,
      timestamp,
      temperature,
      humidity,
      wind_speed,
      pressure
    FROM `{VIEW_ID}`
    ORDER BY timestamp DESC, station_id
    """
    return client.query(query).to_dataframe()

df = load_latest()

st.subheader("Latest observations")
st.dataframe(df, use_container_width=True)

st.subheader("Quick summary")
col1, col2, col3 = st.columns(3)
col1.metric("Stations", df["station_id"].nunique())
col2.metric("Newest timestamp", str(df["timestamp"].max()))
col3.metric("Rows", len(df))
