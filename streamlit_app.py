import pandas as pd
import streamlit as st
from google.cloud import bigquery

PROJECT_ID = "oamk-476515"
CURATED_VIEW = f"{PROJECT_ID}.curated.fmi_observations_latest"
LONGTERM_TABLES = {
    "100971 (Helsinki Kaisaniemi)": f"{PROJECT_ID}.longterm.station_100971",
    "101939 (Sodankylä Luosto)": f"{PROJECT_ID}.longterm.station_101939",
    "101632 (Joensuu Linnunlahti)": f"{PROJECT_ID}.longterm.station_101632",
    "101786 (Oulu Lentoasema)": f"{PROJECT_ID}.longterm.station_101786",
    "101311 (Tampere Siilinkari)": f"{PROJECT_ID}.longterm.station_101311",
}

st.set_page_config(page_title="FMI Weather Dashboard", layout="wide")
st.title("FMI Weather Dashboard")
st.caption("Data source: FMI → Kafka → BigQuery (raw) → BigQuery (processed/longterm)")

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
    FROM `{CURATED_VIEW}`
    ORDER BY timestamp DESC, station_id
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=60)
def load_longterm(table_id: str, hours_back: int):
    query = f"""
    SELECT
      station_id,
      timestamp,
      temperature,
      humidity
    FROM `{table_id}`
    WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)
    ORDER BY timestamp
    """
    return client.query(query).to_dataframe()

df_latest = load_latest()

st.subheader("Latest observations (curated)")
st.dataframe(df_latest, use_container_width=True)

st.subheader("Quick summary")
col1, col2, col3 = st.columns(3)
col1.metric("Stations", df_latest["station_id"].nunique())
col2.metric("Latest timestamp", str(df_latest["timestamp"].max()))
col3.metric("Rows shown", len(df_latest))

st.divider()

st.subheader("Long-term time series (per station)")

left, right = st.columns([2, 1])
with left:
    station_label = st.selectbox("Select station", list(LONGTERM_TABLES.keys()))
with right:
    hours_back = st.selectbox("Time range", [1, 6, 12, 24, 48, 72], index=3)

table_id = LONGTERM_TABLES[station_label]
df_ts = load_longterm(table_id, hours_back)

if df_ts.empty:
    st.info("No data found for the selected time range yet. Run producer + consumer again.")
else:
    st.write(f"Showing {len(df_ts)} records from `{table_id}` (last {hours_back} hours).")

    temp_series = df_ts.set_index("timestamp")["temperature"]
    st.line_chart(temp_series)

    hum_series = df_ts.set_index("timestamp")["humidity"]
    st.line_chart(hum_series)

    st.dataframe(df_ts, use_container_width=True)
