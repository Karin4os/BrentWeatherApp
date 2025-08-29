import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from prophet import Prophet

# Load data
POSTGRES_CONN_STR = (
    "postgresql+psycopg2://airflow:airflow_postg_pwd@postgres:5432/airflow"
)
engine = create_engine(POSTGRES_CONN_STR)
prices = pd.read_sql("SELECT * FROM commodity_prices", engine)
weather = pd.read_sql("SELECT * FROM weather_data", engine)

st.title("📈 Commodity Prices & Weather Impact")

st.subheader("Price Trend")
st.line_chart(prices.set_index(prices.columns[0]))

st.subheader("Weather Trend")
st.line_chart(weather.set_index("date"))

st.subheader("Price Forecast")
df = prices.rename(columns={prices.columns[0]: "ds", prices.columns[1]: "y"})
# Convert '1960M01' to '1960-01-01' using correct regex
df["ds"] = df["ds"].str.replace(r"(\d{4})M(\d{2})", r"\1-\2-01", regex=True)
df["ds"] = pd.to_datetime(df["ds"], errors="coerce")
model = Prophet()
model.fit(df)
future = model.make_future_dataframe(periods=90)
forecast = model.predict(future)
st.line_chart(forecast.set_index("ds")[["yhat"]])
