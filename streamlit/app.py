import streamlit as st
import pandas as pd
import sqlite3
from prophet import Prophet

# Load data
DB_PATH = "data/data.db"  # path inside /app in container
# DB_PATH = "../data/data.db"
conn = sqlite3.connect(DB_PATH)
prices = pd.read_sql("SELECT * FROM commodity_prices", conn)
weather = pd.read_sql("SELECT * FROM weather_data", conn)

st.title("📈 Commodity Prices & Weather Impact")

st.subheader("Price Trend")
st.line_chart(prices.set_index(prices.columns[0]))

st.subheader("Weather Trend")
st.line_chart(weather.set_index("date"))

st.subheader("Price Forecast")
# Convert '1960M01' to a datetime (first day of month)
df = prices.rename(columns={prices.columns[0]: "ds", prices.columns[1]: "y"})
# Convert '1960M01' to datetime (first day of month)
df["ds"] = pd.to_datetime(df["ds"], format="%YM%m")
model = Prophet()
model.fit(df)
future = model.make_future_dataframe(periods=90)
forecast = model.predict(future)
st.line_chart(forecast.set_index("ds")[["yhat"]])
