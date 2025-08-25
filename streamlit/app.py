import streamlit as st
import pandas as pd
import sqlite3
from prophet import Prophet

# Load data
DB_PATH = "data/data.db"  # path inside /app in container
conn = sqlite3.connect(DB_PATH)
prices = pd.read_sql("SELECT * FROM commodity_prices", conn)
weather = pd.read_sql("SELECT * FROM weather_data", conn)

st.title("📈 Commodity Prices & Weather Impact")

st.subheader("Price Trend")
st.line_chart(prices.set_index(prices.columns[0]))

st.subheader("Weather Trend")
st.line_chart(weather.set_index("date"))

st.subheader("Price Forecast")
df = prices.rename(columns={prices.columns[0]: "ds", prices.columns[1]: "y"})
model = Prophet()
model.fit(df)
future = model.make_future_dataframe(periods=90)
forecast = model.predict(future)
st.line_chart(forecast.set_index("ds")[["yhat"]])
