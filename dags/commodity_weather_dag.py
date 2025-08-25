from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import sqlite3
import openpyxl
import io

DB_PATH = "/opt/airflow/data/data.db"


def fetch_prices():
    # URL of the Excel file
    url = "https://thedocs.worldbank.org/en/doc/5d903e848db1d1b83e0ec8f744e55570-0350012021/related/CMO-Historical-Data-Monthly.xlsx"

    # Download the file into memory
    print("Downloading Excel file...")
    resp = requests.get(url)
    resp.raise_for_status()  # Stop if request failed

    # Load into pandas directly from memory
    print("Loading Excel into DataFrame...")
    xls = pd.ExcelFile(io.BytesIO(resp.content))

    # Select the "Monthly Prices" sheet
    sheet_name = "Monthly Prices"
    df = pd.read_excel(xls, sheet_name=sheet_name, header=None)

    # Find the cell containing 'Crude oil, Brent'
    target = "crude oil, brent"
    row_idx, col_idx = None, None
    for r in range(df.shape[0]):
        for c in range(df.shape[1]):
            val = df.iat[r, c]
            if isinstance(val, str) and target in val.lower():
                row_idx, col_idx = r, c
                break
        if col_idx is not None:
            break

    if row_idx is None or col_idx is None:
        raise ValueError("Could not find a cell containing 'Crude oil, Brent'")

    # Extract all numeric values in the column under 'Crude oil, Brent', with their corresponding dates from the first column
    data = []
    for i in range(row_idx + 1, df.shape[0]):
        date_val = df.iat[i, 0]
        price_val = df.iat[i, col_idx]
        if pd.notnull(date_val) and pd.notnull(price_val):
            try:
                price_num = float(price_val)
                data.append({"Date": date_val, "Price": price_num})
            except (ValueError, TypeError):
                continue

    data_df = pd.DataFrame(data)
    conn = sqlite3.connect(DB_PATH)
    data_df.to_sql("commodity_prices", conn, if_exists="replace", index=False)
    conn.close()


def fetch_weather():
    lat, lon = -21.5, -45.0
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=temperature_2m_max,precipitation_sum&timezone=UTC"
    data = requests.get(url).json()
    df = pd.DataFrame(
        {
            "date": data["daily"]["time"],
            "temp_max": data["daily"]["temperature_2m_max"],
            "precip": data["daily"]["precipitation_sum"],
        }
    )
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("weather_data", conn, if_exists="replace", index=False)
    conn.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "commodity_weather_pipeline",
    default_args=default_args,
    description="Fetch commodity prices and weather data daily",
    schedule_interval="@daily",
    catchup=False,
) as dag:

    task_fetch_prices = PythonOperator(
        task_id="fetch_prices", python_callable=fetch_prices
    )

    task_fetch_weather = PythonOperator(
        task_id="fetch_weather", python_callable=fetch_weather
    )

    task_fetch_prices >> task_fetch_weather
