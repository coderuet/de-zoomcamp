# id: 01_getting_started_data_pipeline
# namespace: zoomcamp

# inputs:
#   - id: columns_to_keep
#     type: ARRAY
#     itemType: STRING
#     defaults:
#       - brand
#       - price

# tasks:
#   - id: extract
#     type: io.kestra.plugin.core.http.Download
#     uri: https://dummyjson.com/products

#   - id: transform
#     type: io.kestra.plugin.scripts.python.Script
#     containerImage: python:3.11-alpine
#     inputFiles:
#       data.json: "{{outputs.extract.uri}}"
#     outputFiles:
#       - "*.json"
#     env:
#       COLUMNS_TO_KEEP: "{{inputs.columns_to_keep}}"
#     script: |
#       import json
#       import os

#       columns_to_keep_str = os.getenv("COLUMNS_TO_KEEP")
#       columns_to_keep = json.loads(columns_to_keep_str)

#       with open("data.json", "r") as file:
#           data = json.load(file)

#       filtered_data = [
#           {column: product.get(column, "N/A") for column in columns_to_keep}
#           for product in data["products"]
#       ]

#       with open("products.json", "w") as file:
#           json.dump(filtered_data, file, indent=4)

#   - id: query
#     type: io.kestra.plugin.jdbc.duckdb.Query
#     inputFiles:
#       products.json: "{{outputs.transform.outputFiles['products.json']}}"
#     sql: |
#       INSTALL json;
#       LOAD json;
#       SELECT brand, round(avg(price), 2) as avg_price
#       FROM read_json_auto('{{workingDir}}/products.json')
#       GROUP BY brand
#       ORDER BY avg_price DESC;
#     fetchType: STORE


import textwrap
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import json
from airflow.models.param import Param
from airflow.utils.dates import days_ago


COLUMNS_TO_KEEP = ["brand", "price"]
DATA_FILE = "/tmp/data.json"
PROCESSED_FILE = "/tmp/products.json"


def extract():
    """Extract data from the API and save it as a JSON file."""
    url = "https://dummyjson.com/products"
    response = requests.get(url)
    response.raise_for_status()  # Ensure we get a successful response

    with open(DATA_FILE, "w") as f:
        json.dump(response.json(), f, indent=4)


def transform():
    """Filter the extracted data and keep only the required columns."""
    with open(DATA_FILE, "r") as f:
        data = json.load(f)

    filtered_data = [
        {col: product.get(col, "N/A") for col in COLUMNS_TO_KEEP}
        for product in data.get("products", [])
    ]

    with open(PROCESSED_FILE, "w") as f:
        json.dump(filtered_data, f, indent=4)


with DAG(
    dag_id="etl_dummyjson",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
        execution_timeout=timedelta(seconds=60),
    )

    transform_task = PythonOperator(task_id="transform", python_callable=transform)

    # query_task = PythonOperator(task_id="query")

    # Task dependencies (Extract → Transform → Query)
    extract_task >> transform_task
