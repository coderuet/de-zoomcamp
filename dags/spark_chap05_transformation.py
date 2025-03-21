from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from google.cloud import bigquery
from google.cloud.bigquery import Client
from datetime import datetime
import os
import argparse

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/keys/gcp_cred.json"


def main(
    project_id: str, dataset_id: str, start_time: str, end_time: str, dest_table: str
):
    print(f"oke {project_id} , {dataset_id}")
    spark: SparkSession = (
        SparkSession.builder.appName("Homework Chap 5 ")
        .config(
            "spark.jars",
            "/home/thanh-ubuntu/workspace/DE/DE-study/de-zoomcamp/jars/gcs-connector-hadoop3-latest.jar, /home/thanh-ubuntu/workspace/DE/DE-study/de-zoomcamp/jars/spark-bigquery-latest_2.12.jar",
        )
        .config("spark.hadoop.google.cloud.project.id", project_id)
        .config("spark.hadoop.google.cloud.dataset.id", dataset_id)
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            "/home/thanh-ubuntu/workspace/DE/DE-study/de-zoomcamp/keys/gcp_cred.json",
        )
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        )
        .getOrCreate()
    )
    query = f"""
    SELECT 
        -- Revenue grouping 
        PULocationID AS revenue_zone,
        DATE_TRUNC(pickup_datetime, MONTH) AS revenue_month, 
        service_type, 

        -- Revenue calculation 
        SUM(fare_amount) AS revenue_monthly_fare,
        SUM(extra) AS revenue_monthly_extra,
        SUM(mta_tax) AS revenue_monthly_mta_tax,
        SUM(tip_amount) AS revenue_monthly_tip_amount,
        SUM(tolls_amount) AS revenue_monthly_tolls_amount,
        SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
        SUM(total_amount) AS revenue_monthly_total_amount,
        SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

        -- Additional calculations
        AVG(passenger_count) AS avg_monthly_passenger_count,
        AVG(trip_distance) AS avg_monthly_trip_distance
    FROM `{project_id}.{dataset_id}.trip_data`
    WHERE pickup_datetime BETWEEN TIMESTAMP('{start_time}') AND TIMESTAMP('{end_time}') 
    GROUP BY
        1, 2, 3
    """
    print("query", query)

    print(f"Dataset ID being used: '{dataset_id}'")

    df = (
        spark.read.format("bigquery")
        .option("parentProject", project_id)
        .option("viewsEnabled", "true")
        .option("materializationDataset", dataset_id)
        .option("query", query)
        .load()
    )

    df.show(10)
    df.write.format("bigquery").option("materializationDataset", dataset_id).option(
        "table", dest_table
    ).option("temporaryGcsBucket", "de-zoomcamp-453205").mode("append").save()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Change to match the arguments from Airflow DAG (with hyphens, not underscores)
    parser.add_argument("--project-id", dest="project_id", required=True)
    parser.add_argument("--dataset-id", dest="dataset_id", required=True)
    parser.add_argument("--start-time", dest="start_time", required=True)
    parser.add_argument("--end-time", dest="end_time", required=True)
    parser.add_argument("--dest-table", dest="dest_table", required=True)

    args = parser.parse_args()

    PROJECT_ID = args.project_id
    DATASET_ID = args.dataset_id
    start_time = args.start_time
    end_time = args.end_time
    dest_table = args.dest_table

    print(f"Starting Spark job with PROJECT_ID={PROJECT_ID}, DATASET_ID={DATASET_ID}")
    main(PROJECT_ID, DATASET_ID, start_time, end_time, dest_table)
