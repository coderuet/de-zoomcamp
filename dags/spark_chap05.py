from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from google.cloud import bigquery
from google.cloud.bigquery import Client
from datetime import datetime
import os
import argparse

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/keys/gcp_cred.json"


def get_all_table_from_bigquery(client: Client, dataset_id: str):
    return client.list_tables(dataset=dataset_id)


def get_latest_time(
    client: Client, des_table: str, project_id: str, dataset_id: str
) -> dict:
    datetime_column = "pickup_datetime"
    latest_query = f"""
        SELECT service_type, MAX({datetime_column}) AS latest_datetime
        FROM `{project_id}.{dataset_id}.{des_table}`
        WHERE service_type IN ('yellow', 'green')
        GROUP BY service_type
    """
    print("latest_query", latest_query)
    result = {"green": datetime(1970, 1, 1), "yellow": datetime(1970, 1, 1)}
    try:
        latest_job = client.query(latest_query)
        latest_result = list(latest_job.result())
        print("latest_result", latest_result)

        # Process results by explicitly checking the service_type value
        for row in latest_result:
            if row["service_type"] == "yellow" and row["latest_datetime"]:
                result["yellow"] = row["latest_datetime"]
            elif row["service_type"] == "green" and row["latest_datetime"]:
                result["green"] = row["latest_datetime"]
    except Exception as e:
        print("Error retrieving latest timestamps:", e)
    return result


## this func make error that bigquery -> Pandas load all data to memory -> pandas -> spark -> load all_data to memory -> OEM
# def transformation(client: Client, datetime_value: datetime, type: str):
#     pickup_column = (
#         "lpep_pickup_datetime" if type == "green" else "tpep_pickup_datetime"
#     )
#     drop_off_column = (
#         "lpep_dropoff_datetime" if type == "green" else "tpep_dropoff_datetime"
#     )
#     common_columns = [
#         "VendorID",
#         pickup_column,
#         drop_off_column,
#         "store_and_fwd_flag",
#         "RatecodeID",
#         "PULocationID",
#         "DOLocationID",
#         "passenger_count",
#         "trip_distance",
#         "fare_amount",
#         "extra",
#         "mta_tax",
#         "tip_amount",
#         "tolls_amount",
#         "improvement_surcharge",
#         "total_amount",
#         "payment_type",
#         "congestion_surcharge",
#     ]
#     column_str = ", ".join(common_columns)
#     query = f"""
#         SELECT {column_str}
#         FROM `{PROJECT_ID}.{DATASET_ID}.{type}_tripdata`
#         WHERE {pickup_column} > TIMESTAMP('{datetime_value.strftime('%Y-%m-%d %H:%M:%S')}')
#     """
#     print("query", query)
#     result = client.query(query).result()
#     df = result.to_dataframe()
#     return df


def process_type_data(
    spark: SparkSession,
    type: str,
    datetime_value: datetime,
    project_id: str,
    dataset_id: str,
) -> DataFrame:
    print(f"running process_type_data {project_id} {dataset_id}")
    pickup_column = (
        "lpep_pickup_datetime" if type == "green" else "tpep_pickup_datetime"
    )
    drop_off_column = (
        "lpep_dropoff_datetime" if type == "green" else "tpep_dropoff_datetime"
    )
    datetime_str = datetime_value.strftime("%Y-%m-%d %H:%M:%S")
    common_columns = [
        "VendorID",
        pickup_column,
        drop_off_column,
        "store_and_fwd_flag",
        "RatecodeID",
        "PULocationID",
        "DOLocationID",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "payment_type",
        "congestion_surcharge",
    ]
    # Direct BigQuery read
    df = (
        spark.read.format("bigquery")
        .option("table", f"{project_id}.{dataset_id}.{type}_tripdata")
        .option("filter", f"{pickup_column} > TIMESTAMP('{datetime_str}')")
        .load()
    )

    return (
        df.select(common_columns)
        .withColumnRenamed(
            "lpep_pickup_datetime" if type == "green" else "tpep_pickup_datetime",
            "pickup_datetime",
        )
        .withColumnRenamed(
            "lpep_dropoff_datetime" if type == "green" else "tpep_dropoff_datetime",
            "dropoff_datetime",
        )
        .withColumn("service_type", F.lit(type))
    )


def main(project_id: str, dataset_id: str):
    print(f"oke {project_id} , {dataset_id}")
    bq_client = bigquery.Client(project=project_id)
    spark: SparkSession = (
        SparkSession.builder.appName("Homework Chap 5 ")
        .config("spark.hadoop.google.cloud.project.id", project_id)
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            "/keys/gcp_cred.json",
        )
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        )
        .getOrCreate()
    )
    des_table = "trip_data"
    latest_time = get_latest_time(
        client=bq_client,
        des_table=des_table,
        project_id=project_id,
        dataset_id=dataset_id,
    )
    for type in latest_time.keys():
        df_bigquery = process_type_data(
            spark=spark,
            datetime_value=latest_time[type],
            type=type,
            project_id=project_id,
            dataset_id=dataset_id,
        )
        df_bigquery = df_bigquery.repartition(20)
        df_bigquery.write.format("bigquery").option(
            "table", f"{project_id}.{dataset_id}.trip_data"
        ).option("temporaryGcsBucket", "de-zoomcamp-453205").mode("append").save()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Change to match the arguments from Airflow DAG (with hyphens, not underscores)
    parser.add_argument("--project-id", dest="project_id", required=True)
    parser.add_argument("--dataset-id", dest="dataset_id", required=True)

    args = parser.parse_args()

    PROJECT_ID = args.project_id
    DATASET_ID = args.dataset_id

    print(f"Starting Spark job with PROJECT_ID={PROJECT_ID}, DATASET_ID={DATASET_ID}")
    main(PROJECT_ID, DATASET_ID)
