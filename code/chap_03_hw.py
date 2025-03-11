# https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2025/03-data-warehouse/homework.md
# example file https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet


from google.cloud import storage, bigquery
from google.cloud.storage import Bucket
from google.cloud.bigquery import Table
from typing import List
import requests
import os

BUCKET_NAME = "de-zoomcamp-453205"


def upload_file_to_gcs(url: str, bucket: Bucket) -> str:
    # Initialize GCS client

    file_name = os.path.basename(url)
    blob = bucket.blob(file_name)
    # Stream the file from the URL and upload to GCS
    with requests.get(url, stream=True) as response:
        response.raise_for_status()  # Ensure we got a successful response
        blob.upload_from_string(response.content)

    # print(f"File uploaded to gs://{BUCKET_NAME}/{file_name}")
    return f"gs://{BUCKET_NAME}/{file_name}"


def main():
    print("Running main")
    # use export to push to github
    # client = storage.Client.from_service_account_json(
    #     "/path/to/your-service-account.json"
    # )
    ## Init Client GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    ## Init Client Bigquery
    bq_client = bigquery.Client()
    table_ext_id = "loyal-karma-453205-v6.demo_dataset.yellow_data_trip_2024_ext"
    table_id = "loyal-karma-453205-v6.demo_dataset.yellow_data_trip_2024"

    source_uris: List[str] = []
    # Upload file to gcs
    for i in ["1", "2", "3", "4", "5", "6"]:
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-0{i}.parquet"
        gcs_path = upload_file_to_gcs(url=url, bucket=bucket)
        source_uris.append(gcs_path)
    # Setup create external table from gcs
    external_config = bigquery.ExternalConfig(bigquery.SourceFormat.PARQUET)
    external_config.source_uris = source_uris
    external_config.reference_file_schema_uri = source_uris[0]
    table_ext: Table = bigquery.Table(table_ext_id)
    table_ext.external_data_configuration = external_config
    bq_client.create_table(table=table_ext)
    print(
        "Created table {}.{}.{}".format(
            table_ext.project, table_ext.dataset_id, table_ext.table_id
        )
    )
    # Create native Table
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
    )
    load_job = bq_client.load_table_from_uri(
        source_uris=source_uris, destination=table_id, job_config=job_config
    )

    load_job.result()


if __name__ == "__main__":
    main()


# Question 1 :


# SELECT COUNT(*) FROM `loyal-karma-453205-v6.demo_dataset.yellow_data_trip_2024`
# Result : 20332093 -

# Question 2 :
# SELECT COUNT(DISTINCT(PULocationID)) FROM `loyal-karma-453205-v6.demo_dataset.yellow_data_trip_2024_ext`
# Both Bytes processed is 155.12 MB , Bytes billed is 156MB


# Question 3 :
# SELECT PULocationID FROM `loyal-karma-453205-v6.demo_dataset.yellow_data_trip_2024` -> Bytes processed is 155.12 MB
# SELECT PULocationID,DOLocationID FROM `loyal-karma-453205-v6.demo_dataset.yellow_data_trip_2024` -> Bytes processed is 310.24 MB
#

## Result :BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed

# Question 4 :
# SELECT COUNT(*) FROM `loyal-karma-453205-v6.demo_dataset.yellow_data_trip_2024`
# WHERE fare_amount = 0

# Result : 8333

# Question 5 :
# CREATE TABLE `loyal-karma-453205-v6.demo_dataset.yellow_data_trip_2024_optimized`
# PARTITION BY DATE(tpep_dropoff_datetime)
# CLUSTER BY VendorID
# AS SELECT * FROM `loyal-karma-453205-v6.demo_dataset.yellow_data_trip_2024`

# Partition by tpep_dropoff_datetime and Cluster on VendorID

# Question 6 :
# SELECT DISTINCT(VendorID) FROM `loyal-karma-453205-v6.demo_dataset.yellow_data_trip_2024_optimized`
# WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'

# Native -> 310.24 MB
# Partition and Cluster -> 26.84 MB

# Result : 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

# Question 7 :
# Result  :GCP Bucket

# Question 8 : False

# Question 9 :
# SELECT count(*) typically reads only metadata rather than scanning the full table, meaning it reads very
# 0 Bytes
