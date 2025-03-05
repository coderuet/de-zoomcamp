# link kestra https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/02-workflow-orchestration/flows/02_postgres_taxi.yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.branch import BaseBranchOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from airflow.decorators import task, task_group
import logging
import os
import requests
from airflow.utils.decorators import apply_defaults


TAXI_TYPE = "yellow"
# YEARS = ["2019", "2020"]
# MONTHS = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
YEARS = ["2019"]
MONTHS = ["01", "02"]
logger = logging.getLogger(__name__)
table_name = f"public.{TAXI_TYPE}_tripdata"
staging_table = f"public.{TAXI_TYPE}_tripdata_staging"
default_args = {
    "owner": "airflow",
    "retries": 1,
}


class CheckPostgresConnectionOperator(BaseBranchOperator):
    @apply_defaults
    def __init__(self, conn_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id

    def choose_branch(self, context):
        try:
            hook = PostgresHook(postgres_conn_id=self.conn_id)
            conn = hook.get_conn()
            if conn:
                return "create_table_group"
        except Exception as e:
            logger.error(f"Connection error {e}")
            return "end_task"


def extract_data(taxi_file, output_file):
    logger.info("Starting Download file from source")
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{TAXI_TYPE}/{taxi_file}"
    logger.info(
        f"URL {url}",
    )
    # Download the file
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors

    # Write the downloaded content
    with open(output_file, "wb") as f:
        f.write(response.content)

    # If it's a gzipped file, extract it
    if taxi_file.endswith(".gz"):
        import gzip
        import shutil

        # Extract the gzipped file
        with gzip.open(output_file, "rb") as f_in:
            with open(output_file.replace(".gz", ""), "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        # Optional: Remove the original .gz file
        os.remove(output_file)
    logger.info("Success download")
    return output_file


@task_group
def transformation_group(data_path, time):
    # truncate staging table
    truncate_staging_table = SQLExecuteQueryOperator(
        task_id=f"truncate_table_{TAXI_TYPE}_tripdata_staging_{time}",
        conn_id="postgresql_main_db",
        sql=f"""
            TRUNCATE TABLE {staging_table}
        """,
    )
    # Copy csv to staging
    copy_task = SQLExecuteQueryOperator(
        task_id=f"copy_csv_to_staging_{time}",
        conn_id="postgres_default",
        sql=f"""
        COPY {staging_table} 
        (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, 
        trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, 
        payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, 
        improvement_surcharge, total_amount, congestion_surcharge) 
        FROM '{data_path}' WITH CSV HEADER;
    """,
    )
    # Add unique and file name column
    add_unique_id_and_filename = SQLExecuteQueryOperator(
        task_id=f"add_unique_id_and_file_name_{time}",
        conn_id="postgres_default",
        sql=f"""
        UPDATE {staging_table}
        SET 
            unique_row_id = md5(
            COALESCE(CAST(VendorID AS text), '') ||
            COALESCE(CAST(tpep_pickup_datetime AS text), '') || 
            COALESCE(CAST(tpep_dropoff_datetime AS text), '') || 
            COALESCE(PULocationID, '') || 
            COALESCE(DOLocationID, '') || 
            COALESCE(CAST(fare_amount AS text), '') || 
            COALESCE(CAST(trip_distance AS text), '')      
            ),
        filename = '{data_path}';
    """,
    )
    (truncate_staging_table >> copy_task >> add_unique_id_and_filename)
    return truncate_staging_table


def load_data_func(time):
    # Load data to final table
    merge_data_task = SQLExecuteQueryOperator(
        task_id=f"merge_data_from_staging_to_final_{time}",
        conn_id="postgres_default",
        sql=f"""
        MERGE INTO {table_name} AS T
        USING {staging_table} AS S
        ON T.unique_row_id = S.unique_row_id
        WHEN NOT MATCHED THEN
            INSERT (
            unique_row_id, filename, VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
            passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID,
            DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
            improvement_surcharge, total_amount, congestion_surcharge
        )
        VALUES (
            S.unique_row_id, S.filename, S.VendorID, S.tpep_pickup_datetime, S.tpep_dropoff_datetime,
            S.passenger_count, S.trip_distance, S.RatecodeID, S.store_and_fwd_flag, S.PULocationID,
            S.DOLocationID, S.payment_type, S.fare_amount, S.extra, S.mta_tax, S.tip_amount, S.tolls_amount,
            S.improvement_surcharge, S.total_amount, S.congestion_surcharge
        );
        """,
    )
    return merge_data_task


with DAG(
    "download_data_and_load_postgresql",
    catchup=False,
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
) as dag:
    # Check connection to postgresql
    check_connection_task = CheckPostgresConnectionOperator(
        task_id="check_connection", conn_id="postgresql_main_db"
    )
    connection_success = EmptyOperator(task_id="connection_success")
    connection_failure = EmptyOperator(task_id="connection_failure")

    # Create final table
    create_final_table_task = SQLExecuteQueryOperator(
        task_id=f"create_final_table_{table_name}",
        conn_id="postgresql_main_db",
        sql=f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    unique_row_id text,
                    vendor_id text,
                    tpep_pickup_datetime timestamp,
                    tpep_dropoff_datetime timestamp,
                    passenger_count integer,
                    trip_distance double precision,
                    store_and_fwd_flag text,
                    pulocation_id text,
                    dolocation_id text,
                    payment_type integer,
                    fare_amount double precision,
                    mta_tax double precision,
                    tip_amount double precision,
                    tolls_amount double precision,
                    improvement_surcharge double precision,
                    total_amount double precision,
                    congestion_surcharge double precision
                );
            """,
    )

    # Create staging table
    create_staging_table_task = SQLExecuteQueryOperator(
        task_id=f"create_staging_table_{staging_table}",
        conn_id="postgresql_main_db",
        sql=f"""
            CREATE TABLE IF NOT EXISTS {staging_table} (
                unique_row_id text,
                vendor_id text,
                tpep_pickup_datetime timestamp,
                tpep_dropoff_datetime timestamp,
                passenger_count integer,
                trip_distance double precision,
                store_and_fwd_flag text,
                pulocation_id text,
                dolocation_id text,
                payment_type integer,
                fare_amount double precision,
                mta_tax double precision,
                tip_amount double precision,
                tolls_amount double precision,
                improvement_surcharge double precision,
                total_amount double precision,
                congestion_surcharge double precision
            );
            """,
    )

    check_connection_task >> [connection_success, connection_failure]
    (connection_success >> create_final_table_task >> create_staging_table_task)
    for year in YEARS:
        for month in MONTHS:
            # Download data for specific year and month
            download_task = PythonOperator(
                task_id=f"download_data_{year}_{month}",
                python_callable=extract_data,
                op_args=[year, month],
                execution_timeout=timedelta(minutes=10),
                provide_context=True,
            )

            # Create transformation task for the specific data path
            transformation_task = transformation_group(
                f"/tmp/{TAXI_TYPE}_tripdata_{year}-{month}.csv", f"{year}_{month}"
            )
            load_data_task = load_data_func(f"{year}_{month}")
            # Task dependencies
            (create_staging_table_task >> download_task >> transformation_task)
            transformation_task >> load_data_task
