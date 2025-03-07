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
import pandas as pd
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
import requests
from airflow.utils.decorators import apply_defaults


logger = logging.getLogger(__name__)
default_args = {
    "owner": "airflow",
    "retries": 1,
}


# Define a custom macro that reads from dag_run.conf
def get_config_value(key, default="Not provided"):
    from airflow.models import DagRun
    from airflow.utils.state import State

    dag_run = DagRun.find(dag_id="dag_with_conf", state=State.RUNNING)
    if dag_run and dag_run[0].conf:
        return dag_run[0].conf.get(key, default)
    return default


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
                return "connection_success"
        except Exception as e:
            logger.error(f"Connection error {e}")
            return "connection_failure"


def extract_data(taxi_type, output_file, year, month):
    logger.info("Starting Download file from source")
    download_file = f"{taxi_type}_tripdata_{year}-{month}.csv.gz"
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{download_file}"
    logger.info(f"Download file from URL {url}")
    # Download the file
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors

    # Write the downloaded content
    with open(output_file, "wb") as f:
        f.write(response.content)

    # If it's a gzipped file, extract it
    if output_file.endswith(".gz"):
        import gzip
        import shutil

        # Extract the gzipped file
        with gzip.open(output_file, "rb") as f_in:
            with open(output_file.replace(".gz", ""), "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        # # Optional: Remove the original .gz file
        # os.remove(output_file)
    logger.info("Success download")
    return output_file


def load_csv_to_postgres(table_name, file, chunksize=10000):
    try:
        # Fetch connection details securely from Airflow
        conn = BaseHook.get_connection("postgresql_main_db")

        # Create SQLAlchemy engine without hardcoding credentials
        logger.info(f"Login to database {conn.schema}")
        engine = create_engine(
            f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        )

        # Process CSV in chunks to avoid memory issues
        for chunk in pd.read_csv(file, chunksize=chunksize):
            chunk.to_sql(table_name, engine, if_exists="append", index=False)
            logger.info(f"Loaded chunk of {len(chunk)} rows to {table_name}")

        logger.info(f"Successfully completed loading {file} to {table_name}")

    except Exception as e:
        logger.error(f"Error loading CSV to PostgreSQL: {str(e)}")
        raise

    finally:
        # Ensure engine resources are released
        if "engine" in locals():
            engine.dispose()


with DAG(
    "download_data_and_load_postgresql",
    catchup=False,
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    dagrun_timeout=timedelta(minutes=25),
    schedule=None,
    # user_defined_macros={
    #     "taxi_type": "{{ dag_run.conf.get('taxi_type', 'default') }}",
    #     "table_name": "public.{{ dag_run.conf.get('taxi_type', 'default') }}_tripdata",
    #     "staging_table": "public.{{ dag_run.conf.get('taxi_type', 'default') }}_tripdata_staging",
    # },
) as dag:
    taxi_type = "{{ dag_run.conf.get('taxi_type', 'default') }}"
    table_name = f"public.{taxi_type}_tripdata"
    staging_table = (
        "public.{{ dag_run.conf.get('taxi_type', 'default') }}_tripdata_staging"
    )
    year = "{{ dag_run.conf.get('year') }}"
    month = "{{ dag_run.conf.get('month') }}"
    time = f"{year}_{month}"
    output_file = f"/tmp/{taxi_type}_tripdata_{year}-{month}.csv.gz"
    csv_file = output_file.replace(".gz", "")
    # taxi_type = "{{ dag_run.conf.get('taxi_type') }}"
    # year = "{{ dag_run.conf.get('year') }}"
    # month = "{{ dag_run.conf.get('month') }}"
    # table_name = f"public.{taxi_type}_tripdata"
    # time = f"{year}_{month}"
    # staging_table = f"public.{taxi_type}_tripdata_staging"
    # Check connection to postgresql
    download_task = PythonOperator(
        task_id=f"download_data",
        python_callable=extract_data,
        op_args=[taxi_type, output_file, year, month],
        execution_timeout=timedelta(minutes=10),
        provide_context=True,
    )
    check_connection_task = CheckPostgresConnectionOperator(
        task_id="check_connection", conn_id="postgresql_main_db"
    )
    connection_success = EmptyOperator(task_id="connection_success")
    connection_failure = EmptyOperator(task_id="connection_failure")

    # Create final table
    create_final_table_task = SQLExecuteQueryOperator(
        task_id=f"create_final_table",
        conn_id="postgresql_main_db",
        sql=f"""
                CREATE TABLE IF NOT EXISTS { table_name } (
                    unique_row_id          text,
                    filename               text,
                    VendorID               text,
                    tpep_pickup_datetime   timestamp,
                    tpep_dropoff_datetime  timestamp,
                    passenger_count        integer,
                    trip_distance          double precision,
                    RatecodeID             text,
                    store_and_fwd_flag     text,
                    PULocationID           text,
                    DOLocationID           text,
                    payment_type           integer,
                    fare_amount            double precision,
                    extra                  double precision,
                    mta_tax                double precision,
                    tip_amount             double precision,
                    tolls_amount           double precision,
                    improvement_surcharge  double precision,
                    total_amount           double precision,
                    congestion_surcharge   double precision
                );
            """,
    )

    # Create staging table
    create_staging_table_task = SQLExecuteQueryOperator(
        task_id=f"create_staging_table",
        conn_id="postgresql_main_db",
        sql=f"""
            CREATE TABLE IF NOT EXISTS { staging_table } (
                unique_row_id          text,
                filename               text,
                VendorID               text,
                tpep_pickup_datetime   timestamp,
                tpep_dropoff_datetime  timestamp,
                passenger_count        integer,
                trip_distance          double precision,
                RatecodeID             text,
                store_and_fwd_flag     text,
                PULocationID           text,
                DOLocationID           text,
                payment_type           integer,
                fare_amount            double precision,
                extra                  double precision,
                mta_tax                double precision,
                tip_amount             double precision,
                tolls_amount           double precision,
                improvement_surcharge  double precision,
                total_amount           double precision,
                congestion_surcharge   double precision
            );
            """,
    )
    # Task group transformation
    # truncate staging table -> copy data to staging table -> add unique and file name column to staging
    with TaskGroup(group_id="transformation_group") as transformation_group:
        # truncate staging table
        truncate_staging_table = SQLExecuteQueryOperator(
            task_id=f"truncate_table_staging",
            conn_id="postgresql_main_db",
            sql=f"""
                TRUNCATE TABLE {staging_table}
            """,
        )
        # Copy csv to staging

        # first version use SQLExecuteQueryOperator but only copy from file in postgresql server
        # copy_task = SQLExecuteQueryOperator(
        #     task_id=f"copy_csv_to_staging",
        #     conn_id="postgresql_main_db",
        #     sql=f"""
        #     COPY {staging_table}
        #         (VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,
        #         RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,
        #         extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,
        #         congestion_surcharge)
        #     FROM '{data_path}' WITH CSV HEADER;
        # """,
        # )

        # second version
        load_csv_task = PythonOperator(
            task_id="load_csv_to_staging",
            python_callable=load_csv_to_postgres,
            dag=dag,
            op_args=[staging_table, csv_file],
        )
        # Add unique and file name column
        add_unique_id_and_filename = SQLExecuteQueryOperator(
            task_id=f"add_unique_id_and_file_name",
            conn_id="postgresql_main_db",
            sql=f"""
            UPDATE {staging_table}
            SET
                unique_row_id = (
                    COALESCE(CAST(VendorID AS text), '') ||
                    COALESCE(CAST(tpep_pickup_datetime AS text), '') || 
                    COALESCE(CAST(tpep_dropoff_datetime AS text), '') || 
                    COALESCE(PULocationID, '') || 
                    COALESCE(DOLocationID, '') || 
                    COALESCE(CAST(fare_amount AS text), '') || 
                    COALESCE(CAST(trip_distance AS text), '')
                ),
                filename = '{csv_file}';
        """,
        )
        (truncate_staging_table >> load_csv_task >> add_unique_id_and_filename)
    # Load data to final table
    merge_data_task = SQLExecuteQueryOperator(
        task_id=f"merge_data_from_staging_to_final",
        conn_id="postgresql_main_db",
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
    check_connection_task >> [connection_success, connection_failure]
    (connection_success >> create_final_table_task >> create_staging_table_task)

    # Create transformation task for the specific data path
    # Task dependencies
    (
        create_staging_table_task
        >> download_task
        >> transformation_group
        >> merge_data_task
    )
