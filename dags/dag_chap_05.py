# link kestra https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/02-workflow-orchestration/flows/06_gcp_taxi.yaml
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import logging
from airflow.models import Variable

logger = logging.getLogger(__name__)
default_args = {
    "owner": "airflow",
    "retries": 1,
}

BUCKET_NAME = Variable.get("GCS_BUCKET_NAME", default_var=None)
PROJECT_ID = Variable.get("GCP_PROJECT_ID", default_var=None)
DATASET_ID = Variable.get("BIGQUERY_DATASET_ID", default_var=None)
GCP_KEY_PATH = "/keys/gcp_cred.json"
JARS_PATH = "/opt/airflow/jars"
BQ_JAR = f"{JARS_PATH}/spark-bigquery-latest_2.12.jar"
GCS_JAR = f"{JARS_PATH}/gcs-connector-hadoop3-latest.jar"
with DAG(
    "import_data_to_trip_data",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    dagrun_timeout=timedelta(minutes=45),
) as dag:
    bq_create_trip_data_table = BigQueryInsertJobOperator(
        task_id="bq_create_trip_data_table",
        gcp_conn_id="gcp_connection",
        configuration={
            "query": {
                "query": f"""
                    CREATE TABLE IF NOT EXISTS `{DATASET_ID}.trip_data` 
                    (
                        VendorID STRING,
                        pickup_datetime TIMESTAMP,
                        dropoff_datetime TIMESTAMP,
                        store_and_fwd_flag STRING,
                        RatecodeID STRING,
                        PULocationID STRING,
                        DOLocationID STRING,
                        passenger_count INTEGER,
                        trip_distance NUMERIC,
                        fare_amount NUMERIC,
                        extra NUMERIC,
                        mta_tax NUMERIC,
                        tip_amount NUMERIC,
                        tolls_amount NUMERIC,
                        improvement_surcharge NUMERIC,
                        total_amount NUMERIC,
                        payment_type INT64,
                        congestion_surcharge NUMERIC,
                        service_type STRING
                    );
                """,
                "useLegacySql": False,
            }
        },
        dag=dag,
    )
    spark_submit = SparkSubmitOperator(
        task_id="transform_and_import_data",
        conn_id="spark_connection",
        application="/opt/airflow/dags/spark_chap05.py",
        application_args=[
            "--project-id",
            PROJECT_ID,
            "--dataset-id",
            DATASET_ID,
        ],
        conf={
            "spark.jars": f"{BQ_JAR},{GCS_JAR}",
            "spark.executor.memory": "4g",
            "spark.executor.cores": "2",
            "spark.driver.memory": "2g",
            "spark.hadoop.google.cloud.auth.service.account.enable": "true",
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile": GCP_KEY_PATH,
            "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.google.cloud.project.id": PROJECT_ID,
            "spark.bigquery.project": PROJECT_ID,
            "spark.bigquery.parentProject": PROJECT_ID,
        },
        env_vars={
            "GOOGLE_CLOUD_PROJECT": PROJECT_ID,
            "GCLOUD_PROJECT": PROJECT_ID,
            "GOOGLE_APPLICATION_CREDENTIALS": GCP_KEY_PATH,
        },
        dag=dag,
    )
