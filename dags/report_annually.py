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
    "report_annually",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    dagrun_timeout=timedelta(minutes=45),
) as dag:
    table_name = "trip_data_report_" + str(datetime.now().year)
    time_start = "{{ dag_run.conf.get('start_time', ts[:4] + '-01-01 00:00:00') }}"
    time_end = "{{ dag_run.conf.get('start_time', ts[:4] + '-12-31 23:59:59') }}"
    bq_create_trip_data_table = BigQueryInsertJobOperator(
        task_id="bq_create_trip_data_table",
        gcp_conn_id="gcp_connection",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{DATASET_ID}.{table_name}` AS
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
                        FROM
                            trip_data
                        WHERE pickup_datetime BETWEEN TIMESTAMP('{time_start}') AND TIMESTAMP('{time_end}') 
                        GROUP BY
                            1, 2, 3;
                    
                """,
                "useLegacySql": False,
            }
        },
        dag=dag,
    )
    # spark_submit = SparkSubmitOperator(
    #     task_id="transform_and_import_data",
    #     conn_id="spark_connection",
    #     application="/opt/airflow/dags/spark_chap05_transformation.py",
    #     application_args=[
    #         "--project-id",
    #         PROJECT_ID,
    #         "--dataset-id",
    #         DATASET_ID,
    #         "--start-time",
    #         time_start,
    #         "--end-time",
    #          time-end,
    #         "--dest-table",
    #          table_name
    #     ],
    #     conf={
    #         "spark.jars": f"{BQ_JAR},{GCS_JAR}",
    #         "spark.executor.memory": "4g",
    #         "spark.executor.cores": "2",
    #         "spark.driver.memory": "2g",
    #         "spark.hadoop.google.cloud.auth.service.account.enable": "true",
    #         "spark.hadoop.google.cloud.auth.service.account.json.keyfile": GCP_KEY_PATH,
    #         "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    #         "spark.hadoop.google.cloud.project.id": PROJECT_ID,
    #         "spark.bigquery.project": PROJECT_ID,
    #         "spark.bigquery.parentProject": PROJECT_ID,
    #     },
    #     env_vars={
    #         "GOOGLE_CLOUD_PROJECT": PROJECT_ID,
    #         "GCLOUD_PROJECT": PROJECT_ID,
    #         "GOOGLE_APPLICATION_CREDENTIALS": GCP_KEY_PATH,
    #     },
    #     dag=dag,
    # )
