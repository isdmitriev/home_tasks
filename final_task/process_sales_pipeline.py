import os

from airflow import DAG
from airflow import AirflowException
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import List
import requests
from requests import Response
import logging
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
    GCSHook,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)


def create_gc_bigquery_dataset(**context):
    try:
        create_dataset_operator: BigQueryCreateEmptyDatasetOperator = (
            BigQueryCreateEmptyDatasetOperator(
                dataset_id="sales_dataset",
                task_id="create_dataset",
            )
        )
        create_dataset_operator.execute(context=context)
    except:
        raise AirflowException("DataSet create error")


def create_sales_dataset_bronze_table(**context):
    try:
        schema_fields = [
            {"name": "CustomerId", "type": "STRING", "mode": "NULLABLE"},
            {"name": "PurchaseDate", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Product", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Price", "type": "STRING", "mode": "NULLABLE"},
        ]
        create_bronze_table_operator: BigQueryCreateEmptyTableOperator = (
            BigQueryCreateEmptyTableOperator(
                task_id="create_table",
                table_id="bronze",
                dataset_id="sales_dataset",
                schema_fields=schema_fields,
            )
        )
        create_bronze_table_operator.execute(context=context)
    except:
        raise AirflowException("error in creation table")


def create_sales_dataset_silver_table(**context):
    try:
        schema_fields = [
            {"name": "client_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "purchase_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "price", "type": "INTEGER", "mode": "NULLABLE"},
        ]
        create_silver_table_operator: BigQueryCreateEmptyTableOperator = (
            BigQueryCreateEmptyTableOperator(
                dataset_id="sales_dataset",
                table_id="silver",
                schema_fields=schema_fields,
                task_id="create_table_silver",
                time_partitioning={"field": "purchase_date", "type": "DAY"},
            )
        )
        create_silver_table_operator.execute(context=context)
    except:
        raise AirflowException("Error in table creation")


def restore_sales_from_bronze_to_silver(**context):
    query = """ insert `sales_dataset.silver`(
  client_id,
  purchase_date,
  product_name,
  price
)
select CAST(CustomerId as INTEGER) AS client_id,CAST(REPLACE(REPLACE(PurchaseDate,"Aug","08"),"/","-") AS DATE) AS purchase_date,CAST(Product AS STRING) AS product_name,
CAST(REPLACE(REPLACE(Price, "$",""),"USD","") AS INTEGER) AS price
from `sales_dataset.bronze`;
    """
    restore_data_job: BigQueryInsertJobOperator = BigQueryInsertJobOperator(
        task_id="restore_data_between_tables",
        configuration={
            "query": {
                "query": query,
                "useLegacySql": False,
            }
        },
    )
    restore_data_job.execute(context=context)


def restore_sales_to_big_query(**context):
    gcs_hook: GCSHook = GCSHook()

    bucket_name: str = "sales_ildmi_dataset"
    list_data_directories: List[str] = gcs_hook.list(
        bucket_name="sales_ildmi_dataset", prefix="sales"
    )

    schema_fields = [
        {"name": "CustomerId", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "PurchaseDate", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Product", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Price", "type": "STRING", "mode": "NULLABLE"},
    ]

    restore_task: GCSToBigQueryOperator = GCSToBigQueryOperator(
        bucket=bucket_name,
        source_objects=list_data_directories,
        project_id="de2024-ilya-dmitriev",
        destination_project_dataset_table="sales_dataset.bronze",
        task_id="restore_task_bronze",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=schema_fields,
        source_format="CSV",
    )
    restore_task.execute(context=context)


with DAG(
    dag_id="process_sales_pipeline_dag",
    start_date=datetime(2024, 2, 27),
    schedule_interval="@daily",
    catchup=True,
    template_searchpath="/home/ilya/airflow/dags",
) as dag:
    create_dataset_task = PythonOperator(
        task_id="create_dataset", python_callable=create_gc_bigquery_dataset
    )
    create_table_task = PythonOperator(
        task_id="create_table", python_callable=create_sales_dataset_bronze_table
    )
    restore_task_bronze = PythonOperator(
        task_id="restore_task_bronze", python_callable=restore_sales_to_big_query
    )
    create_table_silver = PythonOperator(
        task_id="create_table_silver", python_callable=create_sales_dataset_silver_table
    )
    restore_tables_data = PythonOperator(
        task_id="restore_data_between_tables",
        python_callable=restore_sales_from_bronze_to_silver,
    )

(
    create_dataset_task
    >> create_table_task
    >> restore_task_bronze
    >> create_table_silver
    >> restore_tables_data
)
