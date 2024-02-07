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
)


def restore_sales_to_gcs(**context):
    try:
        execution_date: datetime = context.get("execution_date")
        ex_date = execution_date.date()
        ex_year = ex_date.year
        ex_month = ex_date.month
        ex_day = ex_date.day
        logging.info(ex_date)
        logging.info(ex_day)
        logging.info(ex_month)
        logging.info(ex_date.year)
        src_directory: str = f"home/ilya/data_storage/raw/sales/{ex_date}/"

        bucket: str = "ildmi_1988_airflow_bucket"
        list_files_to_gcs: List[str] = []
        for file_name in os.listdir(src_directory):
            list_files_to_gcs.append(src_directory + file_name)
        bucket_folder: str = f"src1/sales/v1/{ex_year}/{ex_month}/{ex_day}/"
        gcs_task = LocalFilesystemToGCSOperator(
            task_id="gcs_task", src=list_files_to_gcs, bucket=bucket, dst=bucket_folder
        )
        gcs_task.execute(context=context)
    except:
        raise AirflowException("exception in restore data")


with DAG(
    dag_id="gcs_restore_sales_task_2",
    start_date=datetime(2022, 8, 9),
    end_date=datetime(2022, 8, 11),
    schedule_interval="0 1 * * *",
    catchup=True,
) as dag:
    restore_task = PythonOperator(
        task_id="restore_locale_sales_to_gcs", python_callable=restore_sales_to_gcs
    )
