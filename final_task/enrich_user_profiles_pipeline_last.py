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


def create_table_user_profiles_enriched(**context):
    try:
        schema_fields = [
            {"name": "client_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "registration_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "first_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "last_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "state", "type": "STRING", "mode": "NULLABLE"},
            {"name": "birth_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "phone_number", "type": "STRING", "mode": "NULLABLE"},
            {"name": "full_name", "type": "STRING", "mode": "NULLABLE"},
        ]
        create_gold_table_operator: BigQueryCreateEmptyTableOperator = (
            BigQueryCreateEmptyTableOperator(
                dataset_id="sales_dataset",
                table_id="user_profiles_enriched",
                schema_fields=schema_fields,
                task_id="create_table_gold",
            )
        )
        create_gold_table_operator.execute(context=context)
    except:
        raise AirflowException("error in table creation")


def restore_customers_data_to_gold_table(**context):
    try:
        sql_restore_query = """ truncate table sales_dataset.user_profiles_enriched;  insert `sales_dataset.user_profiles_enriched`(
  client_id,
  registration_date,
  first_name,
  last_name,
  email,
  state,
  birth_date,
  phone_number,
  full_name
)

select CAST(customers.client_id AS INTEGER) as client_id,CAST(customers.registration_date AS DATE) as registration_date,
CAST(customers.first_name AS STRING) AS first_name,CAST(customers.last_name AS STRING) AS last_name,
CAST(customers.email AS STRING) as email,CAST(customers.state AS STRING) AS state,
CAST(us_prof.birth_date AS DATE) as birth_date,CAST(us_prof.phone_number AS STRING) AS phone_number,
CAST(us_prof.full_name AS STRING) as full_name
from `sales_dataset.customers_silver` as customers inner join `sales_dataset.user_profiles` as us_prof
on customers.email=us_prof.email;"""
        restore_data_job: BigQueryInsertJobOperator = BigQueryInsertJobOperator(
            task_id="restore_data_task",
            configuration={
                "query": {
                    "query": sql_restore_query,
                    "useLegacySql": False,
                }
            },
        )
        restore_data_job.execute(context=context)
    except:
        raise AirflowException("error in data restoring")


with DAG(
    dag_id="enrich_user_profiles_dag",
    catchup=True,
) as dag:
    create_table_user_profiles_enriched_task = PythonOperator(
        task_id="create_enrich_table",
        python_callable=create_table_user_profiles_enriched,
    )
    restore_customers_data_to_gold_table = PythonOperator(
        task_id="restore_customers_to_gold_table",
        python_callable=restore_customers_data_to_gold_table,
    )

create_table_user_profiles_enriched_task >> restore_customers_data_to_gold_table
