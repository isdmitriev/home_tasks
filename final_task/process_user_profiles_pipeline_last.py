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


def create_user_profiles_table(**context):
    try:
        user_profiles_schema = [
            {"name": "email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "full_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "state", "type": "STRING", "mode": "NULLABLE"},
            {"name": "birth_date", "type": "STRING", "mode": "NULLABLE"},
            {"name": "phone_number", "type": "STRING", "mode": "NULLABLE"},
        ]
        create_table_operator: BigQueryCreateEmptyTableOperator = (
            BigQueryCreateEmptyTableOperator(
                task_id="create_user_profiles_table",
                schema_fields=user_profiles_schema,
                dataset_id="sales_dataset",
                table_id="user_profiles",
            )
        )
        create_table_operator.execute(context=context)
    except:
        raise AirflowException("error in user_profiles creation")


def restore_user_profiles_json_to_dataset(**context):
    try:
        user_profiles_schema = [
            {"name": "email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "full_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "state", "type": "STRING", "mode": "NULLABLE"},
            {"name": "birth_date", "type": "STRING", "mode": "NULLABLE"},
            {"name": "phone_number", "type": "STRING", "mode": "NULLABLE"},
        ]

        restore_job: GCSToBigQueryOperator = GCSToBigQueryOperator(
            task_id="restore_task",
            bucket="user_profiles_ildmi_data",
            destination_project_dataset_table="sales_dataset.user_profiles",
            source_objects=["user_profiles/user_profiles.json"],
            source_format="NEWLINE_DELIMITED_JSON",
            schema_fields=user_profiles_schema,
            write_disposition="WRITE_TRUNCATE",
        )
        restore_job.execute(context=context)
    except:
        raise AirflowException("error in data restoring")


def merge_user_profiles_to_customers_silver(**context):
    try:
        merge_sql_query = """CREATE TEMP FUNCTION GETLASTNAME(s STRING)
    RETURNS STRING AS (
    SUBSTR(s, STRPOS(s, " "), LENGTH(s) - 1)
    );
CREATE TEMP FUNCTION GETFIRSTNAME(s STRING)
    RETURNS STRING AS (
    SUBSTR(s, 0, STRPOS(s, " ") - 1)
    );
MERGE `sales_dataset.customers_silver` as customers
    USING `sales_dataset.user_profiles` as user_prof
    ON user_prof.email = customers.email
    WHEN MATCHED THEN UPDATE SET
        customers.state = user_prof.state,
        customers.first_name = GETFIRSTNAME(user_prof.full_name),
        customers.last_name = GETLASTNAME(user_prof.full_name)
    WHEN NOT MATCHED BY Source THEN DELETE;"""
        sql_merge_job: BigQueryInsertJobOperator = BigQueryInsertJobOperator(
            task_id="merge_customers_table",
            configuration={
                "query": {
                    "query": merge_sql_query,
                    "useLegacySql": False,
                }
            },
        )
        sql_merge_job.execute(context=context)

    except:
        raise AirflowException("error in merging tables")


with DAG(
    dag_id="user_profiles_pipeline_dag",
    catchup=True,
) as dag:
    create_user_profies_table_task = PythonOperator(
        task_id="create_user_profiles_table", python_callable=create_user_profiles_table
    )
    restore_user_profiles_task = PythonOperator(
        task_id="restore_user_profiles",
        python_callable=restore_user_profiles_json_to_dataset,
    )
    merge_customers_task = PythonOperator(
        task_id="merge_customers",
        python_callable=merge_user_profiles_to_customers_silver,
    )
create_user_profies_table_task >> restore_user_profiles_task >> merge_customers_task
