from airflow import DAG
from airflow import AirflowException
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import List
from airflow.models import Variable

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

DATA_SET_ID = Variable.get("dataSET")
PROGECT_ID = Variable.get("progectId")


def create_customers_table_bronze(**context):
    try:
        schema_fields = [
            {"name": "Id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "FirstName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LastName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "RegistrationDate", "type": "STRING", "mode": "NULLABLE"},
            {"name": "State", "type": "STRING", "mode": "NULLABLE"},
        ]

        create_customers_table_operator: BigQueryCreateEmptyTableOperator = (
            BigQueryCreateEmptyTableOperator(
                task_id="create_bronze_table",
                table_id="customers_bronze",
                dataset_id=DATA_SET_ID,
                schema_fields=schema_fields,
            )
        )
        create_customers_table_operator.execute(context=context)

    except:
        raise AirflowException("error ")


def create_customers_table_silver(**context):
    try:
        schema_fields = [
            {"name": "client_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "first_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "last_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "registration_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "state", "type": "STRING", "mode": "NULLABLE"},
        ]
        create_customers_table_operator: BigQueryCreateEmptyTableOperator = (
            BigQueryCreateEmptyTableOperator(
                task_id="create_silver_table",
                dataset_id="sales_dataset",
                table_id="customers_silver",
                schema_fields=schema_fields,
            )
        )
        create_customers_table_operator.execute(context=context)
    except:
        raise AirflowException("error in table creation")


def restore_customers_to_big_query_task(**context):
    try:
        gcs_hook: GCSHook = GCSHook()
        list_customers_csv: List[str] = gcs_hook.list(
            bucket_name="customers_ildmi_data", prefix="customers"
        )
        schema_fields = [
            {"name": "Id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "FirstName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LastName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "RegistrationDate", "type": "STRING", "mode": "NULLABLE"},
            {"name": "State", "type": "STRING", "mode": "NULLABLE"},
        ]
        gcs_to_bigquery_operator: GCSToBigQueryOperator = GCSToBigQueryOperator(
            task_id="restore_customers",
            bucket="customers_ildmi_data",
            destination_project_dataset_table="sales_dataset.customers_bronze",
            source_objects=list_customers_csv,
            project_id="de2024-ilya-dmitriev",
            source_format="CSV",
            schema_fields=schema_fields,
            write_disposition="WRITE_TRUNCATE",
        )
        gcs_to_bigquery_operator.execute(context=context)
    except:
        raise AirflowException("error data restoring")


with DAG(
    dag_id="proces_customers_pipeline_solution",
    start_date=datetime(2024, 2, 27),
    schedule_interval="@daily",
    catchup=True,
    template_searchpath="/home/ilya/airflow/dags/sql",
) as dag:
    truncate_silver_task = BigQueryInsertJobOperator(
        task_id="truncate_table",
        configuration={
            "query": {
                "query": "{% include 'truncate_customers_silver.sql' %}",
                "useLegacySql": False,
            },
        },
    )
    restore_customers_from_bronze_to_silver_task = BigQueryInsertJobOperator(
        task_id="restore_customers",
        configuration={
            "query": {
                "query": "{% include 'restore_customers.sql' %}",
                "useLegacySql": False,
            },
        },
    )
    customers_bronze_table_create_task = PythonOperator(
        task_id="create_bronze_table", python_callable=create_customers_table_bronze
    )
    customers_silver_table_create_task = PythonOperator(
        task_id="create_silver_table", python_callable=create_customers_table_silver
    )
    customers_restore_bronze_table_task = PythonOperator(
        task_id="restore_customers_to_bronze",
        python_callable=restore_customers_to_big_query_task,
    )

(
    customers_bronze_table_create_task
    >> customers_silver_table_create_task
    >> truncate_silver_task
    >> customers_restore_bronze_table_task
    >> restore_customers_from_bronze_to_silver_task
)