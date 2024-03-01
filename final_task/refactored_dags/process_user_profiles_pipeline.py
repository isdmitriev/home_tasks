from airflow import DAG
from airflow import AirflowException
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import (

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


with DAG(
        dag_id="user_profiles_pipeline_task_solution",
        catchup=True,
        template_searchpath="/home/ilya/airflow/dags/sql",
) as dag:
    create_user_profies_table_task = PythonOperator(
        task_id="create_user_profiles_table", python_callable=create_user_profiles_table
    )
    restore_user_profiles_task = PythonOperator(
        task_id="restore_user_profiles",
        python_callable=restore_user_profiles_json_to_dataset,
    )
    merge_customers_task = BigQueryInsertJobOperator(
        task_id="merge_customers",
        configuration={
            "query": {
                "query": "{% include 'merge_customers.sql' %}",
                "useLegacySql": False,
            },
        },
    )

create_user_profies_table_task >> restore_user_profiles_task >> merge_customers_task
