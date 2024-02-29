from airflow import DAG
from airflow import AirflowException
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import (

    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
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


with DAG(
        dag_id="enrich_user_profiles_dag_solution",
        catchup=True,
        template_searchpath="/home/ilya/airflow/dags/sql",
) as dag:
    create_table_user_profiles_enriched_task = PythonOperator(
        task_id="create_enrich_table",
        python_callable=create_table_user_profiles_enriched,
    )
    restore_customers_data_to_gold_table = BigQueryInsertJobOperator(
        task_id="restore_data_to_gold_table",
        configuration={
            "query": {
                "query": "{% include 'customers_enriched_table.sql' %}",
                "useLegacySql": False,
            },
        },
    )

create_table_user_profiles_enriched_task >> restore_customers_data_to_gold_table
