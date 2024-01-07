from airflow import DAG, AirflowException
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import List
import requests
from requests import Response


def extract_data_from_api():
    interesting_dates: List[str] = ["2022-08-09", "2022-08-10", "2022-08-11"]
    for date in interesting_dates:
        raw_dir: str = f"C:\\tasks\\data_storage\\raw\\sales\\{date}"
        uri: str = "http://localhost:5000/"
        post_data = {"raw_dir": raw_dir, "date": date}
        response: Response = requests.post(uri, json=post_data)
        if (response.status_code == 500):
            raise AirflowException("external server error")


def convert_to_avro():
    interesting_dates: List[str] = ["2022-08-09", "2022-08-10", "2022-08-11"]
    uri: str = "http://localhost:5001"
    for date in interesting_dates:
        stg_dir: str = f"C:\\tasks\\data_storage\\stg\\sales\\{date}"
        raw_dir: str = f"C:\\tasks\\data_storage\\raw\\sales\\{date}"
        post_data = {"stg_dir": stg_dir, "raw_dir": raw_dir}
        response: Response = requests.post(uri, json=post_data)
        if (response.status_code == 500):
            raise AirflowException("external server error")


with DAG(dag_id="process_sales",
         start_date=datetime(2024, 1, 7),
         end_date=datetime(2024, 1, 12),

         schedule_interval=timedelta(minutes=2),
         catchup=False) as dag:
    task1 = PythonOperator(task_id="extract_data_from_api", python_callable=extract_data_from_api)
    task2 = PythonOperator(task_id="convert_to_avro", depends_on_past=True, python_callable=convert_to_avro)
task1 >> task2
