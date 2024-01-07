from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from typing import List
import requests
from requests import Response


def extract_data_from_api():
    interesting_dates: List[str] = ["2022-08-09", "2022-08-10", "2022-08-11"]
    for date in interesting_dates:
        raw_dir: str = f"C:\\tasks\\data_storage\\raw\\sales\\{date}"
        uri: str = "http://localhost:5000"
        post_data = {"raw_dir": raw_dir, "date": date}
        response: Response = requests.post(uri, json=post_data)


def convert_to_avro():
    pass


with DAG(dag_id="process_sales",
         start_date=datetime(2021, 1, 1),
         schedule_interval="@hourly",
         catchup=False) as dag:
    task1 = PythonOperator(task_id="extract_data_from_api", python_callable=extract_data_from_api)
    task2 = PythonOperator(task_id="convert_to_avro", python_callable=convert_to_avro)
task1 >> task2
