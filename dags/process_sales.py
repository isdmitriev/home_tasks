from airflow import DAG, AirflowException
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import List
import requests
from requests import Response


def extract_data_from_api(concrete_date: str | None) -> None:
    uri: str = "http://host.docker.internal:5000/"
    if (concrete_date is None):
        interesting_dates: List[str] = ["2022-08-09", "2022-08-10", "2022-08-11"]
        for date in interesting_dates:
            raw_dir: str = f"C:\\tasks\\data_storage\\raw\\sales\\{date}"
            uri: str = "http://host.docker.internal:5000/"
            post_data = {"raw_dir": raw_dir, "date": date}
            response: Response = requests.post(uri, json=post_data)
            if (response.status_code == 500):
                raise AirflowException("external server error")
    else:
        raw_dir: str = f"C:\\tasks\\data_storage\\raw\\sales\\{concrete_date}"
        post_data = {"raw_dir": raw_dir, "date": concrete_date}
        response: Response = requests.post(uri, json=post_data)
        if (response.status_code == 500):
            raise AirflowException("external server error")


def convert_to_avro(concrete_date: str | None) -> None:
    interesting_dates: List[str] = ["2022-08-09", "2022-08-10", "2022-08-11"]
    uri: str = "http://host.docker.internal:5001/"
    if (concrete_date is None):
        for date in interesting_dates:
            stg_dir: str = f"C:\\tasks\\data_storage\\stg\\sales\\{date}"
            raw_dir: str = f"C:\\tasks\\data_storage\\raw\\sales\\{date}"
            post_data = {"stg_dir": stg_dir, "raw_dir": raw_dir}
            response: Response = requests.post(uri, json=post_data)
            if (response.status_code == 500):
                raise AirflowException("external server error")

    else:
        stg_dir: str = f"C:\\tasks\\data_storage\\stg\\sales\\{concrete_date}"
        raw_dir: str = f"C:\\tasks\\data_storage\\raw\\sales\\{concrete_date}"

        post_data = {"stg_dir": stg_dir, "raw_dir": raw_dir}
        response: Response = requests.post(uri, json=post_data)
        if (response.status_code == 500):
            raise AirflowException("external server error")


with DAG(dag_id="process_sales",
         start_date=datetime(2024, 1, 9),

         schedule_interval="0 1 * * *",
         catchup=True) as dag:
    task1 = PythonOperator(task_id="extract_data_from_api", python_callable=extract_data_from_api,
                           op_args={'concrete_date': None})
    task2 = PythonOperator(task_id="convert_to_avro", python_callable=convert_to_avro, p_args={'concrete_date': None})
task1 >> task2
