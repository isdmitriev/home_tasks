import os
import time
import requests

JOB1_PORT = 5000
JOB2_PORT = 5001


def run_job1():
    print("Starting job1:")
    resp = requests.post(
        "url=f'http://localhost:{JOB1_PORT}",
        json={
            "date": "2022-08-09",
            "dir_name": "C:\\tasks\\data_storage\\raw\\sales\\22-08-09",
        },
    )
    assert resp.status_code == 200
    print("job1 completed!")


def run_job2():
    print("Starting job2:")
    resp = requests.post(
        url=f"http://localhost:{JOB2_PORT}/",
        json={
            "raw_dir": "C:\\tasks\\data_storage\\raw\\sales\\22-08-09",
            "stg_dir": "C:\\tasks\\data_storage\\stg\\sales\\22-08-09",
        },
    )
    assert resp.status_code == 201
    print("job2 completed!")


if __name__ == "__main__":
    run_job1()
    time.sleep(3)
    run_job2()
