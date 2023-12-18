from lesson_02.job1.bll.services.http_service import HTTPRequestService
from lesson_02.job1.bll.services.models import SaleData
from typing import List
import unittest
from lesson_02.job1.dal.file_storage_manager import FileStorageManager
from lesson_02.job1.dal.json_storage_manager import JSONFileService
import os

from lesson_02.job1.main import app


class TestHTTPRequest(unittest.TestCase):
    http_request: HTTPRequestService = HTTPRequestService()
    file_storage_manager: FileStorageManager = FileStorageManager()
    service: JSONFileService = JSONFileService()

    def test_load_data(self):
        result: tuple[bool, List[SaleData]] = self.http_request.get_sales_request(1)
        data: List[SaleData] = result[0]

        self.assertTrue(isinstance(data, List))

    def test_job1(self):
        jb1 = app.test_client()
        post_data = {
            "raw_dir": "C:\\tasks\\data_storage\\raw\\sales\\22-08-09",
            "date": "2022-08-09",
        }
        response = jb1.post("/", json=post_data)
        self.assertTrue(response.status_code == 200)

    def test_job2(self):
        jb1 = app.test_client()
        post_data = {
            "raw_dir": "C:\\tasks\\data_storage\\raw\\sales\\22-08-09",
            "stg_dir": "C:\\tasks\\data_storage\\stg\\sales\\22-08-09",
        }
        response = jb1.post("/", json=post_data)
        self.assertTrue(response.status_code == 200)

    def test_load_from_json_file(self):
        data = self.service.read_sales_from_json()
        self.assertTrue(isinstance(data, List))

    def test_load_from_avro(self):
        data: List[SaleData] = self.file_storage_manager.read_sales_from_avro(
            "sales.avro"
        )

        self.assertTrue(isinstance(data, List))

    def test_create_directory(self):
        self.file_storage_manager.create_directory("C:\\tasks\\data_storage\\sales")
        self.assertTrue(os.path.exists("C:\\tasks\\data_storage\\sales"))
        self.file_storage_manager.delete_directory("C:\\tasksask\\data_storage\\sales")
        self.assertFalse(os.path.exists("C:\\tasksask\\data_storage\\Sales"))
