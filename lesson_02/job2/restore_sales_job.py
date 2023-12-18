from lesson_02.job1.bll.services.models import SaleData
from lesson_02.job1.dal.file_storage_manager import FileStorageManager
from lesson_02.job1.dal.json_storage_manager import JSONFileService
import os
from typing import List


class RestoreSalesJob:
    file_storage_manager: FileStorageManager = FileStorageManager()
    json_service: JSONFileService = JSONFileService()

    def __init__(self):
        pass

    def restore_sales(self, raw_dir: str, stg_dir: str) -> None:
        self.file_storage_manager.create_directory(stg_dir)
        file_names: List[str] = os.listdir(raw_dir)
        for file_name in file_names:
            full_path = raw_dir + f"\\{file_name}"
            full_path_avro = stg_dir + "\\" + file_name.replace(".json", ".avro")
            data: List[SaleData] = self.json_service.read_sales_from_json(full_path)
            self.file_storage_manager.save_sales_to_avro(full_path_avro, data)
