from lesson_02.job1.bll.services.models import SaleData

from lesson_02.job1.dal.json_storage_manager import JSONFileService
from lesson_02.job1.dal.file_storage_manager import FileStorageManager
from typing import List


class FilesService:
    json_manager: JSONFileService = JSONFileService()
    files_manager: FileStorageManager = FileStorageManager()

    def __init__(self):
        pass

    def save_data_to_json(
        self, full_file_name: str, sales_data: List[SaleData]
    ) -> None:
        self.json_manager.save_sales_to_json(full_file_name, sales_data)

    def prepare_file_system(self, raw_dir: str) -> None:
        self.files_manager.create_directory(raw_dir)
        self.files_manager.delete_files_from_directory(raw_dir)
