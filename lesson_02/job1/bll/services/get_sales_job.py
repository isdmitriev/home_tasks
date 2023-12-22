from lesson_02.job1.bll.services.models import SaleData
from lesson_02.job1.bll.services.http_service import HTTPRequestService
from lesson_02.job1.dal.file_storage_manager import FileStorageManager
from typing import List
from lesson_02.job1.bll.services.files_service import FilesService


class SalesJob:
    http_service: HTTPRequestService = HTTPRequestService()
    file_service: FilesService = FilesService()

    def __init__(self):
        pass

    def get_sales_job(self, raw_dir: str, date_sales: str) -> None:
        self.file_service.prepare_file_system(raw_dir)
        for page in range(1, 4):
            request_data: tuple[
                bool, List[SaleData | None]
            ] = self.http_service.get_sales_request(page)

            if request_data[0] == True:
                data: List[SaleData] = request_data[1]

                file_name: str = raw_dir + f"\sales_{date_sales}_{page}.json"

                self.file_service.save_data_to_json(file_name, data)
            else:
                break
