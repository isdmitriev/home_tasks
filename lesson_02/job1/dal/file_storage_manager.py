from fastavro import writer, reader, parse_schema
from lesson_02.job1.bll.services.models import SaleData
from typing import List
import os

SALES_SCHEMA = {
    "doc": "A sales reading.",
    "name": "Sales",
    "namespace": "test",
    "type": "record",
    "fields": [
        {"name": "client", "type": "string"},
        {"name": "purchase_date", "type": "string"},
        {"name": "product", "type": "string"},
        {"name": "price", "type": "int"},
    ],
}
PARSED_SCHEMA = parse_schema(SALES_SCHEMA)


class FileStorageManager:
    def __init__(self):
        pass

    def save_sales_to_avro(self, file_path: str, sales_data: List[SaleData]) -> None:
        resords = []
        for sale_data in sales_data:
            resords.append(sale_data.model_dump())

        with open(file_path, "wb") as out:
            writer(out, PARSED_SCHEMA, resords)

    def read_sales_from_avro(self, file_path: str) -> List[SaleData]:
        result: List[SaleData] = []
        with open(file_path, "rb") as fo:
            for record in reader(fo):
                result.append(SaleData(**record))
        return result

    def create_directory(self, path: str) -> None:
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)

    def delete_directory(self, path: str) -> None:
        if os.path.exists(path):
            os.rmdir(path)

    def delete_files_from_directory(self, path: str) -> None:
        file_names: List[str] = os.listdir(path)
        for file_name in file_names:
            os.remove(path + "\\" + file_name)
