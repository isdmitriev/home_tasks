from pydantic import BaseModel
from typing import List
import json
import os
from lesson_02.job1.bll.services.models import SaleData


class JSONFileService:
    def __init__(self):
        pass

    def save_sales_to_json(self, file_path: str, sales: List[SaleData]) -> None:
        sales_for_json = []
        for sale in sales:
            sales_for_json.append(sale.model_dump())
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(sales_for_json, f, ensure_ascii=False, indent=4)

    def read_sales_from_json(self, file_path: str) -> List[SaleData]:
        result: List[SaleData] = []
        with open(file_path, "r", encoding="utf-8") as f:
            sales_data = json.load(f)
        for sale_data in sales_data:
            result.append(SaleData(**sale_data))
        return result
