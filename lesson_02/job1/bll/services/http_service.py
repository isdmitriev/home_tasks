import requests
from requests import Response

from typing import List
import json
import os
from lesson_02.job1.bll.services.models import SaleData


class HTTPRequestService:
    AUTH_TOKEN = os.environ.get("AUTH_TOKEN")

    def __init__(self):
        pass

    def get_sales_request(self, page: int) -> tuple[bool, List[SaleData] | None]:
        sales: List[SaleData] = []
        uri: str = f"https://fake-api-vycpfa6oca-uc.a.run.app/sales?date=2022-08-09&page={page}"
        request_headers = {"Authorization": self.AUTH_TOKEN}

        http_response: Response = requests.get(uri, headers=request_headers)
        if http_response.status_code == 200:
            response_data = http_response.json()
            for sale_data in response_data:
                sales.append(SaleData(**sale_data))
            return (True, sales)
        else:
            return (False, None)

    def get_request_response(self, page: int):
        request_headers = {"Authorization": self.AUTH_TOKEN}
        uri: str = f"https://fake-api-vycpfa6oca-uc.a.run.app/sales?date=2022-08-09&page={page}"
        http_response: Response = requests.get(uri, headers=request_headers)
        return http_response
