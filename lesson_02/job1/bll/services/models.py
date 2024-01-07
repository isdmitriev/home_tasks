from pydantic import BaseModel
from typing_extensions import Literal, Self


class SaleData(BaseModel):
    client: str
    purchase_date: str
    product: str
    price: int
