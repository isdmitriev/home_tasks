from pydantic import BaseModel


class SaleData(BaseModel):
    client: str
    purchase_date: str
    product: str
    price: int
