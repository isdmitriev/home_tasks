insert `sales_dataset.silver`(
  CustomerId,
  PurchaseDate,
  Product,
  Price
)
select CAST(CustomerId as INTEGER),CAST(REPLACE(REPLACE(PurchaseDate,"Aug","08"),"/","-") AS DATE),CAST(Product AS STRING),
CAST(REPLACE(REPLACE(Price, "$",""),"USD","") AS INTEGER) AS price
from `sales_dataset.bronze`;
