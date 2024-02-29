insert `sales_dataset.silver`(client_id,
                                purchase_date,
                                product_name,
                                price)
select CAST(CustomerId as INTEGER)                                         as client_id,
       CAST(REPLACE(REPLACE(PurchaseDate, "Aug", "08"), "/", "-") AS DATE) as purchase_date,
       CAST(Product AS STRING) as product_name,
       CAST(REPLACE(REPLACE(Price, "$", ""), "USD", "") AS INTEGER)        AS price
from `sales_dataset.bronze`;
