TRUNCATE TABLE sales_dataset.silver;
insert `sales_dataset.customers_silver`(
  client_id,
  registration_date,
  first_name,
  last_name,
  email,
  state
)
select distinct CAST(Id as INTEGER)AS  client_id,CAST(RegistrationDate AS DATE) AS registration_date,
CAST(FirstName AS STRING) AS first_name,CAST(LastName AS STRING) AS last_name,CAST(Email AS STRING)  as email,
CAST(State AS STRING) as state

from `sales_dataset.customers_bronze`;
