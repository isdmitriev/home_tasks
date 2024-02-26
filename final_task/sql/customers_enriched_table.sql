

insert `sales_dataset.user_profiles_enriched`(
  client_id,
  registration_date,
  first_name,
  last_name,
  email,
  state,
  birth_date,
  phone_number,
  full_name
)

select CAST(customers.client_id AS INTEGER) as client_id,CAST(customers.registration_date AS DATE) as registration_date,
CAST(customers.first_name AS STRING) AS first_name,CAST(customers.last_name AS STRING) AS last_name,
CAST(customers.email AS STRING) as email,CAST(customers.state AS STRING) AS state,
CAST(us_prof.birth_date AS DATE) as birth_date,CAST(us_prof.phone_number AS STRING) AS phone_number,
CAST(us_prof.full_name AS STRING) as full_name
from `sales_dataset.customers_silver` as customers inner join `sales_dataset.user_profiles` as us_prof
on customers.email=us_prof.email;
