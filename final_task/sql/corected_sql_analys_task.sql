CREATE
TEMP FUNCTION GET_AGE_BY_BIRTH_DATE(birth_date DATE)
RETURNS INT64 AS (
  EXTRACT(YEAR FROM(CURRENT_DATE()))-EXTRACT(YEAR FROM birth_date)

  );



WITH sales_details_full AS (select us_prof.client_id                                  as client_id,
                                   silver.purchase_date,
                                   silver.product_name,
                                   silver.price,
                                   CONCAT(us_prof.first_name, " ", us_prof.last_name) as client,
                                   GET_AGE_BY_BIRTH_DATE(us_prof.birth_date)          as client_age,
                                   EXTRACT(DAY FROM silver.purchase_date)             as purchase_day,
                                   us_prof.state                                      as state
                            from `sales_dataset.user_profiles_enriched` as us_prof
                                     inner join
                                 `sales_dataset.silver` as silver on us_prof.client_id = silver.client_id),
     sales_details_filtered AS (select state, COUNT(*) as total_tv_purchases
                                from sales_details_full
                                where sales_details_full.client_age >= 20
                                  and sales_details_full.client_age <= 30
                                  and sales_details_full.purchase_day <= 10
                                  and sales_details_full.product_name = "TV"
                                group by state
                                order by total_tv_purchases desc)
select *
from sales_details_filtered
