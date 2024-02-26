CREATE TEMP FUNCTION GETLASTNAME(s STRING)
RETURNS STRING AS(

SUBSTR(s,STRPOS(s," "),LENGTH(s)-1)

);
CREATE TEMP FUNCTION GETFIRSTNAME(s STRING)
RETURNS STRING AS(

SUBSTR(s,0,STRPOS(s," ")-1)
);
MERGE `sales_dataset.customers_silver` as customers
USING `sales_dataset.user_profiles` as user_prof
ON user_prof.email=customers.email
WHEN MATCHED THEN UPDATE SET
customers.state=user_prof.state
WHEN NOT MATCHED BY Source THEN DELETE;
