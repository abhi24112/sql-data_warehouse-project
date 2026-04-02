-- Date Exploration for tables

-- Finding the data of the first and last order
SELECT 
    MIN(order_date) AS minimum_order_date,
    MAX(order_date) AS maximum_order_date,
    DATEDIFF(YEAR,MIN(order_date), MAX(order_date)) AS year_difference
FROM gold.fact_sales;

-- Youngest and Oldest customer (Distribution of age)
SELECT 
    MIN(birthdate) AS oldest_cust,
    MAX(birthdate) AS youngest_cust,
    DATEDIFF(YEAR,MIN(birthdate), MAX(birthdate)) AS year_difference
FROM gold.dim_customers;


DECLARE @today_date DATE  = CAST('2002-11-24' AS DATE)
SELECT 
    age_category, 
    COUNT(*) AS 'no. of customers' 
FROM 
    (SELECT 
        birthdate,
        CASE 
            WHEN DATEDIFF(YEAR,birthdate,@today_date) <= 20  THEN 'young'  
            WHEN DATEDIFF(YEAR,birthdate,@today_date) > 20 AND DATEDIFF(YEAR,birthdate,@today_date) < 60 THEN 'mature'  
            ELSE  'Old'
        END AS age_category
    FROM gold.dim_customers)t
GROUP BY age_category
ORDER BY COUNT(*) DESC