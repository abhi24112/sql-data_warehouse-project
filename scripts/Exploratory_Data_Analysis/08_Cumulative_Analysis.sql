-- Calculate the total sales per month 
-- and the running total of sales over time 

SELECT
    DATETRUNC(YEAR, order_date) AS order_date,
    SUM(sales_amount) AS total_sales
FROM gold.fact_sales
WHERE order_date IS NOT NULL 
GROUP BY DATETRUNC(YEAR, order_date)

SELECT 
    order_date,
    total_sales,
    SUM(total_sales) OVER(ORDER BY order_date) AS running_total_sum,
    SUM(avg_sales) OVER(ORDER BY order_date) AS moving_avg_sales
FROM (
    SELECT
        DATETRUNC(YEAR, order_date) AS order_date,
        SUM(sales_amount) AS total_sales,
        AVG(sales_amount) AS avg_sales
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL 
    GROUP BY DATETRUNC(YEAR, order_date)
)t;
