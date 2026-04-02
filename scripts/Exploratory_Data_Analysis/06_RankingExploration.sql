-- Which 5 products generate the hightest revenue?
SELECT * FROM 
    (
        SELECT 
            p.product_name,
            SUM(fs.sales_amount) AS revenue,
            ROW_NUMBER() OVER(ORDER BY SUM(fs.sales_amount) DESC) AS ranking
        FROM gold.fact_sales fs 
        LEFT JOIN gold.dim_products p
        ON p.product_key = fs.product_key
        GROUP BY p.product_name
    )s
WHERE ranking <= 5;


-- What are the 5 worst performing products in terms of sales?
SELECT * FROM 
    (
        SELECT 
            p.product_name,
            SUM(fs.sales_amount) AS revenue,
            ROW_NUMBER() OVER(ORDER BY SUM(fs.sales_amount)) AS ranking
        FROM gold.fact_sales fs 
        LEFT JOIN gold.dim_products p
        ON p.product_key = fs.product_key
        GROUP BY p.product_name
    )s
WHERE ranking <= 5;

-- The 3 customers with the fewest orders placed
SELECT TOP 3
    c.customer_key,
    c.first_name,
    c.last_name,
    COUNT(DISTINCT order_number) AS total_orders
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
GROUP BY 
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_orders ;


