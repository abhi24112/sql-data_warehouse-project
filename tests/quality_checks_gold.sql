SELECT 
* 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE c.customer_key IS NULL OR p.product_key IS NULL;

SELECT * FROM gold.fact_sales;
SELECT * FROM gold.dim_customers;
SELECT * FROM gold.dim_products;