-- Find the total sales
SELECT 
    SUM(sales_amount) AS Total_sales 
FROM gold.fact_sales;

-- find how many items are sold for each product
SELECT 
    fs.product_key,
    p.product_name,
    COUNT(*)
FROM gold.fact_sales fs
LEFT JOIN gold.dim_products p
ON fs.product_key = p.product_key
GROUP BY fs.product_key, p.product_name

-- Find the average selling price
SELECT 
    AVG(sales_amount) AS Average_sales 
FROM gold.fact_sales;

-- Find the total number of orders
SELECT 
    COUNT(order_number) AS total_orders
FROM gold.fact_sales;
SELECT 
    COUNT(DISTINCT order_number) AS total_orders
FROM gold.fact_sales;

-- Find the total number of products
SELECT 
    COUNT(product_key) AS total_products
FROM gold.dim_products;
SELECT 
    COUNT(DISTINCT product_key) AS total_products
FROM gold.dim_products;

-- Find the total number of customers
SELECT 
    COUNT(customer_key) AS total_customers
FROM gold.dim_customers;
SELECT 
    COUNT(DISTINCT customer_key) AS total_customers
FROM gold.dim_customers;

-- Find the total number of customers that has place an order
SELECT 
    COUNT(customer_key) AS total_customers
FROM gold.fact_sales;
SELECT 
    COUNT(DISTINCT customer_key) AS total_customers
FROM gold.fact_sales;

-- Generare a Report taht shows all key metrics of the business
SELECT 'Total Sales' AS measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity', SUM(quantity) FROM gold.fact_sales
UNION ALL
SELECT 'Average Price', AVG(price) FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Orders', COUNT(DISTINCT order_number) FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Product', COUNT(product_name) FROM gold.dim_products
UNION ALL
SELECT 'Total Nr. Customers', COUNT(customer_key) FROM gold.dim_customers
