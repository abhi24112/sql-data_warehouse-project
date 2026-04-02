-- Explore all countries our customers come from 
SELECT DISTINCT
    country,
    COUNT(*) AS 'no. of counts'
FROM gold.dim_customers
GROUP BY country
ORDER BY COUNT(*) DESC;

-- Explore all Categories "The major Divisions"
SELECT DISTINCT
    category, 
    COUNT(*) AS  'No. of counts'
FROM gold.dim_products
GROUP BY category
ORDER BY COUNT(*) DESC;

SELECT DISTINCT
    category, 
    subcategory,
    product_name
FROM gold.dim_products;
