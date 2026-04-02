/* Analyze the yearly performance of products by comparing their sales 
to both the average sales performance of the product and the previous year's sales */
WITH yearly_product_sales AS (
    SELECT 
        YEAR(fs.order_date) AS order_year,
        p.product_name AS product_name,
        SUM(fs.sales_amount) AS current_sales
    FROM gold.fact_sales fs
    LEFT JOIN gold.dim_products p
    ON p.product_key = fs.product_key
    WHERE fs.order_date IS NOT NULL 
    GROUP BY order_date, product_name
)
SELECT
    order_year,
    product_name,
    current_sales,
    AVG(current_sales) OVER(PARTITION BY product_name) AS avg_sales,
    current_sales - AVG(current_sales) OVER(PARTITION BY product_name) AS diff_avg,
    CASE 
        WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name) > 0 THEN  'Above Avg'
        WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name) < 0 THEN  'Below Avg'
        ELSE  'Avg'
    END AS avg_change,
    -- Year over Year analysis
    LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS py_sales,
    current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS diff_py_sales,
    CASE 
        WHEN current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) > 0 THEN  'Increase'
        WHEN current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) < 0 THEN  'Decreased'
        ELSE  'No Change'
    END AS py_change
FROM yearly_product_sales
ORDER BY product_name, order_year;