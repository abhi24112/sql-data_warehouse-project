-- Which Category contribute the most to overall sales?
WITH category_contribution AS 
(
    SELECT 
        p.category,
        SUM(fs.sales_amount) AS total_sales
    FROM gold.fact_sales fs
    LEFT JOIN gold.dim_products p
    ON p.product_key = fs.product_key
    GROUP BY p.category
)
SELECT
    category,
    total_sales,
    SUM(total_sales) OVER() AS overall_total,
    concat(ROUND((CAST(total_sales AS FLOAT)/ SUM(total_sales) OVER()) * 100, 2), '%') AS percentage_pf_total
FROM category_contribution

