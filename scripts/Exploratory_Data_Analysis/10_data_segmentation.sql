/*Segment products into cost ranges and 
count how many products fall into each segment*/
WITH product_segment AS (    
    SELECT 
        product_key,
        cost,
        CASE 
            WHEN cost < 100 THEN 'Below 100' 
            WHEN cost BETWEEN 100 AND 500 THEN '100-500' 
            WHEN cost BETWEEN 500 AND 1000 THEN '500-1000' 
            ELSE  'Above 1000'
        END AS cost_range
    FROM gold.dim_products
)
SELECT
    cost_range,
    COUNT(*) AS 'No. of Products'
FROM product_segment
GROUP BY cost_range
ORDER BY [NO. OF Products] DESC;


/*Group customers into three segments based on their spending behavior:
	- VIP: Customers with at least 12 months of history and spending more than €5,000.
	- Regular: Customers with at least 12 months of history but spending €5,000 or less.
	- New: Customers with a lifespan less than 12 months.
And find the total number of customers by each group
*/

WITH customer_category AS (
    SELECT 
        customer_key,
        SUM(sales_amount) AS total_spending,
        MIN(order_date) AS first_order,
        MAX(order_date) AS last_order,
        DATEDIFF(MONTH,MIN(order_date), MAX(order_date)) AS 'lifespan(month)'
    FROM gold.fact_sales
    GROUP BY customer_key
)
SELECT 
customer_segment,
COUNT(customer_key) AS 'Total Customers'
FROM (
    SELECT 
        customer_key,
        total_spending,
        [lifespan(MONTH)],
        CASE 
            WHEN [lifespan(MONTH)] >= 12 AND total_spending > 5000 THEN 'VIP'  
            WHEN [lifespan(MONTH)] >= 12 AND total_spending <= 5000 THEN 'Regular'  
            ELSE 'New'   
        END AS customer_segment
    FROM customer_category
)t
GROUP BY customer_segment;

