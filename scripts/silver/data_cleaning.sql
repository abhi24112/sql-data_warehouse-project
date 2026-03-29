
-- ============================================================
-- Cleanding bronze.crm_cust_info
-- ============================================================
-- Cheacking for NULLs or Duplicate in Primary Key
-- Expectation: No Result

SELECT
    cst_id,
    COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id is NULL;

-- Cheacking for unwanted Spaces.
-- Expectation : No Result
SELECT 
*
FROM (
    SELECT 
        cst_key,
        LEN(cst_key) - LEN(TRIM(cst_key)) as unwanted_space_count
    FROM bronze.crm_cust_info
)s WHERE unwanted_space_count != 0;

-- Checking for Data Standardiation and Consistency
SELECT DISTINCT(cst_gndr)
FROM bronze.crm_cust_info;
SELECT DISTINCT(cst_marital_status)
FROM bronze.crm_cust_info;

-- Quality checking for silver table after cleaning
select * from silver.crm_cust_info

-- Checking for NULLs and Duplicates
SELECT
    cst_id,
    COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id is NULL;

-- Cheacking for unwanted Spaces.
-- Expectation : No Result
SELECT 
*
FROM (
    SELECT 
        cst_key,
        LEN(cst_key) - LEN(TRIM(cst_key)) as unwanted_space_count
    FROM silver.crm_cust_info
)s WHERE unwanted_space_count != 0;

-- Checking for Data Standardiation and Consistency
-- Expectation : Standardized data.
SELECT DISTINCT(cst_gndr)
FROM silver.crm_cust_info;
SELECT DISTINCT(cst_marital_status)
FROM silver.crm_cust_info;

-- Checking for Datatype consistency
select
    table_name,
    column_name,
    data_type,
    IS_NULLABLE 
from information_schema.COLUMNS
where table_name = 'crm_cust_info' and table_schema = 'bronze';



-- ============================================================
-- Cleanding bronze.crm_prd_info
-- ============================================================
-- Cheacking for NULLs or Duplicate in Primary Key
-- Expectation: No Result

SELECT
    prd_id,
    COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id is NULL;
-- No duplicate values or null values

-- Cheacking for unwanted Spaces.
-- Expectation : No Result
SELECT 
*
FROM (
    SELECT 
        prd_nm,
        LEN(prd_nm) - LEN(TRIM(prd_nm)) as unwanted_space_count
    FROM bronze.crm_prd_info
)s WHERE unwanted_space_count != 0;
-- No unwanted space in prd_nm

-- Checking for nulls and negative numbers
SELECT
    prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost is NULL;
-- There is no negative number but have 2 NULL values (replace acc to the bussiness requirements) 


-- Checking for Data Standardiation and Consistency
SELECT DISTINCT(prd_line)
FROM bronze.crm_prd_info;

-- Check for Invalid Date Orders
-- date are overlapping 
-- solution: taking prd_start_dt and make it prd_end_dt by taking the next date and decrease by one day
SELECT 
    prd_id,
    prd_key,
    prd_start_dt,
    LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1
FROM bronze.crm_prd_info
where prd_start_dt > prd_end_dt;
SELECT 
    prd_id,
    prd_key,
    prd_start_dt,
    LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1
FROM bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509', 'AC-HE-HL-U509-B')


-- ============================================================
-- Cleanding bronze.crm_sales_details
-- ============================================================

-- Cheacking for NULLs or Duplicate in Primary Key
-- Expectation: No Result
SELECT 
    sls_ord_num,
    COUNT(*)
FROM bronze.crm_sales_details
GROUP BY sls_ord_num
HAVING COUNT(*) > 1;
SELECT 
    *
FROM bronze.crm_sales_details
WHERE sls_ord_num = 'SO55367';
-- Orders can have same order number because a customer can order multiple product at a same time.

-- Checking for unwanted spaces
-- Expectation: No result
SELECT 
    *
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- Checking for data integrity
-- Expectation : NO Result
SELECT 
    *
FROM bronze.crm_sales_details
WHERE sls_prd_key not in (
    select prd_key from silver.crm_prd_info
);
SELECT 
    *
FROM bronze.crm_sales_details
WHERE sls_cust_id not in (
    select cst_id from silver.crm_cust_info
);
-- All sls_prd_key and sls_cust_id can be used to connect the crm_cust_info and crm_prd_info





