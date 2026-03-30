
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
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Cheacking for unwanted Spaces.
-- Expectation : No Result
SELECT 
*
FROM (
    SELECT 
        cst_key,
        LEN(cst_key) - LEN(TRIM(cst_key)) AS unwanted_space_count
    FROM bronze.crm_cust_info
)s WHERE unwanted_space_count != 0;

-- Checking for Data Standardiation and Consistency
SELECT DISTINCT(cst_gndr)
FROM bronze.crm_cust_info;
SELECT DISTINCT(cst_marital_status)
FROM bronze.crm_cust_info;

-- Quality checking for silver table after cleaning
SELECT * FROM silver.crm_cust_info

-- Checking for NULLs and Duplicates
SELECT
    cst_id,
    COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Cheacking for unwanted Spaces.
-- Expectation : No Result
SELECT 
*
FROM (
    SELECT 
        cst_key,
        LEN(cst_key) - LEN(TRIM(cst_key)) AS unwanted_space_count
    FROM silver.crm_cust_info
)s WHERE unwanted_space_count != 0;

-- Checking for Data Standardiation and Consistency
-- Expectation : Standardized data.
SELECT DISTINCT(cst_gndr)
FROM silver.crm_cust_info;
SELECT DISTINCT(cst_marital_status)
FROM silver.crm_cust_info;

-- Checking for Datatype consistency
SELECT
    table_name,
    column_name,
    data_type,
    IS_NULLABLE 
FROM information_schema.COLUMNS
WHERE table_name = 'crm_cust_info' AND table_schema = 'bronze';



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
HAVING COUNT(*) > 1 OR prd_id IS NULL;
-- No duplicate values or null values

-- Cheacking for unwanted Spaces.
-- Expectation : No Result
SELECT 
*
FROM (
    SELECT 
        prd_nm,
        LEN(prd_nm) - LEN(TRIM(prd_nm)) AS unwanted_space_count
    FROM bronze.crm_prd_info
)s WHERE unwanted_space_count != 0;
-- No unwanted space in prd_nm

-- Checking for nulls and negative numbers
SELECT
    prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;
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
WHERE prd_start_dt > prd_end_dt;
SELECT 
    prd_id,
    prd_key,
    prd_start_dt,
    LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509', 'AC-HE-HL-U509-B')


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
WHERE sls_prd_key NOT IN (
    SELECT prd_key FROM silver.crm_prd_info
);
SELECT 
    *
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (
    SELECT cst_id FROM silver.crm_cust_info
);
-- All sls_prd_key and sls_cust_id can be used to connect the crm_cust_info and crm_prd_info

-- Checking Date Datatype 
SELECT
    table_name,
    column_name,
    data_type,
    IS_NULLABLE 
FROM information_schema.COLUMNS
WHERE table_name = 'crm_sales_details' AND table_schema = 'bronze';
-- sls_order_dt, sls_ship_dt, and sls_due_dt has the int datatype need to be date datatype

-- Checking for invalid dates
SELECT 
    NULLIF(sls_order_dt,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8;
SELECT 
    NULLIF(sls_ship_dt,0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8;
SELECT 
    NULLIF(sls_due_dt,0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 OR LEN(sls_due_dt) != 8;

-- There is invalid dates in sls_order_dt


-- Check for the Invalid Date Orders
-- Expectation: No result
SELECT
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- no need to do anything


-- Check Data Consistency : Between Sales, Quantity and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero, or negative

SELECT DISTINCT
    sls_ord_num,
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- Verifing changes in silver.crm_sales_details
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0

-- =====================================================================
-- Cleaning ERP Tables
-- =====================================================================
-- Cleaning erp_cust_az12
-- Expectation : No result

SELECT 
    cid
FROM bronze.erp_cust_az12 
WHERE cid LIKE 'NAS%';

SELECT * FROM (
    SELECT
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))  
        ELSE  cid
    END AS cid,
    bdate,
    gen
FROM bronze.erp_cust_az12
)s
WHERE cid LIKE 'NAS%';

SELECT 
    TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME,DATA_TYPE 
    FROM information_schema.COLUMNS
WHERE TABLE_NAME = 'erp_cust_az12';

-- Checking for dbdate correctness
SELECT
bdate
FROM bronze.erp_cust_az12
WHERE bdate > GETDATE() OR bdate < '1920-01-01';
-- bdate are not correct due to haveing very large and small birthdate.

-- Checking for gen column
SELECT DISTINCT
    gen, COUNT(*)
FROM bronze.erp_cust_az12
GROUP BY gen
ORDER BY COUNT(*) DESC;
-- have data compatibility and consistency issues.

-- verifying
SELECT DISTINCT
    gen, COUNT(*)
FROM (
    SELECT 
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))  
        ELSE  cid
    END AS cid,
    CASE 
        WHEN bdate > GETDATE() THEN NULL  
        ELSE  bdate
    END AS bdate,
    CASE 
        WHEN UPPER(TRIM(gen)) IN  ('M', 'MALE') THEN 'Male'
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        ELSE  'n/a'
    END AS gen
FROM bronze.erp_cust_az12
)s
GROUP BY gen
ORDER BY COUNT(*) DESC;



-- Cleaning erp_loc_a101
-- Expectation : No result

SELECT 
cid
FROM bronze.erp_loc_a101
WHERE cid NOT LIKE 'AW-%'
-- all data is start with AW-

SELECT 
TRIM(REPLACE(cid, '-', '')) AS cid,
cntry
FROM bronze.erp_loc_a101
WHERE TRIM(REPLACE(cid, '-', '')) NOT IN (SELECT cst_key FROM silver.crm_cust_info);
-- NO result mean data is cleaned now.

-- Checking for NULL and 
SELECT 
TRIM(REPLACE(cid, '-', '')) AS cid,
COUNT(*)
FROM bronze.erp_loc_a101
GROUP BY TRIM(REPLACE(cid, '-', ''))
HAVING COUNT(*) > 1 OR TRIM(REPLACE(cid, '-', '')) IS NULL;

-- Checking for ctry NULL
SELECT 
    cntry,
    COUNT(*)
FROM bronze.erp_loc_a101
GROUP BY cntry
HAVING COUNT(*) > 1 OR cntry IS NULL;

-- validating data cleaning
SELECT 
cntry,
COUNT(*)
FROM (
    SELECT 
    REPLACE(cid, '-', '') AS cid,
    CASE 
        WHEN UPPER(TRIM(cntry)) IN ('US', 'USA', 'UNITED STATES') THEN 'USA'
        WHEN UPPER(TRIM(cntry)) = '' OR cntry IS NULL  THEN 'n/a'
        ELSE  cntry
    END AS cntry
    FROM bronze.erp_loc_a101
)s
GROUP BY cntry
HAVING COUNT(*) > 1 OR cntry IS NULL;


-------------------------------------------------------------------
-- Cleaning table erp_px_cat_g1v2

SELECT 
    id,
    cat,
    subcat,
    MAINTENANCE
FROM bronze.erp_px_cat_g1v2;

SELECT * FROM silver.crm_prd_info;

-- Cheaking for NULL and Duplicates in id
-- Expectation: No result
SELECT 
    id,
    COUNT(*)
FROM bronze.erp_px_cat_g1v2
GROUP BY id
HAVING COUNT(*) > 1 OR id IS NULL;


-- checking data integity with silver.crm_prd_info
SELECT 
    id
FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN (
    SELECT cat_id FROM silver.crm_prd_info
);
-- only one cat_id is not in silver.crm_prd_info "CO_PD"

-- Checking duplicate or null in Cat column
SELECT 
    cat,
    COUNT(*)
FROM bronze.erp_px_cat_g1v2
GROUP BY cat
HAVING COUNT(*) >= 1 OR cat IS NULL;

-- cheacking for unwanted spaces in cat and subcat
-- Expectation: No result
SELECT 
    cat,
    subcat
FROM bronze.erp_px_cat_g1v2
WHERE TRIM(cat) != cat OR TRIM(subcat) != subcat; 



-- Checking duplicate or null in subcat column
SELECT 
    subcat,
    COUNT(*)
FROM bronze.erp_px_cat_g1v2
GROUP BY subcat
HAVING COUNT(*) >= 1 OR subcat IS NULL;

-- Checking duplicate or null in subcat column
SELECT 
    maintenance,
    COUNT(*)
FROM bronze.erp_px_cat_g1v2
GROUP BY maintenance
HAVING COUNT(*) >= 1 OR maintenance IS NULL;






