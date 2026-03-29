
-- Loading silver.crm_cust_info

INSERT INTO silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) as cst_firstname,
    TRIM(cst_lastname) as cst_lastname,
    CASE UPPER(TRIM(cst_marital_status))
        WHEN 'M' THEN  'Married'
        WHEN 'S' THEN  'Single'
        ELSE 'n/a'
    END as cst_marital_status, -- Normalize marital status values to readable format
    CASE UPPER(TRIM(cst_gndr))
        WHEN 'M' THEN  'Male'
        WHEN 'F' THEN  'Female'
        ELSE 'n/a'
    END as cst_gndr, -- Normalize gender values to readable format
    cst_create_date
FROM (
    
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY  cst_id ORDER BY cst_create_date desc) as flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
)s
WHERE flag_last = 1; -- Selected the most recent record per customer


-- =======================================================================
-- Loading silver.crm_prd_info
INSERT into silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,
    prd_nm,
    COALESCE(prd_cost, 0) as prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN  'Mountain'
        WHEN 'R' THEN  'Road'
        WHEN 'S' THEN  'other Sales'
        WHEN 'T' THEN  'Touring'
        ELSE 'n/a'
    END as prd_line,
    CAST(prd_start_dt as DATE) as prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 as DATE) as prd_end_dt
FROM bronze.crm_prd_info

-- =======================================================================
-- Loading silver.crm_sales_details

SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details;