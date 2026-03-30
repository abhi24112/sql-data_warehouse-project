-- EXEC silver.load_silver;


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN 
    BEGIN TRY
        DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start DATETIME, @batch_end DATETIME;

        SET @batch_start = GETDATE();

        PRINT '============================================';
        PRINT 'Loading Silver Layer';
        PRINT '============================================';

        PRINT '--------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '--------------------------------------------';

        SET @start_time = GETDATE();

        -- Loading silver.crm_cust_info
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting Data into: silver.crm_cust_info';
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
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE UPPER(TRIM(cst_marital_status))
                WHEN 'M' THEN  'Married'
                WHEN 'S' THEN  'Single'
                ELSE 'n/a'
            END AS cst_marital_status, -- Normalize marital status values to readable format
            CASE UPPER(TRIM(cst_gndr))
                WHEN 'M' THEN  'Male'
                WHEN 'F' THEN  'Female'
                ELSE 'n/a'
            END AS cst_gndr, -- Normalize gender values to readable format
            cst_create_date
        FROM (
            
            SELECT
                *,
                ROW_NUMBER() OVER(PARTITION BY  cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        )s
        WHERE flag_last = 1; -- Selected the most recent record per customer

        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + 'seconds';
        PRINT '--------------------------------------------';


        -- =======================================================================
        -- Loading silver.crm_prd_info

        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting Data into: silver.crm_cust_info';
        INSERT INTO silver.crm_prd_info (
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
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
            prd_nm,
            COALESCE(prd_cost, 0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN  'Mountain'
                WHEN 'R' THEN  'Road'
                WHEN 'S' THEN  'other Sales'
                WHEN 'T' THEN  'Touring'
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + 'seconds';
        PRINT '--------------------------------------------';

        -- =======================================================================
        -- Loading silver.crm_sales_details

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting Data into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details(
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price)
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE 
                WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                ELSE  CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt,
            CASE 
                WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                ELSE  CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE 
                WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                ELSE  CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END AS sls_due_dt,
            CASE 
                WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_price * ABS(sls_quantity) THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE 
                WHEN sls_price <= 0 OR sls_price IS NULL THEN ABS(sls_sales) / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + 'seconds';
        
        PRINT '--------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '--------------------------------------------';
        
        -- Loading silver.erp_cust_az12

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting Data into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12(
            cid,
            bdate,
            gen
        )
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
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + 'seconds';
        PRINT '--------------------------------------------';

        -- =======================================================================
        -- Loading silver.erp_loc_a101

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Inserting Data into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101(
            cid,
            cntry
        )
        SELECT 
        REPLACE(cid, '-', '') AS cid,
        CASE 
            WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
            WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
            WHEN UPPER(TRIM(cntry)) = '' OR cntry IS NULL  THEN 'n/a'
            ELSE  TRIM(cntry)
        END AS cntry
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + 'seconds';
        PRINT '--------------------------------------------';
        -- =======================================================================
        -- Loading silver.erp_px_cat_g1v2

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Inserting Data into: silver.erp_px_cat_g1v2';

        INSERT INTO silver.erp_px_cat_g1v2(
            id,
            cat,
            subcat,
            MAINTENANCE
        )
        SELECT 
            id,
            cat,
            subcat,
            MAINTENANCE
        FROM bronze.erp_px_cat_g1v2;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + 'seconds';
        PRINT '--------------------------------------------';

        SET @batch_end = GETDATE();
        PRINT '============================================';
        PRINT 'Loading Silver Layer is Completed';
        PRINT '>> Total Load Duration:' + CAST(DATEDIFF(SECOND, @batch_start, @batch_end) AS NVARCHAR) + 'seconds';
        PRINT '============================================';


    END TRY
    BEGIN CATCH 
        PRINT '============================================';
        PRINT 'Erro Message' + ERROR_MESSAGE();
        PRINT 'Erro Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Erro Message' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '============================================';    
    END CATCH 
END 

















