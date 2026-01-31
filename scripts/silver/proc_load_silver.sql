/*
===============================================================================
Stored Procedure: silver.load_silver
Description: 
    This procedure performs the ETL process for the 'Silver' layer.
    It transforms and cleans data from the 'Bronze' layer into 'Silver'.
    Author: Mohammed Magdy
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;
    DECLARE @start_time DATETIME, @end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT 'Start Time: ' + CAST(@batch_start_time AS NVARCHAR);
        PRINT '================================================';

        -- ========================================================================
        -- Section: CRM Data Source Transformations
        -- ========================================================================
        PRINT '------------------------------------------------';
        PRINT '>> Loading CRM Tables';
        PRINT '------------------------------------------------';

        -- Processing [silver.crm_cust_info]
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        
        PRINT '>> Inserting Data Into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname, 
            cst_marital_status, cst_gndr, cst_create_date
        )
        SELECT 
            cst_id, cst_key, 
            TRIM(cst_firstname), TRIM(cst_lastname),
            -- Standardizing Marital Status (S -> Single, M -> Married)
            CASE 
                WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
                ELSE 'Unknown'
            END,
            -- Standardizing Gender (F -> Female, M -> Male)
            CASE 
                WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
                ELSE 'Unknown'
            END,
            cst_create_date
        FROM ( 
            -- Deduplication logic using ROW_NUMBER
            SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cus_info
            WHERE cst_id IS NOT NULL
        ) t WHERE flag_last = 1;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Processing [silver.crm_prd_info]
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        
        PRINT '>> Inserting Data Into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
        )
        SELECT 
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, 
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
            prd_nm,
            ISNULL(prd_cost, 0), -- Handling NULL costs
            -- Standardizing Product Line descriptions
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE),
            -- Calculating End Date using LEAD function
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Processing [silver.crm_sales_details]
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;
        
        PRINT '>> Inserting Data Into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
        )
        SELECT 
            sls_ord_num, sls_prd_key, sls_cust_id,
            -- Converting Integer dates (YYYYMMDD) to DATE format
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL 
                 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END,
            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL 
                 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END,
            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL 
                 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END,
            sls_sales, sls_quantity, sls_price
        FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- ========================================================================
        -- Section: ERP Data Source Transformations
        -- ========================================================================
        PRINT '------------------------------------------------';
        PRINT '>> Loading ERP Tables';
        PRINT '------------------------------------------------';

        -- Processing [silver.erp_loc_a101]
        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.erp_loc_a101;
        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT 
            REPLACE(cid, '-', ''), -- Removing hyphens from CID
            CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'Unknown'
                 ELSE TRIM(cntry) END
        FROM bronze.erp_loc_a101;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Processing [silver.erp_cust_az12]
        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.erp_cust_az12;
        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT 
            REPLACE(cid, 'NAS', ''), -- Removing 'NAS' prefix from CID
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END, -- Handling future birthdates
            CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                 ELSE 'Unknown' END
        FROM bronze.erp_cust_az12;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Processing [silver.erp_px_cat_g1v2]
        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT id, cat, subcat, maintenance FROM bronze.erp_px_cat_g1v2;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        SET @batch_end_time = GETDATE();
        PRINT '================================================';
        PRINT 'Silver Layer Load Complete';
        PRINT 'Total Batch Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        -- Error handling block
        PRINT '==========================================================';
        PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT '==========================================================';
    END CATCH
END;
GO