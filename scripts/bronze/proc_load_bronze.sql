/*
===============================================================================
Stored Procedure: bronze.load_bronze
Description: 
    This procedure performs the ETL process for the 'Bronze' layer.
    It tracks the loading duration for each table to monitor performance.
    Author: Mohammed Magdy [cite: 2]
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN 
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;
    DECLARE @start_time DATETIME, @end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Bronze Layer';
        PRINT 'Start Time: ' + CAST(@batch_start_time AS NVARCHAR);
        PRINT '================================================';

        -- ========================================================================
        -- Section: CRM Data Source
        -- ========================================================================
        PRINT '------------------------------------------------';
        PRINT '>> Loading CRM Tables';
        PRINT '------------------------------------------------';

        -- Processing [bronze.crm_cus_info]
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_cus_info;
        BULK INSERT bronze.crm_cus_info 
        FROM 'D:\data warehouse project\datasets\source_crm/cust_info.csv' 
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (crm_cus_info): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Processing [bronze.crm_prd_info]
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_prd_info;
        BULK INSERT bronze.crm_prd_info 
        FROM 'D:\data warehouse project\datasets\source_crm/prd_info.csv' 
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (crm_prd_info): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Processing [bronze.crm_sales_details]
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_sales_details; 
        BULK INSERT bronze.crm_sales_details 
        FROM 'D:\data warehouse project\datasets\source_crm/sales_details.csv' 
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (crm_sales_details): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- ========================================================================
        -- Section: ERP Data Source
        -- ========================================================================
        PRINT '------------------------------------------------';
        PRINT '>> Loading ERP Tables';
        PRINT '------------------------------------------------';

        -- Processing [bronze.erp_loc_a101]
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_loc_a101;
        BULK INSERT bronze.erp_loc_a101 
        FROM 'D:\data warehouse project\datasets\source_erp/LOC_A101.csv' 
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (erp_loc_a101): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Processing [bronze.erp_cust_az12]
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_cust_az12; 
        BULK INSERT bronze.erp_cust_az12 
        FROM 'D:\data warehouse project\datasets\source_erp/cust_az12.csv' 
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (erp_cust_az12): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Processing [bronze.erp_px_cat_g1v2]
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        BULK INSERT bronze.erp_px_cat_g1v2 
        FROM 'D:\data warehouse project\datasets\source_erp/px_cat_g1v2.csv' 
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (erp_px_cat_g1v2): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        SET @batch_end_time = GETDATE();
        PRINT '================================================';
        PRINT 'Bronze Layer Load Complete';
        PRINT 'Total Batch Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        PRINT '==========================================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number:  ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State:   ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================================';
    END CATCH
END;
