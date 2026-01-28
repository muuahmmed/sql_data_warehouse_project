/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script defines the 'bronze' schema structure. It ensures a clean 
    environment by dropping existing tables before re-creating them.
    
    Data Source: CRM and ERP raw datasets
    Author: Mohammed Magdy [cite: 2]
===============================================================================
*/

-- ============================================================================
-- 1. CRM Schema Tables
-- ============================================================================

-- [bronze.crm_cus_info]: Raw customer demographic data
IF OBJECT_ID('bronze.crm_cus_info', 'U') IS NOT NULL DROP TABLE bronze.crm_cus_info;
GO
CREATE TABLE bronze.crm_cus_info (
    cst_id             INT,
    cst_key            NVARCHAR(50),
    cst_firstname      NVARCHAR(50),
    cst_lastname       NVARCHAR(50),
    cst_marital_status NVARCHAR(50), 
    cst_gender         NVARCHAR(50),
    cst_create_date    DATE
);
GO

-- [bronze.crm_prd_info]: Raw product catalog data
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL DROP TABLE bronze.crm_prd_info;
GO
CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);
GO

-- [bronze.crm_sales_details]: Raw transactional sales data
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL DROP TABLE bronze.crm_sales_details;
GO
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
GO

-- ============================================================================
-- 2. ERP Schema Tables
-- ============================================================================

-- [bronze.erp_loc_a101]: Raw location and country mapping
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL DROP TABLE bronze.erp_loc_a101;
GO
CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
GO

-- [bronze.erp_cust_az12]: Raw customer birthdate and gender (ERP source)
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL DROP TABLE bronze.erp_cust_az12;
GO
CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);
GO

-- [bronze.erp_px_cat_g1v2]: Raw product category and subcategory hierarchy
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL DROP TABLE bronze.erp_px_cat_g1v2;
GO
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
GO
