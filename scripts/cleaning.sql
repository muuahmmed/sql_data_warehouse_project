-- 1. Load Raw Data
EXEC bronze.load_bronze;

-- 2. Transform and Load Clean Data
EXEC silver.load_silver;

-- 3. Quality Check
-- Raw data check
SELECT DISTINCT cst_gender FROM bronze.crm_cus_info;

-- Cleaned data check
SELECT DISTINCT cst_gndr FROM silver.crm_cust_info;
------------------------------------------------------------------------------------------------------------------




select * from bronze.crm_cus_info;

-- 1. Create a View to store your cleaning logic
insert into silver.crm_cus_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_material_status,
cst_gender,
cst_create_date)
 
SELECT 
    cst_id, 
    cst_key, 
    TRIM(cst_firstname) AS cst_firstname, 
    TRIM(cst_lastname) AS cst_lastname,
	CASE 
        WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
        ELSE 'Unknown'
    END AS cst_material_status,
    CASE 
        WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
        ELSE 'Unknown'
    END AS cst_gender,
    cst_create_date
FROM ( 
    SELECT *, 
        ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cus_info
    WHERE cst_id IS NOT NULL
) t 
WHERE flag_last = 1;


select distinct cst_gender from bronze.crm_cus_info;

select distinct cst_material_status from bronze.crm_cus_info;


select distinct cst_gender from silver.crm_cus_info 

select * from silver.crm_cus_info


----------------------------------------------------------------------------------------------------------------


insert into silver.crm_prd_info(
	prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
select 
	prd_id, 
	replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
	substring(prd_key, 7, len(prd_key)) as prd_key,
	prd_nm, 
	isnull(prd_cost, 0)as prd_cost,
	case when upper(trim(prd_line)) = 'M' then 'Mountain'
		when upper(trim(prd_line)) = 'R' then 'Road'
		when upper(trim(prd_line)) = 'S' then 'Other Sales'
		when upper(trim(prd_line)) = 'T' then 'Touring'
		else 'Unkown' 
	END as prd_line,
	cast(prd_start_dt as date) as prd_start_dt, 
	cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt )-1 as date) as prd_end_dt

from bronze.crm_prd_info 


-------------------------------------------------------------------------------------------------------
------------------------------cleaning-------------------------------------------

select 
	prd_id, 
	count(*) 
from silver.crm_prd_info 
group by prd_id 
having count(*) > 1 or prd_id is null;

select prd_nm from silver.crm_prd_info where prd_nm != trim(prd_nm);


--------------------------------------------------------------------

select 
	nullif(sls_order_dt, 0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0;

--------------------------------------------------------------------------

select 
	distinct sls_sales as old_sls_sales,
	
	sls_price as old_sls_price,
	sls_quantity,
	case 
		when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
			then sls_quantity * ABS(sls_price)
		else sls_sales
	end as sls_sales,
	case 
		when sls_price is null or sls_price <= 0
			then sls_sales / nullif(sls_quantity,0)
		else sls_price
	end as sls_price

from bronze.crm_sales_details 
where sls_sales != sls_quantity * sls_price 
	or sls_sales is null
	or sls_price is null
	or sls_quantity is null 
	or sls_sales <= 0 or sls_quantity <= 0 or sls_price <=0
	order by sls_sales, sls_quantity, sls_price;