insert into silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)
select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	case when sls_order_dt = 0 or len(sls_order_dt)!= 8 then null
	else cast(cast(sls_order_dt as varchar) as date) 
	end as sls_order_dt,
	case when sls_ship_dt = 0 or len(sls_ship_dt)!= 8 then null
	else cast(cast(sls_ship_dt as varchar) as date) 
	end as sls_ship_dt,
	case when sls_due_dt = 0 or len(sls_due_dt)!= 8 then null
	else cast(cast(sls_due_dt as varchar) as date) 
	end as sls_due_dt,
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
from bronze.crm_sales_details;


select * from silver.crm_sales_details
-------------------------------------------------------------------------------------------------------------------------------------------
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
-------------------------------------------------------------------------------------------------------------------------------------------
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
--------------------------------------------------------------------------------------------------------------------------------------------------
insert into silver.erp_cust_az12(cid, bdate, gen)
select 
	case 
		when cid like '%NAS%' 
			then SUBSTRING(cid, 4, LEN(cid))
		else cid
	end cid,
	case 
		when bdate > getdate()
			then Null
		else bdate
	end as bdate, 
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE', 'FEMAL') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		WHEN gen IS NULL OR TRIM(gen) = '' OR UPPER(TRIM(gen)) = 'NULL' THEN 'n/a'
		ELSE 'n/a' 
	END AS gen
from bronze.erp_cust_az12 ;
--------------------------------------------------------------------------------------------------------------------------------------------------
insert into silver.erp_loc_a101(cid, cntry)
select
	replace(cid, '-', '') as cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		 ELSE TRIM(cntry)
	END AS cntry
from bronze.erp_loc_a101;
--------------------------------------------------------------------------------------------------------------------------------------------------
 insert into silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
 select 
	id, 
	cat, 
	subcat, 
	maintenance
 from bronze.erp_px_cat_g1v2;


 select * from silver.erp_px_cat_g1v2;
