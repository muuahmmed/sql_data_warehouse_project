create view gold.dim_customers as 
	select
		row_number() over(order by cst_id) as customer_key,
		ci.cst_id as customer_id,
		ci.cst_key as customer_number,
		ci.cst_firstname as first_name,
		ci.cst_lastname as last_name,
		ci.cst_material_status as marital_status,
		ci.cst_create_date as create_date,
		ca.bdate as birth_date,
		case 
			when ci.cst_gender != 'Unknown' then ci.cst_gender
			else coalesce(ca.gen, 'Unknown')
		end as gender,
		la.cntry as country
	from silver.crm_cus_info ci left join silver.erp_cust_az12 ca
		on ci.cst_key = ca.cid
		left join silver.erp_loc_a101 la
			on ci.cst_key = la.cid;



select * from gold.dim_customers;

---------------------------------------------------------------------------------------------------------------------------------------------------------------
create view gold.dim_products as 
select
	row_number() over(order by pn.prd_start_dt, pn.prd_key) as product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategory,
	pc.maintenance,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
FROM silver.crm_prd_info pn left join silver.erp_px_cat_g1v2 pc 
	on pn.cat_id = pc.id
where prd_end_dt is Null;

select * from gold.dim_products;
---------------------------------------------------------------------------------------------------------------------------------------------------------------
create view gold.fact_sales as 
SELECT
sd.sls_ord_num as order_id,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
FROM silver.crm_sales_details sd left join gold.dim_products pr
	on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu on sd.sls_cust_id = cu.customer_id;


select * 
from gold.fact_sales f left join gold.dim_customers c
	on c.customer_key = f.customer_key
	left join gold.dim_products p on p.product_key = f.product_key
where c.customer_id is Null
;
