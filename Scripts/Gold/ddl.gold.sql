
/* 
------------------------------------------------------
DDL Script:Craete Gold Views
------------------------------------------------------
Script Purpose:
    This script creats views for the Gold Layer in the data warhouse.
The Gold Layer represents the final diemension and fact tables (star Schema)

Each view performs Transformations and combines data from the silver layer to produce a clean.
enriched,and business -ready dataset.


Usage:
  - These views can be queired directly for analytics and reporting.
--------------------------------------------------------
*/



if object_id  ('gold.dim_customers','V') is not null
	Drop view gold.dim_customers;
Go

 Create view gold.dim_customers As
 select
 Row_number() over(order by cst_id) as Customer_key,
 ci.cst_id as Customer_id,
 ci.cst_key as Customer_Number,
 ci.cst_firstname as Firstname,
 ci.cst_lastname as LastName,
  la.cntry as country,
 ci.cst_marital_status as Marital_status,

 case when ci.cst_gndr !='N/A'  then ci.cst_gndr --crm is the master for gender info
	  else Coalesce(ca.gen,'N/A') 
End as Gender,
 ca.Bdate as Birthdate,
 ci.cst_create_date as create_date

 from silver.crm_cust_info  ci left join 
 silver.erp_Cust_AZ12 ca ON ci.cst_key = ca.CID 
 left join silver.erp_LOC_A101 la ON
 ci.cst_key = la.cid

 
 --select * from gold.dim_customers
 --select distinct gender from gold.dim_customers

 select * from silver.crm_prd_info
  select * from silver.erp_PX_CAT_G1V2
 --Dimension vs Fact??
 --select prd_key ,count(*) from(

 
if object_id  ('gold.dim_products1','V') is not null
	Drop view gold.dim_products1;
Go

create view gold.dim_products1 as
 select
	Row_Number() over(order by pn.prd_start_dt ,pn.prd_key) as primary_key,
	 pn.prd_id as Product_id,
	 pn.prd_key as Product_number,
	 pn.prd_nm as product_name,
	 pn.cat_id as catogery_id,
	 pc.cat as category,
	 pc.subcat as sub_category,
	 pc.Maintenance,
	 pn.prd_cost as cost,
	 pn.prd_line as product_line,
	 pn.prd_start_dt as start_date	 
 from silver.crm_prd_info pn left join silver.erp_PX_CAT_G1V2 pc ON pn.cat_id = pc.ID
-- where pre_end_dt is null --t group by prd_key  having count(*)>1 --Filter out all historical data

 select * from silver.crm_sales_details
 --Dimension vs fact??

 
if object_id  ('gold.fact_sales','V') is not null
	Drop view gold.fact_sales;
Go

 create view gold.fact_sales as
 select
 sd.sls_ord_num as order_number,
 pr.primary_key,
 cu.Customer_key,
 sd.sls_order_dt as order_date,
 sd.sls_ship_dt as shipping_date,
 sd.sls_due_dt as due_dt,
 sd.sls_sales as sales_amount,
 sd.sls_quantity as quantity,
 sd.sls_price as sprice
 from silver.crm_sales_details sd
 left join gold.dim_products1 pr ON
 sd.sls_prd_key = pr.Product_number
 left join gold.dim_customers  cu ON
 sd.sls_cust_id = cu.Customer_id



 select * from gold.dim_customers
 select * from gold.dim_products1
 select * from gold.fact_sales

 --forign key Integrity (Dimensions)
 select * from gold.fact_sales f left join
 gold.dim_customers c ON f.Customer_key = c.Customer_key
 left join gold.dim_products1 p On p.primary_key = f.primary_key
where p.primary_key is null
