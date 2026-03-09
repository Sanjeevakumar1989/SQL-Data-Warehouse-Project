/*
=====================================================================
DDL Script:Create Bronze Layer
====================================================================
Script Purpose:
This Script creates Tables in the 'Bronze' ,Schema,Dropping Exists Tables
if they alredy exist.
Run this script to re-define the DDL structue of 'Bronze' Tables
====================================================================
*/

IF OBJECT_ID ('bronze.crm_cust_info','U') is not null
	Drop Table bronze.crm_cust_info;

GO

Create Table bronze.crm_cust_info (
cst_id INT,
cst_key Nvarchar(50),
cst_firstname Nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status Nvarchar(50),
cst_gndr Nvarchar(50),
cst_create_date Date

);

IF OBJECT_ID ('bronze.crm_prd_info','U') is not null
	Drop Table bronze.crm_prd_info;
Create Table bronze.crm_prd_info (
prd_id INT,
prd_key NVarchar(50),
prd_nm Nvarchar(50),
prd_cost INT,
prd_line Nvarchar(50),
prd_start_dt Datetime,
pre_end_dt Datetime

);


IF OBJECT_ID ('bronze.crm_sales_details','U') is not null
	Drop Table bronze.crm_sales_details;
Create Table bronze.crm_sales_details (
sls_ord_num Nvarchar(50),
sls_prd_key Nvarchar(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_Quantity INT,
sls_price INT
);



IF OBJECT_ID ('bronze.erp_Cust_AZ12','U') is not null
	Drop Table bronze.erp_Cust_AZ12;
Create Table bronze.erp_Cust_AZ12 (
	CID Nvarchar(50),
	BDATE Date,
	GEN Nvarchar(50)
);

IF OBJECT_ID ('bronze.erp_LOC_A101','U') is not null
	Drop Table bronze.erp_LOC_A101;
Create Table bronze.erp_LOC_A101 (
	CID NVarchar(50),
	CNTRY Nvarchar(50)

);

IF OBJECT_ID ('bronze.erp_PX_CAT_G1V2','U') is not null
	Drop Table bronze.erp_PX_CAT_G1V2;
Create Table bronze.erp_PX_CAT_G1V2 (
	ID Nvarchar(50),
	CAT Nvarchar(50),
	Subcat Nvarchar(50),
	Maintenance Nvarchar(50)
);
