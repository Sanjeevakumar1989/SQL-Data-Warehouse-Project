-- check the duplicates primary Key or Null values
-- Expected no results.

--finding the customerid and null values
select cst_id ,count(*)
from bronze.crm_cust_info group by cst_id having count(*)>1 or cst_id is null

--Assign a row_number for each row in a result set , based on the the defined order(ASC/DESC)
select * , Row_number() over(partition by cst_id order by cst_create_date DESC)
from bronze.crm_cust_info


--finding the duplicated values
select * from
(
select * , Row_number() over(partition by cst_id order by cst_create_date DESC) as flag_last
from bronze.crm_cust_info)t where flag_last !=1

--without duplicate values
select * from
(
select * , Row_number() over(partition by cst_id order by cst_create_date DESC) as flag_last
from bronze.crm_cust_info)t where flag_last = 1

--Quality check for unwanted spaces in string values
--Expected no results
select cst_firstname from bronze.crm_cust_info
where cst_firstname != Trim(cst_firstname)


select cst_lastname from bronze.crm_cust_info
where cst_lastname != Trim(cst_lastname)
--going to clean spaces in string


Insert into silver.crm_cust_info (
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_material_status,
cst_gndr,
cst_create_date )

select 
cst_id,
cst_key,
Trim(cst_firstname) as cst_firstname,
Trim(cst_lastname) as cst_lastname,
Case when cst_material_status = 'M' Then 'Married'
	When cst_material_status ='S' then 'Single'
	else 'N/A'
End cst_material_status,

Case when upper(trim(cst_gndr)) = 'F' then 'Female'
	 When upper(trim(cst_gndr)) = 'M' then 'Male'
	 else 'N/A'
END cst_gndr,
cst_create_date
from
(
select * , Row_number() over(partition by cst_id order by cst_create_date DESC) as flag_last
from bronze.crm_cust_info where cst_id is not null)t  where flag_last = 1

-----
--Expection :no results
select cst_key from silver.crm_cust_info
where cst_key != trim(cst_key)

--data standardization & consistancy
select Distinct cst_gndr from silver.crm_cust_info

select * from silver.crm_cust_info

----


IF OBJECT_ID ('silver.crm_cust_info','U') is not null
	Drop Table silver.crm_cust_info;

Create Table silver.crm_cust_info (
cst_id INT,
cst_key Nvarchar(50),
cst_firstname Nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status Nvarchar(50),
cst_gndr Nvarchar(50),
cst_create_date Date,
dwh_create_date Datetime2 Default Getdate()

);

IF OBJECT_ID ('silver.crm_prd_info','U') is not null
	Drop Table silver.crm_prd_info;
Create Table silver.crm_prd_info (
prd_id INT,
prd_key NVarchar(50),
prd_nm Nvarchar(50),
prd_cost INT,
prd_line Nvarchar(50),
prd_start_dt Datetime,
pre_end_dt Datetime,
dwh_create_date Datetime2 default Getdate()

);


IF OBJECT_ID ('silver.crm_sales_details','U') is not null
	Drop Table silver.crm_sales_details;
Create Table silver.crm_sales_details (
sls_ord_num Nvarchar(50),
sls_prd_key Nvarchar(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_Quantity INT,
sls_price INT,
dwh_create_date datetime2 default getdate()
);



IF OBJECT_ID ('silver.erp_Cust_AZ12','U') is not null
	Drop Table silver.erp_Cust_AZ12;
Create Table silver.erp_Cust_AZ12 (
	CID Nvarchar(50),
	BDATE Date,
	GEN Nvarchar(50),
	dwh_create_date datetime2 default getdate()
);

IF OBJECT_ID ('silver.erp_LOC_A101','U') is not null
	Drop Table silver.erp_LOC_A101;
Create Table silver.erp_LOC_A101 (
	CID NVarchar(50),
	CNTRY Nvarchar(50),
	dwh_create_date datetime2 default getdate()
);

IF OBJECT_ID ('silver.erp_PX_CAT_G1V2','U') is not null
	Drop Table silver.erp_PX_CAT_G1V2;
Create Table silver.erp_PX_CAT_G1V2 (
	ID Nvarchar(50),
	CAT Nvarchar(50),
	Subcat Nvarchar(50),
	Maintenance Nvarchar(50),
	dwh_create_date datetime2 default getdate()
);
