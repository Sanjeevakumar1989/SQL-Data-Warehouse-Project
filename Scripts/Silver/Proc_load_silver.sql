/*
-----------------------------------
Stored Procedure:Load,Silver Layer (Bronze -> Silver)
-----------------------------------
Script Prpose:
  This is stored procedure perform like ETL(Extract Transform Load) process to 
populate the 'Silver' schema tables from the 'Bronze' schema.

Action Performed:
  - Truncate silver tables
  - Insert transformed and cleaned data from Bronze into silver Tables.

Parameters:
  -None
  -This stored proceure does not accept any parameteres or return any values.

Usage example:
  Exec silver.load_silver;
---------------------------------------------
*/

create or alter procedure silver.load_silver as 
Begin
	Declare @start_time Datetime, @end_time Datetime,@batch_start_time datetime, @batch_end_time datetime
	Begin Try
		print'--------------------------------------';
		print'    Loading Bronze Customer infoormation         ';
		print'--------------------------------------'
		Set @batch_start_time=Getdate()
		set @start_time=Getdate()
		print'>>Truncating Table:silver.crm_cust_info'
		Truncate table silver.crm_cust_info;
		print'inserting Data into:silver.crm_cust_info'
		Insert into silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
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

		Set @end_time = Getdate()
		print'>> Load duration '+ Cast(Datediff(second,@start_time,@end_time)as Nvarchar) +' seconds'
		print'--------------------------------------';
		print'    Loading Bronze product infoormation         ';
		print'--------------------------------------'

		set @start_time = Getdate()
		Print'>>Trucating table:Silver.crm_prd_info'
		Truncate table Silver.crm_prd_info
		print'>>Inserting Data into:silver.crm_prd_info'
		insert into silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		pre_end_dt
		)

		select  
		prd_id,
		Replace(Substring(prd_key ,1,5),'-','_') as cat_id, --substing(columnname,start,end) Replace the vlaue --Extract categrey id
		substring(prd_key,7,len(prd_key)) as Prd_key, --extract product id
		prd_nm,
		isnull(prd_cost,0) as prd_cost,
		Case Upper(trim(prd_line))
			when 'M' then 'Mountain'
			when 'R' then  'Road'
			when 'T' then 'Touring'
			when 'S' then 'Other sales'
			else 'N/A'
		End as prd_line,--map product line codes to descriptive values
		prd_start_dt,
		pre_end_dt --calculate end date as one day before the next start date
		from bronze.crm_prd_info --where  substring(prd_key,7,len(prd_key)) IN ( select sls_prd_key from bronze.crm_sales_details) 
		Set @end_time=Getdate()
		print'>>Load duration ' + Cast(Datediff(second,@start_time,@end_time) as Nvarchar) +' seconds'

		print'--------------------------------------';
		print'    Bronze.crm_sales_details         ';
		print'--------------------------------------'

		set @start_time = Getdate()
		print'>>Truncating Table:silver.crm_sales_details'
		Truncate Table silver.crm_sales_details
		print'Inserting Data into:silver.crm_sales_details'
		Insert into silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_Quantity,
		sls_price
		)


		select
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		Case  when sls_order_dt <= 0  or len(sls_order_dt) != 8 then Null
			Else Cast(Cast(sls_order_dt as Varchar) as Date)
		End sls_order_dt,	
		Case when sls_ship_dt = 0 or len(sls_ship_dt) !=8 then Null
			 Else cast(cast(sls_ship_dt as varchar )as Date) 
		End sls_ship_dt,
		Case when sls_due_dt <=0 or len(sls_due_dt) != 8 Then Null
			Else Cast(Cast(sls_due_dt as Varchar) as Date)
		End sls_due_dt,
		case when sls_sales is null or sls_sales<=0 or sls_sales != sls_quantity * ABS(sls_price)
			then sls_quantity * ABS(sls_price)
			else sls_sales
		End sls_sales,
		sls_Quantity,
		case when sls_price is null or sls_price <=0 then sls_sales/Nullif(sls_quantity,0)
			else sls_price
		End sls_price
		from bronze.crm_sales_details
		set @end_time=Getdate()
		print'>> Load duration '+ cast(datediff(second,@start_time,@end_time) as Nvarchar) + ' seconds'



		Print'--------------------------'
		print'  Bronze Erp_cust_AZ12     '
		print'---------------------------'

		set @start_time = Getdate()
		print'>>Truncating Table silver.erp_Cust_AZ12'
		Truncate table silver.erp_Cust_AZ12
		print'>> Inserting data into:silver.erp_Cust_AZ12'
		insert into silver.erp_Cust_AZ12
		(Cid,
		Bdate,
		gen)
		select 
		Case when CID like 'NAS%' then Substring(CID,4,Len(CID))
			Else CID
		End as CID,
		case when Bdate >Getdate() then Null
			else Bdate
		End Bdate,
		case when Upper(trim(gen)) in ('F','Female') then 'Female'
			WHEN UPPER(TRIM(GEN)) IN ('M','Male') then 'Male'
			Else 'N/A'
		end Gen
		from bronze.erp_Cust_AZ12
		set @end_time=Getdate()
		print'>> Load duration '+ cast(datediff(second,@start_time,@end_time) as Nvarchar) + ' seconds'


		Print'--------------------------'
		print'  Bronze Erp_cust_AZ12     '
		print'---------------------------'

		Set @start_time = Getdate()
		print'>>Truncating table silver.erp_LOC_A101'
		Truncate table silver.erp_LOC_A101
		print'>>Inserting data into:silver.erp_LOC_A101'
		insert into silver.erp_LOC_A101(
		CID,
		Cntry)

		select 
		Replace (CID,'-','') as CID,
		Case when Trim(Cntry) ='DE' then 'Germany'
			when Trim(cntry) in ('US','USA') then 'United States'
			when Trim(cntry) ='' or Cntry is null then 'N/A'
			Else cntry
		End Cntry
		from bronze.erp_LOC_A101
		set @end_time=Getdate()
		print'>> Load duration '+ cast(datediff(second,@start_time,@end_time) as Nvarchar) + ' seconds'



		print'--------------------------------------'
		print'     Bronze.erp_PX_CAT_G1V2     '
		print'--------------------------------------'

		Set @start_time = Getdate()
		print'>>Truncating table silver.erp_PX_CAT_G1V2'
		Truncate table silver.erp_PX_CAT_G1V2
		print'>>inserting Data into:silver.erp_PX_CAT_G1V2'
		insert into silver.erp_PX_CAT_G1V2(
		ID,
		Cat,
		subcat,
		Maintenance
		)

		select ID,
		cat,
		subcat,
		Maintenance
		from bronze.erp_PX_CAT_G1V2 
		set @end_time=Getdate()
		print'>> Load duration '+ cast(datediff(second,@start_time,@end_time) as Nvarchar) + ' seconds'

		set @batch_end_time = Getdate()
		print'>> Total Batch duratin ' + Cast(Datediff(second,@batch_start_time,@batch_end_time) As Nvarchar) + ' seconds'

	End Try
	Begin catch
	print'-----------------------------'
	print'Error message during the load silver layer'
	print' Error message'+ Error_message();
	print'Error message' + Cast(Error_Number() as NVarchar);
	print'Error message' + cast(Error_state() as Nvarchar);
	print'-----------------------------'
	
	End Catch
END
