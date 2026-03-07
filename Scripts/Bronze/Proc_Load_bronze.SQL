/*
=====================================================
Stored Proceure : Load Bronze Layer (Score -->Bronze)
====================================================
Script Purpose:
    This stored procedure loads data into the 'Bronze' Schema from External CSV files.
    It performs the following Actions
      -Truncates the bronze tables before loading data
      - Use 'Bulk Insert' command load data from CSV files to Bronze Tables.

Parameters:
  None
  This stored procedures does not accept any parameters or return any values

Usage Examples:
  EXEC Bronze.load_bronze;
===================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start DATETIME, @batch_end DATETIME;
	
	BEGIN TRY
		SET @batch_start = GETDATE(); -- Track the whole process
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		-- 1. crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Data into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info 
		FROM 'D:\SQL\sql-data-warehouse-project_2\datasets\source_crm\cust_info.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		-- 2. crm_prd_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info 
		FROM 'D:\SQL\sql-data-warehouse-project_2\datasets\source_crm\prd_info.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		-- 3. crm_sales_details
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details 
		FROM 'D:\SQL\sql-data-warehouse-project_2\datasets\source_crm\sales_details.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

		-- 4. erp_Cust_AZ12
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_Cust_AZ12';
		TRUNCATE TABLE bronze.erp_Cust_AZ12;
		PRINT '>> Inserting Data into: bronze.erp_Cust_AZ12';
		BULK INSERT bronze.erp_Cust_AZ12 
		FROM 'D:\SQL\sql-data-warehouse-project_2\datasets\source_erp\Cust_AZ12.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		-- 5. erp_LOC_A101
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_LOC_A101';
		TRUNCATE TABLE bronze.erp_LOC_A101;
		PRINT '>> Inserting Data into: bronze.erp_LOC_A101';
		BULK INSERT bronze.erp_LOC_A101 
		FROM 'D:\SQL\sql-data-warehouse-project_2\datasets\source_erp\LOC_A101.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		-- 6. erp_PX_CAT_G1V2
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_PX_CAT_G1V2';
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
		PRINT '>> Inserting Data into: bronze.erp_PX_CAT_G1V2';
		BULK INSERT bronze.erp_PX_CAT_G1V2 
		FROM 'D:\SQL\sql-data-warehouse-project_2\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		SET @batch_end = GETDATE();
		PRINT '================================================';
		PRINT 'Bronze Layer Load Complete!';
		PRINT 'Total Batch Duration: ' + CAST(DATEDIFF(second, @batch_start, @batch_end) AS NVARCHAR) + ' seconds';
		PRINT '================================================';

	END TRY
	BEGIN CATCH
		PRINT '---------------------------------------------------------------------';
		PRINT 'ERROR OCCURRED DURING BRONZE LAYER LOAD';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '---------------------------------------------------------------------';
	END CATCH
END;
