==========================================================================================================================
Store Procedure: Load source to bronze
==========================================================================================================================
Script Purposes: Load source data from external csv files
                 Truncate the table before loading source datas
                 BULK INSERT is used to inset all the datas
  
Usage Example: EXEC bronze.load_bronze
==========================================================================================================================
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN

	DECLARE @start_time DATETIME ,@end_time DATETIME ,@batch_time_starts DATETIME, @batch_time_ends DATETIME;

	BEGIN TRY		
		SET @batch_time_starts = GETDATE();
		PRINT '===================================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '===================================================================';

		PRINT '-------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info

		PRINT '>> Inserting Data Into: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\HP\Desktop\Modern_Data_Warehouse_Project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2, --1st row has column name so we eliminate that
			FIELDTERMINATOR = ',', -- column values are sprated by commas
			TABLOCK -- It locks the table during the loading time
		);

		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR ) + ' seconds'
		PRINT '>> -----------------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details

		PRINT '>> Inserting Data Into: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\HP\Desktop\Modern_Data_Warehouse_Project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2, --1st row has column name so we eliminate that
			FIELDTERMINATOR = ',', -- column values are sprated by commas
			TABLOCK -- It locks the table during the loading time
		);

		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR ) + ' seconds'
		PRINT '>> -----------------------------------------'
	
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info

		PRINT '>> Inserting Data Into: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\HP\Desktop\Modern_Data_Warehouse_Project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2, --1st row has column name so we eliminate that
			FIELDTERMINATOR = ',', -- column values are sprated by commas
			TABLOCK -- It locks the table during the loading time
		);

		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR ) + ' seconds';
		PRINT '>> -----------------------------------------';


		PRINT '-------------------------------------------------------------------'
		PRINT 'Loading ERP Tables'
		PRINT '-------------------------------------------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\HP\Desktop\Modern_Data_Warehouse_Project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2, --1st row has column name so we eliminate that
			FIELDTERMINATOR = ',', -- column values are sprated by commas
			TABLOCK -- It locks the table during the loading time
		);

		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR ) + ' seconds';
		PRINT '>> -----------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\HP\Desktop\Modern_Data_Warehouse_Project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2, --1st row has column name so we eliminate that
			FIELDTERMINATOR = ',', -- column values are sprated by commas
			TABLOCK -- It locks the table during the loading time
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR ) + ' seconds';
		PRINT '>> -----------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\HP\Desktop\Modern_Data_Warehouse_Project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2, --1st row has column name so we eliminate that
			FIELDTERMINATOR = ',', -- column values are sprated by commas
			TABLOCK -- It locks the table during the loading time
		);

		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR ) + ' seconds';
		PRINT '>> -----------------------------------------';

		SET @batch_time_ends = GETDATE();
		PRINT '========================================================================='
		PRINT 'Loading Bronze Layer is Completed';
		PRINT 'Total Batch Duration is : '+ CAST(DATEDIFF(SECOND, @batch_time_starts, @batch_time_ends) AS NVARCHAR) + 'seconds'
		PRINT '========================================================================='

	END TRY 
	BEGIN CATCH
	PRINT '=============================================================='
	PRINT 'Error Message: ' + ERROR_MESSAGE();
	PRINT 'Error Message: ' + CAST(ERROR_NUMBER() AS NVARCHAR) ;
	PRINT 'Error Message: ' + CAST(ERROR_STATE() AS NVARCHAR) ;
	PRINT '=============================================================='
	END CATCH
END
