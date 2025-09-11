/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_customer';
		TRUNCATE TABLE bronze.crm_customer;
		PRINT '>> Inserting Data Into: bronze.crm_customer';
		BULK INSERT bronze.crm_customer
		FROM '/var/opt/mssql/data/source_crm/customers.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_customer_addresses';
		TRUNCATE TABLE bronze.crm_customer_addresses;
		PRINT '>> Inserting Data Into: bronze.crm_customer_addresses';
		BULK INSERT bronze.crm_customer_addresses
		FROM '/var/opt/mssql/data/source_crm/customer_addresses.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_product';
		TRUNCATE TABLE bronze.erp_product;
		PRINT '>> Inserting Data Into: bronze.erp_product';
		BULK INSERT bronze.erp_product
		FROM '/var/opt/mssql/data/source_erp/product.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_product_prices';
		TRUNCATE TABLE bronze.erp_product_prices;
		PRINT '>> Inserting Data Into: bronze.erp_product_prices';
		BULK INSERT bronze.erp_product_prices
		FROM '/var/opt/mssql/data/source_erp/product_prices.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_payment_channel';
		TRUNCATE TABLE bronze.erp_payment_channel;

		PRINT '>> Inserting Data Into: bronze.erp_payment_channel';
		BULK INSERT bronze.erp_payment_channel
		FROM '/var/opt/mssql/data/source_erp/payment_channel.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_shipment_company';
		TRUNCATE TABLE bronze.erp_shipment_company;
		PRINT '>> Inserting Data Into: bronze.erp_shipment_company';
		BULK INSERT bronze.erp_shipment_company
		FROM '/var/opt/mssql/data/source_erp/shipment_company.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_orders';
		TRUNCATE TABLE bronze.erp_orders;
		PRINT '>> Inserting Data Into: bronze.erp_orders';
		BULK INSERT bronze.erp_orders
		FROM '/var/opt/mssql/data/source_erp/orders.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_order_detail';
		TRUNCATE TABLE bronze.erp_order_detail;
		PRINT '>> Inserting Data Into: bronze.erp_order_detail';
		BULK INSERT bronze.erp_order_detail
		FROM '/var/opt/mssql/data/source_erp/order_detail.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_invoice';
		TRUNCATE TABLE bronze.erp_invoice;
		PRINT '>> Inserting Data Into: bronze.erp_invoice';
		BULK INSERT bronze.erp_invoice
		FROM '/var/opt/mssql/data/source_erp/invoice.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_invoice_detail';
		TRUNCATE TABLE bronze.erp_invoice_detail;
		PRINT '>> Inserting Data Into: bronze.erp_invoice_detail';
		BULK INSERT bronze.erp_invoice_detail
		FROM '/var/opt/mssql/data/source_erp/invoice_detail.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_payment';
		TRUNCATE TABLE bronze.erp_payment;
		PRINT '>> Inserting Data Into: bronze.erp_payment';
		BULK INSERT bronze.erp_payment
		FROM '/var/opt/mssql/data/source_erp/payment.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_shipment';
		TRUNCATE TABLE bronze.erp_shipment;
		PRINT '>> Inserting Data Into: bronze.erp_shipment';
		BULK INSERT bronze.erp_shipment
		FROM '/var/opt/mssql/data/source_erp/shipment.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
