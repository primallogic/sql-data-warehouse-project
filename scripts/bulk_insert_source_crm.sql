BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\Primal Logic HQ\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
