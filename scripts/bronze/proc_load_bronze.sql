/*
====================================================================
Stored Procedure: Load Bronze Layer (Source - > Bronze)
====================================================================
Script Purpose:
	This stored procedure loads data into the 'bronze' schema from external CSV files.
	It performs the following actions:
	- Truncates the bronze tables before loading data.
	- Uses the 'BULK INSERT' command to load data from CSV files to bronze tables. 

Parameters:
	None.
	This stored procedure does not accept any parameters or return any values. 

Usage Example:
	EXEC bronze.load_bronze;
====================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=====================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=====================================================';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.lahman_batting'; 
		TRUNCATE TABLE bronze.lahman_batting;

		PRINT '>> Inserting Data Into: bronze.lahman_batting';
		BULK INSERT bronze.lahman_batting
		FROM 'C:\Users\jacob\Desktop\lahman_1871-2024_csv\batting.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.lahman_pitching'; 
		TRUNCATE TABLE bronze.lahman_pitching;

		PRINT '>> Inserting Data Into: bronze.lahman_pitching';
		BULK INSERT bronze.lahman_pitching
		FROM 'C:\Users\jacob\Desktop\lahman_1871-2024_csv\pitching.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.lahman_fielding'; 
		TRUNCATE TABLE bronze.lahman_fielding;

		PRINT '>> Inserting Data Into: bronze.lahman_fielding';
		BULK INSERT bronze.lahman_fielding
		FROM 'C:\Users\jacob\Desktop\lahman_1871-2024_csv\fielding.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.lahman_parks';
		TRUNCATE TABLE bronze.lahman_parks;
	
		PRINT '>> Inserting Data Into: bronze.lahman_parks';
		BULK INSERT bronze.lahman_parks
		FROM 'C:\Users\jacob\Desktop\lahman_1871-2024_csv\parks.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.lahman_people';
		TRUNCATE TABLE bronze.lahman_people;
	
		PRINT '>> Inserting Data Into: bronze.lahman_people';
		BULK INSERT bronze.lahman_people
		FROM 'C:\Users\jacob\Desktop\lahman_1871-2024_csv\people.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.lahman_salaries';
		TRUNCATE TABLE bronze.lahman_salaries;

		PRINT '>> Inserting Data Into: bronze.lahman_salaries';
		BULK INSERT bronze.lahman_salaries
		FROM 'C:\Users\jacob\Desktop\lahman_1871-2024_csv\salaries.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.lahman_teams';
		TRUNCATE TABLE bronze.lahman_teams;

		PRINT '>> Inserting Data Into: bronze.lahman_teams';
		BULK INSERT bronze.lahman_teams
		FROM 'C:\Users\jacob\Desktop\lahman_1871-2024_csv\teams.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.lahman_managers';
		TRUNCATE TABLE bronze.lahman_managers;

		PRINT '>> Inserting Data Into: bronze.lahman_managers';
		BULK INSERT bronze.lahman_managers
		FROM 'C:\Users\jacob\Desktop\lahman_1871-2024_csv\managers.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------';

		SET @batch_end_time = GETDATE();
		PRINT '==========================================';
		PRINT 'Bronze Layer Load Completed';
		PRINT '	 - Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '==========================================';
	END TRY
	BEGIN CATCH
		PRINT '====================================';
		PRINT 'ERROR OCCURED DURING	LOADING PROCESS';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '====================================';
	END CATCH
END
