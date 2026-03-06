/*
============================================================================================
Create Database and Schemas
=====================================================================================
Script Purpose:
  This script creates a new database named "Baseball". First, the script checks if the database exisits. 
  If it exists, it is dropped and recreated. Additionally, the script sets up three schemas
  within the database: 'bronze', 'silver', 'gold'. 
*/

USE Master;
GO

-- Drop and recreate the 'Baseball' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Baseball')
BEGIN
  ALTER DATABASE Baseball SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE Baseball;
END;
GO

-- Create the 'Baseball' database
CREATE DATABASE Baseball;
GO

USE Baseball;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
