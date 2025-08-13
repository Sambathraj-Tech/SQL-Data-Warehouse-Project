/*=============================================================================
  Create database 'DataWarehouse'and create schemas 'bronze , silver , gold' ,
  if the database already exists then drop the database and
  create new database DataWarehouse

WARNING:
  If the database is dropped all the datas inside the database also deleted 
=============================================================================*/

USE master;
GO
  
-- Create database
CREATE DATABASE DataWarehouse;
GO
  
USE DataWarehouse;
GO
  
--Create schemas for 'DataWarehouse'
CREATE SCHEMA bronze;
GO
  
CREATE SCHEMA silver;
GO
  
CREATE SCHEMA gold;
