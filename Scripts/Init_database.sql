/*
==========================================
Create Database Schemas
=========================================

Script Purpose:
  This script creates a new database named 'Datawarhouse' after checking it already exists.
  if the database exists,it dropped and recreated.Additionally,the script set up  three Schemas
  within the Database:'bronze','silver','gold'.

Warning:
  Running this script will drop the entire 'DataWarhouse' database if it exists.
  All data in the database will be permanently deleted .Proceed with caution and 
  ensure you have proper backups before running the script.


*/

Use master;
Go
  
--Drop and recreate the 'DataWarehouse' Database
if exists (select 1 from sys.database where name='DataWarhouse')
BEGIN
  Alter Database Datawarhouse Set Single_user with Rollback Immediate;
  Drop Database DataWarhouse;
END
Go
-- Create Database Datawarehouse

create Database DataWarhouse;

Use DataWarhouse

create Schema bronze;
Go
create Schema Silver;
Go
create Schema Gold;
Go
