USE DataWarehouse;

-- Explore All Objest in the database
SELECT * 
FROM DataWarehouse.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'gold';

-- Explore all columns in the database
SELECT * 
FROM DataWarehouse.INFORMATION_SCHEMA.columns
WHERE TABLE_SCHEMA = 'gold' AND TABLE_NAME='dim_customers';