IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'consume')
BEGIN
  CREATE DATABASE consume;
END;
GO

USE consume
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
  DECLARE @create_schema nvarchar(100) = 'CREATE SCHEMA gold';
  EXEC sp_executesql @tsql = @create_schema;
END;
GO

CREATE OR ALTER VIEW gold.enade
AS
SELECT *
FROM 
    OPENROWSET(
        BULK 'https://datalakeigtibootcamp.dfs.core.windows.net/datalake/gold/enade/',
        FORMAT='DELTA'
    ) AS [result]
GO