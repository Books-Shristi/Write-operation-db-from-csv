Use Bookswagon25
Go

SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Table_TempMasterUpload_Dump'
AND DATA_TYPE IN ('numeric','decimal','int','smallint','bigint')

SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Table_TempMasterUpload_Dump'
AND COLUMN_NAME IN (
    'Price','Discount','Height','Width','Weight',
    'Quantity','No_of_Pages'
)

SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Table_TempMasterUpload_Dump'
ORDER BY ORDINAL_POSITION;

SELECT TOP 10 *
FROM Table_TempMasterUpload_Dump
ORDER BY ID_MasterUpload DESC;
