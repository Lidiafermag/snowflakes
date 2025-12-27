CREATE OR REPLACE PROCEDURE load_bronze_customers()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
    TRUNCATE TABLE BRONZE_CUSTOMERS;

    INSERT INTO BRONZE_CUSTOMERS
        SELECT 
            CAST($1 AS VARIANT) as raw_data,
            metadata$filename as filename,                     
            CURRENT_TIMESTAMP() as created_at                  
        FROM @FORMACAO.PUBLIC.NORTH/customers 
        (FILE_FORMAT => 'PARQUET_FORMAT');
    RETURN 'Load Bronze Customers table successfully';
END;
$$;