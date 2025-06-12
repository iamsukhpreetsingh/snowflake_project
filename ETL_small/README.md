    -- CREATING SCHEMA 
    
    CREATE OR REPLACE SCHEMA MYDB.proj_schema;
    
    
    --CREATING TABLE FOR DUMPING RAW TABLE
    
    create OR REPLACE table MYDB.proj_schema.raw_tbl (
    Branch_ID varchar,
    Dealer_ID varchar,
    Model_ID varchar,
    Revenue	int,
    Units_Sold int,
    Date_ID	varchar,
    Month int,
    Year int,
    BranchName string,
    DealerName string,
    Product_Name varchar);
    
    
    -- CREATING FILE FORMAT
    
    CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;
    
    
    
    -- CREATING STORAGE INTEGRATION
    
    CREATE OR REPLACE STORAGE INTEGRATION s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::003956177349:role/S3_FULLACCESS_SNOWFLAKE'
    STORAGE_ALLOWED_LOCATIONS=('s3://snowflake-practitce-bucket/data/');
    
    
    
    -- CREATING EXTERNAL STAGE
    
    CREATE OR REPLACE STAGE ext_stg_s3
    url = 's3://snowflake-practitce-bucket/data/'
    FILE_FORMAT = csv_format
    STORAGE_INTEGRATION = s3_integration;
    
    
    
    
    -- CREATING PIPE TO INGEST DATA INTO RAW TABLE
    
    CREATE OR REPLACE PIPE raw_pipe
    AUTO_INGEST = TRUE
    AS
    COPY INTO MYDB.proj_schema.raw_tbl
    FROM(
    SELECT
            $1::VARCHAR AS Branch_ID,
            $2::VARCHAR AS Dealer_ID,
            $3::VARCHAR AS Model_ID,
            $4::INT AS Revenue,
            $5::INT AS Units_Sold,
            $6::VARCHAR AS Date_ID,
            $7::INT AS Month,
            $8::INT AS Year,
            $9::VARCHAR AS BranchName,
            $10::VARCHAR AS DealerName,
            $11::VARCHAR AS Product_Name
    FROM @ext_stg_s3
    )
    FILE_FORMAT=(FORMAT_NAME=csv_format);
    
    
    -- CREATE SILVER TABLE 
    
    CREATE OR REPLACE TABLE MYDB.proj_schema.SILVER_TABLE(
    Branch_ID varchar,
    Dealer_ID varchar,
    Model_ID varchar,
    Revenue	int,
    Units_Sold int,
    Date_ID	varchar,
    Month int,
    Year int,
    BranchName string,
    DealerName string,
    Product_Name varchar,
    load_date DATE
    );
    
    
    -- CREATING TASKS TO TRANSFORM THE DATA AND SUTOINGEST EVERY MINUTE
    
    CREATE OR REPLACE TASK transform_task
    WAREHOUSE= COMPUTE_WH
    SCHEDULE = '1 M'
    AS
    MERGE INTO MYDB.proj_schema.SILVER_TABLE AS target_tbl
    USING (
    SELECT
        Branch_ID, Dealer_ID, Model_ID, Revenue, Units_Sold, Date_ID, Month, Year, BranchName, DealerName, Product_Name, CURRENT_DATE() AS load_date
        FROM MYDB.proj_schema.raw_tbl
        WHERE Revenue > 999999
    ) AS raw_tbl
    ON target_tbl.Branch_ID = raw_tbl.Branch_ID
    WHEN MATCHED THEN
    UPDATE SET
    target_tbl.Branch_ID = raw_tbl.Branch_ID, 
    target_tbl.Dealer_ID = raw_tbl.Dealer_ID, 
    target_tbl.Model_ID = raw_tbl.Model_ID, 
    target_tbl.Revenue = raw_tbl.Revenue, 
    target_tbl.Units_Sold = raw_tbl.Units_Sold, 
    target_tbl.Date_ID = raw_tbl.Date_ID, 
    target_tbl.Month = raw_tbl.Month, 
    target_tbl.Year = raw_tbl.Year, 
    target_tbl.BranchName = raw_tbl.BranchName, 
    target_tbl.DealerName = raw_tbl.DealerName, 
    target_tbl.Product_Name = raw_tbl.Product_Name,
    target_tbl.load_date = raw_tbl.load_date
    
    WHEN NOT MATCHED THEN 
    INSERT (Branch_ID, Dealer_ID, Model_ID, Revenue, Units_Sold, Date_ID, Month, Year, BranchName, DealerName, Product_Name, load_date)
    VALUES (raw_tbl.Branch_ID, raw_tbl.Dealer_ID, raw_tbl.Model_ID, raw_tbl.Revenue, raw_tbl.Units_Sold, raw_tbl.Date_ID, raw_tbl.Month, raw_tbl.Year, raw_tbl.BranchName, raw_tbl.DealerName, raw_tbl.Product_Name, raw_tbl.load_date);
    
    

ALTER TASK transform_task RESUME;

