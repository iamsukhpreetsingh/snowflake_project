# 🧊 Snowflake Data Pipeline: From S3 to Silver Table

This document outlines the complete setup of a Snowflake data pipeline that ingests raw CSV data from an Amazon S3 bucket, stores it in a raw table, and then transforms and loads it into a silver table based on specific business logic (e.g., filtering records with high revenue).

## 📁 Overview

The pipeline consists of the following components:

1. **Schema Setup**
2. **Raw Table Creation**
3. **File Format Definition**
4. **Storage Integration with S3**
5. **External Stage Configuration**
6. **Pipe for Auto-Ingestion**
7. **Silver Table for Transformed Data**
8. **Task for Scheduled Transformation**

Let’s go through each component step-by-step.

---

## 1️⃣ Schema Creation

We start by creating a dedicated schema to organize all objects related to this project.

```sql
CREATE OR REPLACE SCHEMA MYDB.proj_schema;
```

---

## 2️⃣ Raw Table Definition

A table is created to store the raw data as it comes from the S3 bucket. This includes fields like branch ID, dealer info, revenue, units sold, and more.

```sql
CREATE OR REPLACE TABLE MYDB.proj_schema.raw_tbl (
    Branch_ID VARCHAR,
    Dealer_ID VARCHAR,
    Model_ID VARCHAR,
    Revenue INT,
    Units_Sold INT,
    Date_ID VARCHAR,
    Month INT,
    Year INT,
    BranchName STRING,
    DealerName STRING,
    Product_Name VARCHAR
);
```

---

## 3️⃣ File Format Specification

To properly parse the incoming CSV files, we define a custom file format.

```sql
CREATE OR REPLACE FILE FORMAT csv_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;
```

---

## 4️⃣ Storage Integration with AWS S3

We set up a storage integration to securely connect Snowflake with our S3 bucket using an IAM role.

```sql
CREATE OR REPLACE STORAGE INTEGRATION s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::003956177349:role/S3_FULLACCESS_SNOWFLAKE'
STORAGE_ALLOWED_LOCATIONS=('s3://snowflake-practitce-bucket/data/');
```

---

## 5️⃣ External Stage Setup

An external stage is defined to point to the S3 location where the raw CSV files are stored.

```sql
CREATE OR REPLACE STAGE ext_stg_s3
URL = 's3://snowflake-practitce-bucket/data/'
FILE_FORMAT = csv_format
STORAGE_INTEGRATION = s3_integration;
```

---

## 6️⃣ Pipe for Continuous Ingestion

A pipe is configured to auto-ingest new files from the S3 bucket into the raw table.

```sql
CREATE OR REPLACE PIPE raw_pipe
AUTO_INGEST = TRUE
AS
COPY INTO MYDB.proj_schema.raw_tbl
FROM (
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
```

> ✅ Ensure that S3 Event Notifications are configured to trigger Snowpipe when new files arrive.

---

## 7️⃣ Silver Table for Cleaned Data

A silver table is created to store transformed and filtered data. It also includes a `load_date` column to track when the data was processed.

```sql
CREATE OR REPLACE TABLE MYDB.proj_schema.SILVER_TABLE(
    Branch_ID VARCHAR,
    Dealer_ID VARCHAR,
    Model_ID VARCHAR,
    Revenue INT,
    Units_Sold INT,
    Date_ID VARCHAR,
    Month INT,
    Year INT,
    BranchName STRING,
    DealerName STRING,
    Product_Name VARCHAR,
    load_date DATE
);
```

---

## 8️⃣ Task for Scheduled Transformation

A scheduled task runs every minute to transform data from the raw table and insert/update the silver table. It filters only records where `Revenue > 999999`.

```sql
CREATE OR REPLACE TASK transform_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS
MERGE INTO MYDB.proj_schema.SILVER_TABLE AS target_tbl
USING (
    SELECT
        Branch_ID, Dealer_ID, Model_ID, Revenue, Units_Sold, Date_ID, Month, Year, 
        BranchName, DealerName, Product_Name, CURRENT_DATE() AS load_date
    FROM MYDB.proj_schema.raw_tbl
    WHERE Revenue > 999999
) AS raw_tbl
ON target_tbl.Branch_ID = raw_tbl.Branch_ID
WHEN MATCHED THEN
UPDATE SET
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
INSERT (Branch_ID, Dealer_ID, Model_ID, Revenue, Units_Sold, Date_ID, Month, Year, 
        BranchName, DealerName, Product_Name, load_date)
VALUES (
    raw_tbl.Branch_ID, raw_tbl.Dealer_ID, raw_tbl.Model_ID, raw_tbl.Revenue, 
    raw_tbl.Units_Sold, raw_tbl.Date_ID, raw_tbl.Month, raw_tbl.Year, 
    raw_tbl.BranchName, raw_tbl.DealerName, raw_tbl.Product_Name, raw_tbl.load_date
);

-- Start the task
ALTER TASK transform_task RESUME;
```

---

## 🧪 Summary of Workflow

| Step | Description |
|------|-------------|
| 1 | Raw CSV files uploaded to S3 |
| 2 | Snowpipe auto-ingests data into `raw_tbl` |
| 3 | Every minute, the transformation task runs |
| 4 | Only high-revenue records are inserted or updated in `silver_table` |

---

## 📌 Notes

- Ensure proper IAM permissions are granted for Snowflake to access the S3 bucket.
- The `AUTO_INGEST` pipe relies on S3 Event notifications to trigger ingestion automatically.
- The warehouse used (`COMPUTE_WH`) must be running for the task to execute successfully.
