# 🧊 Snowflake CDC Pipeline using AWS S3

This project demonstrates a **fully automated Change Data Capture (CDC) pipeline** using **Snowflake** and **AWS S3**, utilizing Snowflake-native features like Streams, Tasks, External Stages, and Auto-Ingest Pipes to ingest, process, and manage data efficiently.

---

## 🔧 Step-by-Step Setup

### 1. **Create Database & Schema**

```sql
CREATE DATABASE PROJECTS_DB;
CREATE SCHEMA PROJECTS_DB.CDC;
```

### 2. **Define File Format for CSV**

```sql
CREATE OR REPLACE FILE FORMAT csv_type
TYPE='CSV'
FIELD_DELIMITER=','
SKIP_HEADER=1
ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE;
```

### 3. **Create External Stage with S3 Integration**

```sql
CREATE STORAGE INTEGRATION s3_integration
TYPE=EXTERNAL_STAGE
STORAGE_PROVIDER=S3
ENABLED=TRUE
STORAGE_AWS_ROLE_ARN='arn:aws:iam::...'
STORAGE_ALLOWED_LOCATIONS=('s3://snowflake-practitce-bucket/cdc/');

CREATE OR REPLACE STAGE s3_ext_stage
URL='s3://snowflake-practitce-bucket/cdc/'
FILE_FORMAT=csv_type
STORAGE_INTEGRATION=s3_integration;
```

---

## 📥 Data Ingestion

### 4. **Create Staging & Target Tables**

```sql
CREATE OR REPLACE TABLE CUSTOMER_STAGING (...);
CREATE OR REPLACE TABLE CUSTOMER_TARGET (...);
```

### 5. **Create Auto-Ingest Pipe**

Automatically loads data from S3 into `CUSTOMER_STAGING`.

```sql
CREATE OR REPLACE PIPE staging_pipe
AUTO_INGEST=TRUE
AS
COPY INTO CUSTOMER_STAGING
FROM (
    SELECT 
        $1::INT, $2::STRING, $3::STRING, $4::STRING, CURRENT_TIMESTAMP()
    FROM @s3_ext_stage
)
FILE_FORMAT=(FORMAT_NAME=csv_type)
ON_ERROR=CONTINUE;
```

---

## 🔄 Change Tracking & Transformation

### 6. **Create Stream on Staging Table**

```sql
CREATE OR REPLACE STREAM my_table_stream ON TABLE CUSTOMER_STAGING;
```

### 7. **Task: Merge Staging to Target (SCD1 Logic)**

```sql
CREATE OR REPLACE TASK process_new_data_to_tgt
WHEN SYSTEM$STREAM_HAS_DATA('my_table_stream')
AS
MERGE INTO CUSTOMER_TARGET USING (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY Curr_Timestamp DESC) AS rn
        FROM CUSTOMER_STAGING
    ) WHERE rn = 1
) AS stg
ON ...
WHEN MATCHED THEN UPDATE ...
WHEN NOT MATCHED THEN INSERT ...
```

---

## 📊 Aggregation & Maintenance

### 8. **Create Table for Aggregated Results**

```sql
CREATE OR REPLACE TABLE AGGREGATED_RESULTS_TABLE (
    TOTAL_PROCESSED_RECORD NUMBER,
    TOTAL_UPDATED_RECORDS NUMBER,
    TOTAL_NEW_INSERTED_RECORDS NUMBER
);
```

### 9. **Task: Populate Aggregated Metrics**

```sql
CREATE OR REPLACE TASK AGGREGATED_RESULT
AFTER process_new_data_to_tgt
AS
INSERT INTO AGGREGATED_RESULTS_TABLE
SELECT
    (SELECT COUNT(*) FROM CUSTOMER_STAGING),
    (SELECT COUNT(*) FROM CUSTOMER_STAGING a JOIN CUSTOMER_TARGET b ON a.CustomerID = b.CustomerID),
    (SELECT COUNT(*) FROM CUSTOMER_TARGET b LEFT JOIN CUSTOMER_STAGING a ON a.CustomerID = b.CustomerID WHERE a.CustomerID IS NULL);
```

---

## 🧹 Post-Processing & Cleanup

### 10. **Task: Truncate Staging Table**

```sql
CREATE OR REPLACE TASK truncate_staging
AFTER AGGREGATED_RESULT
AS
TRUNCATE TABLE CUSTOMER_STAGING;
```

### 11. **Task: Auto Suspend Pipeline**

```sql
CREATE OR REPLACE TASK auto_suspend
AFTER truncate_staging
AS
ALTER TASK process_new_data_to_tgt SUSPEND;
```

### 12. **Trigger Task (Resumes pipeline if new data arrives)**

```sql
CREATE OR REPLACE TASK my_task
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('my_table_stream')
AS
ALTER TASK process_new_data_to_tgt RESUME;
```

---

## ✅ Final Activation

```sql
-- Enable tasks
ALTER TASK my_task RESUME;
ALTER TASK auto_suspend RESUME;
ALTER TASK truncate_staging RESUME;
ALTER TASK AGGREGATED_RESULT RESUME;
ALTER TASK process_new_data_to_tgt RESUME;
```

---

## 💡 Highlights

* ✅ **Auto-Ingest** from S3 to Snowflake
* 📈 **Change detection** using `STREAM`
* 🔄 **SCD Type-1** merge strategy
* 📊 **Automated metrics** tracking
* 💰 **Cost control** with auto suspend/resume logic

---
