
# 🧱 End-to-End Data Pipeline in Snowflake: Ingesting, Transforming, and Aggregating JSON Logs

---

## 🎯 Objective

Build a **real-time data pipeline** with the following components:

1. **Ingest** raw JSON logs from an S3 bucket.
2. **Store** raw data in a `VARIANT` column.
3. **Parse** and transform JSON into structured fields using Streams & Tasks.
4. **Aggregate** daily active users.
5. Use **Snowpipe** for auto-ingestion and **Tasks** for orchestration.

---

## 🔁 Architecture Overview (Text-Based)

```
[Amazon S3 Bucket]
       |
[Snowflake External Stage]
       |
[Snowpipe Auto-Ingestion]
       |
[Raw Events Table (Variant)]
       |
[Stream: RAW_DATA_STREAM]
       |
[Task: Parse & Insert into Staging]
       |
[Staging Events Table]
       |
[Stream: STAGING_EVENTS_STREAM]
       |
[Task: Aggregate Daily Active Users]
       |
[Daily Active Users Table]
```

---

## 📦 Step-by-Step Implementation

### 1. Set Up Database and Schema

```sql
USE DATABASE LOGS_PIPELINE;
CREATE SCHEMA MAIN_SCHEMA;
```

---

### 2. Define File Format for JSON

We define a file format to handle incoming JSON files.

```sql
CREATE OR REPLACE FILE FORMAT JSON_FF
TYPE = 'JSON'
COMPRESSION = 'AUTO';
```

---

### 3. Configure Storage Integration with AWS S3

This integration allows Snowflake to securely access your S3 bucket.

```sql
CREATE STORAGE INTEGRATION ext_s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::000000000:role/S3_FULLACCESS_SNOWFLAKE'
STORAGE_ALLOWED_LOCATIONS=('s3://snowflake-practitce-bucket/json/');
```

> ✅ Ensure IAM role permissions are correctly set up in AWS.

---

### 4. Create External Stage

The external stage connects Snowflake to the S3 bucket.

```sql
CREATE OR REPLACE STAGE s3_ext_stg
URL = 's3://snowflake-practitce-bucket/json/'
FILE_FORMAT = JSON_FF
STORAGE_INTEGRATION = ext_s3_integration;
```

Check if files are listed properly:

```sql
LIST @s3_ext_stg;
```

---

### 5. Create Raw Events Table

This table stores the entire JSON blob as a `VARIANT`.

```sql
CREATE OR REPLACE TABLE RAW_EVENTS_TBL(DATA VARIANT);
```

---

### 6. Set Up Snowpipe for Auto-Ingestion

Snowpipe enables continuous ingestion from external stages.

```sql
CREATE OR REPLACE PIPE RAW_TBL_PIPE
AUTO_INGEST = TRUE
AS 
COPY INTO RAW_EVENTS_TBL
FROM @s3_ext_stg;
```

> 🔄 Snowpipe automatically detects new files uploaded to the S3 bucket and loads them.

---

### 7. Create Stream on Raw Events Table

A stream captures changes made to the raw data for downstream processing.

```sql
CREATE OR REPLACE STREAM RAW_DATA_STREAM ON TABLE RAW_EVENTS_TBL;
```

---

### 8. Create Staging Events Table

This table holds structured log events after parsing.

```sql
CREATE OR REPLACE TABLE STAGING_EVENTS (
  user_id STRING,
  device STRING,
  event_id STRING,
  event_type STRING,
  city STRING,
  country STRING,
  timestamp TIMESTAMP
);
```

---

### 9. Task: Parse Raw JSON and Insert into Staging

We use a task triggered by the presence of new data in the stream.

```sql
CREATE OR REPLACE TASK parsing_task
WAREHOUSE = COMPUTE_WH
WHEN SYSTEM$STREAM_HAS_DATA('LOGS_PIPELINE.MAIN_SCHEMA.RAW_DATA_STREAM')
AS
INSERT INTO STAGING_EVENTS (user_id, device, event_id, event_type, city, country, timestamp)
SELECT 
  value:user_id::VARCHAR AS user_id,
  value:device::VARCHAR AS device,
  value:event_id::VARCHAR AS event_id,
  value:event_type::VARCHAR AS event_type,
  value:location.city::VARCHAR AS city,
  value:location.country::VARCHAR AS country,
  value:timestamp::TIMESTAMP AS timestamp
FROM LOGS_PIPELINE.MAIN_SCHEMA.RAW_DATA_STREAM, 
LATERAL FLATTEN(input => DATA:events);
```

Resume the task:

```sql
ALTER TASK parsing_task RESUME;
```

---

### 10. Create Stream on Staging Events Table

This stream will be used for downstream aggregation.

```sql
CREATE OR REPLACE STREAM STAGING_EVENTS_STREAM
ON TABLE STAGING_EVENTS;
```

---

### 11. Create Daily Active Users Table

Stores aggregated results per day.

```sql
CREATE OR REPLACE TABLE DAILY_ACTIVE_USERS (
  activity_date DATE,
  user_count INT
);
```

---

### 12. Task: Aggregate Daily Active Users

This task runs whenever new data appears in the staging stream.

```sql
CREATE OR REPLACE TASK aggregate_daily_users_task
WAREHOUSE = COMPUTE_WH
WHEN SYSTEM$STREAM_HAS_DATA('LOGS_PIPELINE.MAIN_SCHEMA.STAGING_EVENTS_STREAM')
AS
MERGE INTO DAILY_ACTIVE_USERS AS target
USING (
    SELECT
        DATE(timestamp) AS activity_date,
        COUNT(DISTINCT user_id) AS user_count
    FROM STAGING_EVENTS_STREAM
    GROUP BY DATE(timestamp)
) AS source
ON target.activity_date = source.activity_date
WHEN MATCHED THEN
  UPDATE SET user_count = source.user_count
WHEN NOT MATCHED THEN
  INSERT (activity_date, user_count)
  VALUES (source.activity_date, source.user_count);
```

Resume the task:

```sql
ALTER TASK aggregate_daily_users_task RESUME;
```

---

## 📊 Final Output: Query Daily Active Users

You can now query the final table for insights:

```sql
SELECT * FROM DAILY_ACTIVE_USERS ORDER BY activity_date DESC;
```

---

## 🛡️ Best Practices & Tips

- **Use Variants** to store raw JSON data for flexibility and auditing.
- **Stream + Task** pattern ensures decoupled, scalable transformation pipelines.
- **Merge Statement** handles idempotent updates for accurate aggregations.
- **Auto-ingest Snowpipes** allow near real-time data flow from S3.
- Always test with sample data before enabling auto-ingestion.

---

## 🧠 Summary

This pipeline demonstrates how to:

- Build a robust **data lake to warehouse pipeline** in Snowflake.
- Handle **semi-structured JSON** data with ease using `VARIANT`, `FLATTEN`, and `Streams`.
- Orchestrate transformations using **Tasks** instead of traditional ETL tools.
- Perform **aggregation and deduplication** using modern SQL techniques.

