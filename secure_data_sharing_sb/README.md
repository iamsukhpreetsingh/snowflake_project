# 📄: Secure Data Sharing in Snowflake with Reader Accounts 

---

## 🔐 Project Overview

This project implements **secure data sharing** between Snowflake accounts using **Reader Accounts**, **Secure Views**, **Data Shares**, and **Replication Groups**. The goal is to provide a business partner with real-time, filtered access to data while enforcing strict security controls and monitoring.

---

## ✅ What was Achieved

- **Secure Data Filtering**: Share only the data with masked supplier details.
- **Reader Account Setup**: Create a managed account for your partner with limited access.
- **Real-Time Replication**: Enable automatic synchronization every 15 minutes.
- **Comprehensive Monitoring**: Track usage patterns and suspicious activities.
- **Data Leakage Prevention**: Apply multiple layers of security including row-level filtering, column masking, and access policies.

---

## ⚙️ Key Security Features

| Feature | Description |
|--------|-------------|
| **Row-Level Filtering** | Restrict data visibility to only the APAC region. |
| **Column-Level Masking** | Hide sensitive supplier fields unless accessed by authorized roles. |
| **Role-Based Access Control (RBAC)** | Use secure views and roles to control access. |
| **Automated Monitoring** | Detect and alert on suspicious query behavior. |
| **Audit Logging** | Maintain login history and query logs for compliance. |
| **Replication Controls** | Ensure consistent and secure real-time data sync. |

---

## 🧩 Implementation Steps

### 1️⃣ Step 1: Design and Create Secure View

#### 📌 Purpose
Create a secure view that filters only `APAC` region records and masks supplier information based on user roles.

#### 🔍 SQL Snippet
```sql
USE ROLE ACCOUNTADMIN;
USE DATABASE YOUR_DATABASE;
USE SCHEMA YOUR_SCHEMA;

CREATE OR REPLACE SECURE VIEW APAC_INVENTORY_SECURE AS
SELECT 
    product_id,
    product_name,
    category,
    region,
    quantity_available,
    unit_price,
    last_updated,
    CASE 
        WHEN CURRENT_ROLE() IN ('SUPPLIER_ANALYST', 'INTERNAL_USER') THEN supplier_name
        ELSE '***MASKED***'
    END AS supplier_name,
    CASE 
        WHEN CURRENT_ROLE() IN ('SUPPLIER_ANALYST', 'INTERNAL_USER') THEN supplier_contact
        ELSE '***MASKED***'
    END AS supplier_contact,
    CONCAT('SUP_', RIGHT(supplier_id, 3)) AS masked_supplier_id
FROM PRODUCT_INVENTORY 
WHERE region = 'APAC' AND is_active = TRUE;

GRANT SELECT ON VIEW APAC_INVENTORY_SECURE TO ROLE SYSADMIN;
```

---

### 2️⃣ Step 2: Create Reader Account and Data Share

#### 📌 Purpose
Establish a **managed reader account** for the business partner and share the secure view via a **data share**.

#### 🔍 SQL Snippet
```sql
-- Create Reader Account
CREATE MANAGED ACCOUNT partner_supply_chain_reader
    ADMIN_NAME = 'partner_admin'
    ADMIN_PASSWORD = 'SecurePassword123!'
    TYPE = READER
    COMMENT = 'Reader account for business partner supply chain analysis';

-- Create Data Share
CREATE SHARE apac_inventory_share
    COMMENT = 'Secure share of APAC inventory data for partner analysis';

GRANT USAGE ON DATABASE YOUR_DATABASE TO SHARE apac_inventory_share;
GRANT USAGE ON SCHEMA YOUR_DATABASE.YOUR_SCHEMA TO SHARE apac_inventory_share;
GRANT SELECT ON VIEW YOUR_DATABASE.YOUR_SCHEMA.APAC_INVENTORY_SECURE TO SHARE apac_inventory_share;

ALTER SHARE apac_inventory_share ADD ACCOUNTS = PARTNER_SUPPLY_CHAIN_READER;
```

---

### 3️⃣ Step 3: Configure Real-Time Replication

#### 📌 Purpose
Enable **real-time replication** of shared data to ensure the partner always sees the latest data.

#### 🔍 SQL Snippet
```sql
-- Enable Database Replication
ALTER DATABASE YOUR_DATABASE ENABLE REPLICATION TO ACCOUNTS PARTNER_SUPPLY_CHAIN_READER;

-- Create Replication Group
CREATE REPLICATION GROUP apac_inventory_replication_group
    OBJECT_TYPES = ('DATABASES', 'SHARES')
    ALLOWED_DATABASES = ('YOUR_DATABASE')
    ALLOWED_SHARES = ('apac_inventory_share')
    REPLICATION_SCHEDULE = 'USING CRON 0,15,30,45 * * * * UTC';

-- Create Task for Scheduled Refresh
CREATE TASK apac_inventory_replication_task
    WAREHOUSE = 'YOUR_WAREHOUSE'
    SCHEDULE = '15 MINUTE'
    AS
BEGIN
    ALTER REPLICATION GROUP apac_inventory_replication_group REFRESH;
END;

ALTER TASK apac_inventory_replication_task RESUME;
```

---

### 4️⃣ Step 4: Set Up Usage Monitoring and Governance

#### 📌 Purpose
Track all queries and logins related to the shared data for **monitoring**, **alerting**, and **auditing**.

#### 🔍 SQL Snippet
```sql
-- Monitoring View for Query History
CREATE OR REPLACE VIEW SHARE_USAGE_MONITOR AS
SELECT 
    query_id, query_text, database_name, schema_name, user_name, role_name, warehouse_name,
    start_time, end_time, total_elapsed_time, bytes_scanned, rows_produced
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE query_text ILIKE '%APAC_INVENTORY_SECURE%' OR database_name = 'YOUR_DATABASE'
ORDER BY start_time DESC;

-- Login History View
CREATE OR REPLACE VIEW SHARE_ACCESS_HISTORY AS
SELECT 
    event_timestamp, user_name, client_ip, reported_client_type, first_authentication_factor,
    is_success, error_code, error_message
FROM TABLE(INFORMATION_SCHEMA.LOGIN_HISTORY())
WHERE user_name LIKE '%partner%'
ORDER BY event_timestamp DESC;

-- Alert Procedure
CREATE OR REPLACE PROCEDURE monitor_share_usage()
RETURNS STRING LANGUAGE JAVASCRIPT AS
$$
var result = "";
var query = `
    SELECT COUNT(*) as suspicious_count
    FROM SHARE_USAGE_MONITOR 
    WHERE start_time >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
      AND (query_text ILIKE '%SHOW TABLES%' OR query_text ILIKE '%INFORMATION_SCHEMA%')
`;
var stmt = snowflake.createStatement({sqlText: query});
var res = stmt.execute();
res.next();
var count = res.getColumnValue(1);
result = count > 5 ? "ALERT: Suspicious query activity detected" : "Normal activity";
return result;
$$;

-- Run Monitor Task Hourly
CREATE TASK share_monitoring_task
    WAREHOUSE = 'YOUR_WAREHOUSE'
    SCHEDULE = '60 MINUTE'
    AS CALL monitor_share_usage();

ALTER TASK share_monitoring_task RESUME;
```

---

### 5️⃣ Step 5: Test Data Leakage Prevention

#### 📌 Purpose
Ensure partners cannot bypass restrictions or explore underlying tables.

#### 🔍 SQL Snippet
```sql
-- Test Unauthorized Access
CREATE ROLE test_partner_role;
GRANT IMPORTED PRIVILEGES ON DATABASE YOUR_DATABASE TO ROLE test_partner_role;

USE ROLE test_partner_role;
SELECT COUNT(*) FROM APAC_INVENTORY_SECURE; -- Should work
SELECT * FROM PRODUCT_INVENTORY; -- Should fail
SHOW TABLES; -- Limited visibility
DESC TABLE PRODUCT_INVENTORY; -- Should fail

-- Optional Row Access Policy
CREATE ROW ACCESS POLICY apac_region_policy AS (region VARCHAR) 
RETURNS BOOLEAN ->
    CASE 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN') THEN TRUE
        WHEN CURRENT_ROLE() = 'PARTNER_ROLE' AND region = 'APAC' THEN TRUE
        ELSE FALSE
    END;

ALTER TABLE PRODUCT_INVENTORY ADD ROW ACCESS POLICY apac_region_policy ON (region);
```

---

### 6️⃣ Step 6: Partner Account Setup Instructions

#### 📌 Purpose
Instructions for the partner to access the shared data in their own environment.

#### 🔍 SQL Snippet
```sql
-- Partner runs this in their reader account:
CREATE DATABASE partner_apac_inventory FROM SHARE <your_account_locator>.apac_inventory_share;

GRANT IMPORTED PRIVILEGES ON DATABASE partner_apac_inventory TO ROLE <partner_role>;

USE DATABASE partner_apac_inventory;
USE SCHEMA YOUR_SCHEMA;
SELECT * FROM APAC_INVENTORY_SECURE LIMIT 100;
```

---

### 7️⃣ Step 7: Ongoing Monitoring and Maintenance

#### 📌 Purpose
Maintain visibility into usage, replication health, and access anomalies.

#### 🔍 SQL Snippet
```sql
-- Weekly Audit Queries
SELECT * FROM SHARE_ACCESS_HISTORY WHERE is_success = FALSE AND event_timestamp >= DATEADD(week, -1, CURRENT_TIMESTAMP());

SELECT 
    user_name, COUNT(*) as query_count, COUNT(DISTINCT DATE(start_time)) as active_days
FROM SHARE_USAGE_MONITOR
WHERE start_time >= DATEADD(week, -1, CURRENT_TIMESTAMP())
GROUP BY user_name;

-- Replication Health Check
SELECT 
    replication_group_name, target_account, last_refresh_time, refresh_status, error_message
FROM TABLE(INFORMATION_SCHEMA.REPLICATION_GROUP_REFRESH_HISTORY())
WHERE replication_group_name = 'apac_inventory_replication_group'
ORDER BY last_refresh_time DESC;
```

(Note: this is just a demo snippet of what we implemented in projects of the organization)
