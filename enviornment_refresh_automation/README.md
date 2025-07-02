# Snowflake: Environment Refresh Automation
## Project Documentation 

### Executive Summary

This project successfully addressed critical data freshness issues in one of the client's development environment by implementing an automated zero-copy cloning solution using Snowflake's native capabilities. The solution reduced manual DBA intervention from 6 hours to zero while ensuring data privacy compliance through automated PII sanitization.

[Watch the demonstration video here](https://youtu.be/sayayWlLY3M)


**Key Achievements:**
- Automated environment refresh every 12 hours
- Implemented dynamic PII data masking for production tables
- Created intelligent backup retention (3 most recent backups)
- Built validation and rollback mechanisms
- Eliminated 30% development velocity loss due to stale data

---

## Problem Context & Business Impact

### Initial Challenges
- **Stale Data:** Development environment data was 3 months old
- **Manual Overhead:** 6 hours of DBA time per refresh
- **Business Impact:** 30% reduction in development velocity
- **Scale:** production data across tables requiring refresh 

### Solution Requirements
1. Automated environment refresh with optimal frequency
2. Zero-copy cloning to minimize storage costs
3. PII data sanitization for compliance
4. Validation and rollback mechanisms
5. Backup retention management

---

## Architecture Overview

### Component Structure
```
PRODUCTION_DB
├── schemas
    ├── tables

DEVELOPMENT_DB
├── schemas
    ├── tables

UTILITY_DB (Control Database)
├── dynamic_data_masking() - PII sanitization procedure
├── refresh_dev_env() - Environment refresh procedure
└── Task Orchestration:
    ├── backup_script (Parent Task - Every 12 hours)
    ├── data_masking (Child Task - After backup_script)
    └── delete_old_bkups (Child Task - After data_masking)
```

### Data Flow
1. **Production DB** → Clone → **Development DB**
2. **Development DB** → Backup → **Timestamped Backup DB**
3. **PII Sanitization** → Applied to Development DB
4. **Cleanup** → Retain 3 most recent backups

---

## Implementation Details

### Check Script here: https://github.com/iamsukhpreetsingh/snowflake_project/blob/main/enviornment_refresh_automation/env_refresh_script.md

### 1. Utility Database Setup

**Purpose:** Centralized control plane for all automation procedures

```sql
CREATE OR REPLACE DATABASE UTILITY_DB;
CREATE OR REPLACE SCHEMA UTILITY_DB.PUBLIC;
```

**Design Decision:** Separate utility database ensures procedures survive environment refreshes and provides centralized management.

### 2. Dynamic PII Data Masking Procedure

**Procedure:** `UTILITY_DB.PUBLIC.dynamic_data_masking()`

**Key Features:**
- **Dynamic Discovery:** Automatically identifies email columns using pattern matching
- **Regex-Based Masking:** Preserves email format while obfuscating content
- **Dry Run Mode:** Allows validation before execution
- **Comprehensive Reporting:** Detailed execution summary

**Implementation Highlights:**

```sql
-- Pattern Detection
UPPER(COLUMN_NAME) LIKE '%EMAIL%' OR 
UPPER(COLUMN_NAME) LIKE '%CONTACT%' OR 
UPPER(COLUMN_NAME) LIKE '%MAIL%'

-- Email Validation
REGEXP_LIKE(column_name, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')

-- Masking Pattern
REGEXP_REPLACE(email_column, '(.{2}).+(@.+)', '\\1****\\2')
```

**Real-World Application:**
- Automatically handles new tables with email columns
- Maintains referential integrity by preserving email domain structure
- Supports compliance requirements (GDPR, CCPA)

### 3. Environment Refresh Procedure

**Procedure:** `UTILITY_DB.PUBLIC.refresh_dev_env()`

**Process Flow:**
1. **Backup Creation:** Clone current development environment
2. **Environment Replacement:** Drop and recreate from production
3. **Validation:** Verify table count and data integrity
4. **Rollback Capability:** Restore from backup if validation fails

**Critical Features:**

```sql
-- Timestamped Backup Naming
backup_db_name := tgt_db || '_BACKUP_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYY_MM_DD_HH24_MI_SS');

-- Validation Logic
IF (table_count < 12) THEN
    -- Automatic rollback to backup
    EXECUTE IMMEDIATE 'DROP DATABASE ' || target_database;
    EXECUTE IMMEDIATE 'CREATE DATABASE ' || target_database || ' CLONE ' || backup_db_name;
END IF;
```

**Production Considerations:**
- Zero-copy cloning minimizes storage costs
- Automatic rollback ensures environment stability
- Timestamped backups enable audit trail

### 4. Task Orchestration System

**Three-Tier Task Chain:**

#### Parent Task: `backup_script`
- **Schedule:** Every 12 hours (0 0,12 * * * UTC)
- **Function:** Orchestrates environment refresh
- **Warehouse:** COMPUTE_WH

#### Child Task 1: `data_masking`
- **Trigger:** After backup_script success
- **Function:** Applies PII sanitization
- **Dependencies:** Waits for successful clone completion

#### Child Task 2: `delete_old_bkups`
- **Trigger:** After data_masking success
- **Function:** Cleanup old backups (retain 3 most recent)
- **Storage Optimization:** Prevents backup accumulation

**Task Management:**
```sql
-- Production deployment
ALTER TASK UTILITY_DB.PUBLIC.delete_old_bkups RESUME;
ALTER TASK UTILITY_DB.PUBLIC.data_masking RESUME;
ALTER TASK UTILITY_DB.PUBLIC.backup_script RESUME;

-- Maintenance mode
ALTER TASK UTILITY_DB.PUBLIC.delete_old_bkups SUSPEND;
ALTER TASK UTILITY_DB.PUBLIC.data_masking SUSPEND;
ALTER TASK UTILITY_DB.PUBLIC.backup_script SUSPEND;
```

---

## Real-World Implementation Strategy

### Phase 1: Initial Deployment (Week 1-2)
1. **Setup Utility Database** in production Snowflake account
2. **Deploy Procedures** with dry-run mode enabled
3. **Validate PII Detection** across all production tables
4. **Test Rollback Mechanisms** in staging environment

### Phase 2: Pilot Implementation (Week 3-4)
1. **Enable Tasks** with extended schedule (daily instead of 12-hourly)
2. **Monitor Task Execution** and storage costs
3. **Validate Data Quality** in development environment
4. **Collect Performance Metrics**

### Phase 3: Production Optimization (Week 5-6)
1. **Adjust Schedule** to optimal 12-hour frequency
2. **Fine-tune Validation Thresholds** based on observed patterns
3. **Implement Alerting** for task failures
4. **Document Operational Procedures**

### Operational Considerations

#### Storage Cost Management
- **Zero-Copy Benefits:** Clones share underlying data until modified
- **Backup Retention:** Automatic cleanup prevents cost accumulation
- **Monitoring:** Track storage growth patterns

#### Security & Compliance
- **PII Sanitization:** Automatic identification and masking
- **Access Control:** Utility database requires elevated privileges
- **Audit Trail:** Timestamped backups enable change tracking

#### Monitoring & Alerting
```sql
-- Task Status Monitoring
SELECT 
    task_name,
    state,
    last_committed_on,
    error_code,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE scheduled_from >= DATEADD(hour, -24, CURRENT_TIMESTAMP());
```

---

## Performance & Cost Analysis

### Before Implementation
- **Manual Refresh Time:** 6 hours DBA time
- **Data Freshness:** 3 months stale
- **Development Velocity:** 30% reduction
- **Quality Issues:** 12 missed production bugs

### After Implementation
- **Automation:** Complete hands-off operation
- **Data Freshness:** 12-hour maximum staleness
- **Development Velocity:** Restored to baseline
- **Storage Overhead:** ~20% increase (backup retention)
- **Compute Cost:** Minimal (task execution only)

### ROI Calculation
- **DBA Time Saved:** 6 hours × 2 refreshes/month = 12 hours/month
- **Developer Productivity:** 30% improvement across 8-person team
- **Bug Prevention:** Early detection of data-related issues

---

### Emergency Procedures
```sql
-- Manual rollback to latest backup
CALL UTILITY_DB.PUBLIC.refresh_dev_env('PRODUCTION_DB', 'DEVELOPMENT_DB');

-- Suspend all tasks during maintenance
ALTER TASK UTILITY_DB.PUBLIC.backup_script SUSPEND;
```

-----

## Conclusion

This zero-copy cloning automation solution successfully addressed all initial requirements while providing a robust, scalable foundation for environment management. The implementation demonstrates the power of Snowflake's native features combined with thoughtful automation design.

**Key Success Factors:**
- Comprehensive error handling and rollback mechanisms
- Dynamic PII detection and sanitization
- Efficient storage management through automated cleanup
- Minimal operational overhead through task orchestration

The solution has become a critical component of our data engineering infrastructure, enabling faster development cycles while maintaining security and compliance standards.
