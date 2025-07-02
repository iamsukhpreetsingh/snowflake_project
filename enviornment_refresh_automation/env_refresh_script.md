```-- Create Utility DB and Schema

CREATE OR REPLACE DATABASE UTILITY_DB;
CREATE OR REPLACE SCHEMA UTILITY_DB.PUBLIC;



-- Creating Procedure for data masking dynamically for incomming tables in any schema in DB.

CREATE OR REPLACE PROCEDURE UTILITY_DB.PUBLIC.dynamic_data_masking(
    tgt_db STRING DEFAULT 'DEVELOPMENT_DB',
    dry_run BOOLEAN DEFAULT FALSE
)
RETURNS STRING
LANGUAGE SQL
AS 
$$
DECLARE 
    sql_command STRING;
    result_message STRING := 'PII SANITIZATION RESULTS:\n';
    cursor_result RESULTSET;
    schema_name STRING;
    table_name STRING;
    column_name STRING;
    data_type STRING;
    rows_affected INTEGER := 0;
    total_columns_processed INTEGER := 0;
    total_rows_affected INTEGER := 0;
    count_cursor RESULTSET;
    full_table_name STRING;
    where_clause STRING;
BEGIN
    
    result_message := result_message || 'Target Database: ' || tgt_db || '\n';
    
    IF (dry_run) THEN
        result_message := result_message || 'Mode: DRY RUN\n';
    ELSE
        result_message := result_message || 'Mode: LIVE EXECUTION\n';
    END IF;
    
    result_message := result_message || '----------------------------------------\n';
    
    -- Get all email-like columns
    sql_command := 'SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM ';
    sql_command := sql_command || tgt_db;
    sql_command := sql_command || '.INFORMATION_SCHEMA.COLUMNS WHERE (';
    sql_command := sql_command || 'UPPER(COLUMN_NAME) LIKE ''%EMAIL%'' OR ';
    sql_command := sql_command || 'UPPER(COLUMN_NAME) LIKE ''%CONTACT%'' OR ';
    sql_command := sql_command || 'UPPER(COLUMN_NAME) LIKE ''%MAIL%'') AND ';
    sql_command := sql_command || 'DATA_TYPE IN (''VARCHAR'', ''STRING'', ''TEXT'') AND ';
    sql_command := sql_command || 'TABLE_SCHEMA NOT IN (''INFORMATION_SCHEMA'', ''ACCOUNT_USAGE'')';
    
    cursor_result := (EXECUTE IMMEDIATE sql_command);
    
    -- Process each discovered column
    FOR record IN cursor_result DO
        schema_name := record.TABLE_SCHEMA;
        table_name := record.TABLE_NAME;
        column_name := record.COLUMN_NAME;
        data_type := record.DATA_TYPE;
        
        result_message := result_message || 'Processing: ';
        result_message := result_message || schema_name || '.' || table_name || '.' || column_name;
        result_message := result_message || '\n';
        
        -- Build full table name
        full_table_name := tgt_db || '.' || schema_name || '.' || table_name;
        
        -- Build where clause for email validation
        where_clause := ' WHERE ' || column_name || ' IS NOT NULL AND ';
        where_clause := where_clause || column_name || ' != '''' AND ';
        where_clause := where_clause || 'REGEXP_LIKE(' || column_name || ', ''^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'')';
        
        IF (NOT dry_run) THEN
            -- Execute the actual masking
            sql_command := 'UPDATE ' || full_table_name || ' SET ';
            sql_command := sql_command || column_name || ' = REGEXP_REPLACE(';
            sql_command := sql_command || column_name || ', ''(.{2}).+(@.+)'', ''\\1****\\2'')';
            sql_command := sql_command || where_clause;
            
            EXECUTE IMMEDIATE sql_command;
            
            -- Count masked rows
            sql_command := 'SELECT COUNT(*) as ROW_COUNT FROM ' || full_table_name;
            sql_command := sql_command || ' WHERE ' || column_name || ' LIKE ''%****%''';
            
            count_cursor := (EXECUTE IMMEDIATE sql_command);
            FOR count_record IN count_cursor DO
                rows_affected := count_record.ROW_COUNT;
            END FOR;
            
            total_rows_affected := total_rows_affected + rows_affected;
            result_message := result_message || '  > Masked ' || rows_affected || ' rows\n';
        ELSE
            -- Dry run - count potential rows
            sql_command := 'SELECT COUNT(*) as ROW_COUNT FROM ' || full_table_name;
            sql_command := sql_command || where_clause;
            
            count_cursor := (EXECUTE IMMEDIATE sql_command);
            FOR count_record IN count_cursor DO
                rows_affected := count_record.ROW_COUNT;
            END FOR;
            
            total_rows_affected := total_rows_affected + rows_affected;
            result_message := result_message || '  > Would mask ' || rows_affected || ' rows\n';
        END IF;
        
        total_columns_processed := total_columns_processed + 1;
    END FOR;
    
    result_message := result_message || '----------------------------------------\n';
    result_message := result_message || 'SUMMARY:\n';
    result_message := result_message || 'Columns processed: ' || total_columns_processed || '\n';
    
    IF (dry_run) THEN
        result_message := result_message || 'Total rows that would be affected: ' || total_rows_affected || '\n';
        result_message := result_message || 'To execute actual masking, call with dry_run => FALSE\n';
    ELSE
        result_message := result_message || 'Total rows affected: ' || total_rows_affected || '\n';
    END IF;
    
    RETURN result_message;
END;
$$;
    

-- 3. Create refresh_dev_env procedure in UTILITY_DB
CREATE OR REPLACE PROCEDURE UTILITY_DB.PUBLIC.refresh_dev_env(
    src_db STRING DEFAULT 'PRODUCTION_DB',
    tgt_db STRING DEFAULT 'DEVELOPMENT_DB'
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    clone_timestamp STRING DEFAULT TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYY_MM_DD_HH24_MI_SS');
    backup_db_name STRING DEFAULT tgt_db || '_BACKUP_' || clone_timestamp;
    result_message STRING DEFAULT '';
    error_message STRING;
BEGIN
    result_message := 'Starting env refresh at ' || CURRENT_TIMESTAMP()::STRING || '\n';


-- Creating Backup of current Development Eniornment

    BEGIN
        EXECUTE IMMEDIATE 'CREATE DATABASE ' || backup_db_name || ' CLONE ' || tgt_db;
        result_message := result_message || 'Backup created: ' || backup_db_name || '\n';
    EXCEPTION
        WHEN OTHER THEN 
            error_message := 'Failed to create backup: ' || SQLERRM;
            RETURN error_message;
    END;

    
-- Droping current Development Enviornment database

    BEGIN
        EXECUTE IMMEDIATE 'DROP DATABASE ' || tgt_db;
        result_message := result_message || 'Dropped development DB.\n';
    EXCEPTION
        WHEN OTHER THEN 
            error_message := 'Failed to drop dev DB: ' || SQLERRM;
            RETURN error_message;
    END;


-- Clone Production DB to Development with Updated Production Data

    BEGIN
        EXECUTE IMMEDIATE 'CREATE DATABASE ' || tgt_db || ' CLONE ' || src_db;
        result_message := result_message || 'Cloned production to development.\n';
    EXCEPTION
        WHEN OTHER THEN
            EXECUTE IMMEDIATE 'CREATE DATABASE ' || tgt_db || ' CLONE ' || backup_db_name;
            error_message := 'Clone failed. Rolled back using backup. Error: ' || SQLERRM;
            RETURN error_message;
    END;


--  DATA VALIDATION AND ROLLBACK

    SELECT COUNT(*) INTO table_count 
    FROM information_schema.tables 
    WHERE table_catalog = target_database;
    
    IF (table_count < 12) THEN  -- hypothetical scnario where we check if the count of tables is less than 12 then we will roll back
        -- Rollback
        EXECUTE IMMEDIATE 'DROP DATABASE ' || target_database;
        EXECUTE IMMEDIATE 'CREATE DATABASE ' || target_database || ' CLONE ' || backup_db_name;
        RETURN 'Clone validation failed - insufficient tables. Restored from backup.';
    END IF;
    
    result_message := result_message || 'Validation passed: ' || table_count || ' tables found\n';


    -- BEGIN
    --     EXECUTE IMMEDIATE 'CALL UTILITY_DB.PUBLIC.data_masking(''' || tgt_db || ''')';
    -- END;
    

    RETURN result_message || 'Refresh completed at ' || CURRENT_TIMESTAMP()::STRING;
END;
$$;



-- 4. Creating Tasks to automate the whole system and script

-- Task 1: backup_script (every 12 hours) and call refresh_dev_env stored procedure
CREATE OR REPLACE TASK UTILITY_DB.PUBLIC.backup_script
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 0,12 * * * UTC'
AS
BEGIN
  CALL UTILITY_DB.PUBLIC.refresh_dev_env();
END;



-- Task 2: data_masking (child task: runs after backup_script TASK successfully executed) and call dynamic_data_masking stored procedure to mask the sensitive data.
CREATE OR REPLACE TASK UTILITY_DB.PUBLIC.data_masking
  WAREHOUSE = COMPUTE_WH
  AFTER UTILITY_DB.PUBLIC.backup_script
AS
BEGIN
  CALL UTILITY_DB.PUBLIC.dynamic_data_masking();
END;



-- Task 3: delete_old_bkups (child task: runs after data_masking TASK successfully executed) and it retains the most latest 3 development DB backups and delete rest of them.
CREATE OR REPLACE TASK UTILITY_DB.PUBLIC.delete_old_bkups
  WAREHOUSE = COMPUTE_WH
  AFTER UTILITY_DB.PUBLIC.data_masking
AS
BEGIN
  -- Step 1: Get list of DEV_ databases, ordered by CREATED time desc
  LET dbs ARRAY := (
    SELECT ARRAY_AGG(DATABASE_NAME)
    FROM (
      SELECT DATABASE_NAME
      FROM INFORMATION_SCHEMA.DATABASES
      WHERE DATABASE_NAME LIKE 'DEVELOPMENT_DB_BACKUP%%'
      ORDER BY CREATED DESC
      LIMIT 10 OFFSET 3
    )
  );

  -- Step 2: Loop through and drop the rest
  FOR i IN 0 TO ARRAY_SIZE(dbs) - 1 DO
    LET db_name VARCHAR := dbs[i];
    EXECUTE IMMEDIATE 'DROP DATABASE IF EXISTS "' || db_name || '"';
  END FOR;

  RETURN 'BACKUP* databases except the 3 most recent have been dropped.';
END;




-- Resume tasks
ALTER TASK UTILITY_DB.PUBLIC.delete_old_bkups RESUME;
ALTER TASK UTILITY_DB.PUBLIC.data_masking RESUME;
ALTER TASK UTILITY_DB.PUBLIC.backup_script RESUME;


-- Suspend tasks
ALTER TASK UTILITY_DB.PUBLIC.delete_old_bkups SUSPEND;
ALTER TASK UTILITY_DB.PUBLIC.data_masking SUSPEND;
ALTER TASK UTILITY_DB.PUBLIC.backup_script SUSPEND;

```

