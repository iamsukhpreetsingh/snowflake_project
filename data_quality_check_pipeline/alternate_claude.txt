-- =====================================================
-- IMPROVED DATA QUALITY VALIDATION TASK
-- =====================================================

-- First, create a stored procedure for better error handling and logging
CREATE OR REPLACE PROCEDURE SALES.SALES.SP_DATA_QUALITY_CHECK()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    records_processed INT DEFAULT 0;
    anomalies_found INT DEFAULT 0;
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    result_msg STRING;
BEGIN
    
    -- Clear previous anomalies for today (optional - keeps history clean)
    DELETE FROM SALES.SALES.DQ_SALES_ANOMALIES 
    WHERE DATE(detection_timestamp) = CURRENT_DATE();
    
    -- 1. REVENUE VALIDATION (negative or >$50K)
    INSERT INTO SALES.SALES.DQ_SALES_ANOMALIES(
        record_id, anomaly_type, detected_value, expected_range, severity_level
    )
    SELECT
        ORDER_ID, 
        'NEGATIVE_OR_EXCESSIVE_REVENUE',
        TO_VARCHAR(unit_price * QUANTITY),
        '0 to 50000',
        CASE 
            WHEN (unit_price * QUANTITY) < 0 THEN 'CRITICAL'
            WHEN (unit_price * QUANTITY) > 50000 THEN 'WARNING'
            ELSE 'INFO'
        END
    FROM SALES.SALES.SALES_FACT
    WHERE (unit_price * QUANTITY) < 0 OR (unit_price * QUANTITY) > 50000;
    
    -- 2. DATE VALIDATION (future dates or >5 years old)
    INSERT INTO SALES.SALES.DQ_SALES_ANOMALIES(
        record_id, anomaly_type, detected_value, expected_range, severity_level
    )
    SELECT
        ORDER_ID, 
        CASE 
            WHEN ORDER_DATE > CURRENT_DATE() THEN 'FUTURE_ORDER_DATE'
            WHEN ORDER_DATE <= DATEADD(YEAR, -5, CURRENT_DATE()) THEN 'ORDER_TOO_OLD'
            ELSE 'INVALID_DATE'
        END,
        TO_VARCHAR(ORDER_DATE),
        TO_VARCHAR(DATEADD(YEAR, -5, CURRENT_DATE())) || ' to ' || TO_VARCHAR(CURRENT_DATE()),
        'CRITICAL'
    FROM SALES.SALES.SALES_FACT
    WHERE ORDER_DATE > CURRENT_DATE() OR ORDER_DATE <= DATEADD(YEAR, -5, CURRENT_DATE());
    
    -- 3. CUSTOMER VALIDATION (non-existent customer IDs)
    INSERT INTO SALES.SALES.DQ_SALES_ANOMALIES(
        record_id, anomaly_type, detected_value, expected_range, severity_level
    )
    SELECT
        sf.ORDER_ID,
        'INVALID_CUSTOMER_ID',
        TO_VARCHAR(sf.CUSTOMER_ID),
        'Must exist in DIM_CUSTOMER',
        'CRITICAL'
    FROM SALES.SALES.SALES_FACT sf
    LEFT JOIN SALES.SALES.DIM_CUSTOMER dc ON sf.CUSTOMER_ID = dc.CUSTOMER_ID
    WHERE dc.CUSTOMER_ID IS NULL;
    
    -- 4. QUANTITY VALIDATION (not between 1-999)
    INSERT INTO SALES.SALES.DQ_SALES_ANOMALIES(
        record_id, anomaly_type, detected_value, expected_range, severity_level
    )
    SELECT
        ORDER_ID,
        CASE 
            WHEN QUANTITY <= 0 THEN 'ZERO_OR_NEGATIVE_QUANTITY'
            WHEN QUANTITY > 999 THEN 'EXCESSIVE_QUANTITY'
            ELSE 'INVALID_QUANTITY'
        END,
        TO_VARCHAR(QUANTITY),
        '1 to 999',
        'CRITICAL'
    FROM SALES.SALES.SALES_FACT 
    WHERE QUANTITY < 1 OR QUANTITY > 999;
    
    -- Get summary statistics
    SELECT COUNT(*) INTO records_processed FROM SALES.SALES.SALES_FACT;
    SELECT COUNT(*) INTO anomalies_found FROM SALES.SALES.DQ_SALES_ANOMALIES 
    WHERE DATE(detection_timestamp) = CURRENT_DATE();
    
    -- Build result message
    result_msg := 'DQ Check completed. Records processed: ' || records_processed || 
                  ', Anomalies found: ' || anomalies_found || 
                  ', Duration: ' || DATEDIFF('second', start_time, CURRENT_TIMESTAMP()) || ' seconds';
    
    RETURN result_msg;
    
EXCEPTION
    WHEN OTHER THEN
        RETURN 'DQ Check failed: ' || SQLERRM;
END;
$$;

-- Create the improved TASK
CREATE OR REPLACE TASK SALES.SALES.TASK_DAILY_DQ_CHECK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 6 * * * UTC'  -- Daily at 6 AM UTC
    ERROR_INTEGRATION = 'MY_ERROR_INTEGRATION' -- Optional: for error notifications
AS
    CALL SALES.SALES.SP_DATA_QUALITY_CHECK();

-- Create a summary view for easy monitoring
CREATE OR REPLACE VIEW SALES.SALES.VW_DQ_SUMMARY AS
SELECT 
    DATE(detection_timestamp) as check_date,
    anomaly_type,
    severity_level,
    COUNT(*) as anomaly_count,
    MIN(detection_timestamp) as first_detected,
    MAX(detection_timestamp) as last_detected
FROM SALES.SALES.DQ_SALES_ANOMALIES
GROUP BY 1, 2, 3
ORDER BY check_date DESC, anomaly_count DESC;

-- Create a monitoring query for the last 7 days
CREATE OR REPLACE VIEW SALES.SALES.VW_DQ_LAST_7_DAYS AS
SELECT 
    DATE(detection_timestamp) as check_date,
    COUNT(*) as total_anomalies,
    COUNT(CASE WHEN severity_level = 'CRITICAL' THEN 1 END) as critical_count,
    COUNT(CASE WHEN severity_level = 'WARNING' THEN 1 END) as warning_count,
    COUNT(CASE WHEN severity_level = 'INFO' THEN 1 END) as info_count
FROM SALES.SALES.DQ_SALES_ANOMALIES
WHERE detection_timestamp >= DATEADD(DAY, -7, CURRENT_DATE())
GROUP BY 1
ORDER BY 1 DESC;

-- Enable the task
ALTER TASK SALES.SALES.TASK_DAILY_DQ_CHECK RESUME;

-- =====================================================
-- TESTING & VALIDATION QUERIES
-- =====================================================

-- Test the stored procedure manually
CALL SALES.SALES.SP_DATA_QUALITY_CHECK();

-- Check results
SELECT * FROM SALES.SALES.DQ_SALES_ANOMALIES ORDER BY detection_timestamp DESC;

-- Summary by anomaly type
SELECT * FROM SALES.SALES.VW_DQ_SUMMARY;

-- Last 7 days trend
SELECT * FROM SALES.SALES.VW_DQ_LAST_7_DAYS;

-- Detailed anomaly breakdown
SELECT 
    anomaly_type,
    severity_level,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM SALES.SALES.DQ_SALES_ANOMALIES
GROUP BY 1, 2
ORDER BY count DESC;
