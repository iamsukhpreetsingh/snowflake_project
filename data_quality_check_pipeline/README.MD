CREATE DATABASE SALES;

CREATE SCHEMA SALES;

-- 1. Create DIM_CUSTOMER table
CREATE OR REPLACE TABLE SALES.SALES.DIM_CUSTOMER (
    customer_id INT,
    customer_name VARCHAR(100),
    registration_date DATE
);

-- Insert customer data
INSERT INTO SALES.SALES.DIM_CUSTOMER VALUES
(1001, 'John Smith', '2022-01-15'),
(1002, 'Sarah Johnson', '2022-03-22'),
(1003, 'Mike Wilson', '2021-11-08'),
(1004, 'Emma Davis', '2023-02-14'),
(1005, 'David Brown', '2022-07-30'),
(1006, 'Lisa Anderson', '2023-01-10'),
(1007, 'Tom Miller', '2021-12-05'),
(1008, 'Amy Taylor', '2022-09-18'),
(1009, 'Chris Lee', '2023-04-25'),
(1010, 'Jennifer White', '2022-05-12');


-- batch 2
INSERT INTO SALES.SALES.DIM_CUSTOMER VALUES
(1011, 'Robert Garcia', '2022-08-15'),
(1012, 'Michelle Rodriguez', '2023-01-20'),
(1013, 'Kevin Martinez', '2021-10-12'),
(1014, 'Ashley Hernandez', '2022-12-08'),
(1015, 'Brandon Lopez', '2023-03-17'),
(1016, 'Stephanie Gonzalez', '2022-04-25'),
(1017, 'Jason Wilson', '2021-09-30'),
(1018, 'Nicole Moore', '2023-05-14'),
(1019, 'Daniel Taylor', '2022-11-22'),
(1020, 'Rachel Jackson', '2022-06-18');


-- 2. Create SALES_FACT table
CREATE OR REPLACE TABLE SALES.SALES.SALES_FACT (
    order_id INT,
    customer_id INT,
    product_id VARCHAR(10),
    quantity INT,
    unit_price DECIMAL(10,2),
    order_date DATE
);

-- Insert sales data with INTENTIONAL DATA QUALITY ISSUES
INSERT INTO SALES.SALES.SALES_FACT VALUES
-- CLEAN RECORDS
(50001, 1001, 'PROD001', 2, 25.50, '2024-06-20'),
(50002, 1002, 'PROD002', 1, 150.00, '2024-06-21'),
(50003, 1003, 'PROD003', 5, 12.99, '2024-06-19'),
(50004, 1004, 'PROD001', 3, 25.50, '2024-06-18'),
(50005, 1005, 'PROD004', 1, 89.95, '2024-06-22'),

-- DATA QUALITY ISSUES TO DETECT:
-- Negative revenue
(50006, 1006, 'PROD002', 2, -50.00, '2024-06-20'),
-- Unreasonably high price (>$50K)
(50007, 1007, 'PROD005', 1, 75000.00, '2024-06-21'),
-- Future date
(50008, 1008, 'PROD003', 4, 15.99, '2025-12-25'),
-- Very old date (>5 years)
(50009, 1009, 'PROD001', 2, 25.50, '2018-01-15'),
-- Non-existent customer ID
(50010, 9999, 'PROD002', 1, 99.99, '2024-06-20'),
-- Zero quantity
(50011, 1002, 'PROD004', 0, 45.00, '2024-06-21'),
-- Excessive quantity (>999)
(50012, 1003, 'PROD003', 1500, 12.99, '2024-06-22'),
-- Another negative price
(50013, 1001, 'PROD001', 3, -25.50, '2024-06-19'),
-- Another future date
(50014, 1004, 'PROD005', 2, 199.99, '2026-01-01'),
-- Another non-existent customer
(50015, 8888, 'PROD002', 1, 75.50, '2024-06-18'),
-- Edge case: exactly $50K (should pass)
(50016, 1005, 'PROD006', 1, 50000.00, '2024-06-20'),
-- Edge case: exactly 999 quantity (should pass)
(50017, 1006, 'PROD003', 999, 1.99, '2024-06-21'),

-- More clean records for comparison
(50018, 1007, 'PROD001', 4, 25.50, '2024-06-23'),
(50019, 1008, 'PROD004', 2, 89.95, '2024-06-22'),
(50020, 1009, 'PROD002', 1, 150.00, '2024-06-21'),
(50021, 1010, 'PROD003', 6, 12.99, '2024-06-20'),
(50022, 1001, 'PROD005', 1, 299.99, '2024-06-19');


INSERT INTO SALES.SALES.SALES_FACT VALUES
(50023, 1011, 'PROD007', 3, 45.99, '2024-06-24'),
(50024, 1012, 'PROD008', 2, 125.50, '2024-06-23'),
(50025, 1013, 'PROD009', 1, 89.99, '2024-06-22'),
(50026, 1014, 'PROD010', 4, 22.75, '2024-06-21'),
(50027, 1015, 'PROD007', 2, 45.99, '2024-06-20'),
(50028, 1016, 'PROD011', 1, 199.99, '2024-06-19'),
(50029, 1017, 'PROD012', 5, 15.99, '2024-06-18'),
(50030, 1018, 'PROD008', 3, 125.50, '2024-06-17'),

-- MORE ANOMALIES TO DETECT:
-- Negative prices (different scenarios)
(50031, 1019, 'PROD009', 2, -15.99, '2024-06-16'),
(50032, 1020, 'PROD010', 1, -200.00, '2024-06-15'),
(50033, 1011, 'PROD007', 4, -0.01, '2024-06-14'),

-- Extremely high prices (>$50K)
(50034, 1012, 'PROD013', 1, 65000.00, '2024-06-13'),
(50035, 1013, 'PROD014', 2, 100000.50, '2024-06-12'),
(50036, 1014, 'PROD015', 1, 50000.01, '2024-06-11'),

-- Future dates (various scenarios)
(50037, 1015, 'PROD007', 3, 45.99, '2025-01-15'),
(50038, 1016, 'PROD008', 1, 125.50, '2024-12-31'),
(50039, 1017, 'PROD009', 2, 89.99, '2030-06-24'),

-- Very old dates (>5 years)
(50040, 1018, 'PROD010', 1, 22.75, '2017-06-24'),
(50041, 1019, 'PROD011', 2, 199.99, '2016-12-25'),
(50042, 1020, 'PROD012', 3, 15.99, '2015-01-01'),

-- Non-existent customer IDs
(50043, 7777, 'PROD007', 2, 45.99, '2024-06-10'),
(50044, 6666, 'PROD008', 1, 125.50, '2024-06-09'),
(50045, 5555, 'PROD009', 4, 89.99, '2024-06-08'),

-- Invalid quantities (zero, negative, too high)
(50046, 1011, 'PROD010', 0, 22.75, '2024-06-07'),
(50047, 1012, 'PROD011', -5, 199.99, '2024-06-06'),
(50048, 1013, 'PROD012', 2500, 15.99, '2024-06-05'),
(50049, 1014, 'PROD007', 1000, 45.99, '2024-06-04'),

-- Edge cases and boundary testing
(50050, 1015, 'PROD008', 1, 49999.99, '2024-06-03'),  -- Just under $50K (should pass)
(50051, 1016, 'PROD009', 1000, 89.99, '2024-06-02'),  -- Exactly 1000 qty (should fail)
(50052, 1017, 'PROD010', 999, 22.75, '2024-06-01'),   -- Exactly 999 qty (should pass)
(50053, 1018, 'PROD011', 1, 0.00, '2024-05-31'),      -- Zero price (edge case)
(50054, 1019, 'PROD012', 1, 0.01, '2024-05-30'),      -- Minimal price (should pass)

-- Multiple anomalies in single record
(50055, 9999, 'PROD013', 0, -500.00, '2025-12-31'),   -- Bad customer + qty + price + date
(50056, 8888, 'PROD014', 2000, 75000.00, '2015-01-01'), -- Bad customer + qty + price + date

-- More clean records for balance
(50057, 1020, 'PROD007', 2, 45.99, '2024-05-29'),
(50058, 1011, 'PROD008', 1, 125.50, '2024-05-28'),
(50059, 1012, 'PROD009', 3, 89.99, '2024-05-27'),
(50060, 1013, 'PROD010', 4, 22.75, '2024-05-26'),
(50061, 1014, 'PROD011', 1, 199.99, '2024-05-25'),
(50062, 1015, 'PROD012', 5, 15.99, '2024-05-24'),
(50063, 1016, 'PROD007', 2, 45.99, '2024-05-23'),
(50064, 1017, 'PROD008', 1, 125.50, '2024-05-22'),
(50065, 1018, 'PROD009', 3, 89.99, '2024-05-21'),
(50066, 1019, 'PROD010', 2, 22.75, '2024-05-20'),

-- Recent weekend data
(50067, 1020, 'PROD011', 1, 199.99, '2024-06-23'),  -- Sunday
(50068, 1001, 'PROD012', 4, 15.99, '2024-06-22'),   -- Saturday
(50069, 1002, 'PROD007', 2, 45.99, '2024-06-21'),   -- Friday

-- Some null-like scenarios (if your system allows)
-- Note: Snowflake might reject these, so test carefully
(50070, 1003, 'PROD008', 1, 125.50, '2024-05-19'),
(50071, 1004, 'PROD009', 3, 89.99, '2024-05-18'),
(50072, 1005, 'PROD010', 2, 22.75, '2024-05-17');






-- 3. Create the target table for anomalies (you'll populate this)
CREATE OR REPLACE TABLE SALES.SALES.DQ_SALES_ANOMALIES (
    anomaly_id INT IDENTITY(1,1),
    record_id INT,
    anomaly_type VARCHAR(50),
    detected_value VARCHAR(100),
    expected_range VARCHAR(100),
    severity_level VARCHAR(20),
    detection_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);





SELECT * FROM DIM_CUSTOMER;
SELECT * FROM SALES_FACT;


-- Revenue amounts aren't negative or unreasonably high (>$50K per transaction)

select order_id,
       'Revenue must be between 0 and 50000' AS expected_range,
       'CRITICAL' AS severity_level
from SALES_FACT 
where (unit_price*QUANTITY) < 0 or (unit_price*QUANTITY) > 50000;



-- Order dates fall within acceptable ranges (not future dates, not older than 5 years)
SELECT ORDER_ID, ORDER_DATE,
CONCAT(DATEADD(YEAR, -5, CURRENT_DATE), ' - ' ,CURRENT_DATE()) AS expected_range,
'CRITICAL' AS severity_level
FROM SALES_FACT 
WHERE ORDER_DATE > CURRENT_DATE() OR ORDER_DATE <= DATEADD(YEAR, -5, CURRENT_DATE);


-- Customer IDs exist in the DIM_CUSTOMER table

SELECT customer_id ,
'CUSTOMER ID DOESNT EXIST' AS expected_range,
'CRITICAL' AS severity_level
from SALES_FACT 
where customer_id not in (select customer_id from DIM_CUSTOMER);

-- Product quantities are between 1-999
SELECT customer_id,
'1-999' AS expected_range,
'CRITICAL' AS severity_level
FROM SALES_FACT
WHERE QUANTITY >= 999;





create or replace TASK anomly_detecion
warehouse = compute_wh
schedule = '2 M'
AS
BEGIN
insert into DQ_SALES_ANOMALIES(record_id, anomaly_type, detected_value, expected_range, severity_level)
SELECT
        ORDER_ID, 
        'Revenue amount negative or unreasonably high',
        to_varchar(unit_price*QUANTITY),
        'Revenue must be between 0 and 50000',
        'CRITICAL'
        FROM SALES_FACT
        where (unit_price*QUANTITY) < 0 or (unit_price*QUANTITY) > 50000;


insert into DQ_SALES_ANOMALIES(record_id, anomaly_type, detected_value, expected_range, severity_level)
SELECT
        ORDER_ID, 
        'Invalid order date',
        to_varchar(ORDER_DATE),
        CONCAT(TO_CHAR(DATEADD(YEAR, -5, CURRENT_DATE)), ' - ' ,TO_CHAR(CURRENT_DATE())),
        'CRITICAL'
        FROM SALES_FACT
        WHERE ORDER_DATE > CURRENT_DATE() OR ORDER_DATE <= DATEADD(YEAR, -5, CURRENT_DATE);


insert into DQ_SALES_ANOMALIES(record_id, anomaly_type, detected_value, expected_range, severity_level)
SELECT
        sf.ORDER_ID,
        'Missing Customer ID',
        sf.CUSTOMER_ID,
        'Customer ID must exist in DIM_CUSTOMER',
        'CRITICAL'
    FROM SALES_FACT sf
    LEFT JOIN DIM_CUSTOMER dc ON sf.CUSTOMER_ID = dc.CUSTOMER_ID
    WHERE dc.CUSTOMER_ID IS NULL;


INSERT INTO DQ_SALES_ANOMALIES(record_id, anomaly_type, detected_value, expected_range, severity_level)
SELECT
         ORDER_ID,
        'Product quantities NOT between 1-999',
        QUANTITY,
        '1-999',
        'CRITICAL'
    FROM SALES_FACT 
    WHERE QUANTITY < 1 OR QUANTITY > 999;

END;


ALTER TASK anomly_detecion RESUME;

DESCRIBE TASK anomly_detecion;



select * from DQ_SALES_ANOMALIES order by anomaly_type;


-- TRUNCATE TABLE DQ_SALES_ANOMALIES;
