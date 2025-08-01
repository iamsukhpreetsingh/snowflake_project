-- ✅ 5. Data Validation Checker
-- Write a procedure that scans a specific column in a table for NULL values and returns the count of missing values.


CREATE OR REPLACE PROCEDURE DATA_VALIDATION_CHECKER(schema_name STRING, table_name STRING, column_name STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
from snowflake.snowpark.functions import col

def validate_data(session, schema_name, table_name, column_name):
    try:
        current_db = session.get_current_database().strip('"')
        current_schema = schema_name.upper()
        table_name = table_name.upper()
        column_name = column_name.upper()

        df = (session.table(f"{current_db}.INFORMATION_SCHEMA.COLUMNS").filter((col("TABLE_SCHEMA") == schema_name) & (col("TABLE_NAME") == table_name) & (col("COLUMN_NAME") == column_name)))

        return df.count() > 0

    except:
        return False

def main(session, schema_name, table_name, column_name):
    try:
        if not schema_name or not table_name or not column_name:
            return "SCHEMA/TABLE OR COLUMN NOT EXIST"
        full_tbl_name = f'"{schema_name.upper()}"."{table_name.upper()}"'
        
        if validate_data(session, schema_name, table_name, column_name):
            df = session.table(full_tbl_name)
            null_count = df.filter(col(column_name).is_null()).count()

            final = session.create_dataframe([
                    [full_tbl_name, column_name, null_count]
                    ], schema=["TABLE_NAME", "COLUMN_NAME", "NULL_COUNT"])
            final.write.mode("append").save_as_table("NULL_COUNTS")
    
            return f"Success !!"
        else:
            return f"Error table not exist"
    except Exception as e:
        return f"Error: {str(e)}"

$$;



CALL DATA_VALIDATION_CHECKER('TEST_SCHEMA', 'SOURCE_TABLE', 'EMAIL');

