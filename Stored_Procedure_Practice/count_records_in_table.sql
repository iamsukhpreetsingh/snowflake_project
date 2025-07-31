-- THIS STORED PROCEDURE COUNTS THE NUMBER OF ROWS IN A SPECIFIED SCHEMA AND INSERTS THE TABLE_NAME AND COUNT_OF_ROWS IN ANOTHER LOG TABLE



CREATE OR REPLACE TABLE ROW_COUNTS (TABLE_NAME STRING, ROW_COUNT STRING);


CREATE OR REPLACE PROCEDURE ROW_COUNTER(schema_name STRING, table_name STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
from snowflake.snowpark.functions import col

def table_exists(session, schema_name, table_name):
    try:
        current_db = session.get_current_database().strip('"')
        schema_name = schema_name.upper()
        table_name = table_name.upper()

        df = (session.table(f"{current_db}.INFORMATION_SCHEMA.TABLES").filter((col("TABLE_SCHEMA") == schema_name) & (col("TABLE_NAME") == table_name)))

        return df.count() > 0
    except:
        return False
    

def main(session, schema_name, table_name):

    try:
        if not schema_name or not table_name:
            return "schema_name and table_name doesnt exist"
            
        full_tbl_name = f'"{schema_name.upper()}"."{table_name.upper()}"'
        if table_exists(session, schema_name, table_name):
            df = session.table(full_tbl_name)
            row_count = df.count()

            final = session.create_dataframe([[
                full_tbl_name,
                row_count
            
            ]], schema = 
               ["TABLE_NAME", "ROW_COUNT"]
            )
            final.write.mode("append").save_as_table("ROW_COUNTS")
    
            return f"Success !!"
        else:
            return f"Error table not exist"
    except Exception as e:
        return f"Error: {str(e)}"

        
$$;




CALL ROW_COUNTER('S3_JSON', 'CLICKSTREAM_EVENTS');
