-- SELECT * FROM RAG_DB.RAG_SCHEMA.EXTRACTED_DOCUMENTS;


-- CREATE OR REPLACE STAGE RAG_DB.RAG_SCHEMA.INT_STG_DOCS
--   ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
--   DIRECTORY = (ENABLE = TRUE);
  
-- LIST @INT_STG_DOCS;



CREATE OR REPLACE TABLE RAW_TABLE (
    ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    relative_path STRING,
    last_modified TIMESTAMP_TZ(3),
    extracted_text STRING,
    load_time TIMESTAMP
);


-- AI_EXTRACT HAVE LIMIT OF READING FILE OF ONLY 125 PAGES, NO  MORE THAN THAT

-- ##################### RAW CODE FOR TESTING DOCUMENT PROCESSING ######################

-- INSERT INTO RAW_TABLE (relative_path, last_modified, extracted_text, load_time)
--     SELECT 
--         relative_path,
--         last_modified,
--         f.value AS extracted_text,
--         CURRENT_TIMESTAMP() AS load_time
--     FROM DIRECTORY(@RAG_DB.RAG_SCHEMA.INT_STG_DOCS) d,
--     LATERAL FLATTEN(
--         input => AI_EXTRACT(
--             file => TO_FILE('@RAG_DB.RAG_SCHEMA.INT_STG_DOCS' , relative_path),
--             responseFormat => ['Extract all text']
--         ):response
--     ) f
-- WHERE relative_path NOT IN (SELECT relative_path FROM RAW_TABLE);


-- ########## STORED PROCEDURE FOR DETECTING FILE TYPE AND PROCESSING ACCORDINGLY (PDF ->PyPDF / IMAGES -> AI_EXTRACT) #############

CREATE OR REPLACE PROCEDURE RAG_DB.RAG_SCHEMA.LOAD_DOCUMENTS_TO_RAW()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'pypdf')
HANDLER = 'run'
AS
$$
import pypdf
import io

def run(session):
    
    IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp'}
    PDF_EXTENSIONS   = {'.pdf'}
    
    results = {"processed": 0, "images": 0, "pdfs": 0, "skipped": 0, "errors": []}

    # ------------------------------------------------------------------ #
    # 1. Get all files not yet in RAW_TABLE
    # ------------------------------------------------------------------ #
    new_files_df = session.sql("""
        SELECT relative_path, last_modified
        FROM DIRECTORY(@RAG_DB.RAG_SCHEMA.INT_STG_DOCS) d
        WHERE relative_path NOT IN (
            SELECT relative_path FROM RAG_DB.RAG_SCHEMA.RAW_TABLE
        )
    """).collect()

    for row in new_files_df:
        relative_path = row['RELATIVE_PATH']
        last_modified = row['LAST_MODIFIED']

        # Determine extension
        dot_idx = relative_path.rfind('.')
        ext = relative_path[dot_idx:].lower() if dot_idx != -1 else ''

        extracted_text = None

        # ------------------------------------------------------------------ #
        # 2a. IMAGE → AI_EXTRACT
        # ------------------------------------------------------------------ #
        if ext in IMAGE_EXTENSIONS:
            try:
                result_df = session.sql(f"""
                    SELECT f.value AS extracted_text
                    FROM LATERAL FLATTEN(
                        input => AI_EXTRACT(
                            file => TO_FILE('@RAG_DB.RAG_SCHEMA.INT_STG_DOCS', '{relative_path}'),
                            responseFormat => ['Extract all text']
                        ):response
                    ) f
                """).collect()

                extracted_text = ' '.join(
                    [r['EXTRACTED_TEXT'] for r in result_df if r['EXTRACTED_TEXT']]
                )
                results["images"] += 1

            except Exception as e:
                results["errors"].append(f"IMAGE AI_EXTRACT error [{relative_path}]: {str(e)}")
                continue

        # ------------------------------------------------------------------ #
        # 2b. PDF → pypdf
        # ------------------------------------------------------------------ #
        elif ext in PDF_EXTENSIONS:
            try:
                session.file.get(
                    stage_location=f"@RAG_DB.RAG_SCHEMA.INT_STG_DOCS/{relative_path}",
                    target_directory='/tmp'
                )
        
                local_path = f"/tmp/{relative_path.split('/')[-1]}"
                with open(local_path, 'rb') as pdf_file:
                    reader = pypdf.PdfReader(pdf_file)
                    pages = [page.extract_text() or '' for page in reader.pages]
                    extracted_text = '\n'.join(pages).strip()
        
                results["pdfs"] += 1
        
            except Exception as e:
                results["errors"].append(f"PDF pypdf error [{relative_path}]: {str(e)}")
                continue

        # ------------------------------------------------------------------ #
        # 2c. Unsupported extension → skip
        # ------------------------------------------------------------------ #
        else:
            results["skipped"] += 1
            continue

        # ------------------------------------------------------------------ #
        # 3. Insert extracted text into RAW_TABLE
        # ------------------------------------------------------------------ #
        if extracted_text is not None:
            try:
                safe_text = extracted_text.replace("'", "''")
                safe_relative_path = relative_path.replace("'", "''")

                session.sql(f"""
                    INSERT INTO RAG_DB.RAG_SCHEMA.RAW_TABLE
                        (relative_path, last_modified, extracted_text, load_time)
                    VALUES (
                        '{safe_relative_path}',
                        '{last_modified}',
                        '{safe_text}',
                        CURRENT_TIMESTAMP()
                    )
                """).collect()

                results["processed"] += 1

            except Exception as e:
                results["errors"].append(f"INSERT error [{relative_path}]: {str(e)}")

    # ------------------------------------------------------------------ #
    # 4. Return summary
    # ------------------------------------------------------------------ #
    summary = (
        f"Done. Processed: {results['processed']} | "
        f"Images (AI_EXTRACT): {results['images']} | "
        f"PDFs (PyPDF): {results['pdfs']} | "
        f"Skipped: {results['skipped']}"
    )

    if results["errors"]:
        summary += f" | Errors ({len(results['errors'])}): " + "; ".join(results["errors"][:5])

    return summary
$$;


-- CALL RAG_DB.RAG_SCHEMA.LOAD_DOCUMENTS_TO_RAW();


SELECT * FROM RAW_TABLE;


-- ############ CREATING STREAM ON RAW TABLE TO PROCESS ONLY NEW INCOMMING RECORDS ############

-- CREATE OR REPLACE STREAM RAW_TBL_STM 
-- ON TABLE RAW_TABLE 
-- APPEND_ONLY = TRUE;


-- SELECT * FROM RAW_TBL_STM;

-- ##################### RAW CODE FOR TESTING DATA CHUNKING ######################
-- INSERT INTO doc_chunks_temp
-- SELECT
--     r.ID,
--     r.relative_path,
--     r.last_modified,
--     r.load_time,
--     seq.index AS chunk_num,
--     seq.value::string AS chunk_text
-- FROM RAW_TBL_STM r,
-- LATERAL FLATTEN(
--     input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
--         r.extracted_text,   -- your text column
--         'none',             -- use 'none' for plain text; or 'markdown' if the field is Markdown
--         1000,               -- max characters per chunk
--         100                 -- overlap in characters
--     )
-- ) seq;



-- ########## STORED PROCEDURE FOR DOING DOCUMENT CHUNKING #############

CREATE OR REPLACE PROCEDURE insert_doc_chunks()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    err_msg STRING;
BEGIN

    INSERT INTO doc_chunks_temp
    SELECT
        r.ID,
        r.relative_path,
        r.last_modified,
        r.load_time,
        seq.index AS chunk_num,
        seq.value::string AS chunk_text
    FROM RAW_TBL_STM r,
    LATERAL FLATTEN(
        input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
            r.extracted_text,
            'none',
            1000,
            100
        )
    ) seq;

    RETURN 'SUCCESS: Data inserted into doc_chunks_temp';

EXCEPTION
    WHEN OTHER THEN
        err_msg := 'ERROR: ' || ERROR_MESSAGE();
        RETURN err_msg;

END;
$$;


-- CALL insert_doc_chunks();


-- SELECT * FROM doc_chunks_temp;

-- ############ CREATING STREAM ON doc_chunks_temp TABLE TO PROCESS ONLY NEW INCOMMING RECORDS OF CHUNKED DATA ############

-- CREATE STREAM STM_DOC_CHUNK_TEMP 
--     ON TABLE doc_chunks_temp   
--     APPEND_ONLY = TRUE;



-- SELECT * FROM STM_DOC_CHUNK_TEMP;


-- ##################### RAW CODE FOR TESTING DATA EMBEDDING ######################

-- -- CREATE OR REPLACE TABLE chunk_embeddings_temp AS
-- INSERT INTO chunk_embeddings_temp
-- SELECT
--     id,
--     relative_path,
--     chunk_num,
--     chunk_text,
--     SNOWFLAKE.CORTEX.EMBED_TEXT_768(
--         'snowflake-arctic-embed-m-v1.5',
--         chunk_text
--     ) AS chunk_vec
-- FROM STM_DOC_CHUNK_TEMP;


-- ########## STORED PROCEDURE FOR CREATING AND STORING GENERATED VECTOR EMBEDDINGS #############

CREATE OR REPLACE PROCEDURE insert_chunk_embeddings()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    err_msg STRING;
BEGIN

    INSERT INTO chunk_embeddings_temp
    SELECT
        id,
        relative_path,
        chunk_num,
        chunk_text,
        SNOWFLAKE.CORTEX.EMBED_TEXT_768(
            'snowflake-arctic-embed-m-v1.5',
            chunk_text
        ) AS chunk_vec
    FROM STM_DOC_CHUNK_TEMP;

    RETURN 'SUCCESS: Embeddings generated and inserted into chunk_embeddings_temp';

EXCEPTION
    WHEN OTHER THEN
        err_msg := 'ERROR: ' || ERROR_MESSAGE();
        RETURN err_msg;

END;
$$;




-- CALL insert_chunk_embeddings();


-- SELECT * FROM chunk_embeddings_temp;


-- ################################################## IGNORE ##################################################

-- DROP CORTEX SEARCH SERVICE RAG_DB.RAG_SCHEMA.temp_serch_svc;

-- CREATE OR REPLACE CORTEX SEARCH SERVICE RAG_DB.RAG_SCHEMA.temp_serch_svc
-- ON chunk_text
-- PRIMARY KEY (chunk_id)
-- ATTRIBUTES relative_path, chunk_id
-- WAREHOUSE = COMPUTE_WH
-- TARGET_LAG = '1 hour'
-- AS (
--     SELECT 
--         CONCAT(id, '_', chunk_num) AS chunk_id,
--         id,
--         relative_path,
--         chunk_num,
--         chunk_text
--     FROM RAG_DB.RAG_SCHEMA.chunk_embeddings_temp
-- );

-- SHOW CORTEX SEARCH SERVICES IN SCHEMA RAG_DB.RAG_SCHEMA;

-- DESCRIBE CORTEX SEARCH SERVICE TEMP_SEARCH_SVC;

-- SHOW CORTEX SEARCH SERVICES;


-- SELECT
--     PARSE_JSON(
--         SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
--             'RAG_DB.RAG_SCHEMA.TEMP_SERCH_SVC',
--             '{
--                 "query": "Dmart?",
--                 "columns": ["chunk_id", "relative_path", "chunk_text"],
--                 "limit": 10
--             }'
--         )
--     )['results']::VARIANT AS search_results
-- ;

-- ####################################################################################################



-- ##################### RAW CODE FOR TESTING RETRIEVAL PROCESS of related  chunks for context ######################


-- WITH q_embed AS (
--     SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_768(
--         'snowflake-arctic-embed-m-v1.5',
--         'what happened Before the disaster in Havana' -- User query
--     ) AS query_vec
-- )
-- SELECT
--     c.id,
--     c.relative_path,
--     c.chunk_num,
--     c.chunk_text,
--     VECTOR_L2_DISTANCE(c.chunk_vec, q.query_vec) AS score
-- FROM RAG_DB.RAG_SCHEMA.chunk_embeddings_temp c,
--      q_embed q
-- ORDER BY score ASC
-- LIMIT 10;


-- ##################### STORED PROCEDURE FOR RETRIEVAL ######################

CREATE OR REPLACE PROCEDURE RAG_DB.RAG_SCHEMA.search_chunks(
    question STRING,
    top_k INT
)
RETURNS TABLE (
    relative_path STRING,
    chunk_num INT,
    chunk_text STRING,
    score FLOAT
)
LANGUAGE SQL
AS
$$
DECLARE
    v_k INT DEFAULT 0;
    v_question STRING DEFAULT '';
    rs RESULTSET;
BEGIN
    v_k         := top_k;
    v_question  := question;

    rs := (
        WITH q_embed AS (
            SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_768(
                'snowflake-arctic-embed-m-v1.5',
                :v_question
            ) AS query_vec
        )
        SELECT
            c.relative_path,
            c.chunk_num,
            c.chunk_text,
            VECTOR_L2_DISTANCE(c.chunk_vec, q.query_vec) AS score
        FROM RAG_DB.RAG_SCHEMA.chunk_embeddings_temp c,
             q_embed q
        ORDER BY score ASC
        LIMIT :v_k
    );

    RETURN TABLE(rs);
END;
$$;


-- SELECT * FROM TABLE(RAG_DB.RAG_SCHEMA.search_chunks('what happened Before the disaster in Havana?', 5));






CALL RAG_DB.RAG_SCHEMA.LOAD_DOCUMENTS_TO_RAW();  -- Step 1
SELECT * FROM RAW_TABLE;
SELECT * FROM RAW_TBL_STM;
CALL insert_doc_chunks(); -- Step 2
SELECT * FROM doc_chunks_temp;
SELECT * FROM STM_DOC_CHUNK_TEMP;
CALL insert_chunk_embeddings(); -- Step 3
SELECT DISTINCT RELATIVE_PATH FROM RAG_DB.RAG_SCHEMA.CHUNK_EMBEDDINGS_TEMP;




-- TRUNCATE TABLE RAW_TABLE;
-- TRUNCATE TABLE doc_chunks_temp;
-- TRUNCATE TABLE chunk_embeddings_temp;
