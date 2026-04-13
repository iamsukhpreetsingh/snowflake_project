import streamlit as st
from snowflake.core import Root

try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
except Exception:
    from snowflake.snowpark import Session
    session = Session.builder.configs(st.secrets["connections"]["snowflake"]).create()


# --- Snowflake helper: escape_single_quote for SQL injection defense ---
def escape_sql_string(s: str) -> str:
    if s is None:
        return "NULL"
    return s.replace("'", "''")


# --- 1. Vector search via stored procedure ---
def vector_search_chunks(question: str, top_k: int = 4):
    """
    Call search_chunks() stored procedure and return chunks as a list of dicts.
    """
    safe_question = escape_sql_string(question)

    sql = f"""
    SELECT * FROM TABLE(RAG_DB.RAG_SCHEMA.search_chunks('{safe_question}', {top_k}));
    """
    result = session.sql(sql).collect()

    return [
        {
            "relative_path": row.RELATIVE_PATH,
            "chunk_num": row.CHUNK_NUM,
            "chunk_text": row.CHUNK_TEXT,
            "score": row.SCORE
        }
        for row in result
    ]


# --- 2. Build RAG prompt from chunks ---
def build_rag_prompt_from_chunks(question: str, chunks: list) -> str:
    if not chunks:
        return f"Question: {question}\n\nI could not find relevant context in the indexed documents."

    context_parts = []
    for i, c in enumerate(chunks, 1):
        context_parts.append(
            f"[Source {i} | {c['relative_path']}]\n{c['chunk_text']}"
        )

    context = "\n\n---\n\n".join(context_parts)

    return f"""You are a helpful assistant answering questions from the user's documents.
Answer only from the provided context.

Context:
{context}

Question:
{question}

Answer:"""


# --- 3. Call SNOWFLAKE.CORTEX.COMPLETE(...) ---
def generate_answer(model_name: str, prompt: str) -> str:
    safe_prompt = escape_sql_string(prompt)
    safe_model = escape_sql_string(model_name)

    sql = f"""
    SELECT
        SNOWFLAKE.CORTEX.COMPLETE(
            '{safe_model}',
            '{safe_prompt}'
        ) AS RESPONSE
    """

    result = session.sql(sql).collect()
    if result:
        return str(result[0]["RESPONSE"])
    return "No response from the model."


# --- 4. UI layout ---
st.set_page_config(page_title="RAG App with Custom Vector Search", layout="centered")

st.title("RAG App with your own Vector Search")
st.caption("Powered by your `chunk_embeddings_temp` and `RAG_DB.RAG_SCHEMA.search_chunks` stored procedure.")

# --- 5. Sidebar for controls ---
with st.sidebar:
    st.header("Settings")

    model_name = st.selectbox(
        "LLM Model",
        options=[
            "llama3.1-70b",
            "mistral-large2",
            "snowflake-arctic"
        ],
        index=0
    )

    num_chunks = st.slider(
        "Retrieved chunks per query",
        min_value=1,
        max_value=10,
        value=4,
        step=1
    )

    show_sources = st.checkbox("Show source chunks", value=True)

# --- 6. Initialize chat history ---
if "messages" not in st.session_state:
    st.session_state.messages = []

# --- 7. Display chat history ---
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# --- 8. Chat input block ---
if prompt := st.chat_input("Ask about your documents..."):
    # Add user message
    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Searching (vector) and generating answer..."):
            # 1. Vector search via stored procedure
            try:
                chunks = vector_search_chunks(prompt, num_chunks)
            except Exception as e:
                st.error(f"Error calling search_chunks: {e}")
                chunks = []

            # 2. Build RAG prompt
            rag_prompt = build_rag_prompt_from_chunks(prompt, chunks)

            # 3. Call Cortex LLM
            try:
                answer = generate_answer(model_name, rag_prompt)
            except Exception as e:
                st.error(f"Error calling SNOWFLAKE.CORTEX.COMPLETE: {e}")
                answer = "Could not generate an answer."

            st.markdown(answer)

            # 4. Show sources if requested
            if show_sources and chunks:
                with st.expander(f"Sources ({len(chunks)} retrieved)"):
                    for i, c in enumerate(chunks, 1):
                        st.markdown(f"**[{i}] {c['relative_path']}**")
                        st.caption(f"Score (L2 distance): {c['score']:.4f}")
                        st.write(
                            c["chunk_text"][:500] + "..."
                            if len(c["chunk_text"]) > 500
                            else c["chunk_text"]
                        )

        st.session_state.messages.append({"role": "assistant", "content": answer})
