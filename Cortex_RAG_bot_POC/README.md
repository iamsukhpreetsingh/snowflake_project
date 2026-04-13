# Snowflake RAG Chatbot (Cortex-Powered)

This project implements a **Retrieval-Augmented Generation (RAG)** pipeline fully inside **Snowflake**, leveraging **Cortex AI functions** for document processing, chunking, embedding, and semantic search.

---

## Overview

The system ingests documents (PDFs & images), extracts text, splits it into chunks, generates embeddings, and retrieves relevant context for user queries using vector similarity.

---

## Architecture

```
Stage (Docs)
   ↓
RAW_TABLE (Extracted Text)
   ↓ (Stream)
Chunking (doc_chunks_temp)
   ↓ (Stream)
Embeddings (chunk_embeddings_temp)
   ↓
Semantic Retrieval (search_chunks)
```

---

## Cortex Functions Used

### 1. `AI_EXTRACT`

**Why:** Extract text from images and documents directly in Snowflake.

**Used in:**

* Image processing inside `LOAD_DOCUMENTS_TO_RAW`

**Benefit:**

* Eliminates need for external OCR tools
* Works directly on staged files

---

### 2. `SPLIT_TEXT_RECURSIVE_CHARACTER`

**Why:** Break large documents into smaller chunks for better retrieval.

**Used in:**

* `insert_doc_chunks` stored procedure

**Config:**

* Chunk size: `1000 characters`
* Overlap: `100 characters`

**Benefit:**

* Improves semantic search accuracy
* Maintains context continuity

---

### 3. `EMBED_TEXT_768`

**Why:** Convert text into vector embeddings for similarity search.

**Used in:**

* `insert_chunk_embeddings`
* Query embedding inside `search_chunks`

**Model:**

```
snowflake-arctic-embed-m-v1.5
```

**Benefit:**

* Enables vector-based semantic retrieval
* Fully managed inside Snowflake

---

## Pipeline Steps

### Step 1: Document Ingestion

* Procedure: `LOAD_DOCUMENTS_TO_RAW`
* Handles:

  * PDFs → `pypdf`
  * Images → `AI_EXTRACT`
* Stores output in `RAW_TABLE`

---

### Step 2: Chunking

* Procedure: `insert_doc_chunks`
* Uses Cortex splitter
* Outputs to `doc_chunks_temp`

---

### Step 3: Embedding

* Procedure: `insert_chunk_embeddings`
* Generates vector embeddings
* Stores in `chunk_embeddings_temp`

---

### Step 4: Retrieval

* Procedure: `search_chunks(question, top_k)`
* Converts query → embedding
* Uses `VECTOR_L2_DISTANCE` for similarity
* Returns top-k relevant chunks

---

## Query Example

```sql
SELECT * 
FROM TABLE(RAG_DB.RAG_SCHEMA.search_chunks(
    'What happened before the disaster in Havana?',
    5
));
```

---

## Why Cortex for RAG?

* ✅ Fully in-database AI (no external services)
* ✅ Scalable & serverless
* ✅ Simplified architecture (no separate vector DB)
* ✅ Native integration with SQL & Snowpark


