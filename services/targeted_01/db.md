# Database Reference — targeted_01

This document is the working reference for querying the PostgreSQL database (`eliteresearch`) from within the `targeted_01` service.

---

## Connection

**Python environment**: `C:/Users/spatt/anaconda3/envs/eliteresearchagent_v3/python.exe`
**Credentials**: loaded from `.env` at project root (`C:/Users/spatt/Desktop/eliteresearchagent_v3/.env`)

```python
import sys
sys.path.insert(0, 'C:/Users/spatt/Desktop/eliteresearchagent_v3/services/data_loader')
from load_data import get_db_connection

conn = get_db_connection()
cur = conn.cursor()
# ... queries ...
conn.close()
```

The `.env` file supplies:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=eliteresearch
DB_USER=postgres
DB_PASSWORD=...
```

---

## Schemas

| Schema | Purpose |
|---|---|
| `sources` | **Primary schema for this project.** Chunks, embeddings, sources, people. |
| `prosopography` | Prototype career-events schema (nearly empty — 1 person, 12 events). Not used here. |
| `services` | Birth year verification results (74 rows). Not used here. |
| `public` | Empty. |

---

## Schema: `sources` — Table Reference

### `sources.persons_searched`
The top-level entity table. One row per search batch (not per unique person).

| Column | Type | Notes |
|---|---|---|
| `id` | integer PK | Batch ID — not person ID |
| `person_name` | text | Spaced format: `"Abhijit Banerjee"`, `"Amina J. Mohammed"` |
| `search_query` | text | Full search string used to retrieve sources |
| `searched_at` | timestamp | When the search was run |

**⚠️ Duplicate batches**: ~10 people have 2 identical rows from two independent search runs (same URLs, same ranks). Always use `DISTINCT` on chunk/source IDs when aggregating. Affected people include: Gro Harlem Brundtland, Edson Prestes, Robert Badinter, Nanjira Sambuli, Jack Ma, Sadako Ogata, Amre Moussa, David Hannay, Nafis Sadik, Yuichiro Anzai.

**Row count**: 110 rows → 75 unique people

---

### `sources.search_results`
One row per fetched URL (source document). Up to 10 per search batch (rank 1–10).

| Column | Type | Notes |
|---|---|---|
| `id` | integer PK | Source ID |
| `person_search_id` | integer FK → `persons_searched.id` | Links source to person batch |
| `rank` | integer | 1–10, search result rank |
| `url` | text | Source URL |
| `title` | text | Page title (may be null) |
| `fetch_status` | text | `'success'` or `'failed'` |
| `fetch_error` | text | Error message if failed |
| `full_text` | text | Full extracted text of the document (avg ~10 KB, max ~500 KB) |
| `fetched_at` | timestamp | When the URL was fetched |
| `extraction_method` | text | `'html'`, `'pdf_basic'`, or `NULL` |
| `extraction_quality` | text | `'good'` (30 rows) or `NULL` (most rows) |
| `needs_ocr` | boolean | Whether OCR was needed |
| `provenance_narrative` | text | Short description of why this source was retrieved |

**Key stats**:
- Total: 1,093 rows
- Successful fetches: 933 (fetch_status = 'success')
- Failed fetches: 160
- Sources with `full_text` populated: 904
- Sources with chunks: 625 (468 sources have zero chunks — failed or unextracted)
- Extraction methods: html (614), pdf_basic (30), None (449)

---

### `sources.chunks`
Text chunks derived from each source. Fixed-size sliding window (~400 tokens).

| Column | Type | Notes |
|---|---|---|
| `id` | integer PK | Chunk ID |
| `search_result_id` | integer FK → `search_results.id` | Links chunk to source |
| `chunk_index` | integer | Position within the source (0-indexed) |
| `start_token` | integer | Token offset start |
| `end_token` | integer | Token offset end |
| `char_start` | integer | Character offset start |
| `char_end` | integer | Character offset end |
| `token_count` | integer | Actual token count (min=1, max=400, avg=355) |
| `text` | text | The chunk text |
| `created_at` | timestamp | |

**Total**: 2,714 chunks across 75 people

---

### `sources.embeddings`
One embedding per chunk — full 1:1 coverage.

| Column | Type | Notes |
|---|---|---|
| `id` | integer PK | |
| `chunk_id` | integer FK → `chunks.id` | |
| `model` | text | `"embed-v4.0"` (Cohere) — all chunks |
| `embedding` | ARRAY | 1536-dimensional float array |
| `created_at` | timestamp | |

**Coverage**: 2,714 / 2,714 — every chunk has an embedding. No nulls.

---

## Person Name Format

**Database uses spaced format with punctuation**: `"Abhijit Banerjee"`, `"Amina J. Mohammed"`
**File system uses underscores**: `Abhijit_Banerjee`, `Amina_J._Mohammed`

When cross-referencing `targeted_01/data/` filenames with DB queries, convert:
```python
db_name = file_dir_name.replace('_', ' ')  # "Abhijit_Banerjee" → "Abhijit Banerjee"
```

---

## Data Summary by Person

Approximate per-person stats (handling duplicates with DISTINCT):

| Stat | Typical | Range |
|---|---|---|
| Sources per person | ~10–20 search results | 10–20 |
| Successful sources | ~8–10 | varies |
| Chunks per person | ~25–50 | 9–132 |
| Embeddings | = chunk count | same |

Top 5 by chunk count: Salim Ahmed Salim (132), David Cameron (126), Ellen Johnson Sirleaf (119), V Isabel Guerrero Pulgar (99), Gordon Brown (69).

---

## Core Query Patterns

### 1. All chunks for a person (RAG source)
```sql
SELECT DISTINCT
    c.id          AS chunk_id,
    c.chunk_index,
    c.token_count,
    c.text,
    sr.id         AS source_id,
    sr.url,
    sr.title,
    sr.extraction_method,
    sr.rank
FROM sources.persons_searched ps
JOIN sources.search_results   sr ON sr.person_search_id = ps.id
JOIN sources.chunks            c  ON c.search_result_id  = sr.id
WHERE ps.person_name = %s
ORDER BY sr.rank, c.chunk_index;
```

### 2. All chunks + embeddings for a person (vector search)
```sql
SELECT DISTINCT ON (c.id)
    c.id          AS chunk_id,
    c.text,
    c.chunk_index,
    sr.id         AS source_id,
    sr.url,
    sr.title,
    sr.rank,
    e.embedding,
    e.model       AS embedding_model
FROM sources.persons_searched ps
JOIN sources.search_results   sr ON sr.person_search_id = ps.id
JOIN sources.chunks            c  ON c.search_result_id  = sr.id
JOIN sources.embeddings        e  ON e.chunk_id          = c.id
WHERE ps.person_name = %s
ORDER BY c.id, sr.rank;
```

### 3. All sources for a person (with chunk counts)
```sql
SELECT DISTINCT
    sr.id,
    sr.rank,
    sr.url,
    sr.title,
    sr.fetch_status,
    sr.extraction_method,
    COUNT(c.id) AS chunk_count
FROM sources.persons_searched ps
JOIN sources.search_results   sr ON sr.person_search_id = ps.id
LEFT JOIN sources.chunks       c  ON c.search_result_id  = sr.id
WHERE ps.person_name = %s
GROUP BY sr.id, sr.rank, sr.url, sr.title, sr.fetch_status, sr.extraction_method
ORDER BY sr.rank;
```

### 4. All chunks for a single source (by source_id)
```sql
SELECT
    c.id, c.chunk_index, c.token_count, c.text
FROM sources.chunks c
WHERE c.search_result_id = %s
ORDER BY c.chunk_index;
```

### 5. Full text of a single source (for full-document search)
```sql
SELECT
    sr.url, sr.title, sr.full_text, sr.extraction_method, sr.fetch_status
FROM sources.search_results sr
WHERE sr.id = %s;
```

### 6. All sources with full text for a person (non-RAG, full-doc read)
```sql
SELECT DISTINCT
    sr.id, sr.rank, sr.url, sr.title, sr.full_text, sr.extraction_method
FROM sources.persons_searched ps
JOIN sources.search_results   sr ON sr.person_search_id = ps.id
WHERE ps.person_name = %s
  AND sr.full_text IS NOT NULL
  AND sr.full_text != ''
ORDER BY sr.rank;
```

### 7. List all people in the database
```sql
SELECT DISTINCT person_name
FROM sources.persons_searched
ORDER BY person_name;
```

### 8. Embeddings for all chunks of a single source (by source_id)
```sql
SELECT
    c.id AS chunk_id, c.chunk_index, c.text,
    e.embedding, e.model
FROM sources.chunks     c
JOIN sources.embeddings e ON e.chunk_id = c.id
WHERE c.search_result_id = %s
ORDER BY c.chunk_index;
```

---

## Python Helper Patterns

### Load chunks for a person (using existing load_data.py)
```python
import sys
sys.path.insert(0, 'C:/Users/spatt/Desktop/eliteresearchagent_v3/services/data_loader')
from load_data import load_chunks_from_db, get_all_people

# All chunks + embeddings for one person
chunks = load_chunks_from_db("Abhijit Banerjee")
# Returns list of dicts: chunk_id, text, chunk_index, source_url, title,
#                        extraction_method, person_name, embedding, embedding_model

# List all people
people = get_all_people()  # returns list of person_name strings (75 unique)
```

### Custom query with deduplication (recommended for targeted_01)
```python
from psycopg2.extras import RealDictCursor

def get_person_chunks(person_name: str):
    conn = get_db_connection()
    try:
        query = """
            SELECT DISTINCT ON (c.id)
                c.id AS chunk_id, c.text, c.chunk_index, c.token_count,
                sr.id AS source_id, sr.url, sr.title, sr.rank,
                e.embedding, e.model AS embedding_model
            FROM sources.persons_searched ps
            JOIN sources.search_results   sr ON sr.person_search_id = ps.id
            JOIN sources.chunks            c  ON c.search_result_id  = sr.id
            LEFT JOIN sources.embeddings   e  ON e.chunk_id = c.id
            WHERE ps.person_name = %s
            ORDER BY c.id, sr.rank
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (person_name,))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
```

### Cosine similarity (Python-side, for re-ranking or filtering)
```python
import numpy as np

def cosine_similarity(vec_a, vec_b):
    a = np.array(vec_a)
    b = np.array(vec_b)
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))
```

---

## Search Strategies

### RAG (vector similarity + Cohere re-ranking)
1. Pull all chunks + embeddings for the target person using query #2 above
2. Embed the query using `cohere.embed(texts=[query], model="embed-v4.0", input_type="search_query")`
3. Compute cosine similarity against all chunk embeddings
4. Pass top-N candidate chunks to Cohere rerank: `cohere.rerank(model="rerank-v3.5", query=query, documents=[c['text'] for c in candidates])`
5. Use top reranked chunks as context for the generation prompt

### Full-document search (non-RAG)
1. Pull `full_text` for all sources for a person using query #6 above
2. Search directly over full_text with `ILIKE '%keyword%'` or pass full doc to LLM
3. Useful when the answer might span chunk boundaries or the document is short

### Source isolation (CRITICAL)
Always filter by `ps.person_name = %s`. Never mix chunks across people. The join chain `persons_searched → search_results → chunks` enforces person-scoping automatically.

### Handling duplicate persons_searched rows
Use `DISTINCT ON (c.id)` or `DISTINCT c.id` to ensure each chunk is counted once. The duplicate batches contain identical URLs so chunks are interchangeable, but IDs may differ across batches — always deduplicate.

---

## Gotchas

| Issue | Detail |
|---|---|
| Duplicate search batches | ~10 people have 2 `persons_searched` rows → always use `DISTINCT` |
| 468 sources with no chunks | Failed fetches or unextracted PDFs — skip these in chunk-based workflows |
| Name format mismatch | DB: `"Amina J. Mohammed"` / filesystem: `Amina_J._Mohammed` |
| `full_text` can be very large | Max ~500 KB. Don't load all people's full texts into memory at once |
| `extraction_quality` rarely set | Only 30 rows have `'good'`; don't rely on it as a filter |
| Chunk token_count ≤ 400 | Avg 355 tokens. A few boundary chunks can be as small as 1 token |
| `embed-v4.0` is Cohere model | Use same model when embedding queries for cosine similarity |
