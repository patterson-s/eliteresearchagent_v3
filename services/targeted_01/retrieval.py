"""
retrieval.py — Shared DB + Cohere retrieval utilities for targeted_01.

This module is question-agnostic. It handles:
  - Fetching person-scoped chunks + embeddings from PostgreSQL
  - Embedding a retrieval query via Cohere embed-v4.0
  - Scoring chunks by cosine similarity
  - Reranking top candidates via Cohere rerank-v3.5

No prompt logic lives here. All LLM generation calls are in runner.py.

Usage:
    from retrieval import retrieve_for_person

    chunks = retrieve_for_person(
        person_name="Abhijit Banerjee",
        query="job title position 2013 appointed",
        api_key=os.getenv("COHERE_API_KEY")
    )
"""

import os
import sys
import numpy as np
import cohere
from pathlib import Path
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# ── Path setup ────────────────────────────────────────────────────────────────
_SERVICE_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SERVICE_DIR.parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

# Inject data_loader into path so we can import get_db_connection
sys.path.insert(0, str(_PROJECT_ROOT / "services" / "data_loader"))
from load_data import get_db_connection  # noqa: E402 (after sys.path manipulation)


# ── Database ──────────────────────────────────────────────────────────────────

def get_person_chunks(person_name: str) -> List[Dict[str, Any]]:
    """
    Fetch all chunks with embeddings for a given person from the database.

    Uses DISTINCT ON (c.id) to deduplicate the ~10 people who have two
    persons_searched rows from separate search batches. Always scopes
    results to exactly one person via ps.person_name = %s.

    Args:
        person_name: Spaced-format name as stored in DB, e.g. "Abhijit Banerjee".
                     Use person_dir_to_db_name() from runner.py to convert
                     underscore filesystem names before calling this function.

    Returns:
        List of dicts, each containing:
            chunk_id (int), text (str), chunk_index (int), token_count (int),
            source_id (int), url (str), title (str), rank (int),
            embedding (list[float]), embedding_model (str)
        Returns empty list if no chunks found for this person.
    """
    query = """
        SELECT DISTINCT ON (c.id)
            c.id          AS chunk_id,
            c.text,
            c.chunk_index,
            c.token_count,
            sr.id         AS source_id,
            sr.url,
            sr.title,
            sr.rank,
            e.embedding,
            e.model       AS embedding_model
        FROM sources.persons_searched ps
        JOIN sources.search_results   sr ON sr.person_search_id = ps.id
        JOIN sources.chunks            c  ON c.search_result_id  = sr.id
        JOIN sources.embeddings        e  ON e.chunk_id = c.id
        WHERE ps.person_name = %s
        ORDER BY c.id, sr.rank
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (person_name,))
            rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── Domain extraction ─────────────────────────────────────────────────────────

def extract_domain(url: str) -> str:
    """
    Extract the root domain from a URL, stripping the 'www.' prefix.

    Used to determine whether two chunks come from the same source domain,
    which is the basis for judging independence in the verification pass.

    Args:
        url: Any URL string. Handles malformed URLs gracefully.

    Returns:
        Domain string, e.g. "en.wikipedia.org". Returns "" on failure.

    Examples:
        "https://www.britannica.com/bio/X"  ->  "britannica.com"
        "https://en.wikipedia.org/wiki/X"   ->  "en.wikipedia.org"
    """
    try:
        host = urlparse(url).netloc.lower()
        return host[4:] if host.startswith("www.") else host
    except Exception:
        return ""


# ── Cohere embedding ──────────────────────────────────────────────────────────

def embed_query(query: str, api_key: str) -> List[float]:
    """
    Embed a retrieval query using Cohere embed-v4.0 (ClientV2).

    Uses input_type="search_query" to match the encoding scheme under which
    the stored chunk embeddings were produced. embedding_types=["float"] is
    required by ClientV2 to return dense float vectors.

    Args:
        query: The retrieval query string (already filled with person context).
        api_key: Cohere API key from environment.

    Returns:
        1536-dimensional embedding as a list of floats.

    Raises:
        RuntimeError: If the Cohere API call fails.
    """
    co = cohere.ClientV2(api_key=api_key)
    response = co.embed(
        texts=[query],
        model="embed-v4.0",
        input_type="search_query",
        embedding_types=["float"],
    )
    # ClientV2: response.embeddings.float_ is a list of lists of floats
    return response.embeddings.float_[0]


# ── Similarity scoring ────────────────────────────────────────────────────────

def cosine_similarity(vec_a: List[float], vec_b: List[float]) -> float:
    """
    Compute cosine similarity between two embedding vectors.

    Handles the zero-norm edge case by returning 0.0 rather than dividing
    by zero. This can occur with pathologically short chunks (token_count=1).

    Args:
        vec_a: First vector as a list of floats.
        vec_b: Second vector as a list of floats.

    Returns:
        Cosine similarity in [-1.0, 1.0]. For normalized embeddings this is
        effectively in [0.0, 1.0].
    """
    a = np.array(vec_a, dtype=float)
    b = np.array(vec_b, dtype=float)
    norm = np.linalg.norm(a) * np.linalg.norm(b)
    if norm == 0.0:
        return 0.0
    return float(np.dot(a, b) / norm)


def rank_by_similarity(
    chunks: List[Dict[str, Any]],
    query_embedding: List[float],
    top_k: int = 20,
    min_similarity: float = 0.15,
) -> List[Dict[str, Any]]:
    """
    Score all chunks by cosine similarity to the query embedding and return
    the top_k candidates above the minimum similarity threshold.

    Adds two keys to each returned chunk dict:
      - 'similarity' (float): cosine similarity score
      - 'domain' (str): root domain of the chunk's source URL

    Args:
        chunks: List of chunk dicts from get_person_chunks().
        query_embedding: Query vector from embed_query().
        top_k: Maximum candidates to return before reranking. Must be ≥ the
               rerank_top_n used in the subsequent rerank call.
        min_similarity: Minimum cosine similarity to include a chunk. 0.15
                        matches the threshold used in the v2 education service.

    Returns:
        List of chunk dicts sorted descending by similarity, length <= top_k.
        Chunks with no embedding field are silently skipped.
    """
    scored = []
    for chunk in chunks:
        emb = chunk.get("embedding")
        if emb is None:
            continue
        sim = cosine_similarity(query_embedding, emb)
        if sim >= min_similarity:
            c = dict(chunk)
            c["similarity"] = sim
            c["domain"] = extract_domain(chunk.get("url", ""))
            scored.append(c)
    scored.sort(key=lambda x: x["similarity"], reverse=True)
    return scored[:top_k]


# ── Cohere reranking ──────────────────────────────────────────────────────────

def rerank_chunks(
    query: str,
    candidates: List[Dict[str, Any]],
    api_key: str,
    top_n: int = 10,
) -> List[Dict[str, Any]]:
    """
    Rerank candidate chunks using Cohere rerank-v3.5 (ClientV2).

    Adds a 'rerank_score' key (float) to each returned chunk dict.

    On any Cohere API failure, falls back gracefully: returns the top_n
    candidates sorted by their existing similarity score with rerank_score=None.
    This allows the pipeline to continue with degraded precision rather than
    failing completely.

    Args:
        query: The same retrieval query string used for embedding.
        candidates: List of chunk dicts from rank_by_similarity().
        api_key: Cohere API key.
        top_n: Number of top reranked results to return.

    Returns:
        List of chunk dicts sorted descending by rerank_score, length <= top_n.
        On fallback, sorted by similarity with rerank_score=None.
    """
    if not candidates:
        return []
    top_n = min(top_n, len(candidates))
    texts = [c["text"] for c in candidates]
    co = cohere.ClientV2(api_key=api_key)
    try:
        result = co.rerank(
            model="rerank-v3.5",
            query=query,
            documents=texts,
            top_n=top_n,
        )
        reranked = []
        for r in result.results:
            c = dict(candidates[r.index])
            c["rerank_score"] = r.relevance_score
            reranked.append(c)
        return reranked
    except Exception:
        # Fallback: return top_n in similarity order
        fallback = [dict(c) for c in candidates[:top_n]]
        for c in fallback:
            c["rerank_score"] = None
        return fallback


# ── Main entry point ──────────────────────────────────────────────────────────

def retrieve_for_person(
    person_name: str,
    query: str,
    api_key: str,
    similarity_top_k: int = 20,
    similarity_threshold: float = 0.15,
    rerank_top_n: int = 10,
) -> List[Dict[str, Any]]:
    """
    Full retrieval pipeline for one person: DB fetch → embed → cosine rank
    → Cohere rerank.

    This is the single public entry point called by runner.py. It fetches
    chunks live from the database on each call. Given the small corpus size
    (avg 35 chunks per person, max 132), no caching layer is needed.

    Args:
        person_name: Spaced-format DB name, e.g. "Abhijit Banerjee".
                     Must match the person_name field in sources.persons_searched.
        query: Fully resolved retrieval query (placeholders already filled).
        api_key: Cohere API key.
        similarity_top_k: Number of cosine-scored candidates passed to reranker.
        similarity_threshold: Minimum cosine similarity (chunks below this are
                              excluded before reranking). 0.15 is the v2 default.
        rerank_top_n: Final number of chunks returned after reranking.

    Returns:
        List of up to rerank_top_n chunk dicts, each with keys:
            chunk_id, text, chunk_index, token_count, source_id, url, title,
            rank, embedding, embedding_model, similarity, domain, rerank_score.
        Returns empty list if the person has no chunks in DB or if all chunks
        fall below the similarity threshold.
    """
    chunks = get_person_chunks(person_name)
    if not chunks:
        return []

    query_emb = embed_query(query, api_key)
    candidates = rank_by_similarity(
        chunks, query_emb, top_k=similarity_top_k, min_similarity=similarity_threshold
    )
    if not candidates:
        return []

    return rerank_chunks(query, candidates, api_key, top_n=rerank_top_n)
