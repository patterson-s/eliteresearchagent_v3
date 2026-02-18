"""
embedding_match.py — Cohere embed-english-v3.0 semantic similarity matching.

Optional/fallback tier — skipped gracefully if no Cohere API key is available.
Only called when fuzzy matching fails or returns a below-threshold score.
"""

import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
from dotenv import load_dotenv

# Load .env from project root
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

EMBED_MODEL = "embed-english-v3.0"
EMBED_INPUT_TYPE_DOCUMENT = "search_document"
EMBED_INPUT_TYPE_QUERY = "search_query"
EMBED_BATCH_SIZE = 96  # Cohere's max per request
DEFAULT_SIMILARITY_THRESHOLD = 0.82


class EmbeddingMatcher:
    """
    Semantic similarity matcher using Cohere's embed API.

    Usage:
        matcher = EmbeddingMatcher()
        if matcher.is_available():
            result = matcher.find_similar("J-PAL", entries=ngo_entries)
    """

    def __init__(self, api_key: Optional[str] = None):
        self._api_key = api_key or os.getenv("COHERE_API_KEY")
        self._client = None  # lazy init
        self._ontology_embeddings: Optional[np.ndarray] = None
        self._ontology_entries: List[Dict] = []
        self._ontology_strings: List[str] = []  # the strings that were embedded

    def is_available(self) -> bool:
        """Return True if Cohere API key is set and cohere package is importable."""
        if not self._api_key:
            return False
        try:
            import cohere  # noqa: F401
            return True
        except ImportError:
            return False

    def _get_client(self):
        """Lazy Cohere client initialization."""
        if self._client is not None:
            return self._client
        if not self._api_key:
            raise ValueError(
                "COHERE_API_KEY is not set. "
                "Set it in your .env file or pass api_key to EmbeddingMatcher()."
            )
        try:
            import cohere
        except ImportError:
            raise ImportError(
                "cohere package is required for embedding matching. "
                "Install it with: pip install cohere"
            )
        self._client = cohere.ClientV2(api_key=self._api_key)
        return self._client

    def _embed_texts(self, texts: List[str], input_type: str) -> np.ndarray:
        """
        Embed a list of texts using Cohere's embed API.
        Handles batching (Cohere limit: 96 texts per request).
        Returns np.ndarray of shape (len(texts), embedding_dim).
        """
        co = self._get_client()
        all_embeddings = []

        for i in range(0, len(texts), EMBED_BATCH_SIZE):
            batch = texts[i: i + EMBED_BATCH_SIZE]
            response = co.embed(
                texts=batch,
                model=EMBED_MODEL,
                input_type=input_type,
                embedding_types=["float"],
            )
            # Cohere v2: response.embeddings.float_ is a list of lists
            batch_embeddings = response.embeddings.float_
            all_embeddings.extend(batch_embeddings)

        return np.array(all_embeddings, dtype=np.float32)

    def _entry_to_string(self, entry: Dict) -> str:
        """
        Build the string to embed for an ontology entry.
        Use canonical_name + top 2 variations for richer context.
        """
        parts = [entry.get("canonical_name", "")]
        variations = entry.get("variations_found", [])
        parts.extend(variations[:2])
        return " | ".join(p for p in parts if p)

    def build_ontology_index(self, entries: List[Dict]) -> None:
        """
        Embed all provided entries and cache the embedding matrix.
        Call this once per meta_type subset before calling find_similar().
        This is expensive — results are cached in memory.
        """
        if not entries:
            self._ontology_embeddings = np.empty((0, 0), dtype=np.float32)
            self._ontology_entries = []
            self._ontology_strings = []
            return

        strings = [self._entry_to_string(e) for e in entries]
        embeddings = self._embed_texts(strings, EMBED_INPUT_TYPE_DOCUMENT)

        self._ontology_entries = entries
        self._ontology_strings = strings
        self._ontology_embeddings = embeddings

    def _cosine_similarity(self, query_vec: np.ndarray, doc_matrix: np.ndarray) -> np.ndarray:
        """
        Compute cosine similarity between a query vector and a matrix of document vectors.
        Returns 1D array of scores in [-1, 1].
        """
        query_norm = np.linalg.norm(query_vec)
        doc_norms = np.linalg.norm(doc_matrix, axis=1)

        if query_norm == 0:
            return np.zeros(len(doc_matrix))

        # Avoid division by zero for zero-norm doc vectors
        safe_doc_norms = np.where(doc_norms == 0, 1e-10, doc_norms)
        return np.dot(doc_matrix, query_vec) / (safe_doc_norms * query_norm)

    def find_similar(
        self,
        raw_name: str,
        entries: Optional[List[Dict]] = None,
        threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
    ) -> Optional[Tuple[Dict, float]]:
        """
        Find the most semantically similar ontology entry to raw_name.

        Args:
            raw_name: the raw org name to match
            entries: optional list of entries to search; if provided and different
                     from the currently cached set, rebuilds the index
            threshold: cosine similarity threshold (0-1)

        Returns:
            (matched_entry, similarity_score) or None if below threshold
        """
        if not self.is_available():
            return None

        # Rebuild index if entries changed
        if entries is not None and entries is not self._ontology_entries:
            self.build_ontology_index(entries)

        if (self._ontology_embeddings is None
                or len(self._ontology_embeddings) == 0):
            return None

        # Embed the query
        query_embeddings = self._embed_texts([raw_name], EMBED_INPUT_TYPE_QUERY)
        query_vec = query_embeddings[0]

        # Compute similarities
        similarities = self._cosine_similarity(query_vec, self._ontology_embeddings)
        best_idx = int(np.argmax(similarities))
        best_score = float(similarities[best_idx])

        if best_score < threshold:
            return None

        return (self._ontology_entries[best_idx], best_score)

    def find_top_n(
        self,
        raw_name: str,
        entries: Optional[List[Dict]] = None,
        n: int = 5,
        min_score: float = 0.60,
    ) -> List[Tuple[Dict, float]]:
        """
        Return top-N most similar entries above min_score.
        Used by matcher.py to gather candidates for LLM disambiguation.

        Returns: list of (entry, score) sorted by score descending.
        """
        if not self.is_available():
            return []

        if entries is not None and entries is not self._ontology_entries:
            self.build_ontology_index(entries)

        if (self._ontology_embeddings is None
                or len(self._ontology_embeddings) == 0):
            return []

        query_embeddings = self._embed_texts([raw_name], EMBED_INPUT_TYPE_QUERY)
        query_vec = query_embeddings[0]

        similarities = self._cosine_similarity(query_vec, self._ontology_embeddings)

        # Get top-N indices
        top_indices = np.argsort(similarities)[::-1][:n]
        results = []
        for idx in top_indices:
            score = float(similarities[idx])
            if score >= min_score:
                results.append((self._ontology_entries[idx], score))

        return results
