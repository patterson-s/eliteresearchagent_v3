"""
matcher.py — Orchestrator for the full classification + matching pipeline.

Processes a single org name string through:
  1. Keyword-based classification (classifiers.py)
  2. Exact match on canonical_name and variations_found (ontology_db.py)
  3. Fuzzy match via rapidfuzz (fuzzy_match.py)
  4. Semantic embedding match via Cohere (embedding_match.py)
  5. LLM disambiguation via Claude (llm_match.py)
  6. Review queue for medium-confidence matches
  7. Unmatched / stub creation
"""

from pathlib import Path
from typing import Dict, List, Optional, Tuple

from classifiers import (
    classify_org,
    CATEGORY_TO_META_TYPE,
    CATEGORY_TO_SEARCH_META_TYPE,
)
from ontology_db import OntologyDB
from fuzzy_match import (
    fuzzy_match_typed,
    fuzzy_match_all,
    fuzzy_top_n,
    FuzzyMatchResult,
)
from embedding_match import EmbeddingMatcher
from llm_match import llm_disambiguate, llm_classify_org, is_available as llm_available

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

MATCHING_CONFIG = {
    "fuzzy_threshold_accept": 88,   # score >= this: auto-accept match
    "fuzzy_threshold_review": 70,   # score in [70, 88): flag for human review
    "embedding_threshold": 0.82,    # cosine similarity threshold for embedding
    "use_embedding": True,          # set False to disable Cohere embedding
    "use_llm_match": True,          # set False to disable LLM disambiguation
    "use_llm_classify": True,       # set False to disable LLM org classification
    "max_llm_candidates": 5,        # max candidates passed to LLM
    "deduplicate_orgs": True,       # deduplicate org names within a person
}

# ─────────────────────────────────────────────────────────────────────────────
# Result type
# ─────────────────────────────────────────────────────────────────────────────

# MatchResult is a plain dict with these keys:
#   raw_name: str
#   matched_canonical: Optional[str]
#   match_method: Optional[str]  — "exact_canonical"|"exact_variation"|
#                                  "fuzzy_canonical"|"fuzzy_variation"|
#                                  "embedding"|"llm"|None
#   match_confidence: Optional[float]  — 0.0-1.0
#   ontology_tag: Optional[str]        — canonical_tag from un/gov_ontology
#   meta_type: Optional[str]           — from matched entry or classifier
#   matched: bool
#   needs_review: bool                 — True if medium-confidence fuzzy match
#   org_type_classified: str           — output of classify_org()
#   proposed_match_canonical: Optional[str]  — for needs_review=True cases
#   proposed_match_confidence: Optional[float]

MatchResult = Dict


def _get_ontology_tag(entry: Dict) -> Optional[str]:
    """
    Safely extract the canonical_tag from an ontology entry.
    Checks un_ontology first, then gov_ontology.
    Returns None for universities (no tag system) and empty sub-ontologies.
    """
    un = entry.get("un_ontology") or {}
    gov = entry.get("gov_ontology") or {}
    return un.get("canonical_tag") or gov.get("canonical_tag") or None


def _build_result(
    raw_name: str,
    org_type: str,
    matched_entry: Optional[Dict] = None,
    method: Optional[str] = None,
    confidence: Optional[float] = None,
    matched: bool = False,
    needs_review: bool = False,
    proposed_entry: Optional[Dict] = None,
    proposed_confidence: Optional[float] = None,
) -> MatchResult:
    """Build a standardized MatchResult dict."""
    result: MatchResult = {
        "raw_name": raw_name,
        "matched_canonical": None,
        "match_method": method,
        "match_confidence": confidence,
        "ontology_tag": None,
        "meta_type": CATEGORY_TO_META_TYPE.get(org_type, "other"),
        "matched": matched,
        "needs_review": needs_review,
        "org_type_classified": org_type,
        "proposed_match_canonical": None,
        "proposed_match_confidence": None,
    }

    if matched and matched_entry:
        result["matched_canonical"] = matched_entry.get("canonical_name")
        result["ontology_tag"] = _get_ontology_tag(matched_entry)
        result["meta_type"] = matched_entry.get("meta_type", result["meta_type"])

    if needs_review and proposed_entry:
        result["proposed_match_canonical"] = proposed_entry.get("canonical_name")
        result["proposed_match_confidence"] = proposed_confidence
        # Also set meta_type from proposed entry for context
        result["meta_type"] = proposed_entry.get("meta_type", result["meta_type"])

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Main matcher class
# ─────────────────────────────────────────────────────────────────────────────

class OrgMatcher:
    """
    Multi-tier organization matching pipeline.

    Initialize once and reuse across many org names for efficiency
    (avoids reloading the ontology and rebuilding indexes on every call).
    """

    def __init__(
        self,
        config: Dict = None,
        db: Optional[OntologyDB] = None,
    ):
        self.config = config or MATCHING_CONFIG
        self.db = db or OntologyDB()
        self._embedder: Optional[EmbeddingMatcher] = None

        if self.config.get("use_embedding", True):
            self._embedder = EmbeddingMatcher()
            if not self._embedder.is_available():
                self._embedder = None  # disable silently if no key

        # Pre-cache entry lists by meta_type for embedding index building
        self._typed_entries: Dict[str, List[Dict]] = {
            "io": self.db.get_by_meta_type("io"),
            "gov": self.db.get_by_meta_type("gov"),
            "university": self.db.get_by_meta_type("university"),
        }
        self._all_entries: List[Dict] = self.db.get_all()

        # Track embedding indexes built to avoid rebuilding
        self._embed_indexed_type: Optional[str] = None

    def _get_entries_for_type(self, search_meta_type: Optional[str]) -> List[Dict]:
        """Return the appropriate entry list for a search domain."""
        if search_meta_type is None:
            return self._all_entries
        return self._typed_entries.get(search_meta_type, self._all_entries)

    def _ensure_embed_index(self, search_meta_type: Optional[str]) -> None:
        """Build (or rebuild if needed) the embedding index for the given domain."""
        if self._embedder is None:
            return
        key = search_meta_type or "all"
        if self._embed_indexed_type != key:
            entries = self._get_entries_for_type(search_meta_type)
            self._embedder.build_ontology_index(entries)
            self._embed_indexed_type = key

    def match_single(
        self,
        raw_name: str,
        context: Optional[str] = None,
    ) -> MatchResult:
        """
        Run the full matching pipeline for a single raw org name.

        Args:
            raw_name: the raw organization name string from a career event
            context: optional context string for LLM disambiguation
                     (e.g., "Person: Amina Mohammed, role: Architect")

        Returns:
            MatchResult dict
        """
        raw_name = raw_name.strip()
        if not raw_name:
            return _build_result("", "other", matched=False)

        cfg = self.config
        accept_thresh = cfg["fuzzy_threshold_accept"]
        review_thresh = cfg["fuzzy_threshold_review"]

        # ── Step 1: Classify ─────────────────────────────────────────────────
        org_type = classify_org(raw_name)

        # If keyword classification yields "other" AND LLM classify is on,
        # try LLM classification as a hint (but don't block on it)
        if org_type == "other" and cfg.get("use_llm_classify") and llm_available():
            llm_type = llm_classify_org(raw_name)
            if llm_type and llm_type != "other":
                org_type = llm_type

        search_meta_type = CATEGORY_TO_SEARCH_META_TYPE.get(org_type)

        # ── Step 2: Exact matching ────────────────────────────────────────────
        entry = self.db.lookup_canonical(raw_name)
        if entry:
            return _build_result(
                raw_name, org_type,
                matched_entry=entry,
                method="exact_canonical",
                confidence=1.0,
                matched=True,
            )

        entry = self.db.lookup_variation(raw_name)
        if entry:
            return _build_result(
                raw_name, org_type,
                matched_entry=entry,
                method="exact_variation",
                confidence=1.0,
                matched=True,
            )

        # ── Step 3: Fuzzy matching ────────────────────────────────────────────
        fuzzy_candidate: Optional[FuzzyMatchResult] = None

        if search_meta_type:
            fuzzy_result = fuzzy_match_typed(
                raw_name, self.db, search_meta_type,
                threshold=review_thresh,
            )
        else:
            fuzzy_result = fuzzy_match_all(
                raw_name, self.db,
                threshold=review_thresh,
            )

        if fuzzy_result:
            score = fuzzy_result["score"]
            if score >= accept_thresh:
                return _build_result(
                    raw_name, org_type,
                    matched_entry=fuzzy_result["matched_entry"],
                    method=fuzzy_result["match_method"],
                    confidence=round(score / 100.0, 4),
                    matched=True,
                )
            else:
                # Score in review band — store as candidate, continue to embedding/LLM
                fuzzy_candidate = fuzzy_result

        # ── Step 4: Embedding matching ────────────────────────────────────────
        embed_result: Optional[Tuple[Dict, float]] = None

        if self._embedder and cfg.get("use_embedding"):
            self._ensure_embed_index(search_meta_type)
            embed_result = self._embedder.find_similar(
                raw_name,
                threshold=cfg["embedding_threshold"],
            )

            if embed_result:
                entry, score = embed_result
                return _build_result(
                    raw_name, org_type,
                    matched_entry=entry,
                    method="embedding",
                    confidence=round(score, 4),
                    matched=True,
                )

        # ── Step 5: LLM disambiguation ────────────────────────────────────────
        if cfg.get("use_llm_match") and llm_available():
            # Gather top-N candidates from fuzzy + embedding
            entries_for_search = self._get_entries_for_type(search_meta_type)
            top_candidates = fuzzy_top_n(
                raw_name, entries_for_search,
                n=cfg["max_llm_candidates"],
                min_score=40.0,
            )

            # Add embedding top-N if available
            if self._embedder and cfg.get("use_embedding"):
                self._ensure_embed_index(search_meta_type)
                embed_top = self._embedder.find_top_n(
                    raw_name, n=cfg["max_llm_candidates"], min_score=0.50
                )
                # Merge: add embed results not already in top_candidates
                existing_names = {e.get("canonical_name") for e, _ in top_candidates}
                for e, s in embed_top:
                    if e.get("canonical_name") not in existing_names:
                        top_candidates.append((e, s))

            if top_candidates:
                candidates_only = [e for e, _ in top_candidates[:cfg["max_llm_candidates"]]]
                llm_result = llm_disambiguate(
                    raw_name, candidates_only, context=context
                )
                if llm_result:
                    entry, confidence = llm_result
                    return _build_result(
                        raw_name, org_type,
                        matched_entry=entry,
                        method="llm",
                        confidence=round(confidence, 4),
                        matched=True,
                    )

        # ── Step 6: Review queue (medium-confidence fuzzy candidate) ──────────
        if fuzzy_candidate:
            return _build_result(
                raw_name, org_type,
                matched=False,
                needs_review=True,
                proposed_entry=fuzzy_candidate["matched_entry"],
                proposed_confidence=round(fuzzy_candidate["score"] / 100.0, 4),
            )

        # ── Step 7: No match ─────────────────────────────────────────────────
        return _build_result(raw_name, org_type, matched=False)

    def match_person(
        self,
        person_name: str,
        career_events: List[Dict],
    ) -> List[MatchResult]:
        """
        Match all unique organizations from a person's career events.

        Deduplicates org names before matching — each unique org is matched once.
        Returns one MatchResult per unique org name (not per event occurrence).

        Args:
            person_name: the person's name (used for context in LLM calls)
            career_events: list of career event dicts (each has 'organizations' list)

        Returns:
            List of MatchResult dicts, one per unique org string
        """
        # Collect all org names, deduplicate preserving first-occurrence order
        seen = set()
        org_names = []
        for event in career_events:
            for org in event.get("organizations", []):
                org = org.strip()
                if org and org not in seen:
                    seen.add(org)
                    org_names.append(org)

        if not self.config.get("deduplicate_orgs", True):
            # Include duplicates
            org_names = []
            for event in career_events:
                for org in event.get("organizations", []):
                    org = org.strip()
                    if org:
                        org_names.append(org)

        results = []
        for org in org_names:
            context = f"Person: {person_name}"
            result = self.match_single(org, context=context)
            results.append(result)

        return results
