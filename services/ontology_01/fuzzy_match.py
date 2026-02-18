"""
fuzzy_match.py — Rapidfuzz-based matching utilities for organization names.

Receives entry lists from OntologyDB — never loads JSON itself.
"""

import re
from typing import Dict, List, Optional, Tuple

try:
    from rapidfuzz import fuzz, process as rfprocess
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False

from ontology_db import OntologyDB

# Default threshold — overridden by MATCHING_CONFIG in matcher.py
DEFAULT_THRESHOLD = 85.0

# TypedDict-style result (using plain dict for Python 3.7 compat)
# Keys: raw_name, matched_entry, score (0-100), match_method, matched_string
FuzzyMatchResult = Dict


def _check_rapidfuzz() -> None:
    if not RAPIDFUZZ_AVAILABLE:
        raise ImportError(
            "rapidfuzz is required for fuzzy matching. "
            "Install it with: pip install rapidfuzz"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Normalization
# ─────────────────────────────────────────────────────────────────────────────

_PAREN_RE = re.compile(r"\s*\([^)]*\)\s*")


def normalize_for_fuzzy(name: str) -> str:
    """
    Prepare a name for fuzzy comparison:
    - Strip whitespace
    - Collapse internal whitespace to single space
    - Remove parenthetical content (acronyms, subtitles)
    - Lowercase
    - Remove trailing punctuation
    """
    name = name.strip()
    name = _PAREN_RE.sub(" ", name)        # remove (parenthetical content)
    name = re.sub(r"\s+", " ", name)       # collapse spaces
    name = name.lower()
    name = name.rstrip(".,;:")
    return name.strip()


_ACRONYM_RE = re.compile(r"\(([A-Z][A-Z0-9\-]{1,7})\)")


def extract_acronym(name: str) -> Optional[str]:
    """
    Extract an acronym from parentheses if it looks like one.
    "Abdul Latif Jameel Poverty Action Lab (J-PAL)" -> "J-PAL"
    "University of Calcutta" -> None
    Only extracts 2–8 char all-caps strings.
    """
    m = _ACRONYM_RE.search(name)
    if m:
        return m.group(1)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Candidate string building
# ─────────────────────────────────────────────────────────────────────────────

def _build_candidate_strings(entries: List[Dict]) -> List[Tuple[str, Dict, str]]:
    """
    Build a flat list of (normalized_string, entry_dict, field_label) tuples.
    field_label is "canonical" or "variation".
    Used for batch rapidfuzz scoring.
    """
    candidates = []
    for entry in entries:
        cname = entry.get("canonical_name", "")
        if cname:
            candidates.append((normalize_for_fuzzy(cname), entry, "canonical"))
        for var in entry.get("variations_found", []):
            if var:
                candidates.append((normalize_for_fuzzy(var), entry, "variation"))
    return candidates


# ─────────────────────────────────────────────────────────────────────────────
# Core matching
# ─────────────────────────────────────────────────────────────────────────────

def fuzzy_match_against_list(
    raw_name: str,
    candidates: List[Dict],
    threshold: float = DEFAULT_THRESHOLD,
) -> Optional[FuzzyMatchResult]:
    """
    Fuzzy-match raw_name against a list of ontology entries.

    Strategy:
    1. Normalize raw_name (remove parentheticals, lowercase)
    2. Score against canonical_name and each variations_found item
    3. Also try extracted acronym if present
    4. Return best match above threshold, or None

    Args:
        raw_name: the raw organization name string
        candidates: list of ontology entry dicts to match against
        threshold: minimum score (0-100) to accept a match

    Returns:
        FuzzyMatchResult dict or None
    """
    _check_rapidfuzz()

    if not candidates:
        return None

    normalized_query = normalize_for_fuzzy(raw_name)
    acronym = extract_acronym(raw_name)

    # Build strings list for vectorized scoring
    candidate_strings = _build_candidate_strings(candidates)
    if not candidate_strings:
        return None

    strings_only = [cs[0] for cs in candidate_strings]

    best_score = 0.0
    best_idx = -1

    # Primary scorer: token_sort_ratio (handles word order differences)
    # e.g. "University of Zurich" vs "Zurich, University of"
    scores_tsort = rfprocess.cdist(
        [normalized_query], strings_only,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=0,
    )[0]

    # Secondary scorer: WRatio (weighted combo — handles partial matches)
    scores_wratio = rfprocess.cdist(
        [normalized_query], strings_only,
        scorer=fuzz.WRatio,
        score_cutoff=0,
    )[0]

    # Take the max of both scorers per candidate
    for i, (ts, wr) in enumerate(zip(scores_tsort, scores_wratio)):
        combined = max(float(ts), float(wr))
        if combined > best_score:
            best_score = combined
            best_idx = i

    # Also try acronym scoring if present
    if acronym:
        acronym_norm = acronym.lower().strip()
        scores_acro = rfprocess.cdist(
            [acronym_norm], strings_only,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=0,
        )[0]
        for i, score in enumerate(scores_acro):
            if float(score) > best_score:
                best_score = float(score)
                best_idx = i

    if best_score < threshold or best_idx < 0:
        return None

    matched_string, matched_entry, field_label = candidate_strings[best_idx]
    match_method = f"fuzzy_{field_label}"  # "fuzzy_canonical" or "fuzzy_variation"

    return {
        "raw_name": raw_name,
        "matched_entry": matched_entry,
        "score": round(best_score, 2),
        "match_method": match_method,
        "matched_string": matched_string,
    }


def fuzzy_match_typed(
    raw_name: str,
    db: OntologyDB,
    meta_type: str,
    threshold: float = DEFAULT_THRESHOLD,
) -> Optional[FuzzyMatchResult]:
    """
    Fuzzy-match against entries filtered by meta_type.
    Primary entry point for typed matching (un, gov, university).
    """
    entries = db.get_by_meta_type(meta_type)
    return fuzzy_match_against_list(raw_name, entries, threshold)


def fuzzy_match_all(
    raw_name: str,
    db: OntologyDB,
    threshold: float = DEFAULT_THRESHOLD,
) -> Optional[FuzzyMatchResult]:
    """
    Fuzzy-match against ALL ontology entries (no meta_type filter).
    Used as fallback for ngo/private/other types.
    """
    entries = db.get_all()
    return fuzzy_match_against_list(raw_name, entries, threshold)


def fuzzy_top_n(
    raw_name: str,
    candidates: List[Dict],
    n: int = 5,
    min_score: float = 50.0,
) -> List[Tuple[Dict, float]]:
    """
    Return top-N candidate entries with scores above min_score.
    Used by matcher.py to gather candidates for LLM disambiguation.

    Returns: list of (entry, score) tuples, sorted by score descending.
    """
    _check_rapidfuzz()

    if not candidates:
        return []

    normalized_query = normalize_for_fuzzy(raw_name)
    acronym = extract_acronym(raw_name)
    candidate_strings = _build_candidate_strings(candidates)
    if not candidate_strings:
        return []

    strings_only = [cs[0] for cs in candidate_strings]

    scores_tsort = rfprocess.cdist(
        [normalized_query], strings_only,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=0,
    )[0]
    scores_wratio = rfprocess.cdist(
        [normalized_query], strings_only,
        scorer=fuzz.WRatio,
        score_cutoff=0,
    )[0]

    # Per-string best score
    per_string: List[Tuple[float, int]] = []
    for i, (ts, wr) in enumerate(zip(scores_tsort, scores_wratio)):
        per_string.append((max(float(ts), float(wr)), i))

    if acronym:
        acronym_norm = acronym.lower().strip()
        scores_acro = rfprocess.cdist(
            [acronym_norm], strings_only,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=0,
        )[0]
        for i, score in enumerate(scores_acro):
            cur_score, _ = per_string[i]
            if float(score) > cur_score:
                per_string[i] = (float(score), i)

    # Deduplicate by entry — keep max score per entry
    entry_best: Dict[int, float] = {}
    for score, idx in per_string:
        _, entry, _ = candidate_strings[idx]
        eid = id(entry)  # use object id as key (entries are dicts in memory)
        if eid not in entry_best or score > entry_best[eid]:
            entry_best[eid] = score

    # Map back to entry objects
    id_to_entry: Dict[int, Dict] = {}
    for _, entry, _ in candidate_strings:
        id_to_entry[id(entry)] = entry

    results = [
        (id_to_entry[eid], score)
        for eid, score in entry_best.items()
        if score >= min_score
    ]
    results.sort(key=lambda x: x[1], reverse=True)
    return results[:n]
