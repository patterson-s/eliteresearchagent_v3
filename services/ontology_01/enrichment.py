"""
enrichment.py — Web search + LLM field extraction for stub enrichment.

Flow:
  1. search_org()            — Serper API query, returns structured results
  2. extract_fields_with_llm() — Claude reads results, proposes field values
  3. enrich_stub()           — orchestrates both, returns proposals dict
  4. Cache                   — JSON file keyed by canonical_name, avoids re-querying

Proposals dict schema returned by enrich_stub():
{
  "canonical_name": str,
  "variations_found": [str, ...],
  "meta_type": str,           # io|gov|university|ngo|private|other
  "sector": str,
  "location_country": str,    # ISO3 or null
  "location_city": str,
  "suggested_tag": str,       # best-fit from existing tags, or proposed new one
  "confidence": float,        # 0-1
  "sources": [str, ...],      # e.g. ["Wikipedia", "MIT.edu"]
  "reasoning": str,           # one-sentence explanation
  "raw_search_results": {...} # full Serper response, for transparency
}
"""

import json
import os
import re
from pathlib import Path
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv

# ── Path setup ────────────────────────────────────────────────────────────────
_SERVICE_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SERVICE_DIR.parent.parent
_CACHE_FILE = _SERVICE_DIR / "enrichment_cache.json"

load_dotenv(_PROJECT_ROOT / ".env")

# ── Constants ─────────────────────────────────────────────────────────────────
SERPER_API_URL = "https://google.serper.dev/search"
LLM_MODEL = "claude-sonnet-4-5-20250929"
MAX_SNIPPET_CHARS = 400     # truncate each snippet to keep context concise
MAX_SNIPPETS = 4            # top N organic results to pass to LLM
MAX_EXISTING_TAGS = 80      # how many existing tags to show LLM for suggestion


# ─────────────────────────────────────────────────────────────────────────────
# Cache
# ─────────────────────────────────────────────────────────────────────────────

def _load_cache() -> Dict:
    if _CACHE_FILE.exists():
        try:
            with open(_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _save_cache(cache: Dict) -> None:
    try:
        with open(_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
    except OSError:
        pass  # cache failure is non-fatal


def _cache_key(canonical_name: str) -> str:
    return canonical_name.strip().lower()


# ─────────────────────────────────────────────────────────────────────────────
# Serper search
# ─────────────────────────────────────────────────────────────────────────────

def search_org(canonical_name: str, use_cache: bool = True) -> Dict:
    """
    Search for an organization using Serper API.
    Returns structured dict with knowledge_graph, snippets, and sources.
    Results are cached by canonical_name.
    """
    cache = _load_cache()
    key = _cache_key(canonical_name)

    if use_cache and key in cache:
        return cache[key]

    api_key = os.getenv("SERPER_API_KEY")
    if not api_key:
        raise ValueError("SERPER_API_KEY not found in environment. Check your .env file.")

    query = f'"{canonical_name}" organization'
    headers = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json",
    }
    payload = {
        "q": query,
        "num": 6,
        "gl": "us",
        "hl": "en",
    }

    try:
        response = requests.post(SERPER_API_URL, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        raw = response.json()
    except requests.RequestException as e:
        raise RuntimeError(f"Serper API request failed: {e}")

    # Extract structured results
    result = _parse_serper_response(canonical_name, raw)

    # Cache and return
    cache[key] = result
    _save_cache(cache)
    return result


def _parse_serper_response(canonical_name: str, raw: Dict) -> Dict:
    """Parse Serper API response into a clean structured dict."""
    result = {
        "canonical_name": canonical_name,
        "knowledge_graph": None,
        "snippets": [],
        "sources": [],
        "raw": raw,
    }

    # Knowledge Graph (most reliable for well-known orgs)
    kg = raw.get("knowledgeGraph") or {}
    if kg:
        result["knowledge_graph"] = {
            "title": kg.get("title", ""),
            "type": kg.get("type", ""),
            "description": kg.get("description", ""),
            "attributes": kg.get("attributes", {}),
            "website": kg.get("website", ""),
        }

    # Organic snippets (top N)
    organic = raw.get("organic", [])[:MAX_SNIPPETS]
    for item in organic:
        snippet_text = (item.get("snippet") or "")[:MAX_SNIPPET_CHARS]
        source_domain = _extract_domain(item.get("link", ""))
        result["snippets"].append({
            "title": item.get("title", ""),
            "snippet": snippet_text,
            "link": item.get("link", ""),
            "source": source_domain,
        })
        if source_domain and source_domain not in result["sources"]:
            result["sources"].append(source_domain)

    # Answer box (sometimes contains concise description)
    answer_box = raw.get("answerBox") or {}
    if answer_box.get("answer") or answer_box.get("snippet"):
        result["answer_box"] = {
            "answer": answer_box.get("answer", ""),
            "snippet": (answer_box.get("snippet") or "")[:MAX_SNIPPET_CHARS],
            "title": answer_box.get("title", ""),
        }

    return result


def _extract_domain(url: str) -> str:
    """Extract root domain from a URL."""
    if not url:
        return ""
    match = re.search(r"https?://(?:www\.)?([^/]+)", url)
    return match.group(1) if match else ""


# ─────────────────────────────────────────────────────────────────────────────
# LLM field extraction
# ─────────────────────────────────────────────────────────────────────────────

def _build_context(search_results: Dict) -> str:
    """Build a concise context string from search results for the LLM prompt."""
    parts = []

    kg = search_results.get("knowledge_graph")
    if kg and (kg.get("description") or kg.get("type")):
        parts.append("[Knowledge Graph]")
        if kg.get("title"):
            parts.append(f"Title: {kg['title']}")
        if kg.get("type"):
            parts.append(f"Type: {kg['type']}")
        if kg.get("description"):
            parts.append(f"Description: {kg['description']}")
        attrs = kg.get("attributes") or {}
        for k, v in list(attrs.items())[:6]:
            parts.append(f"{k}: {v}")
        parts.append("")

    ab = search_results.get("answer_box")
    if ab and (ab.get("answer") or ab.get("snippet")):
        parts.append("[Answer Box]")
        if ab.get("answer"):
            parts.append(ab["answer"])
        if ab.get("snippet"):
            parts.append(ab["snippet"])
        parts.append("")

    for i, s in enumerate(search_results.get("snippets", []), 1):
        parts.append(f"[Result {i} — {s.get('source', 'unknown')}]")
        if s.get("title"):
            parts.append(f"Title: {s['title']}")
        if s.get("snippet"):
            parts.append(s["snippet"])
        parts.append("")

    return "\n".join(parts).strip()


def _build_extraction_prompt(
    stub: Dict,
    context: str,
    existing_tags: List[str],
) -> str:
    """Build the LLM prompt for field extraction."""
    cname = stub.get("canonical_name", "")
    cur_meta = stub.get("meta_type", "other")
    cur_sector = stub.get("sector", "")

    # Sample of existing tags to help the LLM suggest a fitting one
    tag_sample = existing_tags[:MAX_EXISTING_TAGS]
    tags_str = "\n".join(f"  {t}" for t in tag_sample)

    return f"""You are enriching an organizational ontology entry.
Given web search results about an organization, extract structured metadata and return JSON only.

Organization name: "{cname}"
Current meta_type: {cur_meta}
Current sector: {cur_sector}

--- WEB SEARCH RESULTS ---
{context}
--- END RESULTS ---

Existing tag examples in our ontology (for reference when suggesting a tag):
{tags_str}

Return a JSON object with exactly these fields:
{{
  "canonical_name": "<full official name, corrected if needed>",
  "variations_found": ["<alias1>", "<alias2>"],
  "meta_type": "<one of: io, gov, university, ngo, private, other>",
  "sector": "<one of: intergovernmental, government, academia, ngo, private, other, research, media, finance>",
  "location_country": "<ISO 3-letter country code or null>",
  "location_city": "<city name or null>",
  "suggested_tag": "<hierarchical tag like 'ngo:research:poverty_economics' — fit into existing tag style if possible, else propose new>",
  "confidence": <float 0.0-1.0>,
  "sources": ["<domain1>", "<domain2>"],
  "reasoning": "<one sentence explaining the classification>"
}}

Rules:
- meta_type "io" = intergovernmental org (UN bodies, World Bank, NATO, EU, etc.)
- meta_type "gov" = national/subnational government body
- meta_type "university" = academic institution
- meta_type "ngo" = foundation, think tank, research institute, civil society
- meta_type "private" = for-profit company, bank, media
- meta_type "other" = award body, political party, unclear
- If search results are sparse or ambiguous, set confidence below 0.6
- Return ONLY valid JSON — no markdown fences, no explanation outside the JSON
"""


def extract_fields_with_llm(
    stub: Dict,
    search_results: Dict,
    existing_tags: List[str],
    api_key: Optional[str] = None,
) -> Dict:
    """
    Call Claude to extract structured fields from search results.
    Returns proposals dict, or a low-confidence fallback on failure.
    """
    api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not found. Check your .env file.")

    try:
        import anthropic
    except ImportError:
        raise ImportError("anthropic package required. pip install anthropic")

    context = _build_context(search_results)
    if not context:
        return _fallback_proposal(stub, reason="No search results available")

    prompt = _build_extraction_prompt(stub, context, existing_tags)

    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model=LLM_MODEL,
        max_tokens=800,
        temperature=0.0,
        messages=[{"role": "user", "content": prompt}],
    )

    raw_text = response.content[0].text.strip()

    # Strip markdown fences if present
    raw_text = re.sub(r"^```(?:json)?\s*", "", raw_text, flags=re.IGNORECASE)
    raw_text = re.sub(r"\s*```$", "", raw_text)

    try:
        proposals = json.loads(raw_text.strip())
    except json.JSONDecodeError:
        return _fallback_proposal(stub, reason="LLM returned unparseable JSON")

    # Attach raw search results for transparency
    proposals["raw_search_results"] = search_results
    proposals["enrichment_method"] = "serper+llm"

    return proposals


def _fallback_proposal(stub: Dict, reason: str = "") -> Dict:
    """Return a minimal proposal when enrichment fails."""
    return {
        "canonical_name": stub.get("canonical_name", ""),
        "variations_found": stub.get("variations_found", []),
        "meta_type": stub.get("meta_type", "other"),
        "sector": stub.get("sector", "other"),
        "location_country": stub.get("location_country"),
        "location_city": stub.get("location_city"),
        "suggested_tag": "",
        "confidence": 0.0,
        "sources": [],
        "reasoning": reason or "Enrichment failed — please fill manually.",
        "raw_search_results": None,
        "enrichment_method": "fallback",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main orchestrator
# ─────────────────────────────────────────────────────────────────────────────

def enrich_stub(stub: Dict, existing_tags: List[str], use_cache: bool = True) -> Dict:
    """
    Full enrichment pipeline for a single stub entry.

    Args:
        stub:          the ontology stub dict (canonical_name, meta_type, etc.)
        existing_tags: list of canonical_tags from the ontology (for tag suggestion)
        use_cache:     whether to use cached Serper results

    Returns:
        proposals dict with proposed field values, confidence, and sources
    """
    canonical_name = stub.get("canonical_name", "").strip()
    if not canonical_name:
        return _fallback_proposal(stub, reason="No canonical name to search for")

    # Step 1: Search
    try:
        search_results = search_org(canonical_name, use_cache=use_cache)
    except Exception as e:
        return _fallback_proposal(stub, reason=f"Search failed: {e}")

    # Step 2: LLM extraction
    try:
        proposals = extract_fields_with_llm(stub, search_results, existing_tags)
    except Exception as e:
        return _fallback_proposal(stub, reason=f"LLM extraction failed: {e}")

    return proposals


# ─────────────────────────────────────────────────────────────────────────────
# Confirmed-org autocomplete helper
# ─────────────────────────────────────────────────────────────────────────────

def get_confirmed_orgs(db) -> List[Dict]:
    """
    Return all confirmed (non-stub) ontology entries for the link-to-existing
    autocomplete widget. Includes both original ontology entries and approved stubs.
    """
    return [
        e for e in db.get_all()
        if e.get("status") == "completed"
        or e.get("source") not in ("auto_stub", None)
        and e.get("source") != "auto_stub"
    ]


def merge_stub_into_entry(
    stub_canonical_name: str,
    target_canonical_name: str,
    db,
) -> bool:
    """
    Merge a stub into an existing confirmed entry:
    - Adds stub's canonical_name to target's variations_found (if not already there)
    - Marks stub as status="merged", source="merged_into:<target_canonical_name>"
    - Does NOT delete the stub (preserves data integrity)

    Returns True on success.
    """
    stub = db.lookup_canonical(stub_canonical_name)
    target = db.lookup_canonical(target_canonical_name)

    if not stub or not target:
        return False

    # Add stub name as alias on target
    current_variations = target.get("variations_found", [])
    if stub_canonical_name not in current_variations:
        current_variations = current_variations + [stub_canonical_name]

    # Also carry over any existing variations from the stub that aren't on target
    for v in stub.get("variations_found", []):
        if v and v not in current_variations and v != target_canonical_name:
            current_variations.append(v)

    db.update_entry(target_canonical_name, {"variations_found": current_variations})

    # Mark stub as merged (not deleted)
    db.update_entry(stub_canonical_name, {
        "status": "merged",
        "source": f"merged_into:{target_canonical_name}",
    })

    return True
