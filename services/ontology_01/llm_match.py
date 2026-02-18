"""
llm_match.py — Claude API as last-resort disambiguator for organization matching.

Only called for genuinely ambiguous cases where:
  - Fuzzy score falls in the "review band" (below accept threshold)
  - Embedding matching also fails or is unavailable
  - Top-N candidates exist but none scored highly enough to auto-accept

Also provides llm_classify_org() for cases where keyword classification fails.
"""

import json
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv

# Load .env from project root
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

LLM_MATCH_MODEL = "claude-sonnet-4-5-20250929"
MAX_CANDIDATES = 5

ORG_CATEGORIES = [
    "un_system",
    "intergovernmental",
    "national_government",
    "university",
    "ngo",
    "private",
    "other",
]


def is_available(api_key: Optional[str] = None) -> bool:
    """Return True if Anthropic API key is set and anthropic package is importable."""
    key = api_key or os.getenv("ANTHROPIC_API_KEY")
    if not key:
        return False
    try:
        import anthropic  # noqa: F401
        return True
    except ImportError:
        return False


def _get_client(api_key: Optional[str] = None):
    """Create and return an Anthropic client."""
    key = api_key or os.getenv("ANTHROPIC_API_KEY")
    if not key:
        raise ValueError(
            "ANTHROPIC_API_KEY is not set. "
            "Set it in your .env file or pass api_key explicitly."
        )
    try:
        import anthropic
    except ImportError:
        raise ImportError(
            "anthropic package is required for LLM matching. "
            "Install it with: pip install anthropic"
        )
    return anthropic.Anthropic(api_key=key)


def _parse_json_response(text: str) -> Dict:
    """
    Strip markdown code fences and parse JSON from LLM response.
    Mirrors the pattern used in extract_timeline_with_llm.py.
    """
    # Remove ```json ... ``` or ``` ... ``` wrappers
    text = re.sub(r"^```(?:json)?\s*", "", text.strip(), flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text.strip())
    return json.loads(text.strip())


# ─────────────────────────────────────────────────────────────────────────────
# Organization disambiguation
# ─────────────────────────────────────────────────────────────────────────────

def build_disambiguation_prompt(
    raw_name: str,
    candidates: List[Dict],
    context: Optional[str] = None,
) -> str:
    """
    Build a concise disambiguation prompt.

    Args:
        raw_name: the raw org string to match
        candidates: up to MAX_CANDIDATES ontology entries to choose from
        context: optional context string (e.g., "This org appears in Amina Mohammed's career")
    """
    candidates = candidates[:MAX_CANDIDATES]

    lines = [
        "You are matching a raw organization name to a curated ontology.",
        "",
        f'Raw organization name: "{raw_name}"',
    ]

    if context:
        lines.append(f"Context: {context}")

    lines += [
        "",
        "Candidate ontology entries (numbered from 0):",
    ]

    for i, entry in enumerate(candidates):
        cname = entry.get("canonical_name", "")
        meta = entry.get("meta_type", "")
        sector = entry.get("sector", "")
        variations = entry.get("variations_found", [])[:3]
        var_str = ", ".join(f'"{v}"' for v in variations) if variations else "none"
        lines.append(
            f"  {i}. {cname} | type: {meta}/{sector} | aliases: {var_str}"
        )

    lines += [
        "",
        "Instructions:",
        '- Return JSON only: {"best_match_index": <int or null>, "confidence": <float 0-1>, "reasoning": "<brief>"}',
        '- Set best_match_index to null if none of the candidates match the raw name.',
        "- confidence: 1.0 = certain match, 0.5 = plausible, 0.0 = no match.",
        "- Do not explain outside the JSON.",
    ]

    return "\n".join(lines)


def llm_disambiguate(
    raw_name: str,
    candidates: List[Dict],
    api_key: Optional[str] = None,
    context: Optional[str] = None,
) -> Optional[Tuple[Dict, float]]:
    """
    Ask Claude to select the best ontology match for raw_name from candidates.

    Args:
        raw_name: the raw org string
        candidates: list of ontology entry dicts (up to MAX_CANDIDATES)
        api_key: Anthropic API key (loaded from .env if None)
        context: optional context about the person/career event

    Returns:
        (matched_entry, confidence_score) or None if no match found or on error
    """
    if not candidates:
        return None

    if not is_available(api_key):
        return None

    try:
        client = _get_client(api_key)
        prompt = build_disambiguation_prompt(raw_name, candidates, context)

        response = client.messages.create(
            model=LLM_MATCH_MODEL,
            max_tokens=500,
            temperature=0.0,
            messages=[{"role": "user", "content": prompt}],
        )

        raw_text = response.content[0].text
        result = _parse_json_response(raw_text)

        best_idx = result.get("best_match_index")
        confidence = float(result.get("confidence", 0.0))

        if best_idx is None or not isinstance(best_idx, int):
            return None

        if best_idx < 0 or best_idx >= len(candidates):
            return None

        if confidence < 0.4:  # below 40% confidence → treat as no match
            return None

        return (candidates[best_idx], confidence)

    except (json.JSONDecodeError, KeyError, IndexError, ValueError):
        # LLM returned unparseable output — treat as no match
        return None
    except Exception:
        # API error, network issue, etc. — fail gracefully
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Organization type classification via LLM
# ─────────────────────────────────────────────────────────────────────────────

def llm_classify_org(
    raw_name: str,
    api_key: Optional[str] = None,
) -> Optional[str]:
    """
    Ask Claude to classify an org name into one of the seven categories.
    Only called when keyword classification returns "other" AND
    all matching stages fail — used as a last-resort classification hint.

    Returns one of the seven category strings, or None on failure.
    """
    if not is_available(api_key):
        return None

    categories_str = ", ".join(ORG_CATEGORIES)
    prompt = (
        f'Classify this organization name into exactly one of these categories:\n'
        f'{categories_str}\n\n'
        f'Organization: "{raw_name}"\n\n'
        f'Categories defined:\n'
        f'  un_system: UN bodies, specialized agencies, funds, programmes\n'
        f'  intergovernmental: Non-UN IOs (World Bank, NATO, EU, OECD, IMF)\n'
        f'  national_government: Parliaments, ministries, presidency, central banks\n'
        f'  university: Universities, colleges, polytechnics\n'
        f'  ngo: Foundations, think tanks, research institutes, NGOs\n'
        f'  private: Corporations, media, commercial banks, consultancies\n'
        f'  other: Award bodies, prizes, unclear\n\n'
        f'Return JSON only: {{"category": "<category>", "reasoning": "<one sentence>"}}'
    )

    try:
        client = _get_client(api_key)
        response = client.messages.create(
            model=LLM_MATCH_MODEL,
            max_tokens=200,
            temperature=0.0,
            messages=[{"role": "user", "content": prompt}],
        )

        raw_text = response.content[0].text
        result = _parse_json_response(raw_text)

        category = result.get("category", "").strip().lower()
        if category in ORG_CATEGORIES:
            return category

        return None

    except Exception:
        return None
