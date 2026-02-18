"""
classifiers.py — Zero-cost keyword/pattern classification of raw organization name strings.

No API calls. No imports beyond stdlib. Must be fast — runs before any fuzzy/LLM matching.

Seven categories (in priority order):
  1. un_system           — UN bodies, specialized agencies, funds, programmes
  2. intergovernmental   — Non-UN IOs (World Bank, NATO, EU, IMF, OECD, etc.)
  3. national_government — Parliaments, ministries, presidency, central banks
  4. university          — Universities, colleges, polytechnics
  5. ngo                 — Foundations, think tanks, research institutes
  6. private             — Corporations, media companies, commercial banks
  7. other               — Award bodies, prizes, unclear
"""

import re
from typing import Dict, List, Optional

# ─────────────────────────────────────────────────────────────────────────────
# Keyword rules — tested in priority order (un_system first, other last)
# Each value is a list of lowercase substrings to match against the raw name.
# ─────────────────────────────────────────────────────────────────────────────

_UN_KEYWORDS = [
    "united nations", "un ", " un ", "(un)", "un-", "un:",
    "undp", "unicef", "unesco", "who ", "unhcr", "wfp", "unfpa",
    "ilo", "fao", "iaea", "imo ", "itu ", "wmo", "wipo", "ifad",
    "unep", "unctad", "unaids", "unops", "unido", "unwomen", "un women",
    "unodc", "ohchr", "ocha", "unrwa", "unhabitat", "habitat ",
    "secretary-general", "secretary general",
    "general assembly", "security council", "ecosoc",
    "economic and social council", "trusteeship council",
    "un secretariat", "office of the united nations",
    "world food programme", "world health organization",
    "international labour", "food and agriculture organization",
    "international atomic energy",
    "un high commissioner", "high commissioner for refugees",
    "international maritime organization",
    "international telecommunication union",
    "world meteorological organization",
    "world intellectual property",
    "international fund for agricultural",
    "un environment programme",
    "un conference on trade",
    "joint united nations programme",
]

_INTERGOVERNMENTAL_KEYWORDS = [
    "world bank", "international monetary fund", " imf", "imf ",
    "nato", "north atlantic treaty",
    "european union", " eu ", "(eu)", "council of the european",
    "african union", " au ", "african development bank",
    "asian development bank", "inter-american development bank",
    "islamic development bank",
    "oecd", "organisation for economic co-operation",
    "wto", "world trade organization",
    "g7 ", "g8 ", "g20 ", " g7", " g8", " g20",
    "commonwealth of nations", "british commonwealth",
    "organization of american states", " oas",
    "arab league", "league of arab states",
    "council of europe",
    "apec", "asean", "sco ", "brics",
    "international criminal court", " icc ",
    "international court of justice",
    "bank for international settlements",
    "international finance corporation",
    "multilateral investment guarantee",
    "international development association",
    "international bank for reconstruction",
    "european central bank",
    "european commission", "european parliament", "european council",
    "organization for security and co-operation",
    "organisation of islamic cooperation",
    "economic community of west african",
    "southern african development community",
    "association of southeast asian",
    "shanghai cooperation",
    "mercosur", "mercosul",
    "gulf cooperation council",
    "caribbean community", "caricom",
    "pacific islands forum",
    "intergovernmental panel on climate",
    " ipcc",
]

_NATIONAL_GOV_KEYWORDS = [
    "parliament", "parliamentary",
    "ministry", "minister of",
    "cabinet of", "state cabinet",
    "government of", "govt of",
    "presidency", "president of",
    "prime minister", "premier of",
    "chancellor of",
    "senate ", "congress ",
    "national assembly", "legislative assembly",
    "house of representatives", "house of commons", "house of lords",
    "department of ",
    "federal government", "federal ministry",
    "national government",
    "royal government",
    "imperial government",
    "ambassador", "embassy", "high commission",
    "consulate",
    "foreign affairs", "foreign ministry",
    "central bank of", "bank of england", "bank of japan",
    "bank of canada", "bank of australia", "bank of russia",
    "bank of china", "bank of india", "bank of mexico",
    "bank of korea", "banque de france", "bundesbank",
    "reserve bank", "national bank of",
    "supreme court of", "constitutional court",
    "armed forces", "military of",
    "department of defense", "ministry of defense", "ministry of defence",
    "national security",
    "state department",
    "whitehall",
    "10 downing street", "number 10",
    "élysée", "elysée",
    "kremlin",
    "capitol hill",
    "provincial government", "state government",
    "municipality", "city government", "city council",
    "nth parliament", "1st parliament", "2nd parliament", "3rd parliament",
    "4th parliament", "5th parliament", "6th parliament", "7th parliament",
    "8th parliament", "9th parliament",
]

# NOTE: "bank of " alone is in national_gov above but commercial banks
# are in private — priority order ensures national_gov wins.

_UNIVERSITY_KEYWORDS = [
    "university", "université", "universität", "universiteit",
    "universidad", "università", "universidade",
    "college of ", "college,", " college",
    "institute of technology",
    "school of business", "school of law", "school of medicine",
    "school of public", "school of economics",
    "faculty of",
    "polytechnic",
    "conservatory",
    "seminary",
    "graduate school",
    "business school",
    "law school",
    "medical school",
    "dental school",
    "engineering school",
    "madrasa", "madrasah",
    "ecole ", "école ",
    "hochschule",
    "fachhochschule",
]
# NOTE: "academy" alone is intentionally excluded — "Royal Swedish Academy of Sciences"
# is an IO, not a university. Only include "academy" with specific educational context.
_UNIVERSITY_KEYWORDS_STRICT = [
    "academy of fine arts",
    "national academy of",  # could be IO — low confidence, keep for now
    "military academy",
    "naval academy",
    "air force academy",
]

_NGO_KEYWORDS = [
    "foundation",
    "think tank",
    "institute for",
    "institute of international",
    "institute on",
    "council on ",
    "council for ",
    "center for", "centre for",
    "research institute",
    "research center", "research centre",
    "international committee",
    "international federation",
    "international alliance",
    "red cross", "red crescent",
    "amnesty international",
    "oxfam",
    "greenpeace",
    "médecins sans frontières", "doctors without borders",
    "human rights watch",
    "transparency international",
    "save the children",
    "world wildlife fund", "wwf",
    "care international",
    "action aid",
    "programme for ",
    "program for ",
    "alliance for ",
    "partnership for ",
    "global fund",
    "initiative for ",
    "campaign for ",
    "society for ",
    "association for ",
    "federation of ",
    "network of ",
    "coalition for ",
    "forum for ",
    "platform for ",
    "lab for ", " poverty action lab",
    "policy lab",
    "brookings", "rand corporation", "chatham house",
    "carnegie endowment", "wilson center",
    "peterson institute",
    "atlantic council",
    "council of foreign relations", "council on foreign relations",
    "international crisis group",
    "transparency", "accountability",
    "africa-america institute",
    "non-governmental", "ngo",
]

_PRIVATE_KEYWORDS = [
    " inc.", " inc,", " incorporated",
    " corp.", " corporation",
    " ltd.", " limited",
    " llc", " llp",
    " plc",
    " s.a.", " s.a,",
    " gmbh",
    " ag ",
    " n.v.",
    " p.l.c",
    "holdings",
    "group plc", "group inc", "group corp",
    " consulting", " consultancy",
    " advisory",
    "media group", "news group",
    "broadcasting corporation", "television network",
    "newspaper", "magazine", " press",
    "bank " ,  # generic "bank" NOT covered by national_gov rules above
    "financial services",
    "investment bank", "investment firm",
    "hedge fund", "private equity",
    "venture capital",
    "pharmaceutical", "pharmaceuticals",
    "oil company", "energy company",
    "telecommunications",
    "technology company", "tech company",
    "carlton", "reuters", "bloomberg",
    "mckinsey", "bain ", "bcg ",
    "deloitte", "pwc", "ernst & young", "kpmg",
    "goldman sachs", "morgan stanley", "jp morgan", "jpmorgan",
    "citibank", "citigroup", "barclays", "hsbc", "deutsche bank",
    "ubs ", "credit suisse",
]

# Strings that start with a private keyword but should NOT be classified as private
_PRIVATE_EXCLUSIONS = [
    "world bank",
    "central bank",
    "bank of england", "bank of japan", "bank of canada",
    "bank of australia", "bank of russia", "bank of china",
    "bank of india", "bank of mexico", "bank of korea",
    "reserve bank",
    "national bank",
    "international bank",
    "african development bank",
    "asian development bank",
    "inter-american development bank",
    "islamic development bank",
    "bank for international",
    "european central bank",
]

_OTHER_KEYWORDS: List[str] = []  # default bucket — no positive keywords needed

# ─────────────────────────────────────────────────────────────────────────────
# Structural patterns (regex)
# ─────────────────────────────────────────────────────────────────────────────

# "22nd Parliament of Turkey", "3rd National Assembly of..."
_ORDINAL_PARLIAMENT_RE = re.compile(
    r"\b\d+(st|nd|rd|th)\s+(parliament|national assembly|legislative assembly)\b",
    re.IGNORECASE,
)

# Names ending in "Prize", "Award", "Fellowship", "Medal" → other
_AWARD_SUFFIX_RE = re.compile(
    r"\b(prize|award|fellowship|medal|scholarship|grant)\s*$",
    re.IGNORECASE,
)

# Known award/prize givers: Nobel, Pulitzer, Guggenheim, Sloan, MacArthur, etc.
_AWARD_GIVER_RE = re.compile(
    r"\b(nobel|pulitzer|guggenheim|sloan|macarthur|wolf |turing|fields medal"
    r"|lasker|templeton|ramón cajal|shaw prize|tang prize)\b",
    re.IGNORECASE,
)


# ─────────────────────────────────────────────────────────────────────────────
# Public functions
# ─────────────────────────────────────────────────────────────────────────────

def classify_by_keywords(raw_name: str) -> Optional[str]:
    """
    Test raw_name against keyword lists in priority order.
    Returns the first matching category, or None if nothing matches.
    """
    name_lower = raw_name.lower()

    def _matches(keywords: List[str]) -> bool:
        return any(kw in name_lower for kw in keywords)

    if _matches(_UN_KEYWORDS):
        return "un_system"

    if _matches(_INTERGOVERNMENTAL_KEYWORDS):
        return "intergovernmental"

    if _matches(_NATIONAL_GOV_KEYWORDS):
        return "national_government"

    if _matches(_UNIVERSITY_KEYWORDS):
        return "university"

    if _matches(_NGO_KEYWORDS):
        return "ngo"

    # Private — apply exclusions first
    if _matches(_PRIVATE_KEYWORDS):
        if not _matches(_PRIVATE_EXCLUSIONS):
            return "private"

    return None


def classify_by_structure(raw_name: str) -> Optional[str]:
    """
    Pattern-based classification for cases keyword rules miss.
    """
    if _ORDINAL_PARLIAMENT_RE.search(raw_name):
        return "national_government"

    if _AWARD_SUFFIX_RE.search(raw_name):
        return "other"

    if _AWARD_GIVER_RE.search(raw_name):
        return "other"

    return None


def classify_org(raw_name: str) -> str:
    """
    Master classification function.
    Returns one of: un_system, intergovernmental, national_government,
                    university, ngo, private, other
    """
    if not raw_name or not raw_name.strip():
        return "other"

    result = classify_by_keywords(raw_name)
    if result:
        return result

    result = classify_by_structure(raw_name)
    if result:
        return result

    return "other"


def classify_batch(raw_names: List[str]) -> Dict[str, str]:
    """Classify a list of org names. Returns {raw_name -> category}."""
    return {name: classify_org(name) for name in raw_names}


# Category → meta_type mapping (used by matcher and run_matching)
CATEGORY_TO_META_TYPE: Dict[str, Optional[str]] = {
    "un_system": "io",
    "intergovernmental": "io",
    "national_government": "gov",
    "university": "university",
    "ngo": "ngo",
    "private": "private",
    "other": "other",
}

# Category → search_meta_type (the ontology subsets we actually have)
CATEGORY_TO_SEARCH_META_TYPE: Dict[str, Optional[str]] = {
    "un_system": "io",
    "intergovernmental": "io",
    "national_government": "gov",
    "university": "university",
    "ngo": None,      # search all
    "private": None,  # search all
    "other": None,    # search all
}

# Category → org_types array value (for stub creation)
CATEGORY_TO_ORG_TYPES: Dict[str, List[str]] = {
    "un_system": ["international_organization"],
    "intergovernmental": ["intergovernmental_organization"],
    "national_government": ["government"],
    "university": ["university"],
    "ngo": ["ngo"],
    "private": ["private_sector"],
    "other": ["other"],
}

# Category → sector string (for stub creation)
CATEGORY_TO_SECTOR: Dict[str, str] = {
    "un_system": "intergovernmental",
    "intergovernmental": "intergovernmental",
    "national_government": "government",
    "university": "academia",
    "ngo": "ngo",
    "private": "private",
    "other": "other",
}
