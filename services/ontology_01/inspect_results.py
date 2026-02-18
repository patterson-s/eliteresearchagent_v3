"""
inspect_results.py — Quick diagnostic: print per-org match details for one person.

Usage (from activated project env):
    python inspect_results.py --person "Abhijit Banerjee"
    python inspect_results.py --person "Amina J. Mohammed"
"""

import argparse
import json
import sys
from pathlib import Path

_SERVICE_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SERVICE_DIR.parent.parent
TIMELINE_DATA_DIR = _PROJECT_ROOT / "services" / "WikiPrompt" / "llm_timeline_data"

sys.path.insert(0, str(_SERVICE_DIR))

from matcher import OrgMatcher, MATCHING_CONFIG
from ontology_db import OntologyDB


def find_timeline_file(person_name: str) -> Path:
    name_lower = person_name.lower()
    for path in TIMELINE_DATA_DIR.rglob("*_career_events.json"):
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        pname = data.get("person_name", "")
        if pname.lower() == name_lower or name_lower in pname.lower():
            return path
    raise FileNotFoundError(f"No timeline file found for: {person_name}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--person", required=True)
    parser.add_argument("--no-embed", action="store_true")
    parser.add_argument("--no-llm", action="store_true")
    args = parser.parse_args()

    config = dict(MATCHING_CONFIG)
    if args.no_embed:
        config["use_embedding"] = False
    if args.no_llm:
        config["use_llm_match"] = False
        config["use_llm_classify"] = False

    db = OntologyDB()
    matcher = OrgMatcher(config=config, db=db)

    file_path = find_timeline_file(args.person)
    with open(file_path, encoding="utf-8") as f:
        data = json.load(f)

    person_name = data.get("person_name", args.person)
    results = matcher.match_person(person_name, data["career_events"])

    matched = [r for r in results if r["matched"]]
    review = [r for r in results if r.get("needs_review")]
    unmatched = [r for r in results if not r["matched"] and not r.get("needs_review")]

    print(f"\n{'='*70}")
    print(f"  {person_name}  —  {len(results)} unique orgs")
    print(f"{'='*70}")

    print(f"\n{'MATCHED':} ({len(matched)})")
    print(f"  {'Method':<22} {'Raw Name':<40} Canonical Match")
    print(f"  {'-'*22} {'-'*40} {'-'*30}")
    for r in matched:
        print(f"  [{r['match_method']:<20}] {r['raw_name']:<40} {r['matched_canonical']}")

    print(f"\nNEEDS REVIEW ({len(review)})  — score in [70, 88%)")
    print(f"  {'Conf':>5}  {'Type':<20} {'Raw Name':<40} Proposed Match")
    print(f"  {'-----':>5}  {'-'*20} {'-'*40} {'-'*30}")
    for r in review:
        conf = r.get("proposed_match_confidence") or 0
        org_type = r.get("org_type_classified", "?")
        proposed = r.get("proposed_match_canonical") or "(none)"
        print(f"  {conf:>5.0%}  {org_type:<20} {r['raw_name']:<40} {proposed}")

    print(f"\nUNMATCHED ({len(unmatched)})")
    print(f"  {'Type':<20} Raw Name")
    print(f"  {'-'*20} {'-'*40}")
    for r in unmatched:
        print(f"  {r.get('org_type_classified','?'):<20} {r['raw_name']}")

    print()


if __name__ == "__main__":
    main()
