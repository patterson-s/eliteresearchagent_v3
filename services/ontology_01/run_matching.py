"""
run_matching.py — CLI entry point for the ontology matching pipeline.

Discovers all timeline files, runs OrgMatcher on each, writes sidecar files,
and creates stubs in the ontology for unmatched organizations.

Usage:
    python run_matching.py --all
    python run_matching.py --person "Abhijit Banerjee" --dry-run --verbose
    python run_matching.py --all --workers 4 --no-embed --threshold 85
"""

import argparse
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ── Path setup ────────────────────────────────────────────────────────────────
_SERVICE_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SERVICE_DIR.parent.parent
TIMELINE_DATA_DIR = _PROJECT_ROOT / "services" / "WikiPrompt" / "llm_timeline_data"

sys.path.insert(0, str(_SERVICE_DIR))

from classifiers import (
    CATEGORY_TO_ORG_TYPES,
    CATEGORY_TO_SECTOR,
    CATEGORY_TO_META_TYPE,
)
from matcher import OrgMatcher, MATCHING_CONFIG, MatchResult
from ontology_db import OntologyDB

# ─────────────────────────────────────────────────────────────────────────────
# File discovery
# ─────────────────────────────────────────────────────────────────────────────

def discover_timeline_files(base_dir: Path) -> List[Tuple[str, Path]]:
    """
    Find all *_career_events.json files under base_dir.
    Extracts person_name from the JSON content's 'person_name' field.
    Returns list of (person_name, file_path) tuples, sorted by person_name.
    """
    results = []
    for file_path in sorted(base_dir.rglob("*_career_events.json")):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            person_name = data.get("person_name") or file_path.stem
            results.append((person_name, file_path))
        except (json.JSONDecodeError, OSError) as e:
            logging.warning(f"Could not read {file_path}: {e}")

    results.sort(key=lambda x: x[0])
    return results


def load_career_events(file_path: Path) -> Dict:
    """Load and return a timeline JSON file."""
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if "career_events" not in data:
        raise ValueError(f"No 'career_events' key in {file_path}")
    return data


def collect_org_names(career_events: List[Dict]) -> List[str]:
    """
    Flatten all organizations from all career events.
    Deduplicates while preserving first-occurrence order.
    """
    seen = set()
    org_names = []
    for event in career_events:
        for org in event.get("organizations", []):
            org = org.strip()
            if org and org not in seen:
                seen.add(org)
                org_names.append(org)
    return org_names


# ─────────────────────────────────────────────────────────────────────────────
# Sidecar output
# ─────────────────────────────────────────────────────────────────────────────

def _result_to_org_link(result: MatchResult, stub_created: bool = False) -> Dict:
    """Convert a MatchResult to a sidecar org_link dict."""
    link = {
        "raw_name": result["raw_name"],
        "canonical_name": result.get("matched_canonical"),
        "match_method": result.get("match_method"),
        "match_confidence": result.get("match_confidence"),
        "ontology_tag": result.get("ontology_tag"),
        "meta_type": result.get("meta_type"),
        "matched": result["matched"],
        "needs_review": result.get("needs_review", False),
        "org_type_classified": result.get("org_type_classified", "other"),
        "stub_created": stub_created,
    }
    # Include proposed match info for review queue
    if result.get("needs_review"):
        link["proposed_match_canonical"] = result.get("proposed_match_canonical")
        link["proposed_match_confidence"] = result.get("proposed_match_confidence")
    return link


def build_sidecar(
    person_name: str,
    match_results: List[MatchResult],
    stub_flags: Dict[str, bool],
) -> Dict:
    """
    Build the sidecar output dict.

    Args:
        person_name: the person's name
        match_results: list of MatchResult dicts (one per unique org)
        stub_flags: {raw_name -> stub_created bool}
    """
    org_links = [
        _result_to_org_link(r, stub_created=stub_flags.get(r["raw_name"], False))
        for r in match_results
    ]
    return {
        "person_name": person_name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_orgs": len(org_links),
        "matched_count": sum(1 for r in match_results if r["matched"]),
        "review_needed_count": sum(1 for r in match_results if r.get("needs_review")),
        "stubs_created_count": sum(1 for v in stub_flags.values() if v),
        "org_links": org_links,
    }


def save_sidecar(sidecar: Dict, timeline_file_path: Path) -> Path:
    """
    Save sidecar JSON alongside the timeline file.
    Returns the path it was saved to.
    """
    person_name = sidecar["person_name"]
    # Sanitize for filename: replace spaces with underscores
    safe_name = person_name.replace(" ", "_")
    output_path = timeline_file_path.parent / f"{safe_name}_org_links.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(sidecar, f, indent=2, ensure_ascii=False)
    return output_path


# ─────────────────────────────────────────────────────────────────────────────
# Stub creation
# ─────────────────────────────────────────────────────────────────────────────

def build_stub(result: MatchResult) -> Dict:
    """Build a stub ontology entry for an unmatched organization."""
    org_type = result.get("org_type_classified", "other")
    return {
        "canonical_name": result["raw_name"],
        "org_types": CATEGORY_TO_ORG_TYPES.get(org_type, ["other"]),
        "variations_found": [],
        "meta_type": CATEGORY_TO_META_TYPE.get(org_type, "other"),
        "sector": CATEGORY_TO_SECTOR.get(org_type, "other"),
        "location_country": None,
        "location_city": None,
        "source": "auto_stub",
        "status": "pending_review",
        "un_ontology": {},
        "gov_ontology": {},
    }


def collect_stubs(
    all_results: List[Tuple[str, List[MatchResult]]],
    db: OntologyDB,
) -> Dict[str, bool]:
    """
    Identify orgs that need stubs (unmatched, not in review band, not already in ontology).
    Returns {raw_name -> True} for stubs created.
    Writes all stubs to the ontology in a single batch call.
    """
    stubs_to_create: List[Dict] = []
    stub_names: set = set()
    stub_created: Dict[str, bool] = {}

    for _person_name, results in all_results:
        for result in results:
            raw = result["raw_name"]
            if (
                not result["matched"]
                and not result.get("needs_review")
                and raw not in stub_names
                and db.lookup_canonical(raw) is None
            ):
                stubs_to_create.append(build_stub(result))
                stub_names.add(raw)
                stub_created[raw] = True

    if stubs_to_create:
        db.add_entries(stubs_to_create)
        logging.info(f"Created {len(stubs_to_create)} stub entries in ontology.")

    return stub_created


# ─────────────────────────────────────────────────────────────────────────────
# Per-person processing
# ─────────────────────────────────────────────────────────────────────────────

def process_person(
    person_name: str,
    file_path: Path,
    matcher: OrgMatcher,
    verbose: bool = False,
) -> Tuple[str, List[MatchResult], Optional[Exception]]:
    """
    Process one person's timeline file.
    Returns (person_name, match_results, error_or_None).
    """
    try:
        data = load_career_events(file_path)
        career_events = data.get("career_events", [])
        results = matcher.match_person(person_name, career_events)

        if verbose:
            matched = sum(1 for r in results if r["matched"])
            review = sum(1 for r in results if r.get("needs_review"))
            total = len(results)
            logging.info(
                f"  {person_name}: {total} orgs | "
                f"{matched} matched | {review} needs review | "
                f"{total - matched - review} unmatched"
            )

        return (person_name, results, None)

    except Exception as e:
        logging.error(f"  ERROR processing {person_name}: {e}")
        return (person_name, [], e)


# ─────────────────────────────────────────────────────────────────────────────
# Summary printing
# ─────────────────────────────────────────────────────────────────────────────

def print_summary(
    all_results: List[Tuple[str, List[MatchResult]]],
    stub_created: Dict[str, bool],
    errors: List[Tuple[str, Exception]],
) -> None:
    """Print a summary table of results."""
    total_orgs = 0
    total_matched = 0
    total_review = 0
    total_stubs = len(stub_created)

    print("\n" + "=" * 70)
    print(f"{'Person':<30} {'Orgs':>5} {'Matched':>8} {'Review':>8} {'Unmatched':>10}")
    print("-" * 70)

    for person_name, results in all_results:
        n = len(results)
        matched = sum(1 for r in results if r["matched"])
        review = sum(1 for r in results if r.get("needs_review"))
        unmatched = n - matched - review
        print(f"{person_name:<30} {n:>5} {matched:>8} {review:>8} {unmatched:>10}")
        total_orgs += n
        total_matched += matched
        total_review += review

    print("=" * 70)
    print(
        f"{'TOTAL':<30} {total_orgs:>5} {total_matched:>8} "
        f"{total_review:>8} {total_orgs - total_matched - total_review:>10}"
    )
    print(f"\nStubs created in ontology: {total_stubs}")
    if errors:
        print(f"\nErrors ({len(errors)}):")
        for name, err in errors:
            print(f"  {name}: {err}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Match organization names in timeline files to the unified ontology."
    )

    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--all", action="store_true", help="Process all timeline files")
    scope.add_argument("--person", metavar="NAME", help="Process one person by name")

    parser.add_argument(
        "--dry-run", action="store_true",
        help="Run matching but do not write sidecar files or stubs to ontology"
    )
    parser.add_argument(
        "--workers", type=int, default=4,
        help="Number of parallel worker threads (default: 4)"
    )
    parser.add_argument(
        "--no-embed", action="store_true",
        help="Disable embedding (Cohere) matching tier"
    )
    parser.add_argument(
        "--no-llm", action="store_true",
        help="Disable LLM (Claude) disambiguation tier"
    )
    parser.add_argument(
        "--threshold", type=int, default=None,
        help="Override fuzzy accept threshold (0-100, default: 88)"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Print per-person progress"
    )

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(levelname)s: %(message)s",
    )

    # Build config
    config = dict(MATCHING_CONFIG)
    if args.no_embed:
        config["use_embedding"] = False
    if args.no_llm:
        config["use_llm_match"] = False
        config["use_llm_classify"] = False
    if args.threshold is not None:
        config["fuzzy_threshold_accept"] = int(args.threshold)

    # Discover files
    all_files = discover_timeline_files(TIMELINE_DATA_DIR)
    if not all_files:
        logging.error(f"No timeline files found in {TIMELINE_DATA_DIR}")
        sys.exit(1)

    if args.person:
        name_lower = args.person.lower()
        all_files = [
            (n, p) for n, p in all_files
            if n.lower() == name_lower or name_lower in n.lower()
        ]
        if not all_files:
            logging.error(f"No timeline file found for person: {args.person}")
            logging.info("Available people:")
            for n, _ in discover_timeline_files(TIMELINE_DATA_DIR):
                logging.info(f"  {n}")
            sys.exit(1)

    logging.info(f"Processing {len(all_files)} person(s)...")

    # Initialize shared objects (OntologyDB is read-safe for concurrent access)
    db = OntologyDB()
    matcher = OrgMatcher(config=config, db=db)

    logging.info(f"Ontology loaded: {db.count()} entries")

    # Process files
    all_results: List[Tuple[str, List[MatchResult]]] = []
    errors: List[Tuple[str, Exception]] = []
    person_to_filepath: Dict[str, Path] = {n: p for n, p in all_files}

    workers = min(args.workers, len(all_files))

    if workers > 1 and len(all_files) > 1:
        futures_map = {}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for person_name, file_path in all_files:
                future = executor.submit(
                    process_person, person_name, file_path, matcher, args.verbose
                )
                futures_map[future] = person_name

            for future in as_completed(futures_map):
                person_name, results, err = future.result()
                all_results.append((person_name, results))
                if err:
                    errors.append((person_name, err))
    else:
        for person_name, file_path in all_files:
            person_name, results, err = process_person(
                person_name, file_path, matcher, args.verbose
            )
            all_results.append((person_name, results))
            if err:
                errors.append((person_name, err))

    all_results.sort(key=lambda x: x[0])

    # Create stubs (single-threaded, after all matching completes)
    stub_created: Dict[str, bool] = {}
    if not args.dry_run:
        stub_created = collect_stubs(all_results, db)
    else:
        logging.info("[dry-run] Skipping stub creation.")

    # Write sidecar files
    if not args.dry_run:
        for person_name, results in all_results:
            if not results:
                continue
            file_path = person_to_filepath.get(person_name)
            if file_path is None:
                continue
            sidecar = build_sidecar(person_name, results, stub_created)
            out_path = save_sidecar(sidecar, file_path)
            if args.verbose:
                logging.debug(f"  Saved sidecar: {out_path}")
        logging.info("Sidecar files written.")
    else:
        logging.info("[dry-run] Skipping sidecar file writes.")

    # Print summary
    print_summary(all_results, stub_created, errors)


if __name__ == "__main__":
    main()
