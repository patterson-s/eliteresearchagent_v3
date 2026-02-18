"""
batch_enrich.py — CLI batch enrichment for ontology stubs.

Runs Serper search + Cohere Command-A extraction across all pending stubs
and prints proposals to the terminal. Does NOT write to the ontology unless
--write is explicitly passed.

Usage:
    # Dry-run: show what would be processed, no API calls
    python batch_enrich.py --dry-run

    # Test: enrich first 10 stubs, print results
    python batch_enrich.py --limit 10

    # Focus on high-value types only
    python batch_enrich.py --meta-type university --meta-type io

    # Full run, print only
    python batch_enrich.py --all

    # Full run + write proposals into ontology entries as 'batch_proposals' field
    python batch_enrich.py --all --write

Options:
    --dry-run           List stubs that would be processed, no API calls
    --limit N           Process at most N stubs
    --meta-type TYPE    Only process stubs of this meta_type (repeatable)
    --all               Process all pending stubs (overrides --limit default of 20)
    --delay SECS        Seconds to wait between calls (default: 0.3)
    --no-llm            Serper search only, skip Cohere extraction
    --write             Write batch_proposals field back into each stub in ontology
    --force-search      Bypass Serper cache (re-fetch all)
    --verbose           Show full proposals JSON for each stub
"""

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

# ── Path setup ────────────────────────────────────────────────────────────────
_SERVICE_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SERVICE_DIR.parent.parent

sys.path.insert(0, str(_SERVICE_DIR))

from dotenv import load_dotenv
load_dotenv(_PROJECT_ROOT / ".env")

from ontology_db import OntologyDB
from enrichment import enrich_stub, search_org, _fallback_proposal

# ── Priority order for meta_type processing ───────────────────────────────────
META_TYPE_PRIORITY = ["io", "university", "gov", "ngo", "private", "other"]


# ─────────────────────────────────────────────────────────────────────────────
# Formatting helpers
# ─────────────────────────────────────────────────────────────────────────────

def _conf_symbol(confidence: float) -> str:
    if confidence >= 0.80:
        return "🟢"
    elif confidence >= 0.55:
        return "🟡"
    else:
        return "🔴"


def _truncate(s: str, n: int = 40) -> str:
    if not s:
        return ""
    return s if len(s) <= n else s[: n - 1] + "…"


def _print_header() -> None:
    print()
    print(
        f"{'#':>4}  "
        f"{'Stub Name':<42}  "
        f"{'Type':^10}  "
        f"{'→ Proposed':^10}  "
        f"{'Conf':>5}  "
        f"{'Parent Org':<30}  "
        f"{'Suggested Tag'}"
    )
    print("-" * 130)


def _print_row(
    n: int,
    stub_name: str,
    orig_meta: str,
    proposals: Dict,
) -> None:
    conf = proposals.get("confidence", 0.0)
    prop_meta = proposals.get("meta_type", "?")
    parent = proposals.get("parent_org") or ""
    tag = proposals.get("suggested_tag") or ""
    method = proposals.get("enrichment_method", "")
    is_fallback = method == "fallback"

    sym = _conf_symbol(conf) if not is_fallback else "💀"
    reason = proposals.get("reasoning", "")

    print(
        f"{n:>4}  "
        f"{_truncate(stub_name, 42):<42}  "
        f"{orig_meta:^10}  "
        f"{prop_meta:^10}  "
        f"{sym} {conf:.0%}  "
        f"{_truncate(parent, 30):<30}  "
        f"{_truncate(tag, 50)}"
    )
    if is_fallback:
        print(f"       ↳ FAIL: {reason}")


def _print_verbose(proposals: Dict) -> None:
    clean = {k: v for k, v in proposals.items() if k != "raw_search_results"}
    print(json.dumps(clean, indent=2, ensure_ascii=False))
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Summary helpers
# ─────────────────────────────────────────────────────────────────────────────

def _print_summary(results: List[Dict]) -> None:
    total = len(results)
    if total == 0:
        return

    failures = [r for r in results if r["proposals"].get("enrichment_method") == "fallback"]
    high_conf = [r for r in results if r["proposals"].get("confidence", 0) >= 0.80]
    mid_conf  = [r for r in results if 0.55 <= r["proposals"].get("confidence", 0) < 0.80]
    low_conf  = [r for r in results if r["proposals"].get("confidence", 0) < 0.55
                 and r["proposals"].get("enrichment_method") != "fallback"]
    with_parent = [r for r in results if r["proposals"].get("parent_org")]
    type_changed = [
        r for r in results
        if r["proposals"].get("meta_type") != r["orig_meta"]
        and r["proposals"].get("enrichment_method") != "fallback"
    ]

    print()
    print("=" * 60)
    print(f"  BATCH ENRICHMENT SUMMARY  ({total} stubs processed)")
    print("=" * 60)
    print(f"  🟢 High confidence (≥80%):  {len(high_conf):>4}")
    print(f"  🟡 Mid  confidence (55-79%): {len(mid_conf):>4}")
    print(f"  🔴 Low  confidence (<55%):  {len(low_conf):>4}")
    print(f"  💀 Failed (fallback):        {len(failures):>4}")
    print(f"  🔗 Parent org proposed:      {len(with_parent):>4}")
    print(f"  🔀 Meta-type changed:        {len(type_changed):>4}")
    print("=" * 60)

    if type_changed:
        print()
        print("  Meta-type changes:")
        for r in type_changed:
            print(f"    {r['name']:<40} {r['orig_meta']} → {r['proposals']['meta_type']}")

    if with_parent:
        print()
        print("  Parent org proposals:")
        for r in with_parent:
            print(f"    {r['name']:<40} → {r['proposals']['parent_org']}")


# ─────────────────────────────────────────────────────────────────────────────
# Main batch logic
# ─────────────────────────────────────────────────────────────────────────────

def run_batch(
    stubs: List[Dict],
    existing_tags: List[str],
    limit: Optional[int],
    delay: float,
    no_llm: bool,
    write: bool,
    force_search: bool,
    verbose: bool,
    db: Optional[OntologyDB],
) -> List[Dict]:
    """
    Core batch loop. Returns list of result dicts.
    Each result: {"name": str, "orig_meta": str, "proposals": Dict}
    """
    to_process = stubs[:limit] if limit else stubs
    total = len(to_process)
    existing_tags_sample = existing_tags[:30]

    _print_header()
    results = []

    for n, stub in enumerate(to_process, 1):
        cname = stub.get("canonical_name", "")
        orig_meta = stub.get("meta_type", "other")

        if no_llm:
            # Search-only mode: just check if Serper finds anything
            try:
                sr = search_org(cname, use_cache=not force_search)
                snippets = len(sr.get("snippets", []))
                has_kg = bool(sr.get("knowledge_graph"))
                proposals = _fallback_proposal(
                    stub,
                    reason=f"search-only mode | {snippets} snippets | KG={'yes' if has_kg else 'no'}",
                )
                proposals["enrichment_method"] = "serper_only"
                proposals["confidence"] = 0.5 if snippets > 0 else 0.1
            except Exception as e:
                proposals = _fallback_proposal(stub, reason=f"Search failed: {e}")
        else:
            proposals = enrich_stub(
                stub,
                existing_tags_sample,
                use_cache=not force_search,
            )

        _print_row(n, cname, orig_meta, proposals)

        if verbose:
            _print_verbose(proposals)

        result = {"name": cname, "orig_meta": orig_meta, "proposals": proposals}
        results.append(result)

        # Optionally write batch_proposals back to ontology entry
        if write and db:
            clean_proposals = {
                k: v for k, v in proposals.items()
                if k != "raw_search_results"
            }
            db.update_entry(cname, {"batch_proposals": clean_proposals})

        # Progress indicator every 10
        if n % 10 == 0:
            print(f"\n  ... {n}/{total} processed ...\n")

        if n < total and delay > 0:
            time.sleep(delay)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Batch enrich ontology stubs via Serper + Cohere.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="List stubs that would be processed, no API calls.",
    )
    parser.add_argument(
        "--limit", type=int, default=20,
        help="Max stubs to process (default: 20). Ignored if --all is set.",
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Process all pending stubs (overrides --limit).",
    )
    parser.add_argument(
        "--meta-type", action="append", dest="meta_types", metavar="TYPE",
        help="Only process stubs of this meta_type. Repeatable.",
    )
    parser.add_argument(
        "--delay", type=float, default=0.3,
        help="Seconds between API calls (default: 0.3).",
    )
    parser.add_argument(
        "--no-llm", action="store_true",
        help="Serper search only — skip Cohere extraction.",
    )
    parser.add_argument(
        "--write", action="store_true",
        help="Write batch_proposals field back into each stub in the ontology.",
    )
    parser.add_argument(
        "--force-search", action="store_true",
        help="Bypass Serper cache and re-fetch all search results.",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Print full proposals JSON for each stub.",
    )

    args = parser.parse_args()

    # ── Load DB ───────────────────────────────────────────────────────────────
    ontology_path = _SERVICE_DIR / "unified_ontology.json"
    db = OntologyDB(ontology_path)

    # ── Collect pending stubs ─────────────────────────────────────────────────
    all_stubs = db.get_stubs()
    pending = [
        s for s in all_stubs
        if s.get("status") not in ("completed", "merged", "dismissed")
    ]

    # Filter by meta_type if requested
    if args.meta_types:
        pending = [s for s in pending if s.get("meta_type") in args.meta_types]
        priority = [t for t in META_TYPE_PRIORITY if t in args.meta_types]
    else:
        priority = META_TYPE_PRIORITY

    # Sort by meta_type priority
    priority_index = {t: i for i, t in enumerate(priority)}
    pending.sort(key=lambda s: priority_index.get(s.get("meta_type", "other"), 99))

    existing_tags = db.get_all_tags()
    limit = None if args.all else args.limit

    # ── Dry-run ───────────────────────────────────────────────────────────────
    if args.dry_run:
        display = pending[:limit] if limit else pending
        print(f"\nDRY RUN — would process {len(display)} of {len(pending)} pending stubs\n")
        from collections import Counter
        counts = Counter(s.get("meta_type", "?") for s in display)
        for mt in META_TYPE_PRIORITY:
            if mt in counts:
                print(f"  {mt:12} {counts[mt]:>4}")
        other_types = {k: v for k, v in counts.items() if k not in META_TYPE_PRIORITY}
        for mt, c in sorted(other_types.items()):
            print(f"  {mt:12} {c:>4}")
        print(f"\n  First 10 in queue:")
        for s in display[:10]:
            print(f"    [{s.get('meta_type','?'):10}]  {s.get('canonical_name','')}")
        print()
        return

    # ── Banner ────────────────────────────────────────────────────────────────
    display_count = len(pending[:limit]) if limit else len(pending)
    mode = "SEARCH-ONLY" if args.no_llm else "SERPER + COHERE"
    write_note = " | WRITING to ontology" if args.write else " | print-only (no writes)"
    print(f"\n[BATCH] Enrichment  [{mode}]{write_note}")
    print(f"   Stubs: {display_count} of {len(pending)} pending  |  delay: {args.delay}s")
    if args.meta_types:
        print(f"   Filter: meta_type in {args.meta_types}")
    print()

    # ── Run ───────────────────────────────────────────────────────────────────
    results = run_batch(
        stubs=pending,
        existing_tags=existing_tags,
        limit=limit,
        delay=args.delay,
        no_llm=args.no_llm,
        write=args.write,
        force_search=args.force_search,
        verbose=args.verbose,
        db=db if args.write else None,
    )

    _print_summary(results)


if __name__ == "__main__":
    main()
