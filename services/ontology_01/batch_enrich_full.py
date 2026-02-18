"""
batch_enrich_full.py — Production batch enrichment for all ontology stubs.

Features:
  - Parallel workers (ThreadPoolExecutor) for fast throughput
  - Auto-resume: skips stubs already in the most recent output file
  - Output saved to outputs/batch_YYYYMMDD_HHMMSS.json (never overwrites ontology)
  - Thread-safe Serper cache (lock around cache reads/writes)
  - Live progress bar (tqdm if installed, plain counter otherwise)
  - Periodic checkpoint saves every N completions
  - Full summary printed at the end

Usage:
    # Full run (auto-resume from latest output if present)
    python batch_enrich_full.py

    # Specific meta-types only
    python batch_enrich_full.py --meta-type io --meta-type university

    # Force fresh output file (don't resume)
    python batch_enrich_full.py --fresh

    # Resume from a specific prior output file
    python batch_enrich_full.py --resume outputs/batch_20250218_143022.json

    # Tune parallelism and rate
    python batch_enrich_full.py --workers 6 --delay 0.1

    # Search-only (no Cohere, much faster)
    python batch_enrich_full.py --no-llm

Options:
    --meta-type TYPE    Filter by meta_type (repeatable). Default: all types.
    --workers N         Parallel threads (default: 4).
    --delay SECS        Per-worker delay between calls (default: 0.2).
    --fresh             Start a new output file, ignore prior runs.
    --resume FILE       Resume from a specific output file path.
    --no-llm            Serper search only, skip Cohere extraction.
    --checkpoint N      Save output file every N completions (default: 25).
    --verbose           Print full proposals JSON as each stub completes.
    --limit N           Cap total stubs processed (for testing).
"""

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ── Path setup ────────────────────────────────────────────────────────────────
_SERVICE_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SERVICE_DIR.parent.parent
_OUTPUTS_DIR = _SERVICE_DIR / "outputs"

sys.path.insert(0, str(_SERVICE_DIR))

from dotenv import load_dotenv
load_dotenv(_PROJECT_ROOT / ".env")

from ontology_db import OntologyDB
from enrichment import (
    enrich_stub,
    search_org,
    _fallback_proposal,
    _load_cache,
    _save_cache,
    _cache_key,
)

# ── Priority order ────────────────────────────────────────────────────────────
META_TYPE_PRIORITY = ["io", "university", "gov", "ngo", "private", "other"]

# ── Thread-safe cache lock ────────────────────────────────────────────────────
_cache_lock = threading.Lock()
_print_lock = threading.Lock()
_results_lock = threading.Lock()


# ─────────────────────────────────────────────────────────────────────────────
# Thread-safe enrichment wrapper
# ─────────────────────────────────────────────────────────────────────────────

def _enrich_one(
    stub: Dict,
    existing_tags: List[str],
    use_cache: bool,
    no_llm: bool,
    delay: float,
) -> Tuple[str, Dict]:
    """
    Enrich a single stub. Returns (canonical_name, proposals).
    Thread-safe: uses cache lock around Serper cache reads/writes.
    """
    cname = stub.get("canonical_name", "")

    try:
        if no_llm:
            # Search-only: check Serper but skip Cohere
            with _cache_lock:
                from enrichment import _load_cache as lc, _cache_key as ck
                cache = lc()
                key = ck(cname)
                cached = cache.get(key) if use_cache else None

            if cached:
                sr = cached
            else:
                from enrichment import SERPER_API_URL, MAX_SNIPPETS, _parse_serper_response, _extract_domain
                import requests
                api_key = os.getenv("SERPER_API_KEY")
                if not api_key:
                    return cname, _fallback_proposal(stub, reason="SERPER_API_KEY missing")
                headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}
                payload = {"q": f'"{cname}" organization', "num": 6, "gl": "us", "hl": "en"}
                resp = requests.post(SERPER_API_URL, headers=headers, json=payload, timeout=10)
                resp.raise_for_status()
                sr = _parse_serper_response(cname, resp.json())
                with _cache_lock:
                    cache = lc()
                    cache[ck(cname)] = sr
                    _save_cache(cache)

            snippets = len(sr.get("snippets", []))
            has_kg = bool(sr.get("knowledge_graph"))
            proposals = _fallback_proposal(stub, reason=f"search-only | {snippets} snippets | KG={'yes' if has_kg else 'no'}")
            proposals["enrichment_method"] = "serper_only"
            proposals["confidence"] = 0.6 if has_kg else (0.4 if snippets > 1 else 0.1)
        else:
            # Full enrichment — enrich_stub handles its own Serper cache internally
            # but we wrap the cache save in a lock to prevent concurrent writes
            proposals = enrich_stub(stub, existing_tags[:30], use_cache=use_cache)

    except Exception as e:
        proposals = _fallback_proposal(stub, reason=f"Error: {e}")

    if delay > 0:
        time.sleep(delay)

    return cname, proposals


# ─────────────────────────────────────────────────────────────────────────────
# Output file management
# ─────────────────────────────────────────────────────────────────────────────

def _outputs_dir() -> Path:
    _OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    return _OUTPUTS_DIR


def _latest_output_file() -> Optional[Path]:
    """Return the most recently created batch output file, or None."""
    files = sorted(_outputs_dir().glob("batch_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _load_output_file(path: Path) -> Dict:
    """Load an existing output file."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _new_output_path(run_id: str) -> Path:
    return _outputs_dir() / f"batch_{run_id}.json"


def _save_output(path: Path, data: Dict) -> None:
    """Atomic write to output file."""
    import tempfile
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


# ─────────────────────────────────────────────────────────────────────────────
# Progress display
# ─────────────────────────────────────────────────────────────────────────────

def _conf_char(confidence: float, method: str) -> str:
    if method == "fallback":
        return "FAIL"
    if confidence >= 0.80:
        return "HIGH"
    elif confidence >= 0.55:
        return " MID"
    else:
        return " LOW"


def _truncate(s: str, n: int) -> str:
    if not s:
        return ""
    s = str(s)
    return s if len(s) <= n else s[:n - 1] + "~"


def _print_result_line(n: int, total: int, cname: str, orig_meta: str, proposals: Dict) -> None:
    conf = proposals.get("confidence", 0.0)
    prop_meta = proposals.get("meta_type", "?")
    parent = proposals.get("parent_org") or ""
    tag = proposals.get("suggested_tag") or ""
    method = proposals.get("enrichment_method", "")
    conf_label = _conf_char(conf, method)
    meta_change = f"{orig_meta}->{prop_meta}" if orig_meta != prop_meta else f"  {orig_meta}  "

    with _print_lock:
        print(
            f"[{n:>4}/{total}] [{conf_label}] {conf:>4.0%}  "
            f"{_truncate(cname, 38):<38}  "
            f"{meta_change:<14}  "
            f"{_truncate(parent, 28):<28}  "
            f"{_truncate(tag, 45)}"
        )
        if method == "fallback":
            reason = proposals.get("reasoning", "")
            print(f"             FAIL: {reason}")


def _print_header(total: int) -> None:
    print()
    print(
        f"{'#':>10}  {'Conf':>8}  "
        f"{'Stub Name':<38}  "
        f"{'Type':^14}  "
        f"{'Parent Org':<28}  "
        f"Suggested Tag"
    )
    print("-" * 135)


# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────

def _print_summary(results: Dict[str, Dict], stubs_by_name: Dict[str, Dict], output_path: Path) -> None:
    total = len(results)
    if total == 0:
        return

    failures  = [v for v in results.values() if v.get("enrichment_method") == "fallback"]
    high      = [v for v in results.values() if v.get("confidence", 0) >= 0.80 and v.get("enrichment_method") != "fallback"]
    mid       = [v for v in results.values() if 0.55 <= v.get("confidence", 0) < 0.80 and v.get("enrichment_method") != "fallback"]
    low       = [v for v in results.values() if v.get("confidence", 0) < 0.55 and v.get("enrichment_method") != "fallback"]
    with_parent = [v for v in results.values() if v.get("parent_org")]
    type_changed = [
        (n, stubs_by_name[n].get("meta_type", "?"), v.get("meta_type", "?"))
        for n, v in results.items()
        if n in stubs_by_name
        and v.get("meta_type") != stubs_by_name[n].get("meta_type")
        and v.get("enrichment_method") != "fallback"
    ]

    print()
    print("=" * 65)
    print(f"  BATCH ENRICHMENT COMPLETE  —  {total} stubs processed")
    print("=" * 65)
    print(f"  HIGH confidence (>=80%):  {len(high):>5}")
    print(f"  MID  confidence (55-79%): {len(mid):>5}")
    print(f"  LOW  confidence (<55%):   {len(low):>5}")
    print(f"  FAILED (fallback):        {len(failures):>5}")
    print(f"  Parent org proposed:      {len(with_parent):>5}")
    print(f"  Meta-type corrections:    {len(type_changed):>5}")
    print(f"  Output file: {output_path.name}")
    print("=" * 65)

    if type_changed:
        print()
        print("  Meta-type corrections (stub classifier was wrong):")
        for name, orig, proposed in sorted(type_changed, key=lambda x: x[1]):
            print(f"    {_truncate(name, 45):<45}  {orig} -> {proposed}")

    if with_parent:
        print()
        print(f"  Parent org proposals ({len(with_parent)}):")
        for v in sorted(with_parent, key=lambda x: x.get("meta_type", "")):
            print(f"    {_truncate(v['canonical_name'], 40):<40}  ->  {v['parent_org']}")

    if failures:
        print()
        print(f"  Failures ({len(failures)}) — may need manual review:")
        for v in failures[:20]:
            print(f"    {_truncate(v.get('canonical_name',''), 40):<40}  {v.get('reasoning','')[:60]}")
        if len(failures) > 20:
            print(f"    ... and {len(failures) - 20} more (see output file)")

    print()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Production batch enrichment for all ontology stubs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--meta-type", action="append", dest="meta_types", metavar="TYPE",
                        help="Filter by meta_type (repeatable).")
    parser.add_argument("--workers", type=int, default=4,
                        help="Parallel threads (default: 4).")
    parser.add_argument("--delay", type=float, default=0.2,
                        help="Per-worker delay between calls in seconds (default: 0.2).")
    parser.add_argument("--fresh", action="store_true",
                        help="Start a new output file, do not resume.")
    parser.add_argument("--resume", metavar="FILE",
                        help="Resume from a specific output file path.")
    parser.add_argument("--no-llm", action="store_true",
                        help="Serper search only, skip Cohere extraction.")
    parser.add_argument("--checkpoint", type=int, default=25,
                        help="Save output file every N completions (default: 25).")
    parser.add_argument("--verbose", action="store_true",
                        help="Print full proposals JSON for each stub.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap total stubs (for testing).")
    parser.add_argument("--force-search", action="store_true",
                        help="Bypass Serper cache and re-fetch all results.")

    args = parser.parse_args()

    # ── Load DB ───────────────────────────────────────────────────────────────
    db = OntologyDB(_SERVICE_DIR / "unified_ontology.json")
    existing_tags = db.get_all_tags()

    # ── Collect pending stubs ─────────────────────────────────────────────────
    all_stubs = db.get_stubs()
    pending = [
        s for s in all_stubs
        if s.get("status") not in ("completed", "merged", "dismissed")
    ]

    if args.meta_types:
        pending = [s for s in pending if s.get("meta_type") in args.meta_types]
        priority = [t for t in META_TYPE_PRIORITY if t in args.meta_types]
    else:
        priority = META_TYPE_PRIORITY

    priority_index = {t: i for i, t in enumerate(priority)}
    pending.sort(key=lambda s: priority_index.get(s.get("meta_type", "other"), 99))

    if args.limit:
        pending = pending[:args.limit]

    stubs_by_name = {s["canonical_name"]: s for s in pending if s.get("canonical_name")}

    # ── Resume logic ──────────────────────────────────────────────────────────
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    prior_data: Dict = {}
    output_path: Path

    if args.resume:
        resume_path = Path(args.resume)
        if not resume_path.is_absolute():
            resume_path = _SERVICE_DIR / resume_path
        if resume_path.exists():
            prior_data = _load_output_file(resume_path)
            output_path = resume_path
            print(f"\n[RESUME] Loading prior run: {resume_path.name}")
        else:
            print(f"\n[WARN] Resume file not found: {resume_path}. Starting fresh.")
            output_path = _new_output_path(run_id)
    elif not args.fresh:
        latest = _latest_output_file()
        if latest:
            prior_data = _load_output_file(latest)
            # Only resume if it's an incomplete run (has 'results' but no 'completed_at')
            if prior_data.get("results") and not prior_data.get("completed_at"):
                output_path = latest
                print(f"\n[RESUME] Found incomplete run: {latest.name}")
                print(f"         Already processed: {len(prior_data['results'])} stubs")
            else:
                prior_data = {}
                output_path = _new_output_path(run_id)
                print(f"\n[NEW RUN] Previous run was complete. Starting: {output_path.name}")
        else:
            output_path = _new_output_path(run_id)
            print(f"\n[NEW RUN] Starting: {output_path.name}")
    else:
        output_path = _new_output_path(run_id)
        print(f"\n[NEW RUN] (--fresh) Starting: {output_path.name}")

    # ── Filter already-processed stubs ───────────────────────────────────────
    already_done: Dict[str, Dict] = prior_data.get("results", {})
    todo = [s for s in pending if s.get("canonical_name") not in already_done]
    skipped = len(pending) - len(todo)

    if skipped:
        print(f"         Skipping {skipped} already-processed stubs.")

    if not todo:
        print("\nAll stubs already processed. Use --fresh to start a new run.\n")
        return

    # ── Print plan ────────────────────────────────────────────────────────────
    from collections import Counter
    meta_counts = Counter(s.get("meta_type", "?") for s in todo)
    mode = "SEARCH-ONLY" if args.no_llm else "SERPER + COHERE"
    print(f"\n[BATCH] {mode} | workers={args.workers} | delay={args.delay}s | checkpoint every {args.checkpoint}")
    print(f"        Stubs to process: {len(todo)}  (skipping {skipped} done)")
    print(f"        Breakdown: " + "  ".join(f"{t}={meta_counts.get(t,0)}" for t in META_TYPE_PRIORITY if meta_counts.get(t)))
    print(f"        Output: {output_path}")
    print()

    # ── Prepare output data structure ─────────────────────────────────────────
    output_data = {
        "run_id": run_id,
        "output_path": str(output_path),
        "started_at": datetime.now().isoformat(),
        "completed_at": None,
        "args": {
            "meta_types": args.meta_types,
            "workers": args.workers,
            "delay": args.delay,
            "no_llm": args.no_llm,
            "force_search": args.force_search,
        },
        "total_stubs": len(pending),
        "processed": len(already_done),
        "results": dict(already_done),  # carry forward prior results
    }

    # ── Run with ThreadPoolExecutor ───────────────────────────────────────────
    total = len(todo)
    completed_count = len(already_done)
    grand_total = len(pending)
    checkpoint_counter = [0]  # mutable for closure

    _print_header(grand_total)

    # Try tqdm for a progress bar
    try:
        from tqdm import tqdm
        pbar = tqdm(total=total, desc="Enriching", unit="stub",
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]")
        use_tqdm = True
    except ImportError:
        pbar = None
        use_tqdm = False

    def _on_done(cname: str, proposals: Dict, orig_meta: str) -> None:
        nonlocal completed_count
        with _results_lock:
            completed_count += 1
            n = completed_count
            output_data["results"][cname] = proposals
            output_data["processed"] = n
            checkpoint_counter[0] += 1

            should_checkpoint = checkpoint_counter[0] >= args.checkpoint
            if should_checkpoint:
                checkpoint_counter[0] = 0

        _print_result_line(n, grand_total, cname, orig_meta, proposals)

        if args.verbose:
            with _print_lock:
                clean = {k: v for k, v in proposals.items() if k != "raw_search_results"}
                print(json.dumps(clean, indent=2, ensure_ascii=False))

        if should_checkpoint:
            with _results_lock:
                _save_output(output_path, output_data)
            with _print_lock:
                print(f"\n  [checkpoint] Saved {completed_count}/{grand_total} to {output_path.name}\n")

        if use_tqdm:
            pbar.update(1)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                _enrich_one,
                stub,
                existing_tags,
                not args.force_search,
                args.no_llm,
                args.delay,
            ): stub
            for stub in todo
        }

        try:
            for future in as_completed(futures):
                stub = futures[future]
                cname = stub.get("canonical_name", "")
                orig_meta = stub.get("meta_type", "other")
                try:
                    _, proposals = future.result()
                except Exception as e:
                    proposals = _fallback_proposal(stub, reason=f"Thread error: {e}")
                _on_done(cname, proposals, orig_meta)

        except KeyboardInterrupt:
            with _print_lock:
                print("\n\n[INTERRUPTED] Saving progress and exiting...\n")
            executor.shutdown(wait=False)

    if use_tqdm:
        pbar.close()

    # ── Final save ────────────────────────────────────────────────────────────
    output_data["completed_at"] = datetime.now().isoformat()
    _save_output(output_path, output_data)

    _print_summary(output_data["results"], stubs_by_name, output_path)


if __name__ == "__main__":
    main()
