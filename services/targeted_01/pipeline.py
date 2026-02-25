"""
pipeline.py — Full-pipeline parallel orchestrator for targeted_01.

Runs all 7 questions for one or more people. Within each person:
  - Phase 1: Q1–Q6 (all RAG questions) run simultaneously via ThreadPoolExecutor
  - Phase 2: Q7 (synthesis) runs after Phase 1 completes, reading Phase 1 outputs

For multiple people, persons are processed sequentially.

CLI usage:
    # Single person (spaced or underscore format both accepted)
    python pipeline.py --person "Abhijit Banerjee"
    python pipeline.py --person "Dhananjayan_Sriskandarajah"

    # Multiple persons
    python pipeline.py --person "Abhijit Banerjee" "Gro Harlem Brundtland"

    # All 58 people in data/
    python pipeline.py --all

    # From a text file (one name per line; blank lines and # comments ignored)
    python pipeline.py --people-file names.txt

    # Optional flags
    python pipeline.py --person "Abhijit Banerjee" --verbose
    python pipeline.py --all --output /custom/output/path
"""

import os
import sys
import time
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

# ── Bootstrap: resolve project root and load .env ─────────────────────────────
_SERVICE_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SERVICE_DIR.parent.parent

from dotenv import load_dotenv
load_dotenv(_PROJECT_ROOT / ".env")

# ── Import from runner.py ──────────────────────────────────────────────────────
sys.path.insert(0, str(_SERVICE_DIR))
from runner import (  # noqa: E402
    run_for_person,
    list_all_persons,
    load_question_config,
    db_name_to_dir_name,
    _DATA_DIR,
    _OUTPUT_DIR,
)

# ── Status abbreviations for summary table ─────────────────────────────────────
_STATUS_ABBREV = {
    "found_and_verified":         "FV ",
    "found_no_confirming_sources":"FN ",
    "found":                      "F  ",
    "cannot_determine":           "CD ",
    "no_chunks_retrieved":        "NCR",
    "skipped":                    "SK ",
    "error":                      "ER ",
    "exception":                  "EX ",
    "pending":                    "···",
}


# ── Question discovery ─────────────────────────────────────────────────────────

def discover_questions(
    prompts_dir: Path,
) -> Tuple[List[Path], List[Path]]:
    """
    Scan all subdirectories of prompts_dir and partition them into:
      - rag_dirs:       question dirs where mode is absent or != "synthesis"
      - synthesis_dirs: question dirs where mode == "synthesis"

    Both lists are sorted alphabetically by directory name so ordering is
    deterministic across runs.

    Args:
        prompts_dir: Path to services/targeted_01/prompts/.

    Returns:
        Tuple of (rag_dirs, synthesis_dirs), each a sorted list of Paths.
    """
    rag_dirs = []
    synthesis_dirs = []

    for subdir in sorted(prompts_dir.iterdir()):
        if not subdir.is_dir():
            continue
        config_path = subdir / "config.json"
        if not config_path.exists():
            continue
        try:
            cfg = load_question_config(subdir)
        except Exception:
            continue

        if cfg.get("mode") == "synthesis":
            synthesis_dirs.append(subdir)
        else:
            rag_dirs.append(subdir)

    return rag_dirs, synthesis_dirs


# ── Per-person pipeline ────────────────────────────────────────────────────────

def run_person_pipeline(
    person_dir_name: str,
    rag_dirs: List[Path],
    synthesis_dirs: List[Path],
    data_dir: Path,
    output_dir: Path,
    api_key: str,
    verbose: bool = False,
) -> Dict[str, str]:
    """
    Run the full 2-phase pipeline for a single person.

    Phase 1: All RAG questions (Q1–Q6) are submitted simultaneously to a
    ThreadPoolExecutor. Results are printed as each future completes.

    Phase 2: Synthesis questions (Q7) are run sequentially after Phase 1,
    so they can read Phase 1 output files.

    Verbose mode (--verbose) in pipeline.py does NOT pass verbose=True to
    individual run_for_person() calls (which would interleave chunk-level
    output from multiple threads). Instead, it shows timing and chunk-count
    information as a one-liner per question.

    Args:
        person_dir_name: Underscore-format person name.
        rag_dirs: List of prompt dirs for RAG questions (Q1–Q6).
        synthesis_dirs: List of prompt dirs for synthesis questions (Q7).
        data_dir: Path to services/targeted_01/data/.
        output_dir: Path to services/targeted_01/outputs/.
        api_key: Cohere API key.
        verbose: If True, show per-question timing and retrieval counts.

    Returns:
        Dict mapping question_id → status string for each question run.
    """
    results: Dict[str, str] = {}

    # ── Phase 1: RAG questions in parallel ────────────────────────────────────
    print(f"  Phase 1: running {len(rag_dirs)} questions in parallel...")

    with ThreadPoolExecutor(max_workers=len(rag_dirs)) as pool:
        future_to_dir = {
            pool.submit(
                run_for_person,
                person_dir_name,
                q_dir,
                data_dir,
                output_dir,
                api_key,
                False,          # verbose=False: suppress per-chunk output during parallel run
            ): q_dir
            for q_dir in rag_dirs
        }

        for future in as_completed(future_to_dir):
            q_dir = future_to_dir[future]
            try:
                result = future.result()
                q_id = result.get("question_id", q_dir.name)
                status = result.get("result", {}).get("status", "unknown")

                if verbose:
                    # Extra detail: retrieved chunks and phase-1 timing
                    retrieved = result.get("retrieval", {}).get("chunks_retrieved", "?")
                    scanned = result.get("meta", {}).get("chunks_scanned_extraction", "?")
                    print(f"    [{q_id:<25}]  {status:<35}  "
                          f"chunks: {retrieved} retrieved / {scanned} scanned")
                else:
                    print(f"    [{q_id:<25}]  {status}")

            except Exception as exc:
                q_id = q_dir.name
                status = "exception"
                print(f"    [{q_id:<25}]  EXCEPTION: {exc}")

            results[q_id] = status

    # ── Phase 2: Synthesis questions (sequential) ──────────────────────────────
    if synthesis_dirs:
        print(f"  Phase 2: running {len(synthesis_dirs)} synthesis question(s)...")

    for q_dir in synthesis_dirs:
        try:
            result = run_for_person(
                person_dir_name,
                q_dir,
                data_dir,
                output_dir,
                api_key,
                False,
            )
            q_id = result.get("question_id", q_dir.name)
            status = result.get("result", {}).get("status", "unknown")

            if verbose:
                domain = result.get("result", {}).get("dominant_domain", "")
                detail = f"  domain={domain}" if domain else ""
                print(f"    [{q_id:<25}]  {status}{detail}")
            else:
                print(f"    [{q_id:<25}]  {status}")

        except Exception as exc:
            q_id = q_dir.name
            status = "exception"
            print(f"    [{q_id:<25}]  EXCEPTION: {exc}")

        results[q_id] = status

    return results


# ── Summary table ──────────────────────────────────────────────────────────────

def print_summary(
    all_results: List[Tuple[str, Dict[str, str]]],
    question_order: List[str],
) -> None:
    """
    Print a compact summary table of all persons and their per-question statuses.

    Args:
        all_results: List of (person_dir_name, {question_id: status}) tuples.
        question_order: Ordered list of question_ids for column headers.
    """
    # Build abbreviated column headers (up to 5 chars each)
    col_headers = []
    for q_id in question_order:
        # Use last segment after underscore if the id is long
        short = q_id.split("_")[-1][:4] if "_" in q_id else q_id[:4]
        col_headers.append(short.upper())

    # Column widths
    name_w = max((len(p) for p, _ in all_results), default=20)
    name_w = max(name_w, 20)
    col_w = 5  # 3-4 char abbrev + 1 space gap

    sep = "-" * (name_w + 2 + col_w * len(question_order))

    print(f"\n{'='*60}")
    print("PIPELINE SUMMARY")
    print(sep)
    # Header row
    header = f"  {'Person':<{name_w}}  " + "".join(f"{h:<{col_w}}" for h in col_headers)
    print(header)
    print(sep)

    for person_dir_name, res in all_results:
        row_statuses = []
        for q_id in question_order:
            raw = res.get(q_id, "pending")
            abbrev = _STATUS_ABBREV.get(raw, raw[:3].upper())
            row_statuses.append(abbrev)
        row = f"  {person_dir_name:<{name_w}}  " + "".join(f"{s:<{col_w}}" for s in row_statuses)
        print(row)

    print(sep)
    print("Status: FV =found_and_verified  FN =found_no_confirming_sources  "
          "F  =found")
    print("        CD =cannot_determine   SK =skipped   "
          "ER =error   EX =exception")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    """
    CLI entry point for pipeline.py.

    Parses arguments, discovers question directories, builds the person list,
    then runs the full pipeline for each person sequentially.
    """
    parser = argparse.ArgumentParser(
        description="Full parallel pipeline: runs all 7 questions for one or more people.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pipeline.py --person "Abhijit Banerjee"
  python pipeline.py --person "Dhananjayan_Sriskandarajah" "Gro_Harlem_Brundtland"
  python pipeline.py --all
  python pipeline.py --people-file names.txt
  python pipeline.py --all --verbose
  python pipeline.py --person "Abhijit Banerjee" --output /path/to/outputs
        """,
    )
    parser.add_argument(
        "--person",
        nargs="+",
        metavar="NAME",
        help="One or more person names (spaced or underscore format)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process all people found in data/",
    )
    parser.add_argument(
        "--people-file",
        metavar="FILE",
        help="Path to a text file with one person name per line "
             "(blank lines and # comments are ignored)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show chunk-count detail per question as each finishes",
    )
    parser.add_argument(
        "--output",
        default=None,
        metavar="DIR",
        help="Override output directory (default: outputs/)",
    )
    args = parser.parse_args()

    # ── Resolve paths ──────────────────────────────────────────────────────────
    data_dir = _DATA_DIR
    output_dir = Path(args.output) if args.output else _OUTPUT_DIR
    prompts_dir = _SERVICE_DIR / "prompts"

    # ── API key ────────────────────────────────────────────────────────────────
    api_key = os.getenv("COHERE_API_KEY")
    if not api_key:
        print("ERROR: COHERE_API_KEY environment variable not set.")
        sys.exit(1)

    # ── Discover questions ─────────────────────────────────────────────────────
    rag_dirs, synthesis_dirs = discover_questions(prompts_dir)
    if not rag_dirs and not synthesis_dirs:
        print(f"ERROR: No question directories found under {prompts_dir}")
        sys.exit(1)

    all_question_dirs = rag_dirs + synthesis_dirs
    question_order = []
    for q_dir in all_question_dirs:
        try:
            cfg = load_question_config(q_dir)
            question_order.append(cfg.get("question_id", q_dir.name))
        except Exception:
            question_order.append(q_dir.name)

    print(f"Questions discovered: {len(rag_dirs)} RAG + {len(synthesis_dirs)} synthesis")

    # ── Build person list ──────────────────────────────────────────────────────
    persons: List[str] = []

    if args.all:
        persons = list_all_persons(data_dir)
    elif args.people_file:
        people_file = Path(args.people_file)
        if not people_file.exists():
            print(f"ERROR: --people-file not found: {people_file}")
            sys.exit(1)
        with open(people_file, encoding="utf-8") as f:
            for line in f:
                name = line.strip()
                if not name or name.startswith("#"):
                    continue
                persons.append(name.replace(" ", "_"))
    elif args.person:
        for name in args.person:
            persons.append(name.replace(" ", "_"))
    else:
        parser.print_help()
        print("\nError: specify --person NAME, --all, or --people-file FILE")
        sys.exit(1)

    if not persons:
        print("No persons to process.")
        sys.exit(0)

    # ── Run pipeline ───────────────────────────────────────────────────────────
    print(f"Processing {len(persons)} person(s)\n")
    all_results: List[Tuple[str, Dict[str, str]]] = []

    total_start = time.time()

    for i, person_dir_name in enumerate(persons, 1):
        person_db_name = person_dir_name.replace("_", " ")
        print(f"{'='*60}")
        print(f"[{i}/{len(persons)}]  {person_db_name}")

        person_start = time.time()

        try:
            result = run_person_pipeline(
                person_dir_name=person_dir_name,
                rag_dirs=rag_dirs,
                synthesis_dirs=synthesis_dirs,
                data_dir=data_dir,
                output_dir=output_dir,
                api_key=api_key,
                verbose=args.verbose,
            )
        except Exception as exc:
            print(f"  FATAL ERROR for {person_db_name}: {exc}")
            result = {q_id: "exception" for q_id in question_order}

        elapsed = time.time() - person_start
        print(f"  Done in {elapsed:.1f}s")

        all_results.append((person_dir_name, result))

    total_elapsed = time.time() - total_start

    # ── Summary table ──────────────────────────────────────────────────────────
    print_summary(all_results, question_order)
    print(f"\nTotal elapsed: {total_elapsed:.1f}s for {len(persons)} person(s)")


if __name__ == "__main__":
    main()
