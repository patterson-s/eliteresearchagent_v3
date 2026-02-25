"""
enrich_bio.py — Populate biographical fields in _base.json files for the 17 people
who were missing birth_year, death_status, death_year, and nationality.

Reads existing output from the biographical service (services/biographical/review/)
and writes the results into services/targeted_01/data/{person}/{person}_base.json.

Priority for each person:
  1. {NAME_PREFIX}_bio.json  — consolidated 5-field bio file (if present)
  2. Individual question JSONs — {NAME_PREFIX}_{question}_*.json (fallback)

No new LLM/pipeline calls are made. All data already exists.

Usage:
    C:/Users/spatt/anaconda3/envs/eliteresearchagent_v3/python.exe enrich_bio.py
"""

import glob
import json
import os
import sys
from pathlib import Path

# Force UTF-8 output so Unicode names print cleanly on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR   = Path("C:/Users/spatt/Desktop/eliteresearchagent_v3")
REVIEW_DIR = BASE_DIR / "services" / "biographical" / "review"
DATA_DIR   = BASE_DIR / "services" / "targeted_01" / "data"

# ---------------------------------------------------------------------------
# The 17 people: (db_name, folder_name)
# folder_name must match the exact directory name under DATA_DIR
# ---------------------------------------------------------------------------
PEOPLE = [
    ("Mary Chinery-Hesse",       "Mary_Chinery-Hesse"),
    ("Yuichiro Anzai",           "Yuichiro_Anzai"),
    ("Sophie Soowon Eom",        "Sophie_Soowon_Eom"),
    ("V Isabel Guerrero Pulgar", "V_Isabel_Guerrero_Pulgar"),
    ("Marina Kolesnik",          "Marina_Kolesnik"),
    ("Akaliza Keza Ntwari",      "Akaliza_Keza_Ntwari"),
    ("Edson Prestes",            "Edson_Prestes"),
    ("Jovan Kurbalija",          "Jovan_Kurbalija"),
    ("Gisela Alonso",            "Gisela_Alonso"),
    ("Jean-Michel Severino",     "Jean-Michel_Severino"),
    ("Ngozi Okonjo-Iweala",      "Ngozi_Okonjo-Iweala"),
    ("Graça Machel",             "Graça_Machel"),
    ("Sung-Hwan Kim",            "Sung-Hwan_Kim"),
    ("Mohamed T. El-Ashry",      "Mohamed_T._El-Ashry"),
    ("Ruth Jacoby",              "Ruth_Jacoby"),
    ("Lennart Båge",             "Lennart_Båge"),
    ("Kemal Derviş",             "Kemal_Derviş"),
]

QUESTIONS = ["birth_year", "death_status", "death_year", "nationality"]


# ---------------------------------------------------------------------------
# Bio loading helpers
# ---------------------------------------------------------------------------

def load_bio_from_bio_json(name_prefix: str) -> dict | None:
    """Try to load {name_prefix}_bio.json from the review directory."""
    path = REVIEW_DIR / f"{name_prefix}_bio.json"
    if path.exists():
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return {
            "birth_year":   data.get("birth_year"),
            "death_status": data.get("death_status") or "alive",
            "death_year":   data.get("death_year"),
            "nationality":  data.get("nationality"),
            "source":       f"{name_prefix}_bio.json",
        }
    return None


def load_bio_from_result_jsons(name_prefix: str) -> dict:
    """
    Synthesize bio from individual question result JSONs.
    For each question, takes the most recent matching file and reads result.verified_answer.
    """
    bio = {
        "birth_year":   None,
        "death_status": "alive",  # default
        "death_year":   None,
        "nationality":  None,
        "source":       "result_jsons",
    }

    for question in QUESTIONS:
        pattern = str(REVIEW_DIR / f"{name_prefix}_{question}_*.json")
        matches = sorted(glob.glob(pattern))
        if not matches:
            continue

        latest = matches[-1]  # sorted by timestamp string — last = most recent
        try:
            with open(latest, encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"    [WARN] Could not read {latest}: {e}")
            continue

        verified_answer = data.get("result", {}).get("verified_answer")
        if verified_answer is None:
            continue

        if question == "birth_year":
            bio["birth_year"] = str(verified_answer)

        elif question == "death_status":
            val = str(verified_answer).lower().strip()
            if val in ("alive", "deceased", "unknown"):
                bio["death_status"] = val
            else:
                bio["death_status"] = "alive"

        elif question == "death_year":
            bio["death_year"] = str(verified_answer)

        elif question == "nationality":
            # verified_answer may be a list already, or a JSON string, or a plain string
            if isinstance(verified_answer, list):
                bio["nationality"] = verified_answer
            else:
                try:
                    parsed = json.loads(verified_answer)
                    bio["nationality"] = parsed if isinstance(parsed, list) else [str(parsed)]
                except (json.JSONDecodeError, TypeError):
                    bio["nationality"] = [str(verified_answer)]

    return bio


def load_bio(name_prefix: str) -> dict:
    """Priority: _bio.json first, then individual result JSONs."""
    bio = load_bio_from_bio_json(name_prefix)
    if bio is not None:
        return bio
    return load_bio_from_result_jsons(name_prefix)


# ---------------------------------------------------------------------------
# base.json updater
# ---------------------------------------------------------------------------

def update_base_json(folder_name: str, bio: dict) -> dict:
    """Read, patch, and write the _base.json for a person. Returns the patched dict."""
    base_path = DATA_DIR / folder_name / f"{folder_name}_base.json"
    if not base_path.exists():
        raise FileNotFoundError(f"base.json not found: {base_path}")

    with open(base_path, encoding="utf-8") as f:
        base = json.load(f)

    # Apply bio fields
    base["birth_year"]   = bio["birth_year"]
    base["death_status"] = bio["death_status"]
    base["death_year"]   = bio["death_year"]
    base["nationality"]  = bio["nationality"]

    # Recalculate hlp_nomination_age
    hlp_year   = base.get("hlp_year")
    birth_year = bio["birth_year"]
    if hlp_year and birth_year:
        try:
            base["hlp_nomination_age"] = int(hlp_year) - int(birth_year)
        except (ValueError, TypeError):
            base["hlp_nomination_age"] = None
    else:
        base["hlp_nomination_age"] = None

    with open(base_path, "w", encoding="utf-8") as f:
        json.dump(base, f, indent=2, ensure_ascii=False)

    return base


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"\n{'='*80}")
    print("enrich_bio.py — Applying biographical data to targeted_01 base.json files")
    print(f"{'='*80}\n")

    results = []

    for db_name, folder_name in PEOPLE:
        print(f"Processing: {db_name}")

        bio = load_bio(folder_name)
        source = bio.pop("source", "?")

        try:
            base = update_base_json(folder_name, bio)
            status = "OK"
        except FileNotFoundError as e:
            print(f"  [ERROR] {e}")
            results.append((db_name, "ERROR", None, None, None, None, None))
            continue

        results.append((
            db_name,
            status,
            source,
            base.get("birth_year"),
            base.get("death_status"),
            base.get("nationality"),
            base.get("hlp_nomination_age"),
        ))

        nat_str = str(base.get("nationality")) if base.get("nationality") else "null"
        print(f"  source:  {source}")
        print(f"  birth:   {base.get('birth_year')}  |  death_status: {base.get('death_status')}  |  death_year: {base.get('death_year')}")
        print(f"  nat:     {nat_str}")
        print(f"  hlp_age: {base.get('hlp_nomination_age')}")
        print()

    # Summary table
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    header = f"{'Name':<35} {'Status':<7} {'Birth':>6} {'Death':>9} {'Nat?':>5} {'Age':>5}"
    print(header)
    print("-" * 70)
    for row in results:
        db_name, status, source, birth, death_status, nat, age = row
        nat_flag = "YES" if nat else "null"
        birth_str = birth if birth else "null"
        death_str = death_status if death_status else "null"
        age_str   = str(age) if age is not None else "null"
        print(f"{db_name:<35} {status:<7} {birth_str:>6} {death_str:>9} {nat_flag:>5} {age_str:>5}")

    total = len(results)
    ok    = sum(1 for r in results if r[1] == "OK")
    print(f"\nDone: {ok}/{total} base.json files updated.")

    # Flag anyone with all-null bio
    nulls = [r[0] for r in results if r[1] == "OK" and not r[3] and not r[5]]
    if nulls:
        print(f"\n[WARN] Still all-null bio fields (no evidence found in biographical service):")
        for n in nulls:
            print(f"  {n}")


if __name__ == "__main__":
    main()
