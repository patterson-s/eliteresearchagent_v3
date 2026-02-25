"""
runner.py — Question-agnostic RAG pipeline orchestrator for targeted_01.

Given a question directory (e.g. prompts/HLP_nomination) and a target person,
this script:
  1. Loads the person's base.json and checks skip conditions
  2. Builds a retrieval query from the question config template
  3. Retrieves and reranks the most relevant chunks from the DB
  4. Runs an extraction LLM pass over top N chunks (with early stop)
  5. Runs a verification LLM pass over cross-domain chunks
  6. Saves a full-trace output JSON to outputs/<PersonName>/

Adding a new question requires only a new prompts/<QuestionDir>/ directory
with config.json + extraction.txt + verification.txt. No changes needed here.

CLI usage:
    # Single person (test)
    python runner.py --question prompts/HLP_nomination --person "Abhijit Banerjee" --verbose

    # Skip test (null HLP fields)
    python runner.py --question prompts/HLP_nomination --person "Gordon Brown"

    # Full batch
    python runner.py --question prompts/HLP_nomination --all
"""

import os
import sys
import json
import re
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import cohere
from dotenv import load_dotenv

# ── Path setup ────────────────────────────────────────────────────────────────
_SERVICE_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SERVICE_DIR.parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

sys.path.insert(0, str(_SERVICE_DIR))
from retrieval import retrieve_for_person, get_person_chunks  # noqa: E402

_DATA_DIR = _SERVICE_DIR / "data"
_OUTPUT_DIR = _SERVICE_DIR / "outputs"


# ── Config / prompt loading ───────────────────────────────────────────────────

def load_question_config(question_dir: Path) -> Dict[str, Any]:
    """
    Load and return the parsed config.json for a question directory.

    Args:
        question_dir: Absolute path to a prompts subdirectory,
                      e.g. Path(".../targeted_01/prompts/HLP_nomination").

    Returns:
        Parsed config dict.

    Raises:
        FileNotFoundError: If config.json does not exist in question_dir.
    """
    config_path = question_dir / "config.json"
    if not config_path.exists():
        raise FileNotFoundError(f"No config.json found in {question_dir}")
    with open(config_path, encoding="utf-8") as f:
        return json.load(f)


def load_prompt(prompt_path: Path) -> str:
    """
    Read and return the raw text of a .txt prompt template file.

    Args:
        prompt_path: Absolute path to a .txt prompt file.

    Returns:
        File contents as a string with trailing whitespace stripped.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    if not prompt_path.exists():
        raise FileNotFoundError(f"Prompt file not found: {prompt_path}")
    with open(prompt_path, encoding="utf-8") as f:
        return f.read().strip()


def fill_template(template: str, variables: Dict[str, str]) -> str:
    """
    Replace {{PLACEHOLDER}} tokens in a template string with values.

    Iterates over the variables dict and replaces each {{KEY}} occurrence
    with the corresponding string value. Extra whitespace in values is preserved.
    Unknown placeholders left in the template are kept as-is (no error raised).

    Args:
        template: Template string containing {{KEY}} placeholders.
        variables: Dict mapping placeholder names (without braces) to values.
                   Values are coerced to str before substitution.

    Returns:
        Template string with all known placeholders substituted.
    """
    result = template
    for key, value in variables.items():
        result = result.replace(f"{{{{{key}}}}}", str(value) if value is not None else "")
    return result


# ── Name / year utilities ─────────────────────────────────────────────────────

def person_dir_to_db_name(person_dir_name: str) -> str:
    """
    Convert filesystem directory name to database person_name format.

    The database uses spaced names with dots:  "Amina J. Mohammed"
    The filesystem uses underscores:           "Amina_J._Mohammed"

    Args:
        person_dir_name: Directory name string with underscores.

    Returns:
        Database-format name with spaces.

    Examples:
        "Abhijit_Banerjee"   ->  "Abhijit Banerjee"
        "Amina_J._Mohammed"  ->  "Amina J. Mohammed"
    """
    return person_dir_name.replace("_", " ")


def db_name_to_dir_name(db_name: str) -> str:
    """
    Convert a database person_name to filesystem directory name format.

    Args:
        db_name: Database name string with spaces, e.g. "Amina J. Mohammed".

    Returns:
        Directory-format name with underscores, e.g. "Amina_J._Mohammed".
    """
    return db_name.replace(" ", "_")


def parse_nomination_year(hlp_year: str) -> Optional[int]:
    """
    Extract the first (start) year from an hlp_year range string.

    The base.json stores hlp_year as a range like "2013-2015" or occasionally
    a single year like "2016". Returns the first four-digit year found.

    Args:
        hlp_year: Year string from base.json, e.g. "2013-2015".

    Returns:
        Integer year (the start year), e.g. 2013. Returns None if no year found.

    Examples:
        "2013-2015"  ->  2013
        "2018–2019"  ->  2018  (handles en-dash)
        "2016"       ->  2016
        None         ->  None
    """
    if not hlp_year:
        return None
    match = re.search(r"\b(19|20)\d{2}\b", str(hlp_year))
    return int(match.group()) if match else None


# ── Data loading ──────────────────────────────────────────────────────────────

def list_all_persons(data_dir: Path) -> List[str]:
    """
    Return a sorted list of all person directory names in the data directory.

    Args:
        data_dir: Path to services/targeted_01/data/.

    Returns:
        List of directory name strings, e.g. ["Abhijit_Banerjee", ...].
        Non-directory entries (loose files) are excluded.
    """
    return sorted(
        entry.name
        for entry in data_dir.iterdir()
        if entry.is_dir()
    )


def load_base_json(data_dir: Path, person_dir_name: str) -> Optional[Dict[str, Any]]:
    """
    Load the base.json for a person from their data directory.

    Args:
        data_dir: Root data directory (services/targeted_01/data/).
        person_dir_name: Directory name for the person, e.g. "Abhijit_Banerjee".

    Returns:
        Parsed dict, or None if the file does not exist.
    """
    base_path = data_dir / person_dir_name / f"{person_dir_name}_base.json"
    if not base_path.exists():
        return None
    with open(base_path, encoding="utf-8") as f:
        return json.load(f)


def should_skip(
    base_data: Dict[str, Any],
    skip_if_null: List[str],
) -> Tuple[bool, str]:
    """
    Determine whether a person should be skipped for this question.

    A person is skipped if any field listed in skip_if_null is missing or null
    in their base.json. For the HLP nomination question, this means anyone
    whose hlp_year or hlp_name has not yet been determined.

    Args:
        base_data: Parsed base.json dict.
        skip_if_null: List of field names that must be non-null.
                      Loaded from question config's "skip_if_null" key.

    Returns:
        Tuple of (skip: bool, reason: str).
    """
    for field in skip_if_null:
        if base_data.get(field) is None:
            return True, f"{field} is null in base.json"
    return False, ""


# ── LLM calls ─────────────────────────────────────────────────────────────────

def call_llm(
    prompt: str,
    api_key: str,
    model: str = "command-a-03-2025",
    temperature: float = 0.3,
    max_tokens: int = 1200,
) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Call Cohere Command-A with a prompt and parse the JSON response.

    Uses cohere.ClientV2 per the v3 canonical pattern from enrichment.py.
    Strips markdown fences before JSON parsing. On any failure, returns
    (None, raw_text) rather than raising — callers must handle None.

    Args:
        prompt: Fully substituted prompt string (all placeholders filled).
        api_key: Cohere API key.
        model: Cohere model name.
        temperature: Sampling temperature.
        max_tokens: Maximum response tokens.

    Returns:
        Tuple of:
          - parsed (dict or None): Parsed JSON dict if successful, else None.
          - raw (str): Raw LLM response text (always populated, even on failure).
                       On exception, this is an "ERROR: ..." string.
    """
    co = cohere.ClientV2(api_key=api_key)
    try:
        response = co.chat(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=temperature,
        )
        raw = response.message.content[0].text.strip()

        # Strip markdown fences if present
        cleaned = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned).strip()

        parsed = json.loads(cleaned)
        return parsed, raw

    except json.JSONDecodeError:
        return None, raw if "raw" in dir() else "ERROR: response text unavailable"
    except Exception as e:
        return None, f"ERROR: {e}"


# ── Extraction pass ───────────────────────────────────────────────────────────

def run_extraction_pass(
    person_name: str,
    base_data: Dict[str, Any],
    chunks: List[Dict[str, Any]],
    question_config: Dict[str, Any],
    extraction_template: str,
    api_key: str,
    nomination_year: int,
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Run the extraction prompt over top N chunks, with optional early stopping.

    Iterates over chunks in rerank order. For each chunk, fills the extraction
    template with person context + chunk text, calls the LLM, and records the
    full trace. Stops early if a 'high' confidence answer is found and
    high_confidence_early_stop is enabled in the question config.

    Args:
        person_name: Spaced-format DB name.
        base_data: Parsed base.json dict (for hlp_name, nomination_age, etc.).
        chunks: Reranked chunks from retrieve_for_person().
        question_config: Parsed config.json for the question.
        extraction_template: Raw template text from extraction.txt.
        api_key: Cohere API key.
        nomination_year: Integer start year from hlp_year.

    Returns:
        Tuple of:
          - extraction_trace (list): One dict per chunk scanned, containing:
              chunk_id, chunk_index, source_id, url, domain, similarity,
              rerank_score, raw_llm_output, parsed, job_title_at_nomination,
              organization_at_nomination, confidence, cannot_determine,
              early_stop_triggered, error
          - best_extraction (dict or None): The first parsed response where
              cannot_determine is False and confidence is not null. None if
              no usable answer was found across all scanned chunks.
          - best_chunk (dict or None): The chunk dict corresponding to
              best_extraction (needed to determine primary_domain for verification).
    """
    ext_cfg = question_config.get("extraction", {})
    max_to_scan = ext_cfg.get("max_chunks_to_scan", 5)
    early_stop = ext_cfg.get("high_confidence_early_stop", True)
    model = ext_cfg.get("model", "command-a-03-2025")
    temperature = ext_cfg.get("temperature", 0.3)
    max_tokens = ext_cfg.get("max_tokens", 1200)

    extraction_trace = []
    best_extraction = None
    best_chunk = None

    for i, chunk in enumerate(chunks[:max_to_scan]):
        prompt = fill_template(extraction_template, {
            "PERSON_NAME": person_name,
            "HLP_NAME": base_data.get("hlp_name", ""),
            "NOMINATION_YEAR": str(nomination_year),
            "NOMINATION_AGE": str(base_data.get("hlp_nomination_age", "")),
            "CHUNK_TEXT": chunk["text"],
        })

        parsed, raw = call_llm(prompt, api_key, model, temperature, max_tokens)

        entry = {
            "chunk_id": chunk["chunk_id"],
            "chunk_index": chunk["chunk_index"],
            "source_id": chunk["source_id"],
            "url": chunk["url"],
            "domain": chunk["domain"],
            "similarity": chunk.get("similarity"),
            "rerank_score": chunk.get("rerank_score"),
            "raw_llm_output": raw,
            "parsed": parsed,
            "job_title_at_nomination": None,
            "organization_at_nomination": None,
            "confidence": None,
            "cannot_determine": True,
            "early_stop_triggered": False,
            "error": None,
        }

        if parsed is None:
            entry["error"] = raw if raw.startswith("ERROR") else "JSON parse failure"
        else:
            entry["job_title_at_nomination"] = parsed.get("job_title_at_nomination")
            entry["organization_at_nomination"] = parsed.get("organization_at_nomination")
            entry["confidence"] = parsed.get("confidence")
            entry["cannot_determine"] = parsed.get("cannot_determine", True)

            # Track best answer: first non-null (Q1 single-fact) or richest list (Q2-Q6)
            if not entry["cannot_determine"]:
                primary_list_field = ext_cfg.get("primary_list_field")
                if primary_list_field:
                    # Richest selection: keep whichever extraction has the most list items
                    current_count = len(parsed.get(primary_list_field, []))
                    best_count = len(best_extraction.get(primary_list_field, [])) if best_extraction else -1
                    if current_count > best_count:
                        best_extraction = parsed
                        best_chunk = chunk
                elif best_extraction is None:
                    # First non-null selection — original Q1 behavior
                    best_extraction = parsed
                    best_chunk = chunk

        extraction_trace.append(entry)

        # Early stop on high confidence (single-fact questions only; list questions
        # set high_confidence_early_stop: false so `early_stop` is False here)
        if (
            early_stop
            and best_extraction is not None
            and best_extraction.get("confidence") == "high"
        ):
            entry["early_stop_triggered"] = True
            break

    return extraction_trace, best_extraction, best_chunk


# ── Verification candidate builder ────────────────────────────────────────────

def build_candidate_strings(
    best_extraction: Dict[str, Any],
    q_config: Dict[str, Any],
) -> Tuple[str, str]:
    """
    Build the CANDIDATE_JOB_TITLE and CANDIDATE_ORGANIZATION strings for the
    verification prompt.

    For Q1 (single-fact), uses the fixed fields job_title_at_nomination and
    organization_at_nomination. For Q2-Q6 (list questions), uses the field names
    specified in the question config's verification.candidate_title_field and
    verification.candidate_org_field keys, then formats them into compact strings.

    Args:
        best_extraction: Parsed extraction result dict.
        q_config: Parsed config.json for the question.

    Returns:
        Tuple of (candidate_title: str, candidate_org: str).
        Either may be an empty string if not available.
    """
    ver_cfg = q_config.get("verification", {})
    title_field = ver_cfg.get("candidate_title_field")
    org_field = ver_cfg.get("candidate_org_field")

    # Q1 / default: fixed field names
    if not title_field and not org_field:
        return (
            best_extraction.get("job_title_at_nomination") or "",
            best_extraction.get("organization_at_nomination") or "",
        )

    def _format_val(field_name: Optional[str]) -> str:
        """Extract and format a field value into a compact readable string."""
        if not field_name:
            return ""
        val = best_extraction.get(field_name)
        if val is None:
            return ""
        if isinstance(val, list):
            if not val:
                return ""
            items = val[:3]  # Top 3 for brevity
            if isinstance(items[0], dict):
                parts = []
                for item in items:
                    # Extract the most descriptive sub-fields available
                    label = (
                        item.get("title")
                        or item.get("degree_type")
                        or item.get("sector")
                        or item.get("award")
                        or item.get("organization")
                        or item.get("city")
                        or ""
                    )
                    sub = (
                        item.get("field")         # education
                        or item.get("role_context")  # locations
                        or item.get("evidence")   # sectors
                        or ""
                    )
                    org = (
                        item.get("organization")
                        or item.get("institution")
                        or item.get("awarding_body")
                        or item.get("country")
                        or ""
                    )
                    period = item.get("approximate_period") or item.get("year") or ""
                    desc = label
                    if sub and sub != desc:
                        desc = f"{desc} {sub}".strip() if desc else sub
                    if org and org not in desc:
                        desc = f"{desc}, {org}".strip(", ") if desc else org
                    if period:
                        desc = f"{desc} ({period})"
                    parts.append(desc.strip())
                return "; ".join(p for p in parts if p)
            else:
                return ", ".join(str(x) for x in items)
        return str(val)

    return _format_val(title_field), _format_val(org_field)


# ── Verification pass ─────────────────────────────────────────────────────────

def run_verification_pass(
    person_name: str,
    base_data: Dict[str, Any],
    best_extraction: Dict[str, Any],
    all_chunks: List[Dict[str, Any]],
    used_chunk_ids: Set[int],
    primary_domain: str,
    question_config: Dict[str, Any],
    verification_template: str,
    api_key: str,
    nomination_year: int,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Run verification prompts on chunks from domains different from the primary find.

    Selects chunks NOT from primary_domain and NOT already used in extraction,
    then calls the verification LLM prompt on up to max_verification_chunks of them.
    Counts the number of confirmations (confirms == True).

    Args:
        person_name: Spaced-format DB name.
        base_data: Parsed base.json dict.
        best_extraction: Parsed extraction response with job_title_at_nomination
                         and organization_at_nomination fields.
        all_chunks: All reranked chunks (the full list from retrieve_for_person).
        used_chunk_ids: Set of chunk_ids already scanned in the extraction pass.
        primary_domain: Domain string of the source where the answer was first found.
        question_config: Parsed config.json.
        verification_template: Raw template text from verification.txt.
        api_key: Cohere API key.
        nomination_year: Integer nomination year.

    Returns:
        Tuple of:
          - verification_trace (list): One dict per chunk verified, containing:
              chunk_id, url, domain, raw_llm_output, parsed,
              confirms, confidence, supporting_quote, alternative_title,
              alternative_organization, error
          - confirmation_count (int): Number of verifications where confirms == True.
    """
    ver_cfg = question_config.get("verification", {})
    max_ver = ver_cfg.get("max_verification_chunks", 3)
    exclude_primary = ver_cfg.get("exclude_primary_domain", True)
    ext_cfg = question_config.get("extraction", {})
    model = ext_cfg.get("model", "command-a-03-2025")

    candidate_title, candidate_org = build_candidate_strings(best_extraction, question_config)

    # Select verification candidates
    ver_candidates = []
    for chunk in all_chunks:
        if chunk["chunk_id"] in used_chunk_ids:
            continue
        if exclude_primary and chunk["domain"] == primary_domain:
            continue
        ver_candidates.append(chunk)
        if len(ver_candidates) >= max_ver:
            break

    verification_trace = []
    confirmation_count = 0

    for chunk in ver_candidates:
        prompt = fill_template(verification_template, {
            "PERSON_NAME": person_name,
            "HLP_NAME": base_data.get("hlp_name", ""),
            "NOMINATION_YEAR": str(nomination_year),
            "CANDIDATE_JOB_TITLE": candidate_title or "",
            "CANDIDATE_ORGANIZATION": candidate_org or "",
            "CHUNK_TEXT": chunk["text"],
        })

        parsed, raw = call_llm(prompt, api_key, model, temperature=0.1, max_tokens=800)

        entry = {
            "chunk_id": chunk["chunk_id"],
            "url": chunk["url"],
            "domain": chunk["domain"],
            "raw_llm_output": raw,
            "parsed": parsed,
            "confirms": False,
            "confidence": None,
            "supporting_quote": None,
            "alternative_title": None,
            "alternative_organization": None,
            "error": None,
        }

        if parsed is None:
            entry["error"] = raw if raw.startswith("ERROR") else "JSON parse failure"
        else:
            confirms = parsed.get("confirms", False)
            entry["confirms"] = bool(confirms)
            entry["confidence"] = parsed.get("confidence")
            entry["supporting_quote"] = parsed.get("supporting_quote")
            entry["alternative_title"] = parsed.get("alternative_title")
            entry["alternative_organization"] = parsed.get("alternative_organization")
            if entry["confirms"]:
                confirmation_count += 1

        verification_trace.append(entry)

    return verification_trace, confirmation_count


# ── Result assembly ───────────────────────────────────────────────────────────

def determine_status(
    chunks_retrieved: int,
    best_extraction: Optional[Dict[str, Any]],
    confirmation_count: int,
    skipped: bool,
    skip_reason: str,
    error: Optional[str],
) -> str:
    """
    Determine the result status string from pipeline outcomes.

    Args:
        chunks_retrieved: How many chunks were returned by retrieval.
        best_extraction: The best extracted answer dict, or None.
        confirmation_count: Number of cross-domain verifications confirmed.
        skipped: Whether this person was skipped.
        skip_reason: Reason for skipping (if any).
        error: Error string if a top-level exception occurred.

    Returns:
        One of: 'found_and_verified', 'found_unverified',
                'found_no_confirming_sources', 'cannot_determine',
                'no_chunks_retrieved', 'skipped', 'error'
    """
    if error:
        return "error"
    if skipped:
        return "skipped"
    if chunks_retrieved == 0:
        return "no_chunks_retrieved"
    if best_extraction is None:
        return "cannot_determine"
    if confirmation_count >= 1:
        return "found_and_verified"
    if confirmation_count == 0:
        # Distinguish: had verification chunks but none confirmed vs. no ver chunks at all
        return "found_no_confirming_sources"
    return "found_unverified"


# ── Output saving ─────────────────────────────────────────────────────────────

def save_output(
    output_dir: Path,
    person_dir_name: str,
    output_filename_suffix: str,
    result: Dict[str, Any],
) -> Path:
    """
    Save the full result dict as JSON to outputs/<PersonName>/<PersonName>_<suffix>.json.

    Creates the output directory if it does not exist. Overwrites any
    existing file with the same name (idempotent re-runs).

    Args:
        output_dir: Root outputs directory (services/targeted_01/outputs/).
        person_dir_name: Directory-format person name, e.g. "Abhijit_Banerjee".
        output_filename_suffix: From question config, e.g. "hlp_job_title".
        result: Full result dict to serialize as JSON.

    Returns:
        Path to the saved file.
    """
    person_out = output_dir / person_dir_name
    person_out.mkdir(parents=True, exist_ok=True)
    out_path = person_out / f"{person_dir_name}_{output_filename_suffix}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    return out_path


# ── Synthesis mode ────────────────────────────────────────────────────────────

def run_synthesis_for_person(
    person_dir_name: str,
    q_config: Dict[str, Any],
    data_dir: Path,
    output_dir: Path,
    api_key: str,
    verbose: bool = False,
) -> Dict[str, Any]:
    """
    Run the synthesis pipeline for Q7 (career domain): load prior Q1-Q6 outputs,
    fill a single prompt template, call the LLM once, save output.

    No DB retrieval. No verification pass. Called only when config has
    `"mode": "synthesis"`.

    Args:
        person_dir_name: Directory-format person name.
        q_config: Parsed config.json for the question (must have "depends_on").
        data_dir: Path to services/targeted_01/data/.
        output_dir: Path to services/targeted_01/outputs/.
        api_key: Cohere API key.
        verbose: If True, print progress.

    Returns:
        Full result dict written to disk.
    """
    person_db_name = person_dir_to_db_name(person_dir_name)
    timestamp = datetime.now(timezone.utc).isoformat()
    question_id = q_config["question_id"]
    suffix = q_config["output_filename_suffix"]
    depends_on = q_config.get("depends_on", {})  # {PLACEHOLDER: suffix_to_load}

    ext_cfg = q_config.get("extraction", {})
    model = ext_cfg.get("model", "command-a-03-2025")
    temperature = ext_cfg.get("temperature", 0.1)
    max_tokens = ext_cfg.get("max_tokens", 1500)

    # Load synthesis prompt
    synth_prompt_path = _SERVICE_DIR / q_config["prompts"]["synthesis"]
    synthesis_template = load_prompt(synth_prompt_path)

    # Load base.json
    base_data = load_base_json(data_dir, person_dir_name) or {}

    # Load each prior output
    prior_results = {}
    missing = []
    for placeholder, prior_suffix in depends_on.items():
        prior_path = output_dir / person_dir_name / f"{person_dir_name}_{prior_suffix}.json"
        if not prior_path.exists():
            missing.append(prior_suffix)
        else:
            with open(prior_path, encoding="utf-8") as f:
                prior_data = json.load(f)
            # Pass the "result" block to the prompt
            prior_results[placeholder] = json.dumps(prior_data.get("result", {}), indent=2)

    if missing:
        reason = f"missing prior outputs: {', '.join(missing)}"
        if verbose:
            print(f"  SKIP: {reason}")
        result = {
            "person_dir_name": person_dir_name,
            "person_db_name": person_db_name,
            "question_id": question_id,
            "run_timestamp": timestamp,
            "input": {k: base_data.get(k) for k in ["birth_year", "nationality"]},
            "result": {"status": "skipped"},
            "meta": {"skipped": True, "skip_reason": reason, "error": None},
        }
        save_output(output_dir, person_dir_name, suffix, result)
        return result

    # Fill prompt
    variables = {"PERSON_NAME": person_db_name}
    variables.update(prior_results)
    prompt = fill_template(synthesis_template, variables)

    if verbose:
        print(f"  Running synthesis prompt...")

    parsed, raw = call_llm(prompt, api_key, model, temperature, max_tokens)

    final_result = {
        "status": "found" if parsed and not parsed.get("cannot_determine") else "cannot_determine",
        "dominant_domain": parsed.get("dominant_domain") if parsed else None,
        "is_hybrid": parsed.get("is_hybrid") if parsed else None,
        "hybrid_domains": parsed.get("hybrid_domains") if parsed else [],
        "domain_evidence": parsed.get("domain_evidence") if parsed else None,
        "alternative_domain_suggestion": parsed.get("alternative_domain_suggestion") if parsed else None,
        "confidence": parsed.get("confidence") if parsed else None,
    }
    if parsed is None:
        final_result["status"] = "error"

    if verbose:
        print(f"  STATUS: {final_result['status']}  domain: {final_result.get('dominant_domain')}")

    result = {
        "person_dir_name": person_dir_name,
        "person_db_name": person_db_name,
        "question_id": question_id,
        "question_display": q_config.get("display_name", question_id),
        "run_timestamp": timestamp,
        "input": {k: base_data.get(k) for k in ["birth_year", "nationality"]},
        "synthesis_inputs": {ph: f"[loaded from {suf}]" for ph, suf in depends_on.items()},
        "raw_llm_output": raw,
        "parsed": parsed,
        "result": final_result,
        "meta": {
            "mode": "synthesis",
            "skipped": False,
            "skip_reason": None,
            "error": None if parsed is not None else raw,
        },
    }

    out_path = save_output(output_dir, person_dir_name, suffix, result)
    if verbose:
        print(f"  Saved: {out_path}")
    return result


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run_for_person(
    person_dir_name: str,
    question_dir: Path,
    data_dir: Path,
    output_dir: Path,
    api_key: str,
    verbose: bool = False,
) -> Dict[str, Any]:
    """
    Run the full extraction + verification pipeline for one person.

    Orchestrates: load → skip check → retrieve → extract → verify → save.
    All LLM call errors are caught inside call_llm(). DB errors propagate.

    Args:
        person_dir_name: Directory-format person name, e.g. "Abhijit_Banerjee".
        question_dir: Absolute path to the prompts subdirectory for the question.
        data_dir: Path to services/targeted_01/data/.
        output_dir: Path to services/targeted_01/outputs/.
        api_key: Cohere API key.
        verbose: If True, print detailed progress to stdout.

    Returns:
        Full result dict (same structure as what is written to disk).
    """
    person_db_name = person_dir_to_db_name(person_dir_name)
    timestamp = datetime.now(timezone.utc).isoformat()

    if verbose:
        print(f"\n{'='*60}")
        print(f"Person: {person_db_name}")

    # Load question config + prompts
    q_config = load_question_config(question_dir)
    question_id = q_config["question_id"]
    suffix = q_config["output_filename_suffix"]
    skip_if_null = q_config.get("skip_if_null", [])
    retrieval_cfg = q_config.get("retrieval", {})

    # Dispatch to synthesis mode (Q7) — bypasses retrieval and RAG pipeline entirely
    if q_config.get("mode") == "synthesis":
        return run_synthesis_for_person(
            person_dir_name, q_config, data_dir, output_dir, api_key, verbose
        )

    extraction_prompt_path = _SERVICE_DIR / q_config["prompts"]["extraction"]
    verification_prompt_path = _SERVICE_DIR / q_config["prompts"]["verification"]
    extraction_template = load_prompt(extraction_prompt_path)
    verification_template = load_prompt(verification_prompt_path)

    # Load base.json
    base_data = load_base_json(data_dir, person_dir_name)
    if base_data is None:
        result = {
            "person_dir_name": person_dir_name,
            "person_db_name": person_db_name,
            "question_id": question_id,
            "run_timestamp": timestamp,
            "input": {},
            "retrieval": {},
            "extraction_trace": [],
            "verification_trace": [],
            "result": {"status": "error"},
            "meta": {"skipped": False, "error": "base.json not found"},
        }
        save_output(output_dir, person_dir_name, suffix, result)
        return result

    # Skip check
    skip, skip_reason = should_skip(base_data, skip_if_null)
    if skip:
        if verbose:
            print(f"  SKIP: {skip_reason}")
        result = {
            "person_dir_name": person_dir_name,
            "person_db_name": person_db_name,
            "question_id": question_id,
            "run_timestamp": timestamp,
            "input": {k: base_data.get(k) for k in
                      ["hlp_name", "hlp_year", "hlp_nomination_age", "birth_year", "nationality"]},
            "retrieval": {},
            "extraction_trace": [],
            "verification_trace": [],
            "result": {"status": "skipped"},
            "meta": {"skipped": True, "skip_reason": skip_reason, "error": None},
        }
        save_output(output_dir, person_dir_name, suffix, result)
        return result

    # Parse nomination year
    nomination_year = parse_nomination_year(base_data.get("hlp_year"))
    if nomination_year is None:
        nomination_year = 0  # Will still run but with weak temporal anchoring

    # Build retrieval query
    query_template = retrieval_cfg.get(
        "query_template",
        "{PERSON_NAME} job title position role career {NOMINATION_YEAR} appointed"
    )
    retrieval_query = query_template.replace("{PERSON_NAME}", person_db_name).replace(
        "{NOMINATION_YEAR}", str(nomination_year)
    )

    if verbose:
        print(f"  HLP: {base_data.get('hlp_name')}")
        print(f"  Nomination year: {nomination_year}")
        print(f"  Retrieval query: {retrieval_query}")

    # Retrieve chunks
    chunks = retrieve_for_person(
        person_name=person_db_name,
        query=retrieval_query,
        api_key=api_key,
        similarity_top_k=retrieval_cfg.get("similarity_top_k", 20),
        similarity_threshold=retrieval_cfg.get("similarity_threshold", 0.15),
        rerank_top_n=retrieval_cfg.get("rerank_top_n", 10),
    )

    # Count total chunks in DB (before retrieval threshold filtering)
    all_db_chunks = get_person_chunks(person_db_name)
    chunks_in_db = len(all_db_chunks)

    if verbose:
        print(f"  Chunks in DB: {chunks_in_db} | Retrieved: {len(chunks)}")

    if not chunks:
        result = {
            "person_dir_name": person_dir_name,
            "person_db_name": person_db_name,
            "question_id": question_id,
            "run_timestamp": timestamp,
            "input": {
                "hlp_name": base_data.get("hlp_name"),
                "hlp_year": base_data.get("hlp_year"),
                "nomination_year": nomination_year,
                "hlp_nomination_age": base_data.get("hlp_nomination_age"),
                "birth_year": base_data.get("birth_year"),
                "nationality": base_data.get("nationality"),
            },
            "retrieval": {
                "query": retrieval_query,
                "chunks_in_db": chunks_in_db,
                "chunks_retrieved": 0,
                "top_chunks": [],
            },
            "extraction_trace": [],
            "verification_trace": [],
            "result": {"status": "no_chunks_retrieved"},
            "meta": {"skipped": False, "error": None},
        }
        save_output(output_dir, person_dir_name, suffix, result)
        return result

    # Extraction pass
    if verbose:
        print(f"  Running extraction pass (up to "
              f"{q_config.get('extraction', {}).get('max_chunks_to_scan', 5)} chunks)...")

    extraction_trace, best_extraction, best_chunk = run_extraction_pass(
        person_name=person_db_name,
        base_data=base_data,
        chunks=chunks,
        question_config=q_config,
        extraction_template=extraction_template,
        api_key=api_key,
        nomination_year=nomination_year,
    )

    used_chunk_ids = {e["chunk_id"] for e in extraction_trace}
    primary_domain = best_chunk["domain"] if best_chunk else ""

    if verbose:
        if best_extraction:
            primary_list_field = q_config.get("extraction", {}).get("primary_list_field")
            if primary_list_field:
                count = len(best_extraction.get(primary_list_field, []))
                print(f"  Extraction found: {count} {primary_list_field} "
                      f"({best_extraction.get('confidence')} confidence)")
            else:
                print(f"  Extraction found: '{best_extraction.get('job_title_at_nomination')}' "
                      f"({best_extraction.get('confidence')} confidence)")
        else:
            print("  Extraction: cannot_determine")

    # Verification pass (only if we have a candidate answer)
    verification_trace = []
    confirmation_count = 0

    if best_extraction and not best_extraction.get("cannot_determine", True):
        if verbose:
            print(f"  Running verification pass (primary domain: {primary_domain})...")

        verification_trace, confirmation_count = run_verification_pass(
            person_name=person_db_name,
            base_data=base_data,
            best_extraction=best_extraction,
            all_chunks=chunks,
            used_chunk_ids=used_chunk_ids,
            primary_domain=primary_domain,
            question_config=q_config,
            verification_template=verification_template,
            api_key=api_key,
            nomination_year=nomination_year,
        )

        if verbose:
            print(f"  Verification: {confirmation_count} confirmation(s) "
                  f"from {len(verification_trace)} source(s)")

    # Determine final status
    status = determine_status(
        chunks_retrieved=len(chunks),
        best_extraction=best_extraction,
        confirmation_count=confirmation_count,
        skipped=False,
        skip_reason="",
        error=None,
    )

    # Build final result block: pipeline metadata + all substantive extraction fields.
    # Merging all best_extraction fields makes this question-agnostic: Q1 fields
    # (job_title_at_nomination, organization_at_nomination) and Q2-Q6 fields
    # (degrees_found, locations, jobs, sectors, etc.) all appear directly in the
    # result block, making it usable by Q7 synthesis without special-casing.
    final_result = {
        "status": status,
        "confidence": best_extraction.get("confidence") if best_extraction else None,
        "supporting_quote": best_extraction.get("supporting_quote") if best_extraction else None,
        "confirmation_count": confirmation_count,
        "primary_source_domain": primary_domain or None,
        "notes": None,
    }
    if best_extraction:
        _meta_keys = {"reasoning", "confidence", "supporting_quote", "cannot_determine"}
        for k, v in best_extraction.items():
            if k not in _meta_keys and k not in final_result:
                final_result[k] = v

    if verbose:
        print(f"  STATUS: {status}")

    # Assemble full output
    result = {
        "person_dir_name": person_dir_name,
        "person_db_name": person_db_name,
        "question_id": question_id,
        "question_display": q_config.get("display_name", question_id),
        "run_timestamp": timestamp,
        "input": {
            "hlp_name": base_data.get("hlp_name"),
            "hlp_year": base_data.get("hlp_year"),
            "nomination_year": nomination_year,
            "hlp_nomination_age": base_data.get("hlp_nomination_age"),
            "birth_year": base_data.get("birth_year"),
            "nationality": base_data.get("nationality"),
        },
        "retrieval": {
            "query": retrieval_query,
            "chunks_in_db": chunks_in_db,
            "chunks_retrieved": len(chunks),
            "top_chunks": [
                {
                    "chunk_id": c["chunk_id"],
                    "url": c["url"],
                    "domain": c["domain"],
                    "similarity": round(c.get("similarity", 0), 4),
                    "rerank_score": round(c.get("rerank_score") or 0, 4),
                }
                for c in chunks[:5]
            ],
        },
        "extraction_trace": extraction_trace,
        "verification_trace": verification_trace,
        "result": final_result,
        "meta": {
            "chunks_scanned_extraction": len(extraction_trace),
            "chunks_scanned_verification": len(verification_trace),
            "early_stop": any(e.get("early_stop_triggered") for e in extraction_trace),
            "skipped": False,
            "skip_reason": None,
            "error": None,
        },
    }

    out_path = save_output(output_dir, person_dir_name, suffix, result)
    if verbose:
        print(f"  Saved: {out_path}")

    return result


# ── CLI entry point ───────────────────────────────────────────────────────────

def main():
    """
    CLI entry point for runner.py.

    Accepts --person (single run) or --all (batch run over all 58 people).
    Person names can be provided in either spaced or underscore format.
    """
    parser = argparse.ArgumentParser(
        description="Targeted biographical RAG pipeline for targeted_01",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python runner.py --question prompts/HLP_nomination --person "Abhijit Banerjee" --verbose
  python runner.py --question prompts/HLP_nomination --person Abhijit_Banerjee
  python runner.py --question prompts/HLP_nomination --person "Gordon Brown"
  python runner.py --question prompts/HLP_nomination --all
  python runner.py --question prompts/HLP_nomination --all --verbose
        """,
    )
    parser.add_argument(
        "--question",
        default="prompts/HLP_nomination",
        help="Path to question prompts directory (default: prompts/HLP_nomination)",
    )
    parser.add_argument(
        "--person",
        default=None,
        help="Person name in spaced or underscore format",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process all people found in data/",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print detailed per-chunk progress",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Override output directory path (default: outputs/)",
    )
    args = parser.parse_args()

    # Resolve paths
    question_dir = Path(args.question)
    if not question_dir.is_absolute():
        question_dir = _SERVICE_DIR / question_dir

    data_dir = _DATA_DIR
    output_dir = Path(args.output) if args.output else _OUTPUT_DIR

    # API key
    api_key = os.getenv("COHERE_API_KEY")
    if not api_key:
        print("ERROR: COHERE_API_KEY environment variable not set.")
        sys.exit(1)

    if args.all:
        persons = list_all_persons(data_dir)
        print(f"Processing {len(persons)} people with question: {question_dir.name}")
        results_summary = []
        for i, p in enumerate(persons, 1):
            print(f"[{i:>3}/{len(persons)}] {p}", end="  ")
            try:
                res = run_for_person(p, question_dir, data_dir, output_dir, api_key, args.verbose)
                status = res.get("result", {}).get("status", "unknown")
                print(f"→ {status}")
                results_summary.append({"person": p, "status": status})
            except Exception as e:
                print(f"→ EXCEPTION: {e}")
                results_summary.append({"person": p, "status": "exception", "error": str(e)})

        # Print summary
        from collections import Counter
        counts = Counter(r["status"] for r in results_summary)
        print(f"\n{'='*50}")
        print("BATCH SUMMARY:")
        for status, count in sorted(counts.items()):
            print(f"  {status:<35} {count:>3}")
        print(f"  {'TOTAL':<35} {len(results_summary):>3}")

    elif args.person:
        # Normalize: accept either "Abhijit Banerjee" or "Abhijit_Banerjee"
        person_dir_name = args.person.replace(" ", "_")
        run_for_person(person_dir_name, question_dir, data_dir, output_dir, api_key, verbose=True)

    else:
        parser.print_help()
        print("\nError: Specify --person NAME or --all")
        sys.exit(1)


if __name__ == "__main__":
    main()
