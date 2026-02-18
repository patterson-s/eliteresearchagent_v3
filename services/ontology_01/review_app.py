"""
review_app.py — Streamlit human review tool for the ontology matching service.

Three pages:
  1. Pending Match Reviews  — approve or reject medium-confidence matches
  2. Stub Review            — enrich and approve auto-created stub entries
  3. Ontology Browser       — browse, filter, and inline-edit the full ontology

Run with:
    streamlit run services/ontology_01/review_app.py
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Optional

import streamlit as st

# ── Path setup ────────────────────────────────────────────────────────────────
_SERVICE_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SERVICE_DIR.parent.parent
TIMELINE_DATA_DIR = _PROJECT_ROOT / "services" / "WikiPrompt" / "llm_timeline_data"

sys.path.insert(0, str(_SERVICE_DIR))

from ontology_db import OntologyDB
from enrichment import enrich_stub, merge_stub_into_entry, get_confirmed_orgs

# ─────────────────────────────────────────────────────────────────────────────
# Page config (must be first Streamlit call)
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Ontology Review Tool",
    layout="wide",
    page_icon="🔬",
    initial_sidebar_state="expanded",
)


# ─────────────────────────────────────────────────────────────────────────────
# Cached resources
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_resource
def get_db() -> OntologyDB:
    """Shared OntologyDB instance. Call st.cache_resource.clear() after mutations."""
    return OntologyDB(_SERVICE_DIR / "unified_ontology.json")


def reload_db() -> None:
    """Clear cache and force DB reload on next access."""
    st.cache_resource.clear()


# ─────────────────────────────────────────────────────────────────────────────
# Sidecar file helpers
# ─────────────────────────────────────────────────────────────────────────────

def load_all_sidecar_files() -> List[Dict]:
    """Load all *_org_links.json sidecar files from the timeline data directory."""
    sidecars = []
    for path in sorted(TIMELINE_DATA_DIR.rglob("*_org_links.json")):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            data["_file_path"] = str(path)
            sidecars.append(data)
        except (json.JSONDecodeError, OSError):
            pass
    return sidecars


def get_pending_reviews(sidecars: List[Dict]) -> List[Dict]:
    """
    Collect all org_links where needs_review=True across all sidecar files.
    Returns enriched list with person_name and file_path added.
    """
    pending = []
    for sidecar in sidecars:
        person = sidecar.get("person_name", "Unknown")
        file_path = sidecar.get("_file_path", "")
        for link in sidecar.get("org_links", []):
            if link.get("needs_review"):
                pending.append({
                    **link,
                    "person_name": person,
                    "_sidecar_file_path": file_path,
                })
    return pending


def update_sidecar_link(
    sidecar_file_path: str,
    raw_name: str,
    updates: Dict,
) -> bool:
    """
    Update a specific org_link in a sidecar file.
    Finds the link by raw_name and applies updates.
    Returns True on success.
    """
    path = Path(sidecar_file_path)
    if not path.exists():
        return False
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for i, link in enumerate(data.get("org_links", [])):
            if link.get("raw_name") == raw_name:
                data["org_links"][i] = {**link, **updates}
                # Update summary counts
                data["matched_count"] = sum(
                    1 for l in data["org_links"] if l.get("matched")
                )
                data["review_needed_count"] = sum(
                    1 for l in data["org_links"] if l.get("needs_review")
                )
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                return True
    except (json.JSONDecodeError, OSError, KeyError):
        pass
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Tag helpers
# ─────────────────────────────────────────────────────────────────────────────

def build_hierarchical_tags(tag: str) -> List[str]:
    """
    Build the hierarchical_tags array from a canonical_tag string.
    Supports multiple tags separated by ";".
    "UN:Foo:Bar" -> ["UN", "UN:Foo", "UN:Foo:Bar"]
    "UN:Foo ; ngo:research" -> ["UN", "UN:Foo", "ngo", "ngo:research"]
    """
    if not tag:
        return []
    all_htags: List[str] = []
    for single_tag in tag.split(";"):
        single_tag = single_tag.strip()
        if not single_tag:
            continue
        parts = single_tag.split(":")
        all_htags.extend(
            ":".join(parts[:i]) for i in range(1, len(parts) + 1)
        )
    # Deduplicate while preserving order
    seen: set = set()
    result: List[str] = []
    for ht in all_htags:
        if ht not in seen:
            seen.add(ht)
            result.append(ht)
    return result


def _parse_tags(tag_str: str) -> List[str]:
    """Parse a ';'-separated tag string into a list of clean individual tags."""
    return [t.strip() for t in tag_str.split(";") if t.strip()]


def _canonical_tag_from_tags(tags: List[str]) -> str:
    """Return the first tag as the primary canonical_tag (for DB storage)."""
    return tags[0] if tags else ""


# ─────────────────────────────────────────────────────────────────────────────
# Page 1: Pending Match Reviews
# ─────────────────────────────────────────────────────────────────────────────

def page_pending_reviews(db: OntologyDB, sidecars: List[Dict]) -> None:
    st.header("Pending Match Reviews")
    st.caption(
        "These organizations had a medium-confidence fuzzy match. "
        "Approve if the proposed match is correct, or reject to create a stub."
    )

    pending = get_pending_reviews(sidecars)

    if not pending:
        st.success("No pending reviews — all matches resolved!")
        return

    st.metric("Pending Reviews", len(pending))
    st.divider()

    for i, item in enumerate(pending):
        raw_name = item.get("raw_name", "")
        proposed = item.get("proposed_match_canonical", "")
        confidence = item.get("proposed_match_confidence") or 0.0
        person = item.get("person_name", "Unknown")
        org_type = item.get("org_type_classified", "other")
        sidecar_path = item.get("_sidecar_file_path", "")

        expander_label = f"{raw_name}  —  {person}"
        with st.expander(expander_label, expanded=(i == 0)):
            col1, col2 = st.columns([1, 1])

            with col1:
                st.subheader("Raw Name")
                st.code(raw_name)
                st.caption(f"Person: **{person}**")
                st.caption(f"Classified as: `{org_type}`")
                st.caption(f"Match confidence: `{confidence:.1%}`")

            with col2:
                st.subheader("Proposed Match")
                if proposed:
                    st.write(f"**{proposed}**")
                    # Show the full ontology entry
                    entry = db.lookup_canonical(proposed)
                    if entry:
                        with st.container():
                            st.json(entry, expanded=False)
                    else:
                        st.warning("Proposed entry not found in ontology.")
                else:
                    st.write("_(no proposed match)_")

            st.divider()
            col_a, col_b, col_c = st.columns(3)

            with col_a:
                if st.button("Approve Match", key=f"approve_{i}", type="primary"):
                    entry = db.lookup_canonical(proposed) if proposed else None
                    ontology_tag = None
                    meta_type = item.get("meta_type")
                    if entry:
                        from matcher import _get_ontology_tag
                        ontology_tag = _get_ontology_tag(entry)
                        meta_type = entry.get("meta_type", meta_type)

                    success = update_sidecar_link(
                        sidecar_path, raw_name,
                        {
                            "matched": True,
                            "needs_review": False,
                            "canonical_name": proposed,
                            "match_method": "human_approved",
                            "match_confidence": 1.0,
                            "ontology_tag": ontology_tag,
                            "meta_type": meta_type,
                        }
                    )
                    if success:
                        st.success(f"Approved: {raw_name} → {proposed}")
                        st.rerun()
                    else:
                        st.error("Failed to update sidecar file.")

            with col_b:
                if st.button("Reject → Create Stub", key=f"reject_{i}"):
                    # Create stub if it doesn't exist
                    if not db.lookup_canonical(raw_name):
                        from run_matching import build_stub
                        stub = build_stub({
                            "raw_name": raw_name,
                            "org_type_classified": org_type,
                            "matched": False,
                            "needs_review": False,
                        })
                        db.add_entry(stub)
                        reload_db()

                    update_sidecar_link(
                        sidecar_path, raw_name,
                        {
                            "matched": False,
                            "needs_review": False,
                            "stub_created": True,
                        }
                    )
                    st.info(f"Rejected match. Stub created for: {raw_name}")
                    st.rerun()

            with col_c:
                if st.button("Skip", key=f"skip_{i}"):
                    pass  # Just collapse the expander on next interaction


# ─────────────────────────────────────────────────────────────────────────────
# Page 2: Stub Review
# ─────────────────────────────────────────────────────────────────────────────

def _confidence_badge(confidence: float) -> str:
    """Return a colored emoji badge based on confidence score."""
    if confidence >= 0.80:
        return "🟢"
    elif confidence >= 0.55:
        return "🟡"
    else:
        return "🔴"


def _get_proposals(i: int, stub: Dict) -> Optional[Dict]:
    """Retrieve proposals from session state for this stub."""
    key = f"proposals_{i}"
    return st.session_state.get(key)


def _save_proposals(i: int, proposals: Dict) -> None:
    """Store enrichment proposals in session state."""
    st.session_state[f"proposals_{i}"] = proposals


def _field_val(proposals: Optional[Dict], stub: Dict, field: str, default: str = "") -> str:
    """Return proposal value if available, else stub value, else default."""
    if proposals and field in proposals and proposals[field] is not None:
        return str(proposals[field])
    val = stub.get(field)
    return str(val) if val is not None else default


def page_stub_review(db: OntologyDB) -> None:
    st.header("Stub Review")
    st.caption(
        "Auto-created stubs for organizations not in the ontology. "
        "Use **Auto-Enrich** to pre-fill fields via web search + AI, "
        "**Link to Existing** to merge duplicates, or fill manually and approve."
    )

    # Filter controls
    all_stubs = db.get_stubs()
    # Exclude merged and dismissed from the active queue
    stubs = [
        s for s in all_stubs
        if s.get("status") not in ("merged", "dismissed", "completed")
    ]

    dismissed_count = sum(1 for s in all_stubs if s.get("status") == "dismissed")
    merged_count = sum(1 for s in all_stubs if s.get("status") == "merged")
    approved_count = sum(1 for s in all_stubs if s.get("status") == "completed"
                         and s.get("source") == "auto_stub_approved")

    col_m1, col_m2, col_m3, col_m4 = st.columns(4)
    col_m1.metric("Pending", len(stubs))
    col_m2.metric("Approved", approved_count)
    col_m3.metric("Merged", merged_count)
    col_m4.metric("Dismissed", dismissed_count)

    if not stubs:
        st.success("No stubs pending review!")
        return

    # Filter bar
    with st.expander("Filter stubs", expanded=False):
        filter_col1, filter_col2, filter_col3 = st.columns(3)
        with filter_col1:
            filter_meta = st.multiselect(
                "Meta type",
                options=sorted(set(s.get("meta_type", "other") for s in stubs)),
            )
        with filter_col2:
            filter_text = st.text_input("Search name", placeholder="type to filter...")
        with filter_col3:
            filter_enriched = st.checkbox("Show enriched only")

    # Apply filters
    display_stubs = stubs
    if filter_meta:
        display_stubs = [s for s in display_stubs if s.get("meta_type") in filter_meta]
    if filter_text:
        ft = filter_text.lower()
        display_stubs = [s for s in display_stubs
                         if ft in s.get("canonical_name", "").lower()]
    if filter_enriched:
        display_stubs = [s for s in display_stubs
                         if f"proposals_{stubs.index(s)}" in st.session_state]

    st.caption(f"Showing {len(display_stubs)} of {len(stubs)} pending stubs")
    st.divider()

    # Build confirmed-org list once for the link autocomplete
    confirmed_orgs = get_confirmed_orgs(db)
    confirmed_names = [e.get("canonical_name", "") for e in confirmed_orgs]

    meta_type_options = ["io", "gov", "university", "ngo", "private", "other"]

    for i, stub in enumerate(display_stubs):
        # Use index in all_stubs for stable session_state keys
        stub_idx = stubs.index(stub) if stub in stubs else i
        cname = stub.get("canonical_name", "")
        proposals = _get_proposals(stub_idx, stub)
        conf = proposals.get("confidence", 0.0) if proposals else 0.0
        enriched_label = f"  {_confidence_badge(conf)} enriched" if proposals else ""

        with st.expander(f"{cname}{enriched_label}", expanded=False):

            # ── Section A: Link to existing org ──────────────────────────────
            st.subheader("Link to Existing Organization")
            st.caption(
                "If this org is already in the ontology under a different name, "
                "select it below. The stub name will be added as an alias."
            )
            link_search = st.text_input(
                "Search confirmed orgs",
                key=f"link_search_{stub_idx}",
                placeholder="Type to search...",
            )

            matching_confirmed = []
            if link_search and len(link_search) >= 2:
                ls_lower = link_search.lower()
                matching_confirmed = [
                    n for n in confirmed_names
                    if ls_lower in n.lower()
                ][:15]

            if matching_confirmed:
                link_target = st.selectbox(
                    f"{len(matching_confirmed)} match(es)",
                    options=["(select to link)"] + matching_confirmed,
                    key=f"link_target_{stub_idx}",
                )
                if link_target != "(select to link)":
                    if st.button(
                        f"Merge into → {link_target}",
                        key=f"link_btn_{stub_idx}",
                        type="primary",
                    ):
                        ok = merge_stub_into_entry(cname, link_target, db)
                        if ok:
                            reload_db()
                            st.success(
                                f"Merged! '{cname}' added as alias of '{link_target}'."
                            )
                            st.rerun()
                        else:
                            st.error("Merge failed — entry not found.")
            elif link_search and len(link_search) >= 2:
                st.caption("No confirmed orgs match that search.")

            st.divider()

            # ── Section B: Auto-Enrich ────────────────────────────────────────
            st.subheader("Auto-Enrich via Web Search + AI")

            enrich_col1, enrich_col2 = st.columns([2, 3])
            with enrich_col1:
                if st.button("Auto-Enrich", key=f"enrich_btn_{stub_idx}"):
                    with st.spinner(f"Searching for '{cname}'..."):
                        try:
                            existing_tags = db.get_all_tags()
                            result = enrich_stub(stub, existing_tags, use_cache=True)
                            _save_proposals(stub_idx, result)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Enrichment failed: {e}")

                if proposals:
                    if st.button(
                        "Re-fetch (bypass cache)",
                        key=f"refetch_btn_{stub_idx}",
                        help="Force a fresh search, ignoring cached results",
                    ):
                        with st.spinner("Re-fetching..."):
                            try:
                                existing_tags = db.get_all_tags()
                                result = enrich_stub(stub, existing_tags, use_cache=False)
                                _save_proposals(stub_idx, result)
                                st.rerun()
                            except Exception as e:
                                st.error(f"Re-fetch failed: {e}")

            with enrich_col2:
                if proposals:
                    method = proposals.get("enrichment_method", "")
                    sources = proposals.get("sources", [])
                    reasoning = proposals.get("reasoning", "")
                    st.caption(
                        f"{_confidence_badge(conf)} **Confidence: {conf:.0%}** "
                        f"| Method: `{method}` "
                        f"| Sources: {', '.join(sources) if sources else 'none'}"
                    )
                    if reasoning:
                        st.caption(f"_\"{reasoning}\"_")

            st.divider()

            # ── Section C: Form fields ────────────────────────────────────────
            st.subheader("Fields")
            col1, col2 = st.columns([3, 2])

            with col1:
                new_name = st.text_input(
                    "Canonical Name",
                    value=_field_val(proposals, stub, "canonical_name", cname),
                    key=f"stub_name_{stub_idx}",
                )

                cur_meta = _field_val(proposals, stub, "meta_type", "other")
                meta_idx = meta_type_options.index(cur_meta) if cur_meta in meta_type_options else 5
                new_meta_type = st.selectbox(
                    "Meta Type", options=meta_type_options,
                    index=meta_idx, key=f"stub_meta_{stub_idx}",
                )

                new_sector = st.text_input(
                    "Sector",
                    value=_field_val(proposals, stub, "sector", ""),
                    key=f"stub_sector_{stub_idx}",
                )

                col1a, col1b = st.columns(2)
                with col1a:
                    new_country = st.text_input(
                        "Country (ISO3)",
                        value=_field_val(proposals, stub, "location_country", ""),
                        key=f"stub_country_{stub_idx}",
                        placeholder="e.g. USA, GBR",
                    )
                with col1b:
                    new_city = st.text_input(
                        "City",
                        value=_field_val(proposals, stub, "location_city", ""),
                        key=f"stub_city_{stub_idx}",
                    )

                # Merge proposed + existing variations
                existing_vars = stub.get("variations_found", [])
                proposed_vars = proposals.get("variations_found", []) if proposals else []
                merged_vars = existing_vars[:]
                for v in proposed_vars:
                    if v and v not in merged_vars and v != cname:
                        merged_vars.append(v)

                variations_text = st.text_area(
                    "Variations / Aliases (one per line)",
                    value="\n".join(merged_vars),
                    key=f"stub_vars_{stub_idx}",
                    height=100,
                )

                # ── Parent org ────────────────────────────────────────────────
                st.caption("**Parent Organization** — for orgs that are affiliated "
                           "with or housed within a larger institution (e.g. J-PAL → MIT).")
                parent_search = st.text_input(
                    "Search for parent org",
                    key=f"parent_search_{stub_idx}",
                    placeholder="Type to search confirmed orgs...",
                    value=_field_val(proposals, stub, "parent_org", ""),
                )
                new_parent_org = parent_search  # default to typed value

                if parent_search and len(parent_search) >= 2:
                    ps_lower = parent_search.lower()
                    parent_matches = [
                        n for n in confirmed_names
                        if ps_lower in n.lower()
                    ][:10]
                    if parent_matches:
                        selected_parent = st.selectbox(
                            f"{len(parent_matches)} match(es)",
                            options=["(keep as typed)"] + parent_matches,
                            key=f"parent_select_{stub_idx}",
                        )
                        if selected_parent != "(keep as typed)":
                            new_parent_org = selected_parent
                            st.caption(f"Parent: **{new_parent_org}**")
                    else:
                        st.caption("No confirmed orgs match — will save as typed.")

            with col2:
                st.subheader("Hierarchical Tags")
                st.caption("Separate multiple tags with **;** — e.g. `university:research ; ngo:research:poverty`")

                # Pre-fill tag prefix from AI suggestion
                suggested_tag = proposals.get("suggested_tag", "") if proposals else ""

                tag_prefix = st.text_input(
                    "Tag prefix (type to search existing)",
                    value=suggested_tag[:30] if suggested_tag else "",
                    key=f"stub_tagprefix_{stub_idx}",
                    placeholder="e.g. UN:Funds  or  ngo:research",
                )

                final_tag = suggested_tag  # default to AI suggestion

                if tag_prefix and len(tag_prefix.strip()) >= 2:
                    suggestions = db.get_tag_completions(tag_prefix.strip())
                    if suggestions:
                        options = ["(type custom below)"] + suggestions[:25]
                        selected = st.selectbox(
                            f"Matching tags ({len(suggestions)} found)",
                            options=options,
                            key=f"stub_tagselect_{stub_idx}",
                        )
                        if selected != "(type custom below)":
                            # Append to final_tag with ";" if already has content
                            if final_tag and selected not in final_tag:
                                final_tag = f"{final_tag} ; {selected}"
                            else:
                                final_tag = selected
                            st.success(f"Added: `{selected}`")
                    else:
                        st.caption("No matching tags — use custom below.")

                custom_tag = st.text_input(
                    "Tags (';'-separated for multiple)",
                    value=suggested_tag if suggested_tag and not tag_prefix else "",
                    key=f"stub_customtag_{stub_idx}",
                    placeholder="e.g. ngo:research:poverty_economics ; university:research",
                )
                if custom_tag.strip():
                    final_tag = custom_tag.strip()

                if final_tag:
                    parsed = _parse_tags(final_tag)
                    st.caption(f"**{len(parsed)} tag(s) — hierarchical preview:**")
                    for ht in build_hierarchical_tags(final_tag):
                        st.code(ht)

            st.divider()

            # ── Section D: Action buttons ─────────────────────────────────────
            col_save, col_dismiss = st.columns(2)

            with col_save:
                if st.button(
                    "Approve & Save to Ontology",
                    key=f"stub_save_{stub_idx}",
                    type="primary",
                ):
                    updates: Dict = {
                        "canonical_name": new_name,
                        "meta_type": new_meta_type,
                        "sector": new_sector,
                        "location_country": new_country.strip() or None,
                        "location_city": new_city.strip() or None,
                        "variations_found": [
                            v.strip() for v in variations_text.split("\n")
                            if v.strip()
                        ],
                        "parent_org": new_parent_org.strip() or None,
                        "status": "completed",
                        "source": "auto_stub_approved",
                    }

                    if final_tag:
                        parsed_tags = _parse_tags(final_tag)
                        primary_tag = _canonical_tag_from_tags(parsed_tags)
                        htags = build_hierarchical_tags(final_tag)
                        if new_meta_type in ("io", "university"):
                            updates["un_ontology"] = {
                                "canonical_tag": primary_tag,
                                "all_tags": parsed_tags,
                                "hierarchical_tags": htags,
                                "tag_count": len(htags),
                                "status": "completed",
                            }
                        elif new_meta_type == "gov":
                            updates["gov_ontology"] = {
                                "canonical_tag": primary_tag,
                                "all_tags": parsed_tags,
                                "hierarchical_tags": htags,
                                "country": new_country.strip() or None,
                            }
                        else:
                            # ngo/private/other — store under un_ontology for now
                            updates["un_ontology"] = {
                                "canonical_tag": primary_tag,
                                "all_tags": parsed_tags,
                                "hierarchical_tags": htags,
                                "tag_count": len(htags),
                                "status": "completed",
                            }

                    success = db.update_entry(cname, updates)
                    if success:
                        reload_db()
                        st.success(f"Saved: {new_name}")
                        st.rerun()
                    else:
                        st.error("Update failed — entry not found.")

            with col_dismiss:
                if st.button(
                    "Dismiss",
                    key=f"stub_dismiss_{stub_idx}",
                    help="Mark as dismissed — won't appear in review queue. Data is preserved.",
                ):
                    db.update_entry(cname, {
                        "status": "dismissed",
                        "source": "auto_stub_dismissed",
                    })
                    reload_db()
                    st.info(f"Dismissed: {cname}")
                    st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# Page 3: Ontology Browser
# ─────────────────────────────────────────────────────────────────────────────

def page_ontology_browser(db: OntologyDB) -> None:
    st.header("Ontology Browser")

    all_entries = db.get_all()

    # ── Sidebar filters ────────────────────────────────────────────────────────
    with st.sidebar:
        st.subheader("Filters")

        available_meta_types = sorted(set(
            e.get("meta_type", "") for e in all_entries if e.get("meta_type")
        ))
        filter_meta_type = st.multiselect(
            "Meta Type", options=available_meta_types
        )

        available_sectors = sorted(set(
            e.get("sector", "") for e in all_entries if e.get("sector")
        ))
        filter_sector = st.multiselect("Sector", options=available_sectors)

        available_countries = sorted(set(
            e.get("location_country", "") or ""
            for e in all_entries
        ) - {""})
        filter_country = st.multiselect("Country", options=available_countries)

        filter_source = st.multiselect(
            "Source",
            options=sorted(set(e.get("source", "") for e in all_entries if e.get("source")))
        )

        filter_text = st.text_input(
            "Search name / alias",
            placeholder="Type to filter...",
        )

        filter_stubs_only = st.checkbox("Show stubs only")

        st.divider()
        view_mode = st.radio("View mode", ["Table", "Cards"])

    # ── Apply filters ─────────────────────────────────────────────────────────
    filtered = all_entries

    if filter_meta_type:
        filtered = [e for e in filtered if e.get("meta_type") in filter_meta_type]
    if filter_sector:
        filtered = [e for e in filtered if e.get("sector") in filter_sector]
    if filter_country:
        filtered = [e for e in filtered if e.get("location_country") in filter_country]
    if filter_source:
        filtered = [e for e in filtered if e.get("source") in filter_source]
    if filter_text:
        ft = filter_text.lower()
        filtered = [
            e for e in filtered
            if ft in (e.get("canonical_name") or "").lower()
            or any(ft in (v or "").lower() for v in e.get("variations_found", []))
        ]
    if filter_stubs_only:
        filtered = [
            e for e in filtered
            if e.get("source") == "auto_stub" or e.get("status") == "pending_review"
        ]

    col_m1, col_m2, col_m3 = st.columns(3)
    col_m1.metric("Showing", len(filtered))
    col_m2.metric("Total", len(all_entries))
    col_m3.metric("Stubs", len(db.get_stubs()))

    st.divider()

    # ── Table view ────────────────────────────────────────────────────────────
    if view_mode == "Table":
        import pandas as pd

        rows = []
        for e in filtered:
            un = e.get("un_ontology") or {}
            gov = e.get("gov_ontology") or {}
            tag = un.get("canonical_tag") or gov.get("canonical_tag") or ""
            rows.append({
                "Name": e.get("canonical_name", ""),
                "Meta Type": e.get("meta_type", ""),
                "Sector": e.get("sector", ""),
                "Country": e.get("location_country") or "",
                "Tag": tag,
                "Variations": len(e.get("variations_found", [])),
                "Source": e.get("source", ""),
                "Status": e.get("status", ""),
            })

        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, height=550)

    # ── Cards view ────────────────────────────────────────────────────────────
    else:
        if len(filtered) > 50:
            st.info(f"Showing first 50 of {len(filtered)} results. Use filters to narrow.")
            display = filtered[:50]
        else:
            display = filtered

        for entry in display:
            cname = entry.get("canonical_name", "")
            meta = entry.get("meta_type", "")
            sector = entry.get("sector", "")

            with st.expander(f"{cname}  |  `{meta}` / `{sector}`", expanded=False):
                col1, col2 = st.columns([2, 2])

                with col1:
                    st.json(entry, expanded=False)

                with col2:
                    st.subheader("Edit Variations")
                    current_vars = "\n".join(entry.get("variations_found", []))
                    new_vars = st.text_area(
                        "Variations (one per line)",
                        value=current_vars,
                        key=f"browser_vars_{cname}",
                        height=120,
                    )

                    if st.button("Save variations", key=f"browser_save_vars_{cname}"):
                        updated = [v.strip() for v in new_vars.split("\n") if v.strip()]
                        success = db.update_entry(cname, {"variations_found": updated})
                        if success:
                            reload_db()
                            st.success("Saved!")
                            st.rerun()
                        else:
                            st.error("Update failed.")

                    st.subheader("Edit Status")
                    status_options = ["completed", "pending_review", "flagged"]
                    cur_status = entry.get("status", "completed")
                    new_status = st.selectbox(
                        "Status",
                        options=status_options,
                        index=status_options.index(cur_status)
                        if cur_status in status_options else 0,
                        key=f"browser_status_{cname}",
                    )
                    if st.button("Save status", key=f"browser_save_status_{cname}"):
                        db.update_entry(cname, {"status": new_status})
                        reload_db()
                        st.success("Status updated!")
                        st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# Stats sidebar
# ─────────────────────────────────────────────────────────────────────────────

def render_sidebar_stats(db: OntologyDB, sidecars: List[Dict]) -> str:
    """Render navigation and stats in the sidebar. Returns selected page."""
    st.sidebar.title("Ontology Review")

    page = st.sidebar.radio(
        "Navigate to",
        ["Pending Reviews", "Stub Review", "Ontology Browser"],
    )

    st.sidebar.divider()
    st.sidebar.subheader("Quick Stats")

    all_entries = db.get_all()
    pending_stubs = db.get_pending_stubs()

    match_review_count = sum(
        1 for s in sidecars
        for l in s.get("org_links", [])
        if l.get("needs_review")
    )

    st.sidebar.metric("Ontology Entries", len(all_entries))
    st.sidebar.metric("Stubs Pending", len(pending_stubs))
    st.sidebar.metric("Match Reviews", match_review_count)
    st.sidebar.metric("Sidecar Files", len(sidecars))

    return page


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    db = get_db()
    sidecars = load_all_sidecar_files()

    page = render_sidebar_stats(db, sidecars)

    if page == "Pending Reviews":
        page_pending_reviews(db, sidecars)
    elif page == "Stub Review":
        page_stub_review(db)
    elif page == "Ontology Browser":
        page_ontology_browser(db)


if __name__ == "__main__":
    main()
