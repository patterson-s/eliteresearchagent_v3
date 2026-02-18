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
    "UN:Foo:Bar" -> ["UN", "UN:Foo", "UN:Foo:Bar"]
    """
    if not tag:
        return []
    parts = tag.split(":")
    return [":".join(parts[:i]) for i in range(1, len(parts) + 1)]


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

def page_stub_review(db: OntologyDB) -> None:
    st.header("Stub Review")
    st.caption(
        "Auto-created stub entries for organizations not found in the ontology. "
        "Fill in details and approve to add them permanently."
    )

    stubs = db.get_stubs()

    if not stubs:
        st.success("No stubs pending review!")
        return

    st.metric("Stubs Pending", len(stubs))
    st.divider()

    meta_type_options = ["io", "gov", "university", "ngo", "private", "other"]

    for i, stub in enumerate(stubs):
        cname = stub.get("canonical_name", "")
        with st.expander(f"{cname}", expanded=False):
            col1, col2 = st.columns([3, 2])

            with col1:
                new_name = st.text_input(
                    "Canonical Name", value=cname, key=f"stub_name_{i}"
                )

                cur_meta = stub.get("meta_type", "other")
                meta_idx = meta_type_options.index(cur_meta) if cur_meta in meta_type_options else 5
                new_meta_type = st.selectbox(
                    "Meta Type", options=meta_type_options,
                    index=meta_idx, key=f"stub_meta_{i}"
                )

                new_sector = st.text_input(
                    "Sector", value=stub.get("sector", ""), key=f"stub_sector_{i}"
                )

                col1a, col1b = st.columns(2)
                with col1a:
                    new_country = st.text_input(
                        "Country (ISO3)", value=stub.get("location_country") or "",
                        key=f"stub_country_{i}", placeholder="e.g. USA, GBR"
                    )
                with col1b:
                    new_city = st.text_input(
                        "City", value=stub.get("location_city") or "",
                        key=f"stub_city_{i}"
                    )

                variations_text = st.text_area(
                    "Variations / Aliases (one per line)",
                    value="\n".join(stub.get("variations_found", [])),
                    key=f"stub_vars_{i}",
                    height=100,
                )

            with col2:
                st.subheader("Hierarchical Tag")
                st.caption(
                    "Type a prefix to search existing tags. "
                    "Select from dropdown or enter a custom tag."
                )

                tag_prefix = st.text_input(
                    "Tag prefix (type to search)",
                    value="",
                    key=f"stub_tagprefix_{i}",
                    placeholder="e.g. UN:Funds  or  national_government",
                )

                final_tag = ""
                if tag_prefix and len(tag_prefix.strip()) >= 2:
                    suggestions = db.get_tag_completions(tag_prefix.strip())
                    if suggestions:
                        options = ["(type custom below)"] + suggestions[:25]
                        selected = st.selectbox(
                            f"Matching tags ({len(suggestions)} found)",
                            options=options,
                            key=f"stub_tagselect_{i}",
                        )
                        if selected != "(type custom below)":
                            final_tag = selected
                            st.success(f"Selected: `{final_tag}`")
                    else:
                        st.caption("No matching tags found.")

                custom_tag = st.text_input(
                    "Custom tag (if not selecting above)",
                    value="",
                    key=f"stub_customtag_{i}",
                    placeholder="e.g. ngo:research:poverty_action_lab",
                )
                if custom_tag.strip():
                    final_tag = custom_tag.strip()
                    st.info(f"Custom tag: `{final_tag}`")

                if final_tag:
                    st.caption("**Preview hierarchical_tags:**")
                    for ht in build_hierarchical_tags(final_tag):
                        st.code(ht)

            st.divider()
            col_save, col_delete = st.columns(2)

            with col_save:
                if st.button(
                    "Approve & Save to Ontology",
                    key=f"stub_save_{i}",
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
                        "status": "completed",
                        "source": "auto_stub_approved",
                    }

                    # Build appropriate tag structure
                    if final_tag:
                        htags = build_hierarchical_tags(final_tag)
                        if new_meta_type in ("io", "university"):
                            updates["un_ontology"] = {
                                "canonical_tag": final_tag,
                                "hierarchical_tags": htags,
                                "tag_count": len(htags),
                                "status": "completed",
                            }
                        elif new_meta_type == "gov":
                            updates["gov_ontology"] = {
                                "canonical_tag": final_tag,
                                "hierarchical_tags": [final_tag],
                                "country": new_country.strip() or None,
                            }

                    success = db.update_entry(cname, updates)
                    if success:
                        reload_db()
                        st.success(f"Saved: {new_name}")
                        st.rerun()
                    else:
                        st.error("Entry not found in ontology — it may have been deleted.")

            with col_delete:
                if st.button(
                    "Delete Stub",
                    key=f"stub_delete_{i}",
                    help="Remove this stub without adding it to the ontology",
                ):
                    st.warning(
                        f"Delete '{cname}'? This cannot be undone from here. "
                        "Edit the ontology JSON file directly to remove it.",
                        icon="⚠️",
                    )


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

    stubs = db.get_stubs()
    all_entries = db.get_all()

    pending_count = sum(
        1 for s in sidecars
        for l in s.get("org_links", [])
        if l.get("needs_review")
    )

    st.sidebar.metric("Ontology Entries", len(all_entries))
    st.sidebar.metric("Stubs Pending", len(stubs))
    st.sidebar.metric("Match Reviews", pending_count)
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
