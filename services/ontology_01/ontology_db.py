"""
ontology_db.py — Single source of truth for all reads and writes to unified_ontology.json.

All other ontology_01 modules import from this. Nothing else touches the JSON file directly.
"""

import json
import os
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Set

ONTOLOGY_PATH = Path(__file__).resolve().parent / "unified_ontology.json"


class OntologyDB:
    """
    In-memory ontology store with indexed lookups. Provides atomic writes back to disk.
    """

    def __init__(self, path: Path = ONTOLOGY_PATH):
        self._path = path
        self._entries: List[Dict] = []
        self._canonical_index: Dict[str, Dict] = {}
        self._variation_index: Dict[str, Dict] = {}
        self._meta_type_index: Dict[str, List[Dict]] = {}
        self._tag_completions: Dict[str, Set[str]] = {}
        self._load()
        self._build_indexes()

    def _load(self) -> None:
        if not self._path.exists():
            raise FileNotFoundError(
                f"Ontology file not found at: {self._path}\n"
                "Expected unified_ontology.json in the ontology_01 service directory."
            )
        with open(self._path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if "unified_ontology" not in data:
            raise ValueError(
                f"Expected root key 'unified_ontology' in {self._path}, "
                f"but found keys: {list(data.keys())}"
            )
        self._entries = data["unified_ontology"]

    def _build_indexes(self) -> None:
        self._canonical_index = {}
        self._variation_index = {}
        self._meta_type_index = {}
        self._tag_completions = {}

        for entry in self._entries:
            # Canonical index
            cname = entry.get("canonical_name", "")
            if cname:
                self._canonical_index[cname.lower().strip()] = entry

            # Variation index
            for var in entry.get("variations_found", []):
                if var:
                    self._variation_index[var.lower().strip()] = entry

            # Meta-type index
            meta = entry.get("meta_type", "")
            if meta:
                self._meta_type_index.setdefault(meta, []).append(entry)

            # Tag trie — collect all hierarchical tags from both sub-ontologies
            all_tags: List[str] = []
            un = entry.get("un_ontology") or {}
            gov = entry.get("gov_ontology") or {}
            all_tags.extend(un.get("hierarchical_tags", []))
            all_tags.extend(gov.get("hierarchical_tags", []))

            for tag in all_tags:
                if not tag:
                    continue
                # Store the full tag keyed by every prefix of itself
                parts = tag.split(":")
                for i in range(1, len(parts) + 1):
                    prefix = ":".join(parts[:i])
                    self._tag_completions.setdefault(prefix, set()).add(tag)
                # Also key by empty string so "get all tags" works
                self._tag_completions.setdefault("", set()).add(tag)

    # -------------------------------------------------------------------------
    # Read operations
    # -------------------------------------------------------------------------

    def get_all(self) -> List[Dict]:
        """Return a shallow copy of all entries."""
        return list(self._entries)

    def get_by_meta_type(self, meta_type: str) -> List[Dict]:
        """Return all entries matching the given meta_type."""
        return list(self._meta_type_index.get(meta_type, []))

    def lookup_canonical(self, name: str) -> Optional[Dict]:
        """Case-insensitive exact lookup on canonical_name."""
        return self._canonical_index.get(name.lower().strip())

    def lookup_variation(self, name: str) -> Optional[Dict]:
        """Case-insensitive exact lookup across all variations_found strings."""
        return self._variation_index.get(name.lower().strip())

    def get_all_tags(self) -> List[str]:
        """Return all distinct canonical_tag values across all entries."""
        tags: Set[str] = set()
        for entry in self._entries:
            un = entry.get("un_ontology") or {}
            gov = entry.get("gov_ontology") or {}
            ct = un.get("canonical_tag") or gov.get("canonical_tag")
            if ct:
                tags.add(ct)
        return sorted(tags)

    def get_tag_completions(self, prefix: str) -> List[str]:
        """
        Return all canonical/hierarchical tags that start with the given prefix.
        Used for the Streamlit autocomplete widget.
        """
        prefix = prefix.strip()
        matches: Set[str] = set()

        if not prefix:
            return sorted(self._tag_completions.get("", set()))

        # Direct prefix lookup in trie
        for stored_prefix, tags in self._tag_completions.items():
            if stored_prefix.lower().startswith(prefix.lower()) or prefix.lower().startswith(stored_prefix.lower()):
                for tag in tags:
                    if tag.lower().startswith(prefix.lower()):
                        matches.add(tag)

        return sorted(matches)

    def get_stubs(self) -> List[Dict]:
        """
        Return all auto-created stub entries regardless of sub-status.
        Includes pending, dismissed, and merged — callers filter as needed.
        Use get_pending_stubs() for the active review queue.
        """
        return [
            e for e in self._entries
            if e.get("source") in ("auto_stub",) or e.get("status") == "pending_review"
        ]

    def get_pending_stubs(self) -> List[Dict]:
        """Return stubs that are still pending review (not dismissed, merged, or approved)."""
        return [
            e for e in self.get_stubs()
            if e.get("status") not in ("dismissed", "merged", "completed")
        ]

    def count(self) -> int:
        return len(self._entries)

    # -------------------------------------------------------------------------
    # Write operations
    # -------------------------------------------------------------------------

    def add_entry(self, entry: Dict) -> None:
        """Append a new entry, rebuild indexes, and write to disk."""
        self._entries.append(entry)
        self._build_indexes()
        self._atomic_write()

    def add_entries(self, entries: List[Dict]) -> None:
        """Append multiple entries in one write — more efficient than add_entry() in a loop."""
        self._entries.extend(entries)
        self._build_indexes()
        self._atomic_write()

    def update_entry(self, canonical_name: str, updates: Dict) -> bool:
        """
        Find entry by canonical_name (exact, case-insensitive), apply updates, write to disk.
        Returns True if found and updated, False if not found.
        """
        key = canonical_name.lower().strip()
        for i, entry in enumerate(self._entries):
            if entry.get("canonical_name", "").lower().strip() == key:
                self._entries[i] = {**entry, **updates}
                self._build_indexes()
                self._atomic_write()
                return True
        return False

    def save(self) -> None:
        """Force write current state to disk."""
        self._atomic_write()

    def _atomic_write(self) -> None:
        """
        Write to a temp file then os.replace() for atomicity.
        Prevents corruption on crash mid-write. Works on Windows.
        """
        data = {"unified_ontology": self._entries}
        dir_ = self._path.parent
        # Write to a temp file in the same directory (ensures same filesystem for os.replace)
        fd, tmp_path = tempfile.mkstemp(dir=dir_, suffix=".tmp", prefix=".ontology_")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            os.replace(tmp_path, self._path)
        except Exception:
            # Clean up temp file on failure
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def reload(self) -> None:
        """Reload from disk and rebuild indexes. Useful after external modifications."""
        self._load()
        self._build_indexes()
