"""
report.py — PDF biographical profile report generator for targeted_01.

Reads the 7 structured JSON output files per person (Q1–Q7) and generates
a human-readable, multi-page PDF report. Each question gets its own page,
preceded by a cover page.

CLI:
    python report.py --person "Abhijit Banerjee"
    python report.py --person "Name1" "Name2"
    python report.py --all-complete        # auto-discover people with all 7 files
    python report.py --all-complete --output /path/to/dir
    python report.py --person "Abhijit Banerjee" --input /path/to/outputs
"""

import os
import sys
import json
import argparse
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm, mm
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    HRFlowable,
    PageBreak,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)

# ── Paths ──────────────────────────────────────────────────────────────────────
_SERVICE_DIR = Path(__file__).resolve().parent
_OUTPUT_DIR = _SERVICE_DIR / "outputs"

# ── Constants ──────────────────────────────────────────────────────────────────
EXPECTED_SUFFIXES = [
    "hlp_job_title",
    "education",
    "locations",
    "jobs",
    "sectors",
    "networks",
    "career_domain",
]

SECTION_TITLES = {
    "hlp_job_title":   "Q1 — HLP Job Title at Nomination",
    "education":       "Q2 — Education Trajectory",
    "locations":       "Q3 — Geographical Mobility",
    "jobs":            "Q4 — Organizational Mobility",
    "sectors":         "Q5 — Sectors of Expertise",
    "networks":        "Q6 — Professional Networks & Awards",
    "career_domain":   "Q7 — Career Domain Classification",
}

# Maps each list-question suffix to the field name used to identify the richest
# extraction entry (the one with the most items in that list).
_PRIMARY_LIST_FIELDS = {
    "education": "degrees_found",
    "locations": "locations",
    "jobs":      "jobs",
    "sectors":   "sectors",
    "networks":  "affiliations",
}

# ── Colour palette ─────────────────────────────────────────────────────────────
NAVY       = colors.HexColor("#1a3a5c")
BLUE       = colors.HexColor("#2e86ab")
LIGHTBLUE  = colors.HexColor("#d0e8f2")
ROWALT     = colors.HexColor("#f5f7fa")
TEXTDARK   = colors.HexColor("#1a1a1a")
MUTED      = colors.HexColor("#555555")
GREEN      = colors.HexColor("#2e7d32")
AMBER      = colors.HexColor("#e65100")
RED        = colors.HexColor("#b71c1c")
GREY       = colors.HexColor("#757575")

STATUS_COLOUR = {
    "found_and_verified":          GREEN,
    "found_no_confirming_sources": AMBER,
    "found":                       BLUE,
    "cannot_determine":            GREY,
    "no_chunks_retrieved":         GREY,
    "skipped":                     GREY,
    "error":                       RED,
    "exception":                   RED,
}

STATUS_LABEL = {
    "found_and_verified":          "Found & Verified",
    "found_no_confirming_sources": "Found (unconfirmed)",
    "found":                       "Found",
    "cannot_determine":            "Cannot Determine",
    "no_chunks_retrieved":         "No Source Data",
    "skipped":                     "Skipped",
    "error":                       "Error",
}


# ── Style registry ─────────────────────────────────────────────────────────────

def _make_styles() -> Dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    S = {}

    def s(name, **kw):
        parent = kw.pop("parent", "Normal")
        S[name] = ParagraphStyle(name, parent=base[parent], **kw)

    s("CoverName",
      fontName="Helvetica-Bold", fontSize=28, leading=34,
      textColor=NAVY, spaceAfter=6)
    s("CoverSub",
      fontName="Helvetica", fontSize=13, leading=18,
      textColor=MUTED, spaceAfter=4)
    s("CoverMeta",
      fontName="Helvetica", fontSize=11, leading=15,
      textColor=TEXTDARK, spaceAfter=3)
    s("SectionHeading",
      fontName="Helvetica-Bold", fontSize=16, leading=20,
      textColor=NAVY, spaceBefore=6, spaceAfter=8)
    s("SubHeading",
      fontName="Helvetica-Bold", fontSize=11, leading=14,
      textColor=NAVY, spaceBefore=10, spaceAfter=4)
    s("KeyFinding",
      fontName="Helvetica-Bold", fontSize=18, leading=22,
      textColor=NAVY, spaceAfter=4)
    s("KeySub",
      fontName="Helvetica", fontSize=13, leading=17,
      textColor=BLUE, spaceAfter=8)
    s("Body",
      fontName="Helvetica", fontSize=10, leading=14,
      textColor=TEXTDARK, spaceAfter=4)
    s("BodyBold",
      fontName="Helvetica-Bold", fontSize=10, leading=14,
      textColor=TEXTDARK, spaceAfter=4)
    s("Quote",
      fontName="Helvetica-Oblique", fontSize=9.5, leading=13,
      textColor=MUTED, leftIndent=16, rightIndent=16,
      spaceBefore=8, spaceAfter=8)
    s("Caption",
      fontName="Helvetica", fontSize=8.5, leading=12,
      textColor=MUTED, spaceAfter=3)
    s("DomainBig",
      fontName="Helvetica-Bold", fontSize=36, leading=42,
      textColor=NAVY, spaceAfter=4, alignment=TA_CENTER)
    s("TableHeader",
      fontName="Helvetica-Bold", fontSize=9, leading=12,
      textColor=colors.white)
    s("TableCell",
      fontName="Helvetica", fontSize=9, leading=12,
      textColor=TEXTDARK)
    s("TableCellBold",
      fontName="Helvetica-Bold", fontSize=9, leading=12,
      textColor=TEXTDARK)
    s("SummaryLabel",
      fontName="Helvetica", fontSize=10, leading=13,
      textColor=TEXTDARK)
    s("SummaryStatus",
      fontName="Helvetica-Bold", fontSize=10, leading=13)
    s("Footer",
      fontName="Helvetica", fontSize=8, leading=10,
      textColor=MUTED, alignment=TA_CENTER)
    return S


STYLES = _make_styles()
PAGE_W, PAGE_H = A4
MARGIN = 2 * cm


# ── Header / Footer canvas callback ────────────────────────────────────────────

def _make_header_footer(person_display: str, section_title: str, gen_date: str):
    """Return an onPage callback that draws the running header and footer."""
    def _draw(canvas, doc):
        canvas.saveState()
        w, h = A4

        # ── Header ─────────────────────────────────────────────────────────
        y_header = h - MARGIN + 4 * mm
        canvas.setStrokeColor(NAVY)
        canvas.setLineWidth(0.8)
        canvas.line(MARGIN, y_header - 3 * mm, w - MARGIN, y_header - 3 * mm)

        canvas.setFont("Helvetica-Bold", 8)
        canvas.setFillColor(NAVY)
        canvas.drawString(MARGIN, y_header, person_display.upper())

        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(MUTED)
        canvas.drawRightString(w - MARGIN, y_header, section_title)

        # ── Footer ─────────────────────────────────────────────────────────
        y_footer = MARGIN - 6 * mm
        canvas.setLineWidth(0.5)
        canvas.line(MARGIN, y_footer + 4 * mm, w - MARGIN, y_footer + 4 * mm)

        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(MUTED)
        canvas.drawCentredString(w / 2, y_footer, f"Page {doc.page}")
        canvas.drawRightString(w - MARGIN, y_footer, f"Generated {gen_date}")
        canvas.drawString(MARGIN, y_footer, "eliteresearchagent_v3")

        canvas.restoreState()
    return _draw


# ── Small helpers ──────────────────────────────────────────────────────────────

def _hr():
    return HRFlowable(width="100%", thickness=0.5, color=LIGHTBLUE, spaceAfter=8)


def _status_dot_and_label(status: str) -> str:
    """Return an HTML-coloured status label for use in a Paragraph."""
    col = STATUS_COLOUR.get(status, GREY)
    label = STATUS_LABEL.get(status, status)
    hex_col = col.hexval() if hasattr(col, "hexval") else "#757575"
    return f'<font color="{hex_col}"><b>{label}</b></font>'


def _conf_label(conf: Optional[str]) -> str:
    if conf == "high":
        return '<font color="#2e7d32">High</font>'
    if conf == "medium":
        return '<font color="#e65100">Medium</font>'
    if conf == "low":
        return '<font color="#b71c1c">Low</font>'
    return conf or "—"


def _cell(text: str, bold: bool = False, wrap: bool = True) -> Paragraph:
    style = STYLES["TableCellBold"] if bold else STYLES["TableCell"]
    return Paragraph(str(text) if text else "—", style)


def _val(d: Dict, *keys, default="—"):
    """Safe nested get with fallback."""
    for k in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(k, None)
        if d is None:
            return default
    return d if d != "" else default


def _table_style(has_header: bool = True) -> TableStyle:
    cmds = [
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, ROWALT]),
        ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#cccccc")),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]
    if not has_header:
        cmds = cmds[4:]
    return TableStyle(cmds)


def _no_data_block(suffix: str) -> List:
    """Placeholder when a question's output file is missing."""
    return [
        Spacer(1, 1 * cm),
        Paragraph(f"No data available for {suffix}.", STYLES["Body"]),
    ]


def _section_header(title: str) -> List:
    return [
        Paragraph(title, STYLES["SectionHeading"]),
        _hr(),
    ]


def _supporting_quote(result: Dict) -> List:
    quote = result.get("supporting_quote")
    if not quote or quote == "—":
        return []
    return [
        Paragraph(f'"{quote}"', STYLES["Quote"]),
    ]


def _provenance_line(result: Dict) -> List:
    domain = result.get("primary_source_domain")
    conf = result.get("confidence")
    count = result.get("confirmation_count")
    parts = []
    if conf:
        parts.append(f"Confidence: {conf}")
    if domain:
        parts.append(f"Primary source: {domain}")
    if count is not None:
        parts.append(f"Verified by {count} independent source(s)")
    if not parts:
        return []
    return [Paragraph("  ·  ".join(parts), STYLES["Caption"])]


# ── Reasoning helpers ──────────────────────────────────────────────────────────

def _get_best_reasoning(data: Dict, suffix: str) -> str:
    """
    Extract the step-by-step reasoning text from the best LLM extraction entry.

    For Q7 (career_domain): reasoning sits at the top-level "parsed" object.
    For Q1 (hlp_job_title): use the first extraction_trace entry where
        cannot_determine is False and no error occurred.
    For Q2-Q6 (list questions): use the entry whose primary list field has
        the most items, matching the runner.py richest-selection logic.

    Args:
        data: The full loaded JSON for one question (or None).
        suffix: The question suffix, e.g. "education", "career_domain".

    Returns:
        Reasoning text string, or "" if none available.
    """
    if not data:
        return ""

    # Q7: reasoning is in top-level "parsed"
    if suffix == "career_domain":
        return data.get("parsed", {}).get("reasoning", "") or ""

    trace = data.get("extraction_trace", [])
    if not trace:
        return ""

    list_field = _PRIMARY_LIST_FIELDS.get(suffix)

    if list_field:
        # Richest-entry selection: entry with the most items in primary list
        best_entry = None
        best_count = -1
        for entry in trace:
            parsed = entry.get("parsed") or {}
            if entry.get("error") or parsed.get("cannot_determine", True):
                continue
            count = len(parsed.get(list_field, []))
            if count > best_count:
                best_count = count
                best_entry = parsed
        return (best_entry or {}).get("reasoning", "") or ""
    else:
        # Q1 / single-fact: first non-null, non-error entry
        for entry in trace:
            parsed = entry.get("parsed") or {}
            if entry.get("error") or parsed.get("cannot_determine", True):
                continue
            return parsed.get("reasoning", "") or ""
    return ""


def _reasoning_block(text: str) -> List:
    """
    Render step-by-step reasoning as a labelled section.

    Returns an empty list if text is absent or whitespace-only,
    so callers can always do `story += _reasoning_block(...)` safely.

    Args:
        text: The raw reasoning string from the LLM (typically "Step 1 — ...").

    Returns:
        List of Platypus flowables, or [].
    """
    if not text or not text.strip():
        return []
    return [
        Spacer(1, 6),
        Paragraph("Analytical Reasoning", STYLES["SubHeading"]),
        Paragraph(text.strip(), STYLES["Body"]),
    ]


# ── Page builders ──────────────────────────────────────────────────────────────

def build_cover_page(person_data: Dict, person_display: str) -> List:
    """Cover page: biographical overview + 7-question status summary."""
    story = []

    # Get input metadata from any available file
    input_meta = {}
    for suffix in EXPECTED_SUFFIXES:
        d = person_data.get(suffix)
        if d and d.get("input"):
            input_meta = d["input"]
            break

    # ── Name ───────────────────────────────────────────────────────────────
    story.append(Spacer(1, 1.5 * cm))
    story.append(Paragraph(person_display, STYLES["CoverName"]))

    # ── HLP info ───────────────────────────────────────────────────────────
    hlp_name = input_meta.get("hlp_name", "")
    hlp_year = input_meta.get("hlp_year", "")
    if hlp_name:
        story.append(Paragraph(hlp_name, STYLES["CoverSub"]))
    if hlp_year:
        story.append(Paragraph(f"Panel years: {hlp_year}", STYLES["CoverMeta"]))

    # ── Biographical metadata ──────────────────────────────────────────────
    birth_year  = input_meta.get("birth_year", "")
    nom_age     = input_meta.get("hlp_nomination_age", "")
    nationality = input_meta.get("nationality", [])
    if isinstance(nationality, list):
        nationality = ", ".join(nationality)

    meta_lines = []
    if birth_year:
        meta_lines.append(f"Born: {birth_year}")
    if nom_age:
        meta_lines.append(f"Age at nomination: {nom_age}")
    if nationality:
        meta_lines.append(f"Nationality: {nationality}")
    for line in meta_lines:
        story.append(Paragraph(line, STYLES["CoverMeta"]))

    story.append(Spacer(1, 1 * cm))
    story.append(_hr())

    # ── Status summary table ───────────────────────────────────────────────
    story.append(Paragraph("Research Summary", STYLES["SubHeading"]))
    story.append(Spacer(1, 4))

    rows = [
        [
            Paragraph("Question", STYLES["TableHeader"]),
            Paragraph("Status", STYLES["TableHeader"]),
            Paragraph("Confidence", STYLES["TableHeader"]),
        ]
    ]
    for suffix, title in SECTION_TITLES.items():
        d = person_data.get(suffix)
        if d:
            result = d.get("result", {})
            status  = result.get("status", "—")
            conf    = result.get("confidence", "—")
            status_html = _status_dot_and_label(status)
            conf_html   = _conf_label(conf)
        else:
            status_html = _status_dot_and_label("error")
            conf_html   = "—"

        rows.append([
            _cell(title),
            Paragraph(status_html, STYLES["TableCell"]),
            Paragraph(conf_html,   STYLES["TableCell"]),
        ])

    usable_w = PAGE_W - 2 * MARGIN
    t = Table(rows, colWidths=[usable_w * 0.58, usable_w * 0.28, usable_w * 0.14])
    t.setStyle(_table_style())
    story.append(t)

    # ── Footer note ────────────────────────────────────────────────────────
    story.append(Spacer(1, 1.5 * cm))
    story.append(Paragraph(
        "This report was generated automatically by eliteresearchagent_v3 using "
        "retrieval-augmented generation over web-sourced biographical documents. "
        "Findings should be treated as research-grade summaries, not authoritative records.",
        STYLES["Caption"],
    ))
    story.append(PageBreak())
    return story


def build_q1_page(data: Optional[Dict]) -> List:
    story = _section_header(SECTION_TITLES["hlp_job_title"])
    if data is None:
        return story + _no_data_block("hlp_job_title")

    result = data.get("result", {})
    inp    = data.get("input", {})

    job_title = result.get("job_title_at_nomination") or "Not determined"
    org       = result.get("organization_at_nomination") or ""
    year_ref  = result.get("year_reference") or inp.get("nomination_year") or ""
    nom_age   = inp.get("hlp_nomination_age", "")

    story.append(Paragraph(job_title, STYLES["KeyFinding"]))
    if org:
        story.append(Paragraph(org, STYLES["KeySub"]))
    story.append(Spacer(1, 6))

    # ── Detail row ─────────────────────────────────────────────────────────
    details = []
    if year_ref:
        details.append(f"Year of nomination: {year_ref}")
    if nom_age:
        details.append(f"Age at nomination: {nom_age}")
    if details:
        story.append(Paragraph("   ·   ".join(details), STYLES["Body"]))

    story += _reasoning_block(_get_best_reasoning(data, "hlp_job_title"))
    story += _provenance_line(result)
    story += _supporting_quote(result)
    story.append(PageBreak())
    return story


def build_q2_page(data: Optional[Dict]) -> List:
    story = _section_header(SECTION_TITLES["education"])
    if data is None:
        return story + _no_data_block("education") + [PageBreak()]

    result = data.get("result", {})
    degrees   = result.get("degrees_found", [])
    geo       = result.get("geographic_category", "")
    prestige  = result.get("institution_prestige", "")
    discs     = result.get("disciplines", [])
    elite_i   = set(result.get("elite_institutions_found", []))

    # ── Summary line ───────────────────────────────────────────────────────
    summary_parts = []
    if geo:
        geo_label = {"global_north": "Global North", "global_south": "Global South",
                     "both": "Global North & South", "cannot_determine": "Unknown"}.get(geo, geo)
        summary_parts.append(f"Geographic reach: {geo_label}")
    if prestige:
        pres_label = {"elite": "Elite institutions", "peripheral": "Peripheral institutions",
                      "both": "Elite & peripheral", "cannot_determine": "Unknown"}.get(prestige, prestige)
        summary_parts.append(f"Prestige: {pres_label}")
    if discs:
        summary_parts.append(f"Disciplines: {', '.join(discs)}")
    if summary_parts:
        story.append(Paragraph("   ·   ".join(summary_parts), STYLES["BodyBold"]))
        story.append(Spacer(1, 8))

    # ── Degrees table ──────────────────────────────────────────────────────
    if degrees:
        story.append(Paragraph("Degrees & Credentials", STYLES["SubHeading"]))
        usable_w = PAGE_W - 2 * MARGIN
        headers = [
            Paragraph("Degree", STYLES["TableHeader"]),
            Paragraph("Field", STYLES["TableHeader"]),
            Paragraph("Institution", STYLES["TableHeader"]),
            Paragraph("Country", STYLES["TableHeader"]),
            Paragraph("Year", STYLES["TableHeader"]),
        ]
        rows = [headers]
        for deg in degrees:
            inst = deg.get("institution", "—")
            is_elite = inst in elite_i
            rows.append([
                _cell(deg.get("degree_type", "—"), bold=is_elite),
                _cell(deg.get("field", "—"), bold=is_elite),
                _cell(inst, bold=is_elite),
                _cell(deg.get("institution_country", "—")),
                _cell(deg.get("year_completed") or "—"),
            ])
        col_w = [usable_w * f for f in [0.12, 0.20, 0.38, 0.15, 0.15]]
        t = Table(rows, colWidths=col_w)
        t.setStyle(_table_style())
        story.append(t)
        story.append(Paragraph(
            "Bold entries indicate elite institutions.",
            STYLES["Caption"],
        ))
    else:
        story.append(Paragraph("No degree records found in source material.", STYLES["Body"]))

    story += _reasoning_block(_get_best_reasoning(data, "education"))
    story += _provenance_line(result)
    story += _supporting_quote(result)
    story.append(PageBreak())
    return story


def build_q3_page(data: Optional[Dict]) -> List:
    story = _section_header(SECTION_TITLES["locations"])
    if data is None:
        return story + _no_data_block("locations") + [PageBreak()]

    result    = data.get("result", {})
    locations = result.get("locations", [])
    countries = result.get("countries", [])

    # ── Countries box ──────────────────────────────────────────────────────
    if countries:
        story.append(Paragraph("Countries of residence / employment:", STYLES["SubHeading"]))
        story.append(Paragraph(", ".join(countries), STYLES["BodyBold"]))
        story.append(Spacer(1, 8))

    # ── Locations table ────────────────────────────────────────────────────
    if locations:
        story.append(Paragraph("Location Detail", STYLES["SubHeading"]))
        usable_w = PAGE_W - 2 * MARGIN
        headers = [
            Paragraph("City", STYLES["TableHeader"]),
            Paragraph("Country", STYLES["TableHeader"]),
            Paragraph("Period", STYLES["TableHeader"]),
            Paragraph("Role / Context", STYLES["TableHeader"]),
        ]
        rows = [headers]
        for loc in locations:
            rows.append([
                _cell(loc.get("city") or "—"),
                _cell(loc.get("country") or "—"),
                _cell(loc.get("approximate_period") or "—"),
                _cell(loc.get("role_context") or "—"),
            ])
        col_w = [usable_w * f for f in [0.18, 0.18, 0.18, 0.46]]
        t = Table(rows, colWidths=col_w)
        t.setStyle(_table_style())
        story.append(t)
    else:
        story.append(Paragraph("No location records found in source material.", STYLES["Body"]))

    story += _reasoning_block(_get_best_reasoning(data, "locations"))
    story += _provenance_line(result)
    story += _supporting_quote(result)
    story.append(PageBreak())
    return story


def build_q4_page(data: Optional[Dict]) -> List:
    story = _section_header(SECTION_TITLES["jobs"])
    if data is None:
        return story + _no_data_block("jobs") + [PageBreak()]

    result = data.get("result", {})
    jobs   = result.get("jobs", [])

    # ── Domain distribution summary ────────────────────────────────────────
    if jobs:
        domain_counts = Counter(j.get("domain", "other") for j in jobs)
        summary_parts = [f"{v} {k}" for k, v in sorted(domain_counts.items())]
        story.append(Paragraph(
            "Domain distribution:  " + "   ·   ".join(summary_parts),
            STYLES["BodyBold"],
        ))
        story.append(Spacer(1, 8))

    # ── Jobs table ─────────────────────────────────────────────────────────
    if jobs:
        story.append(Paragraph("Career Positions", STYLES["SubHeading"]))
        usable_w = PAGE_W - 2 * MARGIN
        headers = [
            Paragraph("Title", STYLES["TableHeader"]),
            Paragraph("Organisation", STYLES["TableHeader"]),
            Paragraph("Period", STYLES["TableHeader"]),
            Paragraph("Domain", STYLES["TableHeader"]),
            Paragraph("Type", STYLES["TableHeader"]),
        ]
        rows = [headers]
        for job in jobs:
            is_primary = job.get("prominence") == "primary"
            rows.append([
                _cell(job.get("title", "—"), bold=is_primary),
                _cell(job.get("organization", "—"), bold=is_primary),
                _cell(job.get("approximate_period") or "—"),
                _cell(job.get("domain", "—")),
                _cell("Primary" if is_primary else "Secondary"),
            ])
        col_w = [usable_w * f for f in [0.32, 0.30, 0.12, 0.14, 0.12]]
        t = Table(rows, colWidths=col_w)
        t.setStyle(_table_style())
        story.append(t)
        story.append(Paragraph("Bold entries are primary (full-time) positions.", STYLES["Caption"]))
    else:
        story.append(Paragraph("No job records found in source material.", STYLES["Body"]))

    story += _reasoning_block(_get_best_reasoning(data, "jobs"))
    story += _provenance_line(result)
    story += _supporting_quote(result)
    story.append(PageBreak())
    return story


def build_q5_page(data: Optional[Dict]) -> List:
    story = _section_header(SECTION_TITLES["sectors"])
    if data is None:
        return story + _no_data_block("sectors") + [PageBreak()]

    result         = data.get("result", {})
    sectors        = result.get("sectors", [])
    primary_sector = result.get("primary_sector", "")

    # ── Primary sector ─────────────────────────────────────────────────────
    if primary_sector:
        story.append(Paragraph("Primary Sector", STYLES["SubHeading"]))
        story.append(Paragraph(primary_sector.upper().replace("_", " "), STYLES["KeyFinding"]))
        story.append(Spacer(1, 8))

    # ── Sectors table ──────────────────────────────────────────────────────
    if sectors:
        story.append(Paragraph("All Identified Sectors", STYLES["SubHeading"]))
        usable_w = PAGE_W - 2 * MARGIN
        headers = [
            Paragraph("Sector", STYLES["TableHeader"]),
            Paragraph("Evidence", STYLES["TableHeader"]),
            Paragraph("Prominence", STYLES["TableHeader"]),
        ]
        rows = [headers]
        for sec in sectors:
            is_primary = sec.get("prominence") == "primary"
            rows.append([
                _cell(sec.get("sector", "—").replace("_", " "), bold=is_primary),
                _cell(sec.get("evidence", "—")),
                _cell("Primary" if is_primary else "Secondary"),
            ])
        col_w = [usable_w * f for f in [0.20, 0.65, 0.15]]
        t = Table(rows, colWidths=col_w)
        t.setStyle(_table_style())
        story.append(t)
    else:
        story.append(Paragraph("No sector records found in source material.", STYLES["Body"]))

    story += _reasoning_block(_get_best_reasoning(data, "sectors"))
    story += _provenance_line(result)
    story += _supporting_quote(result)
    story.append(PageBreak())
    return story


def build_q6_page(data: Optional[Dict]) -> List:
    story = _section_header(SECTION_TITLES["networks"])
    if data is None:
        return story + _no_data_block("networks") + [PageBreak()]

    result       = data.get("result", {})
    affiliations = result.get("affiliations", [])
    awards       = result.get("awards", [])
    usable_w     = PAGE_W - 2 * MARGIN

    # ── Affiliations table ─────────────────────────────────────────────────
    if affiliations:
        story.append(Paragraph("Affiliations & Memberships", STYLES["SubHeading"]))
        headers = [
            Paragraph("Organisation", STYLES["TableHeader"]),
            Paragraph("Type", STYLES["TableHeader"]),
            Paragraph("Period", STYLES["TableHeader"]),
        ]
        rows = [headers]
        for aff in affiliations:
            rows.append([
                _cell(aff.get("organization", "—")),
                _cell(aff.get("affiliation_type", "—")),
                _cell(aff.get("approximate_period") or "—"),
            ])
        col_w = [usable_w * f for f in [0.62, 0.20, 0.18]]
        t = Table(rows, colWidths=col_w)
        t.setStyle(_table_style())
        story.append(t)
        story.append(Spacer(1, 10))

    # ── Awards table ───────────────────────────────────────────────────────
    if awards:
        story.append(Paragraph("Awards & Distinctions", STYLES["SubHeading"]))
        headers = [
            Paragraph("Award", STYLES["TableHeader"]),
            Paragraph("Awarding Body", STYLES["TableHeader"]),
            Paragraph("Year", STYLES["TableHeader"]),
        ]
        rows = [headers]
        for aw in awards:
            rows.append([
                _cell(aw.get("award", "—"), bold=True),
                _cell(aw.get("awarding_body") or "—"),
                _cell(aw.get("year") or "—"),
            ])
        col_w = [usable_w * f for f in [0.50, 0.36, 0.14]]
        t = Table(rows, colWidths=col_w)
        t.setStyle(_table_style())
        story.append(t)

    if not affiliations and not awards:
        story.append(Paragraph("No network or award records found in source material.", STYLES["Body"]))

    story += _reasoning_block(_get_best_reasoning(data, "networks"))
    story += _provenance_line(result)
    story += _supporting_quote(result)
    story.append(PageBreak())
    return story


def build_q7_page(data: Optional[Dict]) -> List:
    story = _section_header(SECTION_TITLES["career_domain"])
    if data is None:
        return story + _no_data_block("career_domain") + [PageBreak()]

    result         = data.get("result", {})
    domain         = result.get("dominant_domain", "")
    is_hybrid      = result.get("is_hybrid", False)
    hybrid_domains = result.get("hybrid_domains", [])
    evidence       = result.get("domain_evidence", {})
    conf           = result.get("confidence", "")
    alt_suggestion = result.get("alternative_domain_suggestion")

    # ── Dominant domain ────────────────────────────────────────────────────
    if domain:
        display = domain.upper().replace("_", " ")
        if is_hybrid and hybrid_domains:
            display = " / ".join(d.upper().replace("_", " ") for d in hybrid_domains)
        story.append(Paragraph(display, STYLES["DomainBig"]))
        hybrid_note = "(Hybrid classification)" if is_hybrid else ""
        if hybrid_note:
            story.append(Paragraph(hybrid_note, STYLES["KeySub"]))
        story.append(Spacer(1, 8))

    # ── Confidence ─────────────────────────────────────────────────────────
    if conf:
        story.append(Paragraph(f"Classification confidence: {conf}", STYLES["BodyBold"]))
        story.append(Spacer(1, 8))

    # ── Domain evidence ────────────────────────────────────────────────────
    if evidence:
        story.append(Paragraph("Evidence by Domain", STYLES["SubHeading"]))
        for dom_key, dom_text in evidence.items():
            if not dom_text or dom_text.strip() == "":
                continue
            label = dom_key.upper().replace("_", " ")
            story.append(Paragraph(
                f"<b>{label}:</b>  {dom_text}",
                STYLES["Body"],
            ))

    story += _reasoning_block(_get_best_reasoning(data, "career_domain"))

    # ── Alternative suggestion ─────────────────────────────────────────────
    if alt_suggestion:
        story.append(Spacer(1, 8))
        story.append(Paragraph(
            f"<b>Note:</b> {alt_suggestion}",
            STYLES["Body"],
        ))

    story.append(Spacer(1, 8))
    story.append(Paragraph(
        "This classification is synthesised from Q1–Q6 outputs using an LLM "
        "reasoning step. It reflects the overall career pattern, not any single role.",
        STYLES["Caption"],
    ))
    story.append(PageBreak())
    return story


# ── Report builder ─────────────────────────────────────────────────────────────

def build_report(
    person_data: Dict,
    person_dir_name: str,
    output_path: Path,
    gen_date: str,
) -> None:
    """
    Build and save a full PDF report for one person.

    Uses BaseDocTemplate with a per-section PageTemplate so the header/footer
    callback can carry the correct section title onto each page.

    Args:
        person_data: Dict keyed by suffix → loaded JSON (or None if missing).
        person_dir_name: Underscore-format person name.
        output_path: Destination .pdf path.
        gen_date: Date string for footer (e.g. "2026-02-25").
    """
    person_display = person_dir_name.replace("_", " ")

    # Build story in sections; each section has its own PageTemplate with the
    # correct section title in the header.
    sections = [
        ("Biographical Overview", build_cover_page(person_data, person_display)),
        (SECTION_TITLES["hlp_job_title"], build_q1_page(person_data.get("hlp_job_title"))),
        (SECTION_TITLES["education"],     build_q2_page(person_data.get("education"))),
        (SECTION_TITLES["locations"],     build_q3_page(person_data.get("locations"))),
        (SECTION_TITLES["jobs"],          build_q4_page(person_data.get("jobs"))),
        (SECTION_TITLES["sectors"],       build_q5_page(person_data.get("sectors"))),
        (SECTION_TITLES["networks"],      build_q6_page(person_data.get("networks"))),
        (SECTION_TITLES["career_domain"], build_q7_page(person_data.get("career_domain"))),
    ]

    # Create one PageTemplate per section, each with its own on_page callback
    doc = BaseDocTemplate(
        str(output_path),
        pagesize=A4,
        leftMargin=MARGIN,
        rightMargin=MARGIN,
        topMargin=MARGIN + 10 * mm,
        bottomMargin=MARGIN + 8 * mm,
    )

    frames = []
    templates = []
    for i, (section_title, _) in enumerate(sections):
        frame = Frame(
            MARGIN, MARGIN + 8 * mm,
            PAGE_W - 2 * MARGIN,
            PAGE_H - 2 * MARGIN - 18 * mm,
            id=f"frame_{i}",
        )
        on_page = _make_header_footer(person_display, section_title, gen_date)
        pt = PageTemplate(id=f"section_{i}", frames=[frame], onPage=on_page)
        templates.append(pt)

    doc.addPageTemplates(templates)

    # Stitch the full story, inserting NextPageTemplate directives before each section
    from reportlab.platypus import NextPageTemplate
    full_story = []
    for i, (section_title, section_story) in enumerate(sections):
        full_story.append(NextPageTemplate(f"section_{i}"))
        full_story.extend(section_story)

    doc.build(full_story)


# ── Data loading ────────────────────────────────────────────────────────────────

def load_person_data(outputs_dir: Path, person_dir_name: str) -> Dict[str, Optional[Dict]]:
    """Load all available output JSONs for a person. Returns None for missing files."""
    person_dir = outputs_dir / person_dir_name
    result = {}
    for suffix in EXPECTED_SUFFIXES:
        path = person_dir / f"{person_dir_name}_{suffix}.json"
        if path.exists():
            with open(path, encoding="utf-8") as f:
                result[suffix] = json.load(f)
        else:
            result[suffix] = None
    return result


def discover_complete_persons(outputs_dir: Path) -> List[str]:
    """Return sorted list of person_dir_names that have all 7 expected output files."""
    complete = []
    if not outputs_dir.exists():
        return complete
    for person_dir in sorted(outputs_dir.iterdir()):
        if not person_dir.is_dir():
            continue
        name = person_dir.name
        has_all = all(
            (person_dir / f"{name}_{suffix}.json").exists()
            for suffix in EXPECTED_SUFFIXES
        )
        if has_all:
            complete.append(name)
    return complete


# ── CLI entry point ─────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate PDF biographical profile reports from targeted_01 outputs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python report.py --person "Abhijit Banerjee"
  python report.py --person "Abhijit_Banerjee" "Gro_Harlem_Brundtland"
  python report.py --all-complete
  python report.py --all-complete --output /path/to/reports
  python report.py --person "Abhijit Banerjee" --input /custom/outputs --output /my/reports
        """,
    )
    parser.add_argument(
        "--person",
        nargs="+",
        metavar="NAME",
        help="One or more person names (spaced or underscore format)",
    )
    parser.add_argument(
        "--all-complete",
        action="store_true",
        help="Auto-discover and report on all people with all 7 output files",
    )
    parser.add_argument(
        "--input",
        default=None,
        metavar="DIR",
        help="Override input outputs directory (default: outputs/)",
    )
    parser.add_argument(
        "--output",
        default=None,
        metavar="DIR",
        help="Directory for saving PDFs (default: same as input, in person subfolder)",
    )
    args = parser.parse_args()

    # ── Resolve paths ─────────────────────────────────────────────────────
    outputs_dir = Path(args.input) if args.input else _OUTPUT_DIR
    out_dir     = Path(args.output) if args.output else None
    if out_dir:
        out_dir.mkdir(parents=True, exist_ok=True)

    gen_date = datetime.now().strftime("%Y-%m-%d")

    # ── Build person list ──────────────────────────────────────────────────
    persons: List[str] = []
    if args.all_complete:
        persons = discover_complete_persons(outputs_dir)
        if not persons:
            print(f"No complete persons found (all 7 files) under {outputs_dir}")
            sys.exit(0)
        print(f"Found {len(persons)} complete person(s): {', '.join(persons)}")
    elif args.person:
        for name in args.person:
            persons.append(name.replace(" ", "_"))
    else:
        parser.print_help()
        print("\nError: specify --person NAME or --all-complete")
        sys.exit(1)

    # ── Generate reports ───────────────────────────────────────────────────
    for i, person_dir_name in enumerate(persons, 1):
        person_display = person_dir_name.replace("_", " ")
        print(f"[{i}/{len(persons)}]  {person_display}  ...", end="  ", flush=True)

        person_data = load_person_data(outputs_dir, person_dir_name)

        if out_dir:
            pdf_path = out_dir / f"{person_dir_name}_report.pdf"
        else:
            pdf_path = outputs_dir / person_dir_name / f"{person_dir_name}_report.pdf"

        try:
            build_report(person_data, person_dir_name, pdf_path, gen_date)
            print(f"saved -> {pdf_path}")
        except Exception as e:
            print(f"ERROR: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()
