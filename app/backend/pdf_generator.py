"""
BricksHouse Insurance – PDF Quote Generator (standalone module)

Usage:
    # As a library
    from pdf_generator import generate_quote_pdf
    result = generate_quote_pdf(quote_data_dict, "/tmp/quote.pdf", executive_summary="...")

    # From the command line (reads a JSON file)
    python pdf_generator.py quote_data.json /output/path.pdf

Light background with dark-mode accent colours from the BricksHouse UI.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Colour palette – light background, dark accents from the app UI
# ---------------------------------------------------------------------------
_C = {
    # Page & body
    "page_bg":       (255, 255, 255),    # white page
    "text":          (17, 24, 39),       # #111827  – near-black body text
    "text_secondary":(75, 85, 99),       # #4B5563  – secondary text
    "text_dim":      (107, 114, 128),    # #6B7280  – dimmed / footer text

    # Dark header bar (matches sidebar #1B1F2A)
    "header_bg":     (27, 31, 42),       # #1B1F2A  – sidebar dark
    "header_text":   (255, 255, 255),    # white on dark header

    # Accent colours
    "primary":       (255, 54, 33),      # #FF3621  – brick-primary (red)
    "accent":        (0, 169, 114),      # #00A972  – brick-accent (green)

    # Dark accents for headings & table headers
    "dark":          (27, 49, 57),       # #1B3139  – brick-dark
    "darker":        (19, 25, 30),       # #13191E  – brick-darker
    "muted":         (42, 74, 84),       # #2A4A54  – brick-muted

    # Section headings – dark teal/blue from the app
    "section":       (27, 49, 57),       # #1B3139  – same as brick-dark

    # Table
    "table_hdr_bg":  (27, 49, 57),       # #1B3139  – dark header row
    "table_hdr_text":(255, 255, 255),    # white on dark
    "row_even":      (255, 255, 255),    # white
    "row_alt":       (241, 245, 249),    # #F1F5F9  – very light slate
    "subtotal_bg":   (226, 232, 240),    # #E2E8F0  – light slate
    "total_bg":      (27, 49, 57),       # #1B3139  – dark teal
    "total_text":    (255, 255, 255),    # white

    # Info box
    "info_box_bg":   (241, 245, 249),    # #F1F5F9
    "info_box_border":(203, 213, 225),   # #CBD5E1

    # White (for header text, etc.)
    "white":         (255, 255, 255),
}

COMPANY_NAME = "BricksHouse Insurance"

# Logo file – bundled alongside this module
_LOGO_PATH = Path(__file__).resolve().parent / "logo.png"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe(text: Any) -> str:
    """Sanitise text for Helvetica (Latin-1 only)."""
    if not text:
        return ""
    s = str(text)
    for src, dst in [
        ("\u2014", "--"), ("\u2013", "-"), ("\u2018", "'"), ("\u2019", "'"),
        ("\u201c", '"'), ("\u201d", '"'), ("\u2022", "-"), ("\u2026", "..."),
        ("\u00a0", " "), ("\u200b", ""),
    ]:
        s = s.replace(src, dst)
    return s.encode("latin-1", errors="replace").decode("latin-1")


def _rgb(name: str) -> tuple[int, int, int]:
    return _C[name]


# ---------------------------------------------------------------------------
# PDF class
# ---------------------------------------------------------------------------

def _build_pdf(data: dict, output_path: str, executive_summary: str | None) -> str:
    """Build the PDF and write it to *output_path*.  Returns the path."""
    from fpdf import FPDF

    class QuotePDF(FPDF):
        def cell(self, *a, **kw):
            a = list(a)
            for i in range(len(a)):
                if isinstance(a[i], str):
                    a[i] = _safe(a[i])
            for k in ("text", "txt"):
                if k in kw and isinstance(kw[k], str):
                    kw[k] = _safe(kw[k])
            return super().cell(*a, **kw)

        def multi_cell(self, *a, **kw):
            a = list(a)
            for i in range(len(a)):
                if isinstance(a[i], str):
                    a[i] = _safe(a[i])
            for k in ("text", "txt"):
                if k in kw and isinstance(kw[k], str):
                    kw[k] = _safe(kw[k])
            return super().multi_cell(*a, **kw)

        # -- repeating header (dark bar at top) ----------------------------
        def header(self):
            self.set_fill_color(*_rgb("header_bg"))
            self.rect(0, 0, 210, 20, "F")

            # Logo
            if _LOGO_PATH.exists():
                try:
                    self.image(str(_LOGO_PATH), x=10, y=3, h=14)
                except Exception:
                    pass

            self.set_xy(27, 3)
            self.set_font("Helvetica", "B", 13)
            self.set_text_color(*_rgb("header_text"))
            self.cell(0, 7, COMPANY_NAME)
            self.set_xy(27, 10)
            self.set_font("Helvetica", "", 7)
            self.set_text_color(180, 185, 200)
            self.cell(0, 5, "Powered by AI/ML Risk Assessment")

            # Red accent line under header
            self.set_draw_color(*_rgb("primary"))
            self.set_line_width(0.8)
            self.line(0, 20, 210, 20)
            self.set_y(24)

        # -- repeating footer ----------------------------------------------
        def footer(self):
            self.set_y(-16)
            # Thin separator line
            self.set_draw_color(*_rgb("info_box_border"))
            self.set_line_width(0.3)
            self.line(10, self.get_y(), 200, self.get_y())
            self.ln(2)
            self.set_font("Helvetica", "I", 6)
            self.set_text_color(*_rgb("text_dim"))
            self.cell(
                0, 3,
                "This proposal is for illustrative purposes. "
                "All coverage subject to policy terms and conditions.",
                new_x="LMARGIN", new_y="NEXT", align="C",
            )
            self.cell(
                0, 3,
                f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} | "
                f"Page {self.page_no()}/{{nb}}",
                align="C",
            )

    # -- Create the document -----------------------------------------------
    pdf = QuotePDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=22)
    pdf.add_page()

    # Page-safe margin: content bottom boundary (page height - footer)
    _PAGE_BOTTOM = 297 - 22  # = 275 mm

    # Helper: section heading (dark text with red accent bar)
    # `need` = estimated mm the section heading + body will occupy.
    # If that won't fit on the current page, start a new one.
    def section(title: str, need: float = 30):
        if pdf.get_y() + need > _PAGE_BOTTOM:
            pdf.add_page()
        pdf.ln(2)
        pdf.set_font("Helvetica", "B", 13)
        pdf.set_text_color(*_rgb("section"))
        # Red accent bar
        y = pdf.get_y()
        pdf.set_fill_color(*_rgb("primary"))
        pdf.rect(10, y + 1, 3, 8, "F")
        pdf.set_x(16)
        pdf.cell(0, 9, title, new_x="LMARGIN", new_y="NEXT")

    # Helper: body text line
    def body_line(text: str, bold: bool = False):
        pdf.set_font("Helvetica", "B" if bold else "", 10)
        pdf.set_text_color(*_rgb("text"))
        pdf.cell(0, 6, text, new_x="LMARGIN", new_y="NEXT")

    # ── Title ────────────────────────────────────────────────────────
    pdf.set_font("Helvetica", "B", 22)
    pdf.set_text_color(*_rgb("dark"))
    pdf.cell(0, 14, "Commercial Insurance Proposal",
             new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.ln(2)

    # ── Quote info box ───────────────────────────────────────────────
    pdf.set_fill_color(*_rgb("info_box_bg"))
    pdf.set_draw_color(*_rgb("info_box_border"))
    y0 = pdf.get_y()
    pdf.rect(10, y0, 190, 20, "DF")

    pdf.set_xy(14, y0 + 3)
    pdf.set_font("Helvetica", "B", 10)
    pdf.set_text_color(*_rgb("text"))
    pdf.cell(90, 7, f"Quote #: {data.get('quote_number', 'N/A')}")
    pdf.cell(90, 7, f"Date: {datetime.now().strftime('%B %d, %Y')}",
             new_x="LMARGIN", new_y="NEXT")
    pdf.set_x(14)
    pdf.cell(90, 7,
             f"Policy Period: {data.get('effective_date', 'TBD')} to "
             f"{data.get('expiration_date', 'TBD')}")
    pdf.cell(90, 7,
             f"Status: {(data.get('decision_tag') or 'N/A').replace('-', ' ').title()}",
             new_x="LMARGIN", new_y="NEXT")
    pdf.set_y(y0 + 24)

    # ── Named Insured ────────────────────────────────────────────────
    section("Named Insured", need=40)
    pdf.set_text_color(*_rgb("text"))
    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(0, 7, data.get("business_name", "N/A"),
             new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 10)
    industry = (data.get("risk_category") or "").replace("_", " ").title()
    body_line(f"Industry: {industry}")
    rev = data.get("annual_revenue", 0) or 0
    pay = data.get("annual_payroll", 0) or 0
    body_line(f"Annual Revenue: ${rev:,.0f}  |  Annual Payroll: ${pay:,.0f}")
    emp = data.get("num_employees", 0) or 0
    loc = data.get("num_locations", 0) or 0
    body_line(f"Employees: {emp}  |  Locations: {loc}")
    pdf.ln(2)

    # ── Executive Summary ────────────────────────────────────────────
    if executive_summary:
        # Estimate height: ~5mm per line, ~90 chars per line at font 10
        _es_lines = max(3, len(str(executive_summary)) // 90 + 1)
        section("Executive Summary", need=15 + _es_lines * 5)
        pdf.set_font("Helvetica", "", 10)
        pdf.set_text_color(*_rgb("text_secondary"))
        pdf.multi_cell(0, 5, str(executive_summary).strip())
        pdf.ln(2)

    # ── Coverage Schedule ────────────────────────────────────────────
    # We need header(7) + rows will be built below; estimate conservatively.
    # The table itself handles page breaks row-by-row, but we want at least
    # the heading + table header + a few rows to stay together.
    section("Coverage Schedule", need=50)
    col_w = [65, 45, 40, 40]

    # Table header – dark background
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(*_rgb("table_hdr_bg"))
    pdf.set_text_color(*_rgb("table_hdr_text"))
    for i, h in enumerate(
        ["Coverage Line", "Basis / Limit", "Rate", "Annual Premium"]
    ):
        pdf.cell(col_w[i], 7, h, border=0, fill=True, align="C")
    pdf.ln()

    # Build rows (content unchanged)
    rows = []
    gl = data.get("gl_limit_requested", 0) or 0
    tiv = data.get("property_tiv", 0) or 0
    if data.get("gl_premium", 0):
        rows.append(("General Liability - Occurrence",
                     f"${gl:,.0f} / ${gl * 2:,.0f}",
                     "$1.50 / $1K rev",
                     f"${data['gl_premium']:,.0f}"))
    if data.get("liquor_premium", 0):
        rows.append(("Liquor Liability",
                     f"${gl:,.0f} sublimit", "Flat",
                     f"${data['liquor_premium']:,.0f}"))
    if data.get("property_building_premium", 0):
        rows.append(("Commercial Property - Building",
                     f"${tiv * 0.7:,.0f} repl. cost",
                     "$0.65 / $100",
                     f"${data['property_building_premium']:,.0f}"))
    if data.get("property_contents_premium", 0):
        rows.append(("Commercial Property - Contents",
                     f"${tiv * 0.3:,.0f} repl. cost",
                     "$0.80 / $100",
                     f"${data['property_contents_premium']:,.0f}"))
    if data.get("bi_premium", 0):
        rows.append(("Business Income",
                     f"${data.get('bi_limit', 0):,.0f} annual",
                     "$0.25 / $100",
                     f"${data['bi_premium']:,.0f}"))
    if data.get("equipment_premium", 0):
        rows.append(("Equipment Breakdown",
                     f"${loc * 100_000:,.0f} sublimit", "Flat",
                     f"${data['equipment_premium']:,.0f}"))
    if data.get("wc_premium", 0):
        rows.append(("Workers Compensation",
                     "Statutory Limits",
                     "$1.50 / $100 payroll",
                     f"${data['wc_premium']:,.0f}"))
    if data.get("auto_premium", 0):
        fleet = data.get("auto_fleet_size", 0) or 0
        rows.append(("Commercial Auto",
                     f"{fleet} vehicles",
                     "$2,000 / vehicle",
                     f"${data['auto_premium']:,.0f}"))
    if data.get("cyber_premium", 0):
        rows.append(("Cyber Liability",
                     f"${data.get('cyber_limit_requested', 0):,.0f} limit",
                     "$2.50 / $1K",
                     f"${data['cyber_premium']:,.0f}"))
    if data.get("umbrella_premium", 0):
        rows.append(("Umbrella / Excess Liability",
                     f"${data.get('umbrella_limit_requested', 0):,.0f} limit",
                     "$1,500 / $1M",
                     f"${data['umbrella_premium']:,.0f}"))
    if data.get("terrorism_premium", 0):
        rows.append(("Terrorism (TRIA)",
                     "All applicable coverages", "~1%",
                     f"${data['terrorism_premium']:,.0f}"))
    rows.append(("Policy & Inspection Fees",
                 "--", "Flat",
                 f"${data.get('policy_fees', 150):,.0f}"))

    # Table body – alternating white / light slate
    alt = False
    for row in rows:
        bg = "row_alt" if alt else "row_even"
        pdf.set_fill_color(*_rgb(bg))
        pdf.set_text_color(*_rgb("text"))
        pdf.set_font("Helvetica", "", 8)
        for i, val in enumerate(row):
            pdf.cell(col_w[i], 6, val, border=0, fill=True,
                     align="R" if i == 3 else "L")
        pdf.ln()
        alt = not alt

    # Subtotal – light slate
    pdf.set_font("Helvetica", "B", 9)
    pdf.set_fill_color(*_rgb("subtotal_bg"))
    pdf.set_text_color(*_rgb("text"))
    pdf.cell(sum(col_w[:3]), 7, "Technical Premium Subtotal",
             border=0, fill=True, align="R")
    pdf.cell(col_w[3], 7,
             f"${data.get('subtotal_premium', 0):,.0f}",
             border=0, fill=True, align="R")
    pdf.ln()

    # Underwriter adjustments (surcharge / discount) – only if present
    surcharge = data.get("surcharge_pct") or 0
    discount = data.get("discount_pct") or 0
    if surcharge or discount:
        pdf.set_font("Helvetica", "I", 8)
        pdf.set_fill_color(*_rgb("row_alt"))
        pdf.set_text_color(*_rgb("text_secondary"))
        if surcharge:
            adj_amt = (data.get("subtotal_premium", 0) or 0) * surcharge / 100
            pdf.cell(sum(col_w[:3]), 6,
                     f"Underwriter Surcharge ({surcharge:.1f}%)",
                     border=0, fill=True, align="R")
            pdf.cell(col_w[3], 6, f"+${adj_amt:,.0f}",
                     border=0, fill=True, align="R")
            pdf.ln()
        if discount:
            subtotal_after_surcharge = (data.get("subtotal_premium", 0) or 0) * (1 + surcharge / 100)
            adj_amt = subtotal_after_surcharge * discount / 100
            pdf.cell(sum(col_w[:3]), 6,
                     f"Underwriter Discount ({discount:.1f}%)",
                     border=0, fill=True, align="R")
            pdf.cell(col_w[3], 6, f"-${adj_amt:,.0f}",
                     border=0, fill=True, align="R")
            pdf.ln()

    # Total – dark teal bar (accent)
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_fill_color(*_rgb("total_bg"))
    pdf.set_text_color(*_rgb("total_text"))
    label = "TOTAL ANNUAL PREMIUM"
    if surcharge or discount:
        label = "ADJUSTED ANNUAL PREMIUM"
    pdf.cell(sum(col_w[:3]), 9, label,
             border=0, fill=True, align="R")
    pdf.cell(col_w[3], 9,
             f"${data.get('total_premium', 0):,.0f}",
             border=0, fill=True, align="R")
    pdf.ln()
    pdf.ln(3)

    # ── Payment Options ──────────────────────────────────────────────
    # heading(11) + 3 lines(6 each) + spacing = ~35mm
    section("Payment Options", need=35)
    total = data.get("total_premium", 0) or 1
    down = total * 0.25
    monthly = (total - down) / 9
    body_line(f"  1. Pay in Full: ${total * 0.95:,.0f} (5% discount)")
    body_line(f"  2. Quarterly: 4 installments of ${total / 4:,.0f}")
    body_line(f"  3. Monthly: ${down:,.0f} down + 9 installments of ${monthly:,.0f}")
    pdf.ln(2)

    # ── Risk Assessment ──────────────────────────────────────────────
    # heading(11) + 5 lines(6) + review ~25mm + spacing = ~70mm
    _review_lines = max(0, len(str(data.get("review_summary", ""))) // 90 + 1) * 5
    section("Risk Assessment Summary", need=50 + _review_lines)
    rs = data.get("risk_score", 0) or 0
    rb = data.get("risk_band", "N/A")
    lr = data.get("predicted_loss_ratio", 0) or 0
    pa = (data.get("pricing_action", "N/A") or "N/A").replace("_", " ").title()
    nc = data.get("num_claims_5yr", 0) or 0
    ca = data.get("total_claims_amount", 0) or 0
    sf = "Yes" if data.get("has_safety_procedures") else "No"
    tr = "Yes" if data.get("has_employee_training") else "No"
    for line in [
        f"ML Risk Score: {rs:.1f} / 100 ({rb})",
        f"Predicted Loss Ratio: {lr:.2f}",
        f"Pricing Recommendation: {pa}",
        f"5-Year Loss History: {nc} claim(s) totaling ${ca:,.0f}",
        f"Safety Procedures: {sf}  |  Employee Training: {tr}",
    ]:
        body_line(line)

    review = data.get("review_summary", "")
    if review:
        pdf.ln(2)
        pdf.set_font("Helvetica", "I", 9)
        pdf.set_text_color(*_rgb("text_dim"))
        pdf.multi_cell(0, 4.5, str(review).strip())
    pdf.ln(2)

    # ── Underwriter Notes (only for uw-approved/uw-declined) ─────────
    uw_notes = data.get("underwriter_notes") or ""
    if uw_notes:
        _uw_lines = max(2, len(uw_notes) // 90 + 1) * 5
        section("Underwriter Notes", need=25 + _uw_lines)
        pdf.set_font("Helvetica", "", 10)
        pdf.set_text_color(*_rgb("text"))
        if surcharge or discount:
            adjustments = []
            if surcharge:
                adjustments.append(f"+{surcharge:.1f}% surcharge")
            if discount:
                adjustments.append(f"-{discount:.1f}% discount")
            body_line(f"Premium Adjustments: {', '.join(adjustments)}")
        pdf.set_font("Helvetica", "I", 9)
        pdf.set_text_color(*_rgb("text_secondary"))
        pdf.multi_cell(0, 4.5, str(uw_notes).strip())
        pdf.ln(2)

    # ── Key Conditions & Exclusions ──────────────────────────────────
    # heading(11) + 7 lines(4.5) + spacing = ~45mm
    section("Key Conditions & Exclusions", need=45)
    pdf.set_font("Helvetica", "", 8)
    pdf.set_text_color(*_rgb("text_dim"))
    for c in [
        "All coverage subject to policy terms, conditions, and exclusions.",
        "Quote valid for 30 days from date of issue.",
        f"Premium reflects AI/ML risk assessment as of "
        f"{datetime.now().strftime('%B %Y')}.",
        "Material changes in operations, exposures, or loss history "
        "may affect pricing.",
        "Standard exclusions: war, nuclear, intentional acts, pollution "
        "(unless endorsed).",
        "Deductibles apply per occurrence unless otherwise stated.",
        "Additional insured and waiver of subrogation available "
        "by endorsement.",
    ]:
        pdf.cell(0, 4.5, f"  - {c}", new_x="LMARGIN", new_y="NEXT")

    # ── Write file ───────────────────────────────────────────────────
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    pdf.output(output_path)
    return output_path


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_quote_pdf(
    quote_data: dict | str,
    output_path: str,
    executive_summary: str | None = None,
) -> tuple[str | None, str, str | None]:
    """Generate a BricksHouse Insurance quote PDF.

    Parameters
    ----------
    quote_data : dict or JSON string
        All quote fields (same schema as pipe_quote_creation).
    output_path : str
        Where to write the PDF file.
    executive_summary : str, optional
        LLM-generated executive summary paragraph.

    Returns
    -------
    (pdf_path, pdf_status, pdf_error)
        pdf_status is "generated" on success, "generated_txt" for text
        fallback, or "error".
    """
    if isinstance(quote_data, str):
        try:
            quote_data = json.loads(quote_data)
        except Exception as e:
            return (None, "error", f"JSON parse: {str(e)[:200]}")

    try:
        from fpdf import FPDF  # noqa: F401
    except ImportError:
        return _text_fallback(quote_data, output_path, executive_summary)

    try:
        path = _build_pdf(quote_data, output_path, executive_summary)
        return (path, "generated", None)
    except Exception as e:
        return (None, "error", str(e)[:500])


def _text_fallback(
    data: dict, output_path: str, executive_summary: str | None,
) -> tuple[str | None, str, str | None]:
    """Plain-text fallback when fpdf2 is not installed."""
    try:
        txt_path = output_path.replace(".pdf", ".txt")
        os.makedirs(os.path.dirname(txt_path) or ".", exist_ok=True)
        with open(txt_path, "w") as f:
            f.write(f"{'=' * 60}\n")
            f.write(f"{COMPANY_NAME.upper()} - COMMERCIAL INSURANCE PROPOSAL\n")
            f.write(f"{'=' * 60}\n\n")
            f.write(f"Quote #: {data.get('quote_number', 'N/A')}\n")
            f.write(f"Business: {data.get('business_name', 'N/A')}\n")
            f.write(f"Industry: {data.get('risk_category', 'N/A')}\n")
            f.write(f"Risk: {data.get('risk_band', 'N/A')} "
                    f"({data.get('risk_score', 0):.0f}/100)\n")
            f.write(f"Total Premium: ${data.get('total_premium', 0):,.0f}\n")
            f.write(f"Policy: {data.get('effective_date', 'TBD')} to "
                    f"{data.get('expiration_date', 'TBD')}\n\n")
            if executive_summary:
                f.write(f"Summary:\n{executive_summary}\n\n")
            f.write("Coverage Breakdown:\n")
            for k in [
                "gl_premium", "liquor_premium",
                "property_building_premium", "property_contents_premium",
                "bi_premium", "equipment_premium", "wc_premium",
                "auto_premium", "cyber_premium", "umbrella_premium",
                "terrorism_premium", "policy_fees",
            ]:
                v = data.get(k, 0) or 0
                if v > 0:
                    label = k.replace("_premium", "").replace("_", " ").title()
                    f.write(f"  {label:40s} ${v:>12,.0f}\n")
            f.write(f"  {'Subtotal':40s} "
                    f"${data.get('subtotal_premium', 0):>12,.0f}\n")
            f.write(f"  {'TOTAL ANNUAL PREMIUM':40s} "
                    f"${data.get('total_premium', 0):>12,.0f}\n")
        return (txt_path, "generated_txt", None)
    except Exception as e:
        return (None, "error", f"Text fallback: {str(e)[:200]}")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(f"Usage: python {sys.argv[0]} <quote_data.json> <output.pdf> [executive_summary]")
        sys.exit(1)

    json_path = sys.argv[1]
    out_path = sys.argv[2]
    summary = sys.argv[3] if len(sys.argv) > 3 else None

    with open(json_path) as f:
        qdata = json.load(f)

    path, status, error = generate_quote_pdf(qdata, out_path, summary)
    if error:
        print(f"ERROR: {error}")
        sys.exit(1)
    print(f"[{status}] {path}")
