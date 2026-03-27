#!/usr/bin/env python3
"""
Generate the Email-to-Quote Underwriter Combined Flow diagram as SVG,
then convert to PNG using Chrome headless.
"""

import subprocess

SVG_PATH = "/Users/david.vincent/vibe/mail2quote/underwriter_flow_diagram.svg"
HTML_PATH = "/Users/david.vincent/vibe/mail2quote/underwriter_flow_diagram.html"
PNG_PATH  = "/Users/david.vincent/vibe/mail2quote/underwriter_flow_diagram.png"

# ── Canvas ────────────────────────────────────────────────────────────────────
W  = 1500
H  = 1900

# ── Colours ───────────────────────────────────────────────────────────────────
BLUE_F  = "#BBDEFB"   # existing node fill
BLUE_S  = "#1565C0"   # existing node stroke / arrow
GREEN_F = "#C8E6C9"   # new UW node fill
GREEN_S = "#2E7D32"   # new UW node stroke / arrow
ORG_F   = "#FFE0B2"   # shared-sink fill
ORG_S   = "#E65100"   # shared-sink stroke / arrow
PUR_S   = "#6A1B9A"   # JOIN badge / reference arrows
GREY_F  = "#ECEFF1"
GREY_S  = "#546E7A"
DARK    = "#212121"
WHITE   = "#FFFFFF"

# ── X centres ─────────────────────────────────────────────────────────────────
XM  = 430   # existing flow centre
XUW = 1050  # UW flow centre

# ── Node geometry ─────────────────────────────────────────────────────────────
NW = 240    # standard node width
NH = 46     # standard node height
NR = 8      # corner radius

# ── Y grid – every row is a top-edge ─────────────────────────────────────────
# Existing-flow rows
R_ER   = 110   # pipe_email_received
R_EP   = 200   # pipe_email_parsed
R_EE   = 290   # pipe_email_enriched
R_QF   = 380   # pipe_quote_features
R_RS   = 470   # pipe_quote_risk_scoring
R_QRV  = 560   # pipe_quote_review
R_DIAM = 660   # routing diamond centre
R_WAIT = 730   # "awaits underwriter" label
R_QC   = 810   # pipe_quote_creation   (shared sink)
R_PDF  = 920   # pipe_pdf_created
R_CMP  = 1030  # pipe_completed        (shared sink)

# UW-flow rows
R_LB   = 110   # Lakebase PostgreSQL
R_UWT  = 230   # underwriter table
R_UWD  = 370   # uw decision diamond centre
R_JA   = 460   # JOIN (uw-approved) badge
R_JDI  = 590   # JOIN (uw-declined/uw-info) badge

# ── Helpers ───────────────────────────────────────────────────────────────────
def esc(s):
    return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def rect_el(x, y, w, h, fill, stroke, rx=NR, dash=""):
    d = f' stroke-dasharray="{dash}"' if dash else ""
    return (f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" ry="{rx}" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="2"{d}/>')

def txt(x, y, content, fill=DARK, size=13, weight="normal", anchor="middle"):
    return (f'<text x="{x}" y="{y}" font-family="Segoe UI,Arial,sans-serif" '
            f'font-size="{size}" font-weight="{weight}" fill="{fill}" '
            f'text-anchor="{anchor}" dominant-baseline="central">{esc(content)}</text>')

def node_box(cx, cy_top, label, fill, stroke, w=NW, h=NH, sub=None):
    x = cx - w // 2
    els = [rect_el(x, cy_top, w, h, fill, stroke)]
    if sub:
        els.append(txt(cx, cy_top + h * 0.35, label, DARK, 12, "bold"))
        els.append(txt(cx, cy_top + h * 0.68, sub, GREY_S, 10))
    else:
        els.append(txt(cx, cy_top + h // 2, label, DARK, 12, "bold"))
    return "\n".join(els)

def diamond_el(cx, cy, label, fill, stroke, hw=80, hh=36):
    pts = f"{cx},{cy-hh} {cx+hw},{cy} {cx},{cy+hh} {cx-hw},{cy}"
    return (f'<polygon points="{pts}" fill="{fill}" stroke="{stroke}" stroke-width="2"/>\n'
            + txt(cx, cy, label, DARK, 11, "bold"))

def join_badge(cx, cy_top, label1, label2, w=210, h=44):
    x = cx - w // 2
    els = [rect_el(x, cy_top, w, h, "#EDE7F6", PUR_S, rx=22)]
    els.append(txt(cx, cy_top + h * 0.35, label1, PUR_S, 9, "bold"))
    els.append(txt(cx, cy_top + h * 0.68, label2, PUR_S, 9))
    return "\n".join(els)

def marker(color):
    cid = color.replace("#", "")
    return (f'<marker id="ah_{cid}" markerWidth="10" markerHeight="7" '
            f'refX="9" refY="3.5" orient="auto">'
            f'<polygon points="0 0,10 3.5,0 7" fill="{color}"/></marker>')

def line(x1, y1, x2, y2, color, dash=""):
    cid = color.replace("#","")
    d = f' stroke-dasharray="{dash}"' if dash else ""
    return (f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" '
            f'stroke="{color}" stroke-width="2.5"{d} marker-end="url(#ah_{cid})"/>')

def polyline(pts, color, dash=""):
    cid = color.replace("#","")
    ps  = " ".join(f"{x},{y}" for x,y in pts)
    d   = f' stroke-dasharray="{dash}"' if dash else ""
    return (f'<polyline points="{ps}" fill="none" '
            f'stroke="{color}" stroke-width="2.5"{d} marker-end="url(#ah_{cid})"/>')

def edge_label(x, y, content, color, anchor="middle"):
    return (f'<rect x="{x-len(content)*3}" y="{y-8}" '
            f'width="{len(content)*6}" height="16" fill="white" opacity="0.85"/>\n'
            + txt(x, y, content, color, 10, "bold", anchor))


# ── Build ─────────────────────────────────────────────────────────────────────
parts = []

# background
parts.append(f'<rect width="{W}" height="{H}" fill="#F5F7FA"/>')

# title bar
parts.append(rect_el(20, 12, W-40, 60, "#0D47A1", "#0D47A1", rx=10))
parts.append(txt(W//2, 42, "Email-to-Quote Pipeline: Underwriter Combined Flow",
                 WHITE, 20, "bold"))

# ── Lane backgrounds ──────────────────────────────────────────────────────────
parts.append(f'<rect x="210" y="90" width="450" height="{R_CMP+NH+60-90}" rx="14" '
             f'fill="none" stroke="{BLUE_S}" stroke-width="1.5" stroke-dasharray="7,4" opacity="0.5"/>')
parts.append(txt(XM, 104, "EXISTING FLOW  (no changes)", BLUE_S, 11, "bold"))

parts.append(f'<rect x="820" y="90" width="460" height="{R_JDI+44+60-90}" rx="14" '
             f'fill="none" stroke="{GREEN_S}" stroke-width="1.5" stroke-dasharray="7,4" opacity="0.5"/>')
parts.append(txt(XUW, 104, "NEW UNDERWRITER FLOW  (additive)", GREEN_S, 11, "bold"))

# ── EXISTING FLOW ─────────────────────────────────────────────────────────────
EX_NODES = [
    (R_ER,  "pipe_email_received"),
    (R_EP,  "pipe_email_parsed"),
    (R_EE,  "pipe_email_enriched"),
    (R_QF,  "pipe_quote_features"),
    (R_RS,  "pipe_quote_risk_scoring"),
    (R_QRV, "pipe_quote_review"),
]
for row, label in EX_NODES:
    parts.append(node_box(XM, row, label, BLUE_F, BLUE_S))

# straight arrows between linear nodes
for i in range(len(EX_NODES)-1):
    y1 = EX_NODES[i][0] + NH
    y2 = EX_NODES[i+1][0]
    parts.append(line(XM, y1, XM, y2, BLUE_S))

# arrow into routing diamond
parts.append(line(XM, R_QRV + NH, XM, R_DIAM - 36, BLUE_S))

# routing diamond
parts.append(diamond_el(XM, R_DIAM, "routing decision", BLUE_F, BLUE_S))

# ── auto-approved → down to pipe_quote_creation ───────────────────────────────
parts.append(line(XM, R_DIAM + 36, XM, R_QC, BLUE_S))
parts.append(edge_label(XM + 55, R_DIAM + 60, "auto-approved", BLUE_S))

# ── pipe_quote_creation (shared sink) ─────────────────────────────────────────
parts.append(node_box(XM, R_QC, "pipe_quote_creation", ORG_F, ORG_S, w=NW+30))
parts.append(node_box(XM, R_PDF, "pipe_pdf_created",   BLUE_F, BLUE_S))
parts.append(node_box(XM, R_CMP, "pipe_completed",     ORG_F, ORG_S, w=NW+30))

parts.append(line(XM, R_QC + NH, XM, R_PDF, BLUE_S))
parts.append(line(XM, R_PDF + NH, XM, R_CMP, BLUE_S))

# ── pending-review → waiting box (left side) ──────────────────────────────────
WX = XM - 210
WY = R_WAIT
parts.append(rect_el(WX - 95, WY, 190, 44, GREY_F, GREY_S, rx=8))
parts.append(txt(WX, WY + 22, "awaits underwriter", GREY_S, 11, "bold"))
parts.append(polyline([(XM-80, R_DIAM), (WX+95, R_DIAM), (WX+95, WY+22)], GREY_S))
parts.append(edge_label(XM - 130, R_DIAM - 14, "pending-review", GREY_S))

# ── auto-declined → pipe_completed (far-left bent arrow) ──────────────────────
DX = XM - 240
parts.append(polyline([
    (XM - 80, R_DIAM + 8),
    (DX,      R_DIAM + 8),
    (DX,      R_CMP + NH//2),
    (XM - (NW+30)//2, R_CMP + NH//2),
], BLUE_S, dash="6,3"))
parts.append(edge_label(DX - 10, R_DIAM + 80, "auto-declined", BLUE_S, "end"))

# ── NEW UNDERWRITER FLOW ──────────────────────────────────────────────────────
parts.append(node_box(XUW, R_LB,  "Lakebase PostgreSQL",   GREEN_F, GREEN_S,
                      w=NW+60, sub="external source"))
parts.append(node_box(XUW, R_UWT, "underwriter table",     GREEN_F, GREEN_S,
                      w=NW+80, sub="decision: uw-approved | uw-declined | uw-info"))

parts.append(line(XUW, R_LB + NH, XUW, R_UWT, GREEN_S))
parts.append(edge_label(XUW + 80, R_LB + NH + 28, "JDBC / SDP ingest", GREEN_S))

parts.append(line(XUW, R_UWT + NH, XUW, R_UWD - 36, GREEN_S))
parts.append(diamond_el(XUW, R_UWD, "uw decision field", GREEN_F, GREEN_S, hw=90))

# ── uw-approved path → JOIN badge → pipe_quote_creation ──────────────────────
parts.append(line(XUW, R_UWD + 36, XUW, R_JA, GREEN_S))
parts.append(edge_label(XUW + 65, R_UWD + 62, "uw-approved", GREEN_S))

parts.append(join_badge(XUW, R_JA,
    "JOIN: underwriter",
    "+ pipe_quote_review  →  pipe_quote_creation"))

# pipe_quote_review right edge → JOIN badge left
REV_RX = XM + NW // 2
REV_MY = R_QRV + NH // 2
JA_LX  = XUW - 105   # left edge of JOIN badge
parts.append(polyline([
    (REV_RX,   REV_MY),
    (JA_LX - 40, REV_MY),
    (JA_LX - 40, R_JA + 22),
    (JA_LX,      R_JA + 22),
], PUR_S, dash="6,3"))
parts.append(edge_label(730, REV_MY - 14, "pipe_quote_review ref", PUR_S))

# JOIN badge bottom → bent left → pipe_quote_creation right edge
QC_RY = R_QC + NH // 2
parts.append(polyline([
    (XUW,                R_JA + 44),
    (XUW,                R_JA + 70),
    (XM + (NW+30)//2 + 20, R_JA + 70),
    (XM + (NW+30)//2 + 20, QC_RY),
    (XM + (NW+30)//2,    QC_RY),
], GREEN_S))
parts.append(edge_label((XUW + XM) // 2 + 60, R_JA + 56,
                         "2nd source", GREEN_S))

# ── uw-declined / uw-info path → JOIN badge → pipe_completed ─────────────────
parts.append(polyline([
    (XUW + 90, R_UWD + 8),
    (XUW + 160, R_UWD + 8),
    (XUW + 160, R_JDI + 22),
    (XUW + 105, R_JDI + 22),
], ORG_S))
parts.append(edge_label(XUW + 190, R_UWD + 80, "uw-declined / uw-info", ORG_S, "start"))

parts.append(join_badge(XUW, R_JDI,
    "JOIN: underwriter",
    "+ pipe_quote_review  →  pipe_completed"))

# pipe_quote_review right edge → JOIN_DI badge (re-use similar route)
parts.append(polyline([
    (REV_RX,    REV_MY),
    (JA_LX - 40, REV_MY),
    (JA_LX - 40, R_JDI + 22),
    (XUW - 105,  R_JDI + 22),
], PUR_S, dash="6,3"))

# JOIN_DI badge bottom → bent left → pipe_completed right edge
CMP_RY = R_CMP + NH // 2
parts.append(polyline([
    (XUW,                  R_JDI + 44),
    (XUW,                  R_JDI + 68),
    (XM + (NW+30)//2 + 40, R_JDI + 68),
    (XM + (NW+30)//2 + 40, CMP_RY),
    (XM + (NW+30)//2,      CMP_RY),
], ORG_S))
parts.append(edge_label((XUW + XM) // 2 + 80, R_JDI + 54,
                         "2nd/3rd source", ORG_S))

# ── multi-source badge on shared sinks ───────────────────────────────────────
for sy in [R_QC, R_CMP]:
    bx = XM + (NW+30)//2 + 50
    by = sy + 10
    parts.append(rect_el(bx, by, 110, 20, ORG_F, ORG_S, rx=5))
    parts.append(txt(bx + 55, by + 10, "multi-source", ORG_S, 9, "bold"))

# ── Legend ────────────────────────────────────────────────────────────────────
LX = 60
LY = H - 150
parts.append(rect_el(LX-14, LY-20, 460, 130, WHITE, "#BDBDBD", rx=8))
parts.append(txt(LX + 100, LY - 5, "Legend", DARK, 12, "bold"))
items = [
    (BLUE_F,  BLUE_S,  "Existing pipeline tables  (not modified)"),
    (GREEN_F, GREEN_S, "New underwriter flow  (additive)"),
    (ORG_F,   ORG_S,   "Shared sink tables  (multi-source input)"),
    ("#EDE7F6", PUR_S, "JOIN operation  (underwriter + pipe_quote_review)"),
]
for i, (fill, stroke, label) in enumerate(items):
    iy = LY + 18 + i * 26
    parts.append(rect_el(LX, iy, 24, 16, fill, stroke, rx=4))
    parts.append(txt(LX + 32, iy + 8, label, DARK, 10, anchor="start"))

# ── Assemble SVG ──────────────────────────────────────────────────────────────
colors = [BLUE_S, GREEN_S, ORG_S, PUR_S, GREY_S]
markers_xml = "\n    ".join(marker(c) for c in colors)

svg = f"""<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">
  <defs>
    {markers_xml}
  </defs>
  {''.join(p + chr(10) for p in parts)}
</svg>"""

with open(SVG_PATH, "w") as f:
    f.write(svg)
print(f"SVG written: {SVG_PATH}")

result = subprocess.run([
    "sips", "-s", "format", "png", SVG_PATH, "--out", PNG_PATH
], capture_output=True, text=True)

if result.returncode == 0:
    print(f"PNG written: {PNG_PATH}")
else:
    print("sips error:", result.stderr)
