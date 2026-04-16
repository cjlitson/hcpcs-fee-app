from __future__ import annotations

from datetime import datetime

from core.version import APP_VERSION


def generate_purchase_document_docx(filepath, payload: dict) -> None:
    try:
        from docx import Document
        from docx.enum.text import WD_ALIGN_PARAGRAPH
    except ImportError:
        raise ImportError("python-docx is required for document generation. Run: pip install python-docx")

    doc = Document()
    heading = doc.add_paragraph("HCPCS Worksheet")
    heading.runs[0].bold = True

    def _meta(label: str, value: str) -> None:
        p = doc.add_paragraph()
        p.add_run(f"{label}: ").bold = True
        p.add_run(str(value or ""))

    _meta("Veteran Last Name", payload.get("veteran_last_name", ""))
    _meta("Last 4", payload.get("last4", ""))
    _meta("Consult Date", payload.get("consult_date", ""))
    _meta("Deliver To", payload.get("deliver_to", ""))
    _meta("Vendor", payload.get("vendor", ""))

    doc.add_paragraph("")
    table = doc.add_table(rows=1, cols=5)
    table.style = "Table Grid"
    headers = ["HCPCS Code", "Quantity", "Cost", "Description / Details", "Line Total"]
    for idx, text in enumerate(headers):
        run = table.rows[0].cells[idx].paragraphs[0].add_run(text)
        run.bold = True

    total = 0.0
    for item in payload.get("items", []):
        qty = int(item.get("quantity") or 0)
        cost = float(item.get("cost") or 0.0)
        line_total = qty * cost
        total += line_total
        row = table.add_row().cells
        row[0].text = str(item.get("hcpcs_code", ""))
        row[1].text = str(qty)
        row[2].text = f"${cost:,.2f}"
        row[3].text = str(item.get("description", ""))
        row[4].text = f"${line_total:,.2f}"
        row[1].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.RIGHT
        row[2].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.RIGHT
        row[4].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.RIGHT

    total_row = table.add_row().cells
    total_row[3].text = "Grand Total"
    total_row[4].text = f"${total:,.2f}"
    total_row[3].paragraphs[0].runs[0].bold = True
    total_row[4].paragraphs[0].runs[0].bold = True
    total_row[3].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.RIGHT
    total_row[4].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.RIGHT

    comments = (payload.get("comments") or "").strip()
    if comments:
        doc.add_paragraph("")
        p = doc.add_paragraph()
        p.add_run("Additional Comments: ").bold = True
        p.add_run(comments)

    footer = doc.add_paragraph(
        f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} by VA HCPCS Fee Schedule Manager v{APP_VERSION}"
    )
    footer.runs[0].italic = True
    doc.save(str(filepath))
