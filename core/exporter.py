import csv
from datetime import datetime

from core.version import APP_VERSION


def _chosen_allowable(record, is_rural=False):
    """Return the display allowable amount based on rural flag.

    Priority:
    - rural=True  → allowable_r, fallback to allowable_nr if r is None/0
    - rural=False → allowable_nr, fallback to legacy allowable
    """
    if is_rural:
        r = record.get("allowable_r")
        if r:
            return r
        # Fall back to NR when rural amount not available
    nr = record.get("allowable_nr")
    if nr is not None:
        return nr
    return record.get("allowable")


def export_to_csv(records, filepath, is_rural=False, zip_code=""):
    if not records:
        return
    fieldnames = [
        "hcpcs_code", "description", "state_abbr", "year",
        "allowable_nr", "allowable_r", "allowable", "modifier", "data_source",
        "zip_code", "rural_status",
    ]
    rural_status = ""
    if zip_code:
        rural_status = "Rural (R)" if is_rural else "Non-Rural (NR)"
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in records:
            row = {k: r.get(k, "") for k in fieldnames}
            chosen = _chosen_allowable(r, is_rural=is_rural)
            row["allowable"] = "" if chosen is None else chosen
            if row["allowable_nr"] is None:
                row["allowable_nr"] = ""
            if row["allowable_r"] is None:
                row["allowable_r"] = ""
            row["zip_code"] = zip_code
            row["rural_status"] = rural_status
            writer.writerow(row)


def export_to_excel(records, filepath, is_rural=False, zip_code=""):
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment
    except ImportError:
        raise ImportError("openpyxl is required for Excel export. Run: pip install openpyxl")

    rural_status = ""
    if zip_code:
        rural_status = "Rural (R)" if is_rural else "Non-Rural (NR)"

    wb = Workbook()
    ws = wb.active
    ws.title = "HCPCS Fee Schedule"

    # Header
    headers = [
        "HCPCS Code", "Description", "State", "Year",
        "Allowable (NR)", "Allowable (R)", "Allowable ($)", "Modifier", "Source",
        "ZIP Code", "Rural Status",
    ]
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill("solid", fgColor="003366")
    for col, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center")

    # Data
    na_fill = PatternFill("solid", fgColor="FFF9C4")
    alt_fill = PatternFill("solid", fgColor="EEF2F7")
    for row_i, r in enumerate(records, 2):
        chosen = _chosen_allowable(r, is_rural=is_rural)
        is_na = chosen is None
        nr = r.get("allowable_nr")
        rv = r.get("allowable_r")
        values = [
            r.get("hcpcs_code", ""),
            r.get("description", ""),
            r.get("state_abbr", ""),
            r.get("year", ""),
            "" if nr is None else nr,
            "" if rv is None else rv,
            "" if is_na else chosen,
            r.get("modifier", "") or "",
            r.get("data_source", "") or "",
            zip_code,
            rural_status,
        ]
        for col_i, v in enumerate(values, 1):
            cell = ws.cell(row=row_i, column=col_i, value=v)
            if is_na:
                cell.fill = na_fill
            elif row_i % 2 == 0:
                cell.fill = alt_fill

    # Column widths
    widths = [12, 60, 8, 6, 14, 14, 14, 10, 20, 10, 14]
    for col, w in enumerate(widths, 1):
        ws.column_dimensions[ws.cell(row=1, column=col).column_letter].width = w

    wb.save(filepath)


def export_to_pdf(records, filepath, is_rural=False, zip_code=""):
    try:
        from reportlab.lib.pagesizes import landscape, letter
        from reportlab.lib import colors
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib.units import inch
    except ImportError:
        raise ImportError("reportlab is required for PDF export. Run: pip install reportlab")

    doc = SimpleDocTemplate(str(filepath), pagesize=landscape(letter),
                            topMargin=0.5 * inch, bottomMargin=0.5 * inch,
                            leftMargin=0.5 * inch, rightMargin=0.5 * inch)
    styles = getSampleStyleSheet()
    story = []

    # Title
    title = Paragraph("<b>VA HCPCS Fee Schedule Report</b>", styles["Title"])
    story.append(title)

    zip_part = ""
    if zip_code:
        rural_label = "Rural (R)" if is_rural else "Non-Rural (NR)"
        zip_part = f"  |  ZIP: {zip_code} ({rural_label})"

    subtitle = Paragraph(
        f"Generated: {datetime.now().strftime('%B %d, %Y %I:%M %p')}  |  {len(records):,} records{zip_part}",
        styles["Normal"],
    )
    story.append(subtitle)
    story.append(Spacer(1, 0.2 * inch))

    # Table data
    col_headers = ["HCPCS Code", "Description", "State", "Year", "Allowable ($)", "Modifier"]
    table_data = [col_headers]
    for r in records:
        chosen = _chosen_allowable(r, is_rural=is_rural)
        table_data.append([
            r.get("hcpcs_code", ""),
            (r.get("description", "") or "")[:80],
            r.get("state_abbr", ""),
            str(r.get("year", "")),
            "" if chosen is None else f"${chosen:,.2f}",
            r.get("modifier", "") or "",
        ])

    col_widths = [1.0 * inch, 4.5 * inch, 0.6 * inch, 0.6 * inch, 1.0 * inch, 0.8 * inch]
    table = Table(table_data, colWidths=col_widths, repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#003366")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 9),
        ("FONTSIZE", (0, 1), (-1, -1), 7),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#EEF2F7")]),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("WORDWRAP", (1, 1), (1, -1), True),
    ]))
    story.append(table)
    doc.build(story)


def _purchase_meta_lines(meta):
    generated = meta.get("generated_at") or datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        ("Year", str(meta.get("year") or "—")),
        ("State", meta.get("state") or "—"),
        ("ZIP", meta.get("zip_code") or "—"),
        ("Rural Status", meta.get("rural_status") or "Non-Rural (NR)"),
        ("Date Generated", generated),
    ]
    return lines


def export_purchase_list_to_csv(items, filepath, meta=None):
    meta = meta or {}
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["VA HCPCS Purchase List"])
        for key, value in _purchase_meta_lines(meta):
            w.writerow([key, value])
        w.writerow([])
        w.writerow(["HCPCS Code", "Description", "Quantity", "Unit Price", "Line Total"])
        total = 0.0
        for item in items:
            unit = item.get("unit_price")
            line = item.get("line_total")
            if isinstance(line, (int, float)):
                total += line
            w.writerow([
                item.get("hcpcs_code", ""),
                item.get("description", ""),
                item.get("quantity", 1),
                "" if unit is None else f"{unit:.2f}",
                "" if line is None else f"{line:.2f}",
            ])
        w.writerow([])
        w.writerow(["", "", "", "Grand Total", f"{total:.2f}"])


def export_purchase_list_to_excel(items, filepath, meta=None):
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment
    except ImportError:
        raise ImportError("openpyxl is required for Excel export. Run: pip install openpyxl")

    meta = meta or {}
    wb = Workbook()
    ws = wb.active
    ws.title = "Purchase List"
    row = 1
    ws.cell(row=row, column=1, value="VA HCPCS Purchase List").font = Font(bold=True, size=14)
    row += 2
    for key, value in _purchase_meta_lines(meta):
        ws.cell(row=row, column=1, value=key).font = Font(bold=True)
        ws.cell(row=row, column=2, value=value)
        row += 1
    row += 1

    headers = ["HCPCS Code", "Description", "Quantity", "Unit Price", "Line Total"]
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill("solid", fgColor="003366")
    for col, h in enumerate(headers, 1):
        c = ws.cell(row=row, column=col, value=h)
        c.font = header_font
        c.fill = header_fill
        c.alignment = Alignment(horizontal="center")
    row += 1

    total = 0.0
    alt_fill = PatternFill("solid", fgColor="EEF2F7")
    for idx, item in enumerate(items):
        unit = item.get("unit_price")
        line = item.get("line_total")
        if isinstance(line, (int, float)):
            total += line
        vals = [
            item.get("hcpcs_code", ""),
            item.get("description", ""),
            item.get("quantity", 1),
            None if unit is None else float(unit),
            None if line is None else float(line),
        ]
        for col, val in enumerate(vals, 1):
            c = ws.cell(row=row, column=col, value=val)
            if idx % 2 == 1:
                c.fill = alt_fill
        row += 1

    ws.cell(row=row + 1, column=4, value="Grand Total").font = Font(bold=True)
    ws.cell(row=row + 1, column=5, value=total).font = Font(bold=True)
    ws.column_dimensions["A"].width = 14
    ws.column_dimensions["B"].width = 60
    ws.column_dimensions["C"].width = 10
    ws.column_dimensions["D"].width = 14
    ws.column_dimensions["E"].width = 14

    for i in range(1, row + 2):
        ws.cell(row=i, column=4).number_format = '"$"#,##0.00'
        ws.cell(row=i, column=5).number_format = '"$"#,##0.00'

    wb.save(filepath)


def export_purchase_list_to_pdf(items, filepath, meta=None):
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.lib import colors
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet
    except ImportError:
        raise ImportError("reportlab is required for PDF export. Run: pip install reportlab")

    meta = meta or {}
    doc = SimpleDocTemplate(str(filepath), pagesize=letter, topMargin=36, bottomMargin=36, leftMargin=36, rightMargin=36)
    styles = getSampleStyleSheet()
    story = [Paragraph("<font name='Helvetica-Bold' color='#003366'>VA HCPCS Purchase List</font>", styles["Title"]), Spacer(1, 8)]
    for key, value in _purchase_meta_lines(meta):
        story.append(Paragraph(f"<b>{key}:</b> {value}", styles["Normal"]))
    story.append(Spacer(1, 10))
    data = [["HCPCS Code", "Description", "Quantity", "Unit Price", "Line Total"]]
    total = 0.0
    for item in items:
        line = item.get("line_total")
        if isinstance(line, (int, float)):
            total += line
        data.append([
            item.get("hcpcs_code", ""),
            (item.get("description", "") or "")[:70],
            str(item.get("quantity", 1)),
            "—" if item.get("unit_price") is None else f"${item['unit_price']:,.2f}",
            "—" if line is None else f"${line:,.2f}",
        ])
    data.append(["", "", "", "Grand Total", f"${total:,.2f}"])
    table = Table(data, colWidths=[80, 250, 60, 80, 80], repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#003366")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (2, 1), (-1, -1), "RIGHT"),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -2), [colors.white, colors.HexColor("#EEF2F7")]),
        ("FONTNAME", (3, -1), (4, -1), "Helvetica-Bold"),
        ("LINEABOVE", (3, -1), (4, -1), 1, colors.HexColor("#003366")),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(table)
    story.append(Spacer(1, 10))
    story.append(Paragraph(
        f"<font size='8' color='#666666'>Generated by VA HCPCS Fee Schedule Manager v{APP_VERSION}</font>",
        styles["Normal"],
    ))
    doc.build(story)


def export_purchase_list_to_docx(items, filepath, meta=None):
    try:
        from docx import Document
        from docx.enum.text import WD_ALIGN_PARAGRAPH
        from docx.oxml import OxmlElement
        from docx.oxml.ns import qn
        from docx.shared import Pt, RGBColor
    except ImportError:
        raise ImportError("python-docx is required for Word export. Run: pip install python-docx")

    def _set_cell_fill(cell, color_hex):
        tc_pr = cell._tc.get_or_add_tcPr()
        shading = OxmlElement("w:shd")
        shading.set(qn("w:val"), "clear")
        shading.set(qn("w:color"), "auto")
        shading.set(qn("w:fill"), color_hex)
        tc_pr.append(shading)

    meta = meta or {}
    doc = Document()
    heading = doc.add_paragraph()
    heading_run = heading.add_run("VA HCPCS Purchase List")
    heading_run.bold = True
    heading_run.font.size = Pt(20)
    heading_run.font.color.rgb = RGBColor(0x00, 0x33, 0x66)
    heading.alignment = WD_ALIGN_PARAGRAPH.CENTER

    for key, value in _purchase_meta_lines(meta):
        p = doc.add_paragraph()
        p.add_run(f"{key}: ").bold = True
        p.add_run(str(value))

    doc.add_paragraph("")
    table = doc.add_table(rows=1, cols=5)
    table.style = "Table Grid"
    headers = ["HCPCS Code", "Description", "Quantity", "Unit Price", "Line Total"]
    for idx, text in enumerate(headers):
        header_cell = table.rows[0].cells[idx]
        h = header_cell.paragraphs[0].add_run(text)
        h.bold = True
        h.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
        header_cell.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
        _set_cell_fill(header_cell, "003366")

    total = 0.0
    for idx, item in enumerate(items):
        row = table.add_row().cells
        line = item.get("line_total")
        if isinstance(line, (int, float)):
            total += line
        row[0].text = str(item.get("hcpcs_code", ""))
        row[1].text = str(item.get("description", "") or "")[:70]
        row[2].text = str(item.get("quantity", 1))
        row[3].text = "—" if item.get("unit_price") is None else f"${item['unit_price']:,.2f}"
        row[4].text = "—" if line is None else f"${line:,.2f}"
        row[2].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.RIGHT
        row[3].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.RIGHT
        row[4].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.RIGHT
        if idx % 2 == 1:
            for cell in row:
                _set_cell_fill(cell, "EEF2F7")

    total_row = table.add_row().cells
    total_row[3].text = "Grand Total"
    total_row[4].text = f"${total:,.2f}"
    total_row[3].paragraphs[0].runs[0].bold = True
    total_row[4].paragraphs[0].runs[0].bold = True
    total_row[3].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.RIGHT
    total_row[4].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.RIGHT

    doc.add_paragraph("")
    footer = doc.add_paragraph(f"Generated by VA HCPCS Fee Schedule Manager v{APP_VERSION}")
    footer.runs[0].font.size = Pt(8)
    footer.runs[0].font.color.rgb = RGBColor(0x66, 0x66, 0x66)
    doc.save(str(filepath))
