from __future__ import annotations

from datetime import datetime

from core.version import APP_VERSION


def generate_purchase_document_docx(filepath, payload: dict) -> None:
    """Generate an invoice-style Word document with improved formatting."""
    try:
        from docx import Document
        from docx.enum.text import WD_ALIGN_PARAGRAPH
        from docx.shared import Pt, RGBColor
    except ImportError:
        raise ImportError("python-docx is required for document generation. Run: pip install python-docx")

    doc = Document()

    # Title
    title = doc.add_paragraph()
    title_run = title.add_run("VA HCPCS Worksheet")
    title_run.bold = True
    title_run.font.size = Pt(18)
    title_run.font.color.rgb = RGBColor(0, 51, 102)
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    doc.add_paragraph("")  # Spacing

    # Patient Information Section
    section_heading = doc.add_paragraph()
    section_heading.add_run("PATIENT INFORMATION").bold = True
    section_heading.add_run("").font.size = Pt(12)

    info_table = doc.add_table(rows=3, cols=4)
    info_table.style = "Table Grid"

    # Row 1
    cells = info_table.rows[0].cells
    cells[0].text = "Veteran Last Name:"
    cells[0].paragraphs[0].runs[0].bold = True
    cells[1].text = str(payload.get("veteran_last_name", ""))
    cells[2].text = "Last 4:"
    cells[2].paragraphs[0].runs[0].bold = True
    cells[3].text = str(payload.get("last4", ""))

    # Row 2
    cells = info_table.rows[1].cells
    cells[0].text = "Consult Date:"
    cells[0].paragraphs[0].runs[0].bold = True
    cells[1].text = str(payload.get("consult_date", ""))
    cells[2].text = "Deliver To:"
    cells[2].paragraphs[0].runs[0].bold = True
    cells[3].text = str(payload.get("deliver_to", ""))

    # Row 3
    cells = info_table.rows[2].cells
    cells[0].text = "Vendor:"
    cells[0].paragraphs[0].runs[0].bold = True
    cells[1].text = str(payload.get("vendor", ""))
    cells[1].merge(cells[3])

    doc.add_paragraph("")  # Spacing

    # Items Section
    section_heading = doc.add_paragraph()
    section_heading.add_run("ORDER ITEMS").bold = True
    section_heading.add_run("").font.size = Pt(12)

    table = doc.add_table(rows=1, cols=5)
    table.style = "Table Grid"
    headers = ["HCPCS Code", "Quantity", "Unit Cost", "Description", "Line Total"]
    header_cells = table.rows[0].cells
    for idx, text in enumerate(headers):
        run = header_cells[idx].paragraphs[0].add_run(text)
        run.bold = True
        run.font.color.rgb = RGBColor(255, 255, 255)
        header_cells[idx].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
        # Apply background color to header (note: python-docx doesn't fully support cell shading via API,
        # but we can set the text color to white to improve visibility)

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
        row[1].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
        row[2].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.RIGHT
        row[4].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.RIGHT

    total_row = table.add_row().cells
    total_row[3].text = "GRAND TOTAL"
    total_row[4].text = f"${total:,.2f}"
    total_row[3].paragraphs[0].runs[0].bold = True
    total_row[3].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.RIGHT
    total_row[4].paragraphs[0].runs[0].bold = True
    total_row[4].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.RIGHT

    comments = (payload.get("comments") or "").strip()
    if comments:
        doc.add_paragraph("")
        p = doc.add_paragraph()
        p.add_run("Additional Comments: ").bold = True
        p.add_run(comments)

    doc.add_paragraph("")
    footer = doc.add_paragraph(
        f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} by VA HCPCS Fee Schedule Manager v{APP_VERSION}"
    )
    footer.runs[0].italic = True
    footer.runs[0].font.size = Pt(9)
    footer.runs[0].font.color.rgb = RGBColor(128, 128, 128)
    doc.save(str(filepath))


def generate_purchase_document_excel(filepath, payload: dict) -> None:
    """Generate an invoice-style Excel document."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
    except ImportError:
        raise ImportError("openpyxl is required for Excel generation. Run: pip install openpyxl")

    wb = Workbook()
    ws = wb.active
    ws.title = "HCPCS Worksheet"

    # Title
    ws['A1'] = "VA HCPCS Worksheet"
    ws['A1'].font = Font(size=18, bold=True, color="003366")
    ws['A1'].alignment = Alignment(horizontal='center')
    ws.merge_cells('A1:E1')

    # Patient Information
    row = 3
    ws[f'A{row}'] = "PATIENT INFORMATION"
    ws[f'A{row}'].font = Font(bold=True, size=12)

    row += 1
    info_data = [
        ("Veteran Last Name:", payload.get("veteran_last_name", ""), "Last 4:", payload.get("last4", "")),
        ("Consult Date:", payload.get("consult_date", ""), "Deliver To:", payload.get("deliver_to", "")),
        ("Vendor:", payload.get("vendor", ""), "", ""),
    ]

    for info_row in info_data:
        ws[f'A{row}'] = info_row[0]
        ws[f'A{row}'].font = Font(bold=True)
        ws[f'B{row}'] = info_row[1]
        ws[f'C{row}'] = info_row[2]
        ws[f'C{row}'].font = Font(bold=True)
        ws[f'D{row}'] = info_row[3]
        row += 1

    # Items Section
    row += 1
    ws[f'A{row}'] = "ORDER ITEMS"
    ws[f'A{row}'].font = Font(bold=True, size=12)

    row += 1
    headers = ["HCPCS Code", "Quantity", "Unit Cost", "Description", "Line Total"]
    for col_idx, header in enumerate(headers, start=1):
        cell = ws.cell(row=row, column=col_idx, value=header)
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill(start_color="003366", end_color="003366", fill_type="solid")
        cell.alignment = Alignment(horizontal='center')

    total = 0.0
    for item in payload.get("items", []):
        row += 1
        qty = int(item.get("quantity") or 0)
        cost = float(item.get("cost") or 0.0)
        line_total = qty * cost
        total += line_total

        ws.cell(row=row, column=1, value=str(item.get("hcpcs_code", "")))
        ws.cell(row=row, column=2, value=qty).alignment = Alignment(horizontal='center')
        ws.cell(row=row, column=3, value=cost).number_format = '$#,##0.00'
        ws.cell(row=row, column=4, value=str(item.get("description", "")))
        ws.cell(row=row, column=5, value=line_total).number_format = '$#,##0.00'

    row += 1
    ws.cell(row=row, column=4, value="GRAND TOTAL").font = Font(bold=True)
    ws.cell(row=row, column=4).alignment = Alignment(horizontal='right')
    ws.cell(row=row, column=5, value=total).font = Font(bold=True)
    ws.cell(row=row, column=5).number_format = '$#,##0.00'

    # Comments
    comments = (payload.get("comments") or "").strip()
    if comments:
        row += 2
        ws.cell(row=row, column=1, value="Additional Comments:").font = Font(bold=True)
        row += 1
        ws.cell(row=row, column=1, value=comments)
        ws.merge_cells(f'A{row}:E{row}')

    # Footer
    row += 2
    footer_text = f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} by VA HCPCS Fee Schedule Manager v{APP_VERSION}"
    ws.cell(row=row, column=1, value=footer_text).font = Font(italic=True, size=9, color="808080")
    ws.merge_cells(f'A{row}:E{row}')

    # Adjust column widths
    ws.column_dimensions['A'].width = 15
    ws.column_dimensions['B'].width = 10
    ws.column_dimensions['C'].width = 12
    ws.column_dimensions['D'].width = 50
    ws.column_dimensions['E'].width = 15

    wb.save(str(filepath))


def generate_purchase_document_pdf(filepath, payload: dict) -> None:
    """Generate an invoice-style PDF document."""
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.enums import TA_CENTER, TA_RIGHT
    except ImportError:
        raise ImportError("reportlab is required for PDF generation. Run: pip install reportlab")

    doc = SimpleDocTemplate(str(filepath), pagesize=letter)
    styles = getSampleStyleSheet()
    story = []

    # Title
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=18,
        textColor=colors.HexColor('#003366'),
        spaceAfter=20,
        alignment=TA_CENTER,
    )
    story.append(Paragraph("VA HCPCS Worksheet", title_style))
    story.append(Spacer(1, 12))

    # Patient Information
    heading_style = ParagraphStyle(
        'SectionHeading',
        parent=styles['Heading2'],
        fontSize=12,
        spaceAfter=10,
    )
    story.append(Paragraph("PATIENT INFORMATION", heading_style))

    info_data = [
        ["Veteran Last Name:", payload.get("veteran_last_name", ""), "Last 4:", payload.get("last4", "")],
        ["Consult Date:", payload.get("consult_date", ""), "Deliver To:", payload.get("deliver_to", "")],
        ["Vendor:", payload.get("vendor", ""), "", ""],
    ]

    info_table = Table(info_data, colWidths=[1.5*inch, 2*inch, 1.5*inch, 2*inch])
    info_table.setStyle(TableStyle([
        ('FONT', (0, 0), (0, -1), 'Helvetica-Bold', 10),
        ('FONT', (2, 0), (2, -1), 'Helvetica-Bold', 10),
        ('FONT', (1, 0), (1, -1), 'Helvetica', 10),
        ('FONT', (3, 0), (3, -1), 'Helvetica', 10),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('LEFTPADDING', (0, 0), (-1, -1), 6),
        ('RIGHTPADDING', (0, 0), (-1, -1), 6),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
    ]))
    story.append(info_table)
    story.append(Spacer(1, 20))

    # Items
    story.append(Paragraph("ORDER ITEMS", heading_style))

    items_data = [["HCPCS Code", "Quantity", "Unit Cost", "Description", "Line Total"]]
    total = 0.0
    for item in payload.get("items", []):
        qty = int(item.get("quantity") or 0)
        cost = float(item.get("cost") or 0.0)
        line_total = qty * cost
        total += line_total
        items_data.append([
            str(item.get("hcpcs_code", "")),
            str(qty),
            f"${cost:,.2f}",
            str(item.get("description", "")),
            f"${line_total:,.2f}",
        ])

    items_data.append(["", "", "", "GRAND TOTAL", f"${total:,.2f}"])

    items_table = Table(items_data, colWidths=[1*inch, 0.8*inch, 1*inch, 3*inch, 1*inch])
    items_table.setStyle(TableStyle([
        # Header row
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#003366')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('FONT', (0, 0), (-1, 0), 'Helvetica-Bold', 10),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        # Data rows
        ('FONT', (0, 1), (-1, -2), 'Helvetica', 9),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('ALIGN', (1, 1), (1, -1), 'CENTER'),
        ('ALIGN', (2, 1), (2, -1), 'RIGHT'),
        ('ALIGN', (4, 1), (4, -1), 'RIGHT'),
        # Total row
        ('FONT', (3, -1), (4, -1), 'Helvetica-Bold', 10),
        ('ALIGN', (3, -1), (3, -1), 'RIGHT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('LEFTPADDING', (0, 0), (-1, -1), 6),
        ('RIGHTPADDING', (0, 0), (-1, -1), 6),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
    ]))
    story.append(items_table)

    # Comments
    comments = (payload.get("comments") or "").strip()
    if comments:
        story.append(Spacer(1, 20))
        story.append(Paragraph(f"<b>Additional Comments:</b> {comments}", styles['Normal']))

    # Footer
    story.append(Spacer(1, 20))
    footer_text = f"<i>Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} by VA HCPCS Fee Schedule Manager v{APP_VERSION}</i>"
    footer_style = ParagraphStyle(
        'Footer',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.grey,
    )
    story.append(Paragraph(footer_text, footer_style))

    doc.build(story)
