from datetime import datetime

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QTextDocument
from PyQt6.QtPrintSupport import QPrintPreviewDialog
from PyQt6.QtWidgets import (
    QApplication,
    QButtonGroup,
    QDialog,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QRadioButton,
    QVBoxLayout,
)

from core.exporter import (
    export_purchase_list_to_csv,
    export_purchase_list_to_docx,
    export_purchase_list_to_excel,
    export_purchase_list_to_pdf,
    export_to_csv,
    export_to_excel,
    export_to_pdf,
)


class ExportDialog(QDialog):
    def __init__(
        self,
        records=None,
        parent=None,
        is_rural=False,
        zip_code="",
        purchase_items=None,
        purchase_meta=None,
    ):
        super().__init__(parent)
        self.records = records or []
        self.purchase_items = purchase_items or []
        self.purchase_meta = purchase_meta or {}
        self.is_purchase_mode = bool(self.purchase_items)
        self.is_rural = is_rural
        self.zip_code = zip_code
        self.setWindowTitle("Export Data")
        self.setMinimumWidth(420)
        self._init_ui()

    def _init_ui(self):
        layout = QVBoxLayout(self)
        count = len(self.purchase_items) if self.is_purchase_mode else len(self.records)
        layout.addWidget(QLabel(f"Export {count:,} records as:"))

        self.btn_group = QButtonGroup(self)
        self.csv_radio = QRadioButton("CSV (.csv)")
        self.excel_radio = QRadioButton("Excel (.xlsx)")
        self.pdf_radio = QRadioButton("PDF (.pdf)")
        self.word_radio = QRadioButton("Word (.docx)")
        self.csv_radio.setChecked(True)

        radios = [self.csv_radio, self.excel_radio, self.pdf_radio]
        if self.is_purchase_mode:
            radios.append(self.word_radio)
        for rb in radios:
            self.btn_group.addButton(rb)
            layout.addWidget(rb)
            rb.toggled.connect(self._sync_po_visibility)

        self.po_row = QHBoxLayout()
        self.po_label = QLabel("PO #:")
        self.po_edit = QLineEdit()
        self.po_edit.setPlaceholderText("Optional purchase order number")
        self.po_row.addWidget(self.po_label)
        self.po_row.addWidget(self.po_edit, 1)
        layout.addLayout(self.po_row)
        self._sync_po_visibility()

        btns = QHBoxLayout()
        cancel_btn = QPushButton("Cancel")
        preview_btn = QPushButton("Print Preview")
        export_btn = QPushButton("Export")
        copy_btn = QPushButton("Copy to Clipboard")
        export_btn.setStyleSheet("background-color: #003366; color: white; padding: 6px 16px; font-weight: bold;")
        cancel_btn.clicked.connect(self.reject)
        preview_btn.clicked.connect(self._show_preview)
        export_btn.clicked.connect(self._do_export)
        copy_btn.clicked.connect(self._copy_to_clipboard)
        btns.addStretch()
        if self.is_purchase_mode:
            btns.addWidget(copy_btn)
        btns.addWidget(preview_btn)
        btns.addWidget(cancel_btn)
        btns.addWidget(export_btn)
        layout.addLayout(btns)

    def _sync_po_visibility(self):
        show_po = self.is_purchase_mode and (self.pdf_radio.isChecked() or self.word_radio.isChecked())
        self.po_label.setVisible(show_po)
        self.po_edit.setVisible(show_po)
        self.po_edit.setEnabled(show_po)

    def _selected_format(self):
        if self.csv_radio.isChecked():
            return "csv"
        if self.excel_radio.isChecked():
            return "xlsx"
        if self.pdf_radio.isChecked():
            return "pdf"
        if self.is_purchase_mode and self.word_radio.isChecked():
            return "docx"
        return "csv"

    def _purchase_export_meta(self):
        meta = dict(self.purchase_meta)
        po_number = self.po_edit.text().strip()
        if po_number:
            meta["po_number"] = po_number
        return meta

    def _do_export(self):
        fmt = self._selected_format()
        ext_map = {
            "csv": ("CSV Files (*.csv)", "purchase_list.csv" if self.is_purchase_mode else "hcpcs_export.csv"),
            "xlsx": ("Excel Files (*.xlsx)", "purchase_list.xlsx" if self.is_purchase_mode else "hcpcs_export.xlsx"),
            "pdf": ("PDF Files (*.pdf)", "purchase_list.pdf" if self.is_purchase_mode else "hcpcs_export.pdf"),
            "docx": ("Word Documents (*.docx)", "purchase_list.docx"),
        }
        ext, default = ext_map[fmt]
        path, _ = QFileDialog.getSaveFileName(self, "Save Export", default, ext)
        if not path:
            return
        try:
            if self.is_purchase_mode:
                meta = self._purchase_export_meta()
                if fmt == "docx":
                    export_purchase_list_to_docx(self.purchase_items, path, meta=meta)
                elif fmt == "pdf":
                    export_purchase_list_to_pdf(self.purchase_items, path, meta=meta)
                elif fmt == "xlsx":
                    export_purchase_list_to_excel(self.purchase_items, path, meta=meta)
                else:
                    export_purchase_list_to_csv(self.purchase_items, path, meta=meta)
            else:
                if fmt == "csv":
                    export_to_csv(self.records, path, is_rural=self.is_rural, zip_code=self.zip_code)
                elif fmt == "xlsx":
                    export_to_excel(self.records, path, is_rural=self.is_rural, zip_code=self.zip_code)
                else:
                    export_to_pdf(self.records, path, is_rural=self.is_rural, zip_code=self.zip_code)
            QMessageBox.information(self, "Export Complete", f"Data exported successfully to:\n{path}")
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Export Error", f"Export failed:\n{e}")

    def _copy_to_clipboard(self):
        if not self.is_purchase_mode:
            return
        headers = ["HCPCS Code", "Description", "Quantity", "Unit Price", "Line Total"]
        lines = ["\t".join(headers)]
        for item in self.purchase_items:
            unit = item.get("unit_price")
            line = item.get("line_total")
            lines.append("\t".join([
                str(item.get("hcpcs_code", "")),
                str(item.get("description", "")),
                str(item.get("quantity", 1)),
                "" if unit is None else f"{unit:.2f}",
                "" if line is None else f"{line:.2f}",
            ]))
        QApplication.clipboard().setText("\n".join(lines))
        QMessageBox.information(self, "Copied", "Purchase list copied to clipboard.")

    def _show_preview(self):
        html = self._build_preview_html()
        doc = QTextDocument()
        doc.setHtml(html)
        preview = QPrintPreviewDialog(self)
        preview.setWindowTitle("Print Preview")
        preview.paintRequested.connect(doc.print_)
        preview.exec()

    def _build_preview_html(self):
        if self.is_purchase_mode:
            meta = self._purchase_export_meta()
            meta_rows = [
                ("Year", meta.get("year") or "—"),
                ("State", meta.get("state") or "—"),
                ("ZIP", meta.get("zip_code") or "—"),
                ("Rural Status", meta.get("rural_status") or "Non-Rural (NR)"),
                ("Date Generated", meta.get("generated_at") or datetime.now().strftime("%Y-%m-%d %H:%M")),
            ]
            po_number = (meta.get("po_number") or "").strip()
            if po_number:
                meta_rows.insert(0, ("PO #", po_number))
            body = ["<h2>VA HCPCS Purchase List</h2>"]
            body.extend([f"<p><b>{k}:</b> {v}</p>" for k, v in meta_rows])
            body.append("<table border='1' cellspacing='0' cellpadding='4'><tr>"
                        "<th>HCPCS Code</th><th>Description</th><th>Qty</th><th>Unit Price</th><th>Line Total</th></tr>")
            total = 0.0
            for item in self.purchase_items:
                line = item.get("line_total")
                if isinstance(line, (int, float)):
                    total += line
                body.append(
                    "<tr>"
                    f"<td>{item.get('hcpcs_code', '')}</td>"
                    f"<td>{item.get('description', '')}</td>"
                    f"<td align='right'>{item.get('quantity', 1)}</td>"
                    f"<td align='right'>{'—' if item.get('unit_price') is None else f'${item['unit_price']:,.2f}'}</td>"
                    f"<td align='right'>{'—' if line is None else f'${line:,.2f}'}</td>"
                    "</tr>"
                )
            body.append(f"<tr><td colspan='4' align='right'><b>Grand Total</b></td><td align='right'><b>${total:,.2f}</b></td></tr>")
            body.append("</table>")
            return "".join(body)

        body = ["<h2>VA HCPCS Fee Schedule Report</h2>"]
        if self.zip_code:
            rural = "Rural (R)" if self.is_rural else "Non-Rural (NR)"
            body.append(f"<p><b>ZIP:</b> {self.zip_code} ({rural})</p>")
        body.append("<table border='1' cellspacing='0' cellpadding='4'><tr>"
                    "<th>HCPCS Code</th><th>Description</th><th>State</th><th>Year</th>"
                    "<th>Allowable ($)</th><th>Modifier</th></tr>")
        for r in self.records:
            chosen = r.get("allowable_r") if self.is_rural else None
            if chosen is None:
                chosen = r.get("allowable_nr")
            if chosen is None:
                chosen = r.get("allowable")
            body.append(
                "<tr>"
                f"<td>{r.get('hcpcs_code', '')}</td>"
                f"<td>{r.get('description', '')}</td>"
                f"<td>{r.get('state_abbr', '')}</td>"
                f"<td>{r.get('year', '')}</td>"
                f"<td align='right'>{'' if chosen is None else f'${chosen:,.2f}'}</td>"
                f"<td>{r.get('modifier', '') or ''}</td>"
                "</tr>"
            )
        body.append("</table>")
        return "".join(body)
