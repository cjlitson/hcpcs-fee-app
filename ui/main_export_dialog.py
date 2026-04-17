from datetime import datetime

from PyQt6.QtGui import QTextDocument
from PyQt6.QtWidgets import (
    QButtonGroup,
    QDialog,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QRadioButton,
    QVBoxLayout,
)

from core.exporter import export_to_csv, export_to_excel, export_to_pdf


class MainExportDialog(QDialog):
    def __init__(self, records, parent=None, is_rural=False, zip_code=""):
        super().__init__(parent)
        self.records = records or []
        self.is_rural = is_rural
        self.zip_code = zip_code
        self.setWindowTitle("Export Data")
        self.setMinimumWidth(420)
        self._init_ui()

    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel(f"Export {len(self.records):,} records as:"))

        self.btn_group = QButtonGroup(self)
        self.csv_radio = QRadioButton("CSV (.csv)")
        self.excel_radio = QRadioButton("Excel (.xlsx)")
        self.pdf_radio = QRadioButton("PDF (.pdf)")
        self.csv_radio.setChecked(True)
        for rb in [self.csv_radio, self.excel_radio, self.pdf_radio]:
            self.btn_group.addButton(rb)
            layout.addWidget(rb)

        btns = QHBoxLayout()
        cancel_btn = QPushButton("Cancel")
        preview_btn = QPushButton("Print Preview")
        export_btn = QPushButton("Export")
        export_btn.setStyleSheet("background-color: #003366; color: white; padding: 6px 16px; font-weight: bold;")
        cancel_btn.clicked.connect(self.reject)
        preview_btn.clicked.connect(self._show_preview)
        export_btn.clicked.connect(self._do_export)
        btns.addStretch()
        btns.addWidget(preview_btn)
        btns.addWidget(cancel_btn)
        btns.addWidget(export_btn)
        layout.addLayout(btns)

    def _selected_format(self):
        if self.csv_radio.isChecked():
            return "csv"
        if self.excel_radio.isChecked():
            return "xlsx"
        if self.pdf_radio.isChecked():
            return "pdf"
        return "csv"

    def _do_export(self):
        fmt = self._selected_format()
        ext_map = {
            "csv": ("CSV Files (*.csv)", "hcpcs_export.csv"),
            "xlsx": ("Excel Files (*.xlsx)", "hcpcs_export.xlsx"),
            "pdf": ("PDF Files (*.pdf)", "hcpcs_export.pdf"),
        }
        ext, default = ext_map[fmt]
        path, _ = QFileDialog.getSaveFileName(self, "Save Export", default, ext)
        if not path:
            return
        try:
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

    def _show_preview(self):
        html = self._build_preview_html()
        doc = QTextDocument()
        doc.setHtml(html)
        try:
            from PyQt6.QtPrintSupport import QPrintPreviewDialog
        except ImportError as exc:
            QMessageBox.warning(self, "Print Preview Unavailable", f"Unable to load print preview:\n{str(exc)}")
            return
        preview = QPrintPreviewDialog(self)
        preview.setWindowTitle("Print Preview")
        preview.paintRequested.connect(doc.print_)
        preview.exec()

    def _build_preview_html(self):
        body = ["<h2>VA HCPCS Fee Schedule Report</h2>"]
        if self.zip_code:
            rural = "Rural (R)" if self.is_rural else "Non-Rural (NR)"
            body.append(f"<p><b>ZIP:</b> {self.zip_code} ({rural})</p>")
        body.append(
            f"<p><b>Date Generated:</b> {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>"
        )
        body.append(
            "<table border='1' cellspacing='0' cellpadding='4'><tr>"
            "<th>HCPCS Code</th><th>Description</th><th>State</th><th>Year</th>"
            "<th>Allowable ($)</th><th>Modifier</th></tr>"
        )
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
