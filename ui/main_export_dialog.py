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

        selector_style = (
            "QRadioButton {"
            " border: 1px solid #9AA6B2;"
            " border-radius: 6px;"
            " padding: 8px 10px;"
            " background: #FFFFFF;"
            " color: #1F2933;"
            " font-weight: 600;"
            "}"
            "QRadioButton::indicator { width: 0px; height: 0px; }"
            "QRadioButton:checked {"
            " background: #003366;"
            " border: 1px solid #003366;"
            " color: #FFFFFF;"
            "}"
            "QRadioButton:hover { border: 1px solid #005A9C; }"
        )
        self.btn_group = QButtonGroup(self)
        self.csv_radio = QRadioButton("CSV (.csv)")
        self.excel_radio = QRadioButton("Excel (.xlsx)")
        self.pdf_radio = QRadioButton("PDF (.pdf)")
        self.csv_radio.setChecked(True)
        for rb in [self.csv_radio, self.excel_radio, self.pdf_radio]:
            self.btn_group.addButton(rb)
            rb.setStyleSheet(selector_style)
            layout.addWidget(rb)

        btns = QHBoxLayout()
        cancel_btn = QPushButton("Cancel")
        export_btn = QPushButton("Export")
        export_btn.setStyleSheet("background-color: #003366; color: white; padding: 6px 16px; font-weight: bold;")
        cancel_btn.clicked.connect(self.reject)
        export_btn.clicked.connect(self._do_export)
        btns.addStretch()
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
