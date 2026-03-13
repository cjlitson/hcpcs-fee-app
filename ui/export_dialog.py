from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QFileDialog, QRadioButton, QButtonGroup, QMessageBox
)
from PyQt6.QtCore import Qt
from core.exporter import export_to_csv, export_to_excel, export_to_pdf


class ExportDialog(QDialog):
    def __init__(self, records, parent=None):
        super().__init__(parent)
        self.records = records
        self.setWindowTitle("Export Data")
        self.setMinimumWidth(380)
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
        export_btn = QPushButton("Export...")
        export_btn.setStyleSheet("background-color: #003366; color: white; padding: 6px 16px; font-weight: bold;")
        cancel_btn.clicked.connect(self.reject)
        export_btn.clicked.connect(self._do_export)
        btns.addStretch()
        btns.addWidget(cancel_btn)
        btns.addWidget(export_btn)
        layout.addLayout(btns)

    def _do_export(self):
        if self.csv_radio.isChecked():
            ext = "CSV Files (*.csv)"
            default = "hcpcs_export.csv"
        elif self.excel_radio.isChecked():
            ext = "Excel Files (*.xlsx)"
            default = "hcpcs_export.xlsx"
        else:
            ext = "PDF Files (*.pdf)"
            default = "hcpcs_export.pdf"

        path, _ = QFileDialog.getSaveFileName(self, "Save Export", default, ext)
        if not path:
            return
        try:
            if self.csv_radio.isChecked():
                export_to_csv(self.records, path)
            elif self.excel_radio.isChecked():
                export_to_excel(self.records, path)
            else:
                export_to_pdf(self.records, path)
            QMessageBox.information(self, "Export Complete", f"Data exported successfully to:\n{path}")
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Export Error", f"Export failed:\n{e}")
