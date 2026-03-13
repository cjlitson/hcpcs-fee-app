import os
from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QFileDialog, QTableWidget, QTableWidgetItem, QProgressBar,
    QMessageBox, QComboBox
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from core.importer import import_visn_csv
from core.database import get_selected_states


class ImportWorker(QThread):
    finished = pyqtSignal(int)
    error = pyqtSignal(str)

    def __init__(self, filepath, selected_states):
        super().__init__()
        self.filepath = filepath
        self.selected_states = selected_states

    def run(self):
        try:
            count = import_visn_csv(self.filepath, selected_states=self.selected_states)
            self.finished.emit(count)
        except Exception as e:
            self.error.emit(str(e))


class ImportDialog(QDialog):
    import_complete = pyqtSignal(int)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Import CSV File")
        self.setMinimumSize(700, 520)
        self.filepath = None
        self.worker = None
        self._init_ui()

    def _init_ui(self):
        layout = QVBoxLayout(self)

        # File selection
        file_row = QHBoxLayout()
        self.file_label = QLabel("No file selected")
        self.file_label.setStyleSheet("border: 1px solid #ccc; padding: 4px; background: #f9f9f9;")
        browse_btn = QPushButton("Browse...")
        browse_btn.clicked.connect(self._browse)
        file_row.addWidget(QLabel("CSV File:"))
        file_row.addWidget(self.file_label, 1)
        file_row.addWidget(browse_btn)
        layout.addLayout(file_row)

        # State filter
        state_row = QHBoxLayout()
        self.state_combo = QComboBox()
        self.state_combo.addItem("Only tracked states (recommended)", "tracked")
        self.state_combo.addItem("All states in file", "all")
        state_row.addWidget(QLabel("Import states:"))
        state_row.addWidget(self.state_combo)
        state_row.addStretch()
        layout.addLayout(state_row)

        # Preview
        layout.addWidget(QLabel("Preview (first 10 rows):"))
        self.preview_table = QTableWidget(0, 6)
        self.preview_table.setHorizontalHeaderLabels(["HCPCS Code", "Description", "State", "Year", "Allowable", "Modifier"])
        self.preview_table.horizontalHeader().setStretchLastSection(True)
        self.preview_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.preview_table.setMinimumHeight(200)
        layout.addWidget(self.preview_table)

        # Progress
        self.progress = QProgressBar()
        self.progress.setVisible(False)
        layout.addWidget(self.progress)

        self.status_label = QLabel("")
        self.status_label.setStyleSheet("color: #003366;")
        layout.addWidget(self.status_label)

        # Buttons
        btn_row = QHBoxLayout()
        self.import_btn = QPushButton("Import")
        self.import_btn.setStyleSheet("background-color: #003366; color: white; padding: 6px 16px; font-weight: bold;")
        self.import_btn.setEnabled(False)
        cancel_btn = QPushButton("Cancel")
        self.import_btn.clicked.connect(self._do_import)
        cancel_btn.clicked.connect(self.reject)
        btn_row.addStretch()
        btn_row.addWidget(cancel_btn)
        btn_row.addWidget(self.import_btn)
        layout.addLayout(btn_row)

    def _browse(self):
        path, _ = QFileDialog.getOpenFileName(self, "Select CSV File", "", "CSV Files (*.csv);;All Files (*)")
        if path:
            self.filepath = path
            self.file_label.setText(os.path.basename(path))
            self.import_btn.setEnabled(True)
            self._load_preview(path)

    def _load_preview(self, path):
        from core.importer import parse_visn_csv
        try:
            records = parse_visn_csv(path)[:10]
            self.preview_table.setRowCount(len(records))
            for row_i, r in enumerate(records):
                vals = [
                    r.get("hcpcs_code", ""),
                    r.get("description", "")[:60],
                    r.get("state_abbr", ""),
                    str(r.get("year", "")),
                    str(r.get("allowable", "#N/A")) if r.get("allowable") is not None else "#N/A",
                    r.get("modifier", "") or "",
                ]
                for col_i, v in enumerate(vals):
                    self.preview_table.setItem(row_i, col_i, QTableWidgetItem(str(v)))
            self.status_label.setText(f"Preview loaded. File looks valid.")
        except Exception as e:
            self.status_label.setText(f"Warning: {e}")

    def _do_import(self):
        if not self.filepath:
            return

        selected_states = None
        if self.state_combo.currentData() == "tracked":
            selected_states = [abbr for abbr, _ in get_selected_states()]

        self.import_btn.setEnabled(False)
        self.progress.setVisible(True)
        self.progress.setRange(0, 0)
        self.status_label.setText("Importing...")

        self.worker = ImportWorker(self.filepath, selected_states)
        self.worker.finished.connect(self._on_done)
        self.worker.error.connect(self._on_error)
        self.worker.start()

    def _on_done(self, count):
        self.progress.setVisible(False)
        self.status_label.setText(f"Done! Imported {count:,} records.")
        self.import_complete.emit(count)
        QMessageBox.information(self, "Import Complete", f"Successfully imported {count:,} records.")
        self.accept()

    def _on_error(self, msg):
        self.progress.setVisible(False)
        self.import_btn.setEnabled(True)
        self.status_label.setText(f"Error: {msg}")
        QMessageBox.critical(self, "Import Error", msg)