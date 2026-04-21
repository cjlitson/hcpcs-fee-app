from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QDialog,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

from core.database import delete_custom_code, list_custom_codes, save_custom_code


class CustomInputsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Manage Custom Inputs")
        self.resize(760, 500)
        self._init_ui()
        self._refresh()

    def _init_ui(self):
        root = QVBoxLayout(self)
        form = QGridLayout()

        self.code_edit = QLineEdit()
        self.code_edit.setPlaceholderText("HCPCS code")
        self.price_edit = QLineEdit()
        self.price_edit.setPlaceholderText("0.00")
        self.description_edit = QLineEdit()
        self.description_edit.setPlaceholderText("Description")
        add_btn = QPushButton("Save Custom Code")
        add_btn.clicked.connect(self._save_current)

        form.addWidget(QLabel("HCPCS Code"), 0, 0)
        form.addWidget(self.code_edit, 0, 1)
        form.addWidget(QLabel("Price"), 0, 2)
        form.addWidget(self.price_edit, 0, 3)
        form.addWidget(QLabel("Description"), 1, 0)
        form.addWidget(self.description_edit, 1, 1, 1, 3)
        form.addWidget(add_btn, 0, 4, 2, 1)
        root.addLayout(form)

        search_row = QHBoxLayout()
        search_row.addWidget(QLabel("Find"))
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Search code or description…")
        self.search_edit.textChanged.connect(self._refresh)
        search_row.addWidget(self.search_edit, 1)
        root.addLayout(search_row)

        self.table = QTableWidget(0, 4)
        self.table.setHorizontalHeaderLabels(["HCPCS Code", "Description", "Price", "Source"])
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table.itemSelectionChanged.connect(self._load_selected)
        root.addWidget(self.table, 1)

        btns = QHBoxLayout()
        delete_btn = QPushButton("Delete Selected")
        delete_btn.clicked.connect(self._delete_selected)
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        btns.addWidget(delete_btn)
        btns.addStretch()
        btns.addWidget(close_btn)
        root.addLayout(btns)

    def _refresh(self):
        items = list_custom_codes(self.search_edit.text().strip())
        self.table.setRowCount(len(items))
        for row, item in enumerate(items):
            self.table.setItem(row, 0, QTableWidgetItem(item.get("hcpcs_code", "")))
            self.table.setItem(row, 1, QTableWidgetItem(item.get("description", "")))
            price_item = QTableWidgetItem(f"{float(item.get('price', 0.0)):.2f}")
            price_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            self.table.setItem(row, 2, price_item)
            self.table.setItem(row, 3, QTableWidgetItem(item.get("source", "")))

    def _load_selected(self):
        row = self.table.currentRow()
        if row < 0:
            return
        self.code_edit.setText((self.table.item(row, 0).text() if self.table.item(row, 0) else "").strip())
        self.description_edit.setText((self.table.item(row, 1).text() if self.table.item(row, 1) else "").strip())
        self.price_edit.setText((self.table.item(row, 2).text() if self.table.item(row, 2) else "").strip())

    def _save_current(self):
        code = self.code_edit.text().strip()
        if not code:
            QMessageBox.warning(self, "Code Required", "Enter an HCPCS code.")
            return
        try:
            price = float((self.price_edit.text() or "0").replace("$", "").replace(",", ""))
        except ValueError:
            QMessageBox.warning(self, "Invalid Price", "Enter a valid numeric price.")
            return
        save_custom_code(code, self.description_edit.text().strip(), price)
        self._refresh()
        QMessageBox.information(self, "Saved", f"Saved custom code '{code.upper()}' as UserInput.")

    def _delete_selected(self):
        row = self.table.currentRow()
        if row < 0:
            QMessageBox.information(self, "Select Row", "Select a custom code to delete.")
            return
        code = (self.table.item(row, 0).text() if self.table.item(row, 0) else "").strip().upper()
        if not code:
            return
        ans = QMessageBox.question(
            self,
            "Delete Custom Code",
            f"Delete custom code '{code}'?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
            QMessageBox.StandardButton.Cancel,
        )
        if ans != QMessageBox.StandardButton.Yes:
            return
        delete_custom_code(code)
        self._refresh()
