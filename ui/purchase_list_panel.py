from datetime import datetime

from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtWidgets import (
    QDialog,
    QFileDialog,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
    QInputDialog,
)

from core.database import (
    delete_bundle,
    get_fees,
    is_rural_zip,
    list_bundles,
    load_bundle,
    save_bundle,
)
from core.exporter import (
    export_purchase_list_to_csv,
    export_purchase_list_to_docx,
    export_purchase_list_to_excel,
    export_purchase_list_to_pdf,
)
from ui.purchase_list_dialog import BundlePickerDialog


class PurchaseListPanel(QWidget):
    count_changed = pyqtSignal(int)

    def __init__(self, parent=None, year_combo=None, state_combo=None, zip_edit=None):
        super().__init__(parent)
        self._year_combo = year_combo
        self._state_combo = state_combo
        self._zip_edit = zip_edit
        self._bundle_name = None
        self._init_ui()

    def _init_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(6)

        header = QHBoxLayout()
        self.title_label = QLabel("Purchase List (0)")
        self.title_label.setStyleSheet("font-weight: bold; font-size: 14px;")
        header.addWidget(self.title_label)
        header.addStretch()
        self.bundle_label = QLabel("Bundle: —")
        header.addWidget(self.bundle_label)
        root.addLayout(header)

        controls = QHBoxLayout()
        controls.addStretch()
        select_all_btn = QPushButton("Select All")
        deselect_all_btn = QPushButton("Deselect All")
        select_all_btn.clicked.connect(self.select_all_items)
        deselect_all_btn.clicked.connect(self.deselect_all_items)
        controls.addWidget(select_all_btn)
        controls.addWidget(deselect_all_btn)
        root.addLayout(controls)

        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels(
            ["", "HCPCS Code", "Description", "Qty", "Unit Price", "Line Total"]
        )
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setAlternatingRowColors(True)
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        root.addWidget(self.table, 1)

        totals = QHBoxLayout()
        totals.addStretch()
        totals.addWidget(QLabel("Grand Total:"))
        self.grand_total_label = QLabel("$0.00")
        self.grand_total_label.setStyleSheet("font-weight: bold; color: #003366;")
        totals.addWidget(self.grand_total_label)
        root.addLayout(totals)

        btns = QHBoxLayout()
        save_bundle_btn = QPushButton("Save Bundle…")
        load_bundle_btn = QPushButton("Load Bundle…")
        export_btn = QPushButton("Export…")
        clear_btn = QPushButton("Clear")
        save_bundle_btn.clicked.connect(self._save_bundle)
        load_bundle_btn.clicked.connect(self._load_bundle)
        export_btn.clicked.connect(self._export)
        clear_btn.clicked.connect(self._clear_list)
        btns.addWidget(save_bundle_btn)
        btns.addWidget(load_bundle_btn)
        btns.addWidget(export_btn)
        btns.addWidget(clear_btn)
        root.addLayout(btns)

    @staticmethod
    def _checkbox_item(checked=False):
        item = QTableWidgetItem("")
        item.setFlags(
            Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable
        )
        item.setCheckState(Qt.CheckState.Checked if checked else Qt.CheckState.Unchecked)
        return item

    def _effective_year(self):
        return self._year_combo.currentData() if self._year_combo else None

    def _state_abbr(self):
        return self._state_combo.currentData() if self._state_combo else None

    def _zip_code(self):
        return self._zip_edit.text().strip() if self._zip_edit else ""

    def _is_rural(self):
        zip5 = self._zip_code()
        year = self._effective_year()
        if not year or len(zip5) != 5 or not zip5.isdigit():
            return False
        return is_rural_zip(year, zip5)

    def add_code(self, hcpcs_code, description_hint=""):
        code = (hcpcs_code or "").strip().upper()
        if not code:
            return
        for row in range(self.table.rowCount()):
            existing = self.table.item(row, 1)
            if existing and existing.text().upper() == code:
                spin = self.table.cellWidget(row, 3)
                if isinstance(spin, QSpinBox):
                    spin.setValue(spin.value() + 1)
                return
        row = self.table.rowCount()
        self.table.insertRow(row)
        self.table.setItem(row, 0, self._checkbox_item(False))
        self.table.setItem(row, 1, QTableWidgetItem(code))
        description_text = (description_hint or "").strip()
        if not description_text:
            description_text = self._lookup_description(code)
        self.table.setItem(row, 2, QTableWidgetItem(description_text))
        qty_spin = QSpinBox()
        qty_spin.setMinimum(1)
        qty_spin.setMaximum(9999)
        qty_spin.setValue(1)
        qty_spin.valueChanged.connect(self.refresh_prices)
        self.table.setCellWidget(row, 3, qty_spin)
        self.table.setItem(row, 4, QTableWidgetItem("—"))
        self.table.setItem(row, 5, QTableWidgetItem("—"))
        self.refresh_prices()

    def _lookup_description(self, code):
        records = get_fees(
            state_abbr=self._state_abbr(),
            year=self._effective_year(),
            hcpcs_code=code,
        )
        for rec in records:
            if (rec.get("hcpcs_code") or "").upper() == code and rec.get("description"):
                return rec["description"]
        fallback = get_fees(hcpcs_code=code)
        for rec in fallback:
            if (rec.get("hcpcs_code") or "").upper() == code and rec.get("description"):
                return rec["description"]
        return ""

    def _lookup_price(self, code):
        records = get_fees(
            state_abbr=self._state_abbr(),
            year=self._effective_year(),
            hcpcs_code=code,
        )
        exact = [r for r in records if (r.get("hcpcs_code") or "").upper() == code.upper()]
        if not exact:
            return None
        preferred = next((r for r in exact if not r.get("modifier")), exact[0])
        if self._is_rural():
            return preferred.get("allowable_r") or preferred.get("allowable_nr") or preferred.get("allowable")
        return preferred.get("allowable_nr") or preferred.get("allowable")

    def refresh_prices(self):
        total = 0.0
        for row in range(self.table.rowCount()):
            code_item = self.table.item(row, 1)
            if not code_item:
                continue
            code = code_item.text().strip().upper()
            price = self._lookup_price(code)
            qty_widget = self.table.cellWidget(row, 3)
            qty = qty_widget.value() if isinstance(qty_widget, QSpinBox) else 1
            line_total = None if price is None else float(price) * qty
            unit_txt = "—" if price is None else f"${float(price):,.2f}"
            line_txt = "—" if line_total is None else f"${line_total:,.2f}"
            if line_total is not None:
                total += line_total
            unit_item = QTableWidgetItem(unit_txt)
            line_item = QTableWidgetItem(line_txt)
            unit_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            line_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            self.table.setItem(row, 4, unit_item)
            self.table.setItem(row, 5, line_item)
        self.grand_total_label.setText(f"${total:,.2f}")
        self._update_header()

    def _update_header(self):
        count = self.table.rowCount()
        self.title_label.setText(f"Purchase List ({count})")
        self.count_changed.emit(count)

    def _collect_items(self):
        items = []
        for row in range(self.table.rowCount()):
            code_item = self.table.item(row, 1)
            desc_item = self.table.item(row, 2)
            qty_widget = self.table.cellWidget(row, 3)
            if not code_item:
                continue
            code = code_item.text().strip().upper()
            qty = qty_widget.value() if isinstance(qty_widget, QSpinBox) else 1
            price = self._lookup_price(code)
            line = None if price is None else float(price) * qty
            items.append(
                {
                    "hcpcs_code": code,
                    "description": desc_item.text() if desc_item else "",
                    "quantity": qty,
                    "unit_price": None if price is None else float(price),
                    "line_total": line,
                    "sort_order": row,
                }
            )
        return items

    def _save_bundle(self):
        items = self._collect_items()
        if not items:
            QMessageBox.information(self, "No Items", "Add at least one HCPCS code before saving a bundle.")
            return
        default_name = self._bundle_name or ""
        name, ok = QInputDialog.getText(self, "Save Bundle", "Bundle name:", text=default_name)
        if not ok:
            return
        name = name.strip()
        if not name:
            return
        existing = next((b for b in list_bundles() if b["name"].strip().lower() == name.lower()), None)
        if existing:
            ans = QMessageBox.question(
                self,
                "Overwrite Bundle",
                f"A bundle named '{name}' already exists. Overwrite it?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
                QMessageBox.StandardButton.Cancel,
            )
            if ans != QMessageBox.StandardButton.Yes:
                return
            delete_bundle(existing["id"])
        save_bundle(name, items)
        self._bundle_name = name
        self.bundle_label.setText(f"Bundle: {name}")
        QMessageBox.information(self, "Bundle Saved", f"Saved bundle '{name}'.")

    def _load_bundle(self):
        dlg = BundlePickerDialog(self)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        payload = load_bundle(dlg.selected_bundle_id)
        if not payload:
            return
        self._clear_list(confirm=False)
        self._bundle_name = payload.get("name")
        self.bundle_label.setText(f"Bundle: {self._bundle_name}")
        desc_cache = {}
        for item in payload.get("items", []):
            code = (item.get("hcpcs_code") or "").upper()
            if not code:
                continue
            description_text = (item.get("description") or "").strip()
            if not description_text:
                if code not in desc_cache:
                    desc_cache[code] = self._lookup_description(code)
                description_text = desc_cache[code]
            row = self.table.rowCount()
            self.table.insertRow(row)
            self.table.setItem(row, 0, self._checkbox_item(False))
            self.table.setItem(row, 1, QTableWidgetItem(code))
            self.table.setItem(row, 2, QTableWidgetItem(description_text))
            qty_spin = QSpinBox()
            qty_spin.setMinimum(1)
            qty_spin.setMaximum(9999)
            qty_spin.setValue(max(1, int(item.get("quantity", 1) or 1)))
            qty_spin.valueChanged.connect(self.refresh_prices)
            self.table.setCellWidget(row, 3, qty_spin)
            self.table.setItem(row, 4, QTableWidgetItem("—"))
            self.table.setItem(row, 5, QTableWidgetItem("—"))
        self.refresh_prices()

    def _export(self):
        items = self._collect_items()
        if not items:
            QMessageBox.information(self, "No Items", "No purchase list items to export.")
            return
        default_name = "purchase_list"
        filters = "Word Documents (*.docx);;PDF Files (*.pdf);;Excel Files (*.xlsx);;CSV Files (*.csv)"
        path, _ = QFileDialog.getSaveFileName(self, "Export Purchase List", default_name, filters)
        if not path:
            return
        suffix = path.lower().split(".")[-1] if "." in path else ""
        meta = {
            "bundle_name": self._bundle_name or "",
            "year": self._effective_year(),
            "state": self._state_abbr() or "",
            "zip_code": self._zip_code(),
            "rural_status": "Rural (R)" if self._is_rural() else "Non-Rural (NR)",
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
        try:
            if suffix == "docx":
                export_purchase_list_to_docx(items, path, meta=meta)
            elif suffix == "pdf":
                export_purchase_list_to_pdf(items, path, meta=meta)
            elif suffix == "xlsx":
                export_purchase_list_to_excel(items, path, meta=meta)
            else:
                if suffix != "csv":
                    path = f"{path}.csv"
                export_purchase_list_to_csv(items, path, meta=meta)
            QMessageBox.information(self, "Export Complete", f"Purchase list exported to:\n{path}")
        except Exception as exc:
            QMessageBox.critical(self, "Export Error", f"Export failed:\n{exc}")

    def remove_checked_items(self):
        removed = 0
        for row in reversed(range(self.table.rowCount())):
            check_item = self.table.item(row, 0)
            if check_item and check_item.checkState() == Qt.CheckState.Checked:
                self.table.removeRow(row)
                removed += 1
        if removed:
            self.refresh_prices()
        return removed

    def select_all_items(self):
        for row in range(self.table.rowCount()):
            item = self.table.item(row, 0)
            if item:
                item.setCheckState(Qt.CheckState.Checked)

    def deselect_all_items(self):
        for row in range(self.table.rowCount()):
            item = self.table.item(row, 0)
            if item:
                item.setCheckState(Qt.CheckState.Unchecked)

    def _clear_list(self, confirm=True):
        if confirm and self.table.rowCount() > 0:
            ans = QMessageBox.question(
                self,
                "Clear List",
                "Remove all items from the purchase list?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
                QMessageBox.StandardButton.Cancel,
            )
            if ans != QMessageBox.StandardButton.Yes:
                return
        self.table.setRowCount(0)
        self._bundle_name = None
        self.bundle_label.setText("Bundle: —")
        self.refresh_prices()
