from datetime import datetime

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QDialog,
    QFileDialog,
    QGridLayout,
    QHBoxLayout,
    QHeaderView,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMenu,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

from core.database import (
    delete_bundle,
    get_available_years,
    get_current_year_or_fallback,
    get_fees,
    get_selected_states,
    is_rural_zip,
    list_bundles,
    load_bundle,
    rename_bundle,
    save_bundle,
)
from core.exporter import (
    export_purchase_list_to_csv,
    export_purchase_list_to_excel,
    export_purchase_list_to_pdf,
)


class BundlePickerDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Load Bundle")
        self.setMinimumSize(620, 360)
        self.selected_bundle_id = None
        self._init_ui()
        self._refresh()

    def _init_ui(self):
        root = QVBoxLayout(self)
        self.table = QTableWidget(0, 4)
        self.table.setHorizontalHeaderLabels(["Name", "# Items", "Created", "Last Updated"])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.doubleClicked.connect(self._load_selected)
        root.addWidget(self.table, 1)

        btns = QHBoxLayout()
        rename_btn = QPushButton("Rename")
        delete_btn = QPushButton("Delete")
        load_btn = QPushButton("Load")
        cancel_btn = QPushButton("Cancel")
        rename_btn.clicked.connect(self._rename_selected)
        delete_btn.clicked.connect(self._delete_selected)
        load_btn.clicked.connect(self._load_selected)
        cancel_btn.clicked.connect(self.reject)
        btns.addWidget(rename_btn)
        btns.addWidget(delete_btn)
        btns.addStretch()
        btns.addWidget(cancel_btn)
        btns.addWidget(load_btn)
        root.addLayout(btns)

    def _refresh(self):
        bundles = list_bundles()
        self.table.setRowCount(len(bundles))
        for row, bundle in enumerate(bundles):
            name_item = QTableWidgetItem(bundle["name"])
            name_item.setData(Qt.ItemDataRole.UserRole, bundle["id"])
            self.table.setItem(row, 0, name_item)
            self.table.setItem(row, 1, QTableWidgetItem(str(bundle.get("item_count", 0))))
            self.table.setItem(row, 2, QTableWidgetItem(bundle.get("created_at", "") or ""))
            self.table.setItem(row, 3, QTableWidgetItem(bundle.get("updated_at", "") or ""))
        if bundles:
            self.table.selectRow(0)

    def _current_bundle_id(self):
        row = self.table.currentRow()
        if row < 0:
            return None
        item = self.table.item(row, 0)
        if not item:
            return None
        return item.data(Qt.ItemDataRole.UserRole)

    def _rename_selected(self):
        bundle_id = self._current_bundle_id()
        if bundle_id is None:
            return
        current = self.table.item(self.table.currentRow(), 0).text()
        new_name, ok = QInputDialog.getText(self, "Rename Bundle", "New bundle name:", text=current)
        if not ok:
            return
        new_name = new_name.strip()
        if not new_name:
            return
        try:
            rename_bundle(bundle_id, new_name)
            self._refresh()
        except Exception as exc:
            QMessageBox.critical(self, "Rename Failed", str(exc))

    def _delete_selected(self):
        bundle_id = self._current_bundle_id()
        if bundle_id is None:
            return
        name = self.table.item(self.table.currentRow(), 0).text()
        ans = QMessageBox.question(
            self,
            "Delete Bundle",
            f"Delete bundle '{name}'?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
            QMessageBox.StandardButton.Cancel,
        )
        if ans != QMessageBox.StandardButton.Yes:
            return
        delete_bundle(bundle_id)
        self._refresh()

    def _load_selected(self):
        bundle_id = self._current_bundle_id()
        if bundle_id is None:
            return
        self.selected_bundle_id = bundle_id
        self.accept()


class PurchaseListDialog(QDialog):
    def __init__(self, parent=None, default_year=None, default_state=None, default_zip="", initial_code=None):
        super().__init__(parent)
        self.setWindowTitle("Purchase List Builder")
        self.setMinimumSize(1040, 620)
        self._bundle_name = None
        self._init_ui()
        self._load_filters(default_year=default_year, default_state=default_state, default_zip=default_zip)
        if initial_code:
            self._add_code(initial_code)

    def _init_ui(self):
        root = QVBoxLayout(self)
        controls = QGridLayout()
        controls.addWidget(QLabel("Year:"), 0, 0)
        self.year_combo = QComboBox()
        self.year_combo.currentIndexChanged.connect(self._on_pricing_inputs_changed)
        controls.addWidget(self.year_combo, 0, 1)

        controls.addWidget(QLabel("State:"), 0, 2)
        self.state_combo = QComboBox()
        self.state_combo.currentIndexChanged.connect(self._on_pricing_inputs_changed)
        controls.addWidget(self.state_combo, 0, 3)

        controls.addWidget(QLabel("ZIP:"), 0, 4)
        self.zip_edit = QLineEdit()
        self.zip_edit.setPlaceholderText("5-digit ZIP (optional)")
        self.zip_edit.textChanged.connect(self._on_pricing_inputs_changed)
        controls.addWidget(self.zip_edit, 0, 5)
        self.rural_label = QLabel("Non-Rural (NR)")
        controls.addWidget(self.rural_label, 0, 6)
        controls.setColumnStretch(7, 1)
        root.addLayout(controls)

        search_row = QHBoxLayout()
        search_row.addWidget(QLabel("HCPCS Search:"))
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Enter HCPCS code or keyword")
        self.search_edit.returnPressed.connect(self._search_codes)
        search_row.addWidget(self.search_edit, 1)
        search_btn = QPushButton("Search")
        search_btn.clicked.connect(self._search_codes)
        search_row.addWidget(search_btn)
        self.search_results = QComboBox()
        self.search_results.setMinimumWidth(360)
        search_row.addWidget(self.search_results, 1)
        add_btn = QPushButton("Add")
        add_btn.clicked.connect(self._add_selected_search_result)
        search_row.addWidget(add_btn)
        root.addLayout(search_row)

        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels(
            ["HCPCS Code", "Description", "Quantity", "Unit Price ($)", "Line Total ($)", "Remove"]
        )
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setAlternatingRowColors(True)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        root.addWidget(self.table, 1)

        totals = QHBoxLayout()
        self.bundle_label = QLabel("Bundle: —")
        totals.addWidget(self.bundle_label)
        totals.addStretch()
        totals.addWidget(QLabel("Grand Total:"))
        self.grand_total_label = QLabel("$0.00")
        self.grand_total_label.setStyleSheet("font-weight: bold; color: #003366;")
        totals.addWidget(self.grand_total_label)
        root.addLayout(totals)

        btns = QHBoxLayout()
        save_bundle_btn = QPushButton("Save as Bundle…")
        load_bundle_btn = QPushButton("Load Bundle…")
        export_btn = QPushButton("Export…")
        clear_btn = QPushButton("Clear List")
        close_btn = QPushButton("Close")
        save_bundle_btn.clicked.connect(self._save_bundle)
        load_bundle_btn.clicked.connect(self._load_bundle)
        export_btn.clicked.connect(self._export)
        clear_btn.clicked.connect(self._clear_list)
        close_btn.clicked.connect(self.close)
        btns.addWidget(save_bundle_btn)
        btns.addWidget(load_bundle_btn)
        btns.addWidget(export_btn)
        btns.addWidget(clear_btn)
        btns.addStretch()
        btns.addWidget(close_btn)
        root.addLayout(btns)

    def _load_filters(self, default_year=None, default_state=None, default_zip=""):
        self.year_combo.blockSignals(True)
        self.state_combo.blockSignals(True)
        self.year_combo.clear()
        years = get_available_years()
        for y in years:
            self.year_combo.addItem(str(y), y)
        target_year = default_year if default_year is not None else get_current_year_or_fallback()
        if target_year is not None:
            idx = self.year_combo.findData(target_year)
            if idx >= 0:
                self.year_combo.setCurrentIndex(idx)
        self.state_combo.clear()
        states = get_selected_states()
        for abbr, name in states:
            self.state_combo.addItem(f"{name} ({abbr})", abbr)
        if default_state:
            idx = self.state_combo.findData(default_state)
            if idx >= 0:
                self.state_combo.setCurrentIndex(idx)
        elif states:
            self.state_combo.setCurrentIndex(0)
        self.zip_edit.setText(default_zip or "")
        self.year_combo.blockSignals(False)
        self.state_combo.blockSignals(False)
        self._on_pricing_inputs_changed()

    def _on_pricing_inputs_changed(self):
        self._update_rural_label()
        self._refresh_all_prices()

    def _effective_year(self):
        return self.year_combo.currentData()

    def _is_rural(self):
        zip5 = self.zip_edit.text().strip()
        year = self._effective_year()
        if not year or len(zip5) != 5 or not zip5.isdigit():
            return False
        return is_rural_zip(year, zip5)

    def _update_rural_label(self):
        zip5 = self.zip_edit.text().strip()
        if len(zip5) == 5 and zip5.isdigit():
            self.rural_label.setText("Rural (R)" if self._is_rural() else "Non-Rural (NR)")
        else:
            self.rural_label.setText("Non-Rural (NR)")

    def _search_codes(self):
        self.search_results.clear()
        term = self.search_edit.text().strip()
        if not term:
            return
        records = get_fees(
            state_abbr=self.state_combo.currentData(),
            year=self._effective_year(),
            hcpcs_code=term,
            keyword=term,
        )
        seen = set()
        for rec in records:
            code = (rec.get("hcpcs_code") or "").upper()
            if not code or code in seen:
                continue
            seen.add(code)
            desc = rec.get("description", "") or ""
            self.search_results.addItem(f"{code} — {desc[:90]}", {"code": code, "description": desc})

    def _add_selected_search_result(self):
        payload = self.search_results.currentData()
        if not payload:
            return
        self._add_code(payload["code"], payload.get("description", ""))

    def _add_code(self, hcpcs_code, description_hint=""):
        code = (hcpcs_code or "").strip().upper()
        if not code:
            return
        for row in range(self.table.rowCount()):
            existing = self.table.item(row, 0)
            if existing and existing.text().upper() == code:
                spin = self.table.cellWidget(row, 2)
                if isinstance(spin, QSpinBox):
                    spin.setValue(spin.value() + 1)
                return
        row = self.table.rowCount()
        self.table.insertRow(row)
        self.table.setItem(row, 0, QTableWidgetItem(code))
        self.table.setItem(row, 1, QTableWidgetItem(description_hint or self._lookup_description(code)))
        qty_spin = QSpinBox()
        qty_spin.setMinimum(1)
        qty_spin.setMaximum(9999)
        qty_spin.setValue(1)
        qty_spin.valueChanged.connect(self._refresh_all_prices)
        self.table.setCellWidget(row, 2, qty_spin)
        self.table.setItem(row, 3, QTableWidgetItem("—"))
        self.table.setItem(row, 4, QTableWidgetItem("—"))
        remove_btn = QPushButton("Remove")
        remove_btn.clicked.connect(lambda _=False, r=row: self._remove_row(r))
        self.table.setCellWidget(row, 5, remove_btn)
        self._refresh_all_prices()

    def _lookup_description(self, code):
        records = get_fees(
            state_abbr=self.state_combo.currentData(),
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
            state_abbr=self.state_combo.currentData(),
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

    def _remove_row(self, row):
        if row < 0 or row >= self.table.rowCount():
            return
        self.table.removeRow(row)
        self._rebind_remove_buttons()
        self._refresh_all_prices()

    def _rebind_remove_buttons(self):
        for row in range(self.table.rowCount()):
            btn = self.table.cellWidget(row, 5)
            if not isinstance(btn, QPushButton):
                continue
            try:
                btn.clicked.disconnect()
            except Exception:
                pass
            btn.clicked.connect(lambda _=False, r=row: self._remove_row(r))

    def _refresh_all_prices(self):
        total = 0.0
        for row in range(self.table.rowCount()):
            code_item = self.table.item(row, 0)
            if not code_item:
                continue
            code = code_item.text().strip().upper()
            price = self._lookup_price(code)
            qty_widget = self.table.cellWidget(row, 2)
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
            self.table.setItem(row, 3, unit_item)
            self.table.setItem(row, 4, line_item)
        self.grand_total_label.setText(f"${total:,.2f}")

    def _collect_items(self):
        items = []
        for row in range(self.table.rowCount()):
            code_item = self.table.item(row, 0)
            desc_item = self.table.item(row, 1)
            qty_widget = self.table.cellWidget(row, 2)
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
        for item in payload.get("items", []):
            code = (item.get("hcpcs_code") or "").upper()
            if not code:
                continue
            row = self.table.rowCount()
            self.table.insertRow(row)
            self.table.setItem(row, 0, QTableWidgetItem(code))
            self.table.setItem(row, 1, QTableWidgetItem(self._lookup_description(code)))
            qty_spin = QSpinBox()
            qty_spin.setMinimum(1)
            qty_spin.setMaximum(9999)
            qty_spin.setValue(max(1, int(item.get("quantity", 1) or 1)))
            qty_spin.valueChanged.connect(self._refresh_all_prices)
            self.table.setCellWidget(row, 2, qty_spin)
            self.table.setItem(row, 3, QTableWidgetItem("—"))
            self.table.setItem(row, 4, QTableWidgetItem("—"))
            remove_btn = QPushButton("Remove")
            remove_btn.clicked.connect(lambda _=False, r=row: self._remove_row(r))
            self.table.setCellWidget(row, 5, remove_btn)
        self._rebind_remove_buttons()
        self._refresh_all_prices()

    def _export(self):
        items = self._collect_items()
        if not items:
            QMessageBox.information(self, "No Items", "No purchase list items to export.")
            return
        default_name = "purchase_list"
        filters = "Excel Files (*.xlsx);;CSV Files (*.csv);;PDF Files (*.pdf)"
        path, _ = QFileDialog.getSaveFileName(self, "Export Purchase List", default_name, filters)
        if not path:
            return
        suffix = path.lower().split(".")[-1] if "." in path else ""
        meta = {
            "bundle_name": self._bundle_name or "",
            "year": self._effective_year(),
            "state": self.state_combo.currentData() or "",
            "zip_code": self.zip_edit.text().strip(),
            "rural_status": "Rural (R)" if self._is_rural() else "Non-Rural (NR)",
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
        try:
            if suffix == "xlsx":
                export_purchase_list_to_excel(items, path, meta=meta)
            elif suffix == "pdf":
                export_purchase_list_to_pdf(items, path, meta=meta)
            else:
                if suffix != "csv":
                    path = f"{path}.csv"
                export_purchase_list_to_csv(items, path, meta=meta)
            QMessageBox.information(self, "Export Complete", f"Purchase list exported to:\n{path}")
        except Exception as exc:
            QMessageBox.critical(self, "Export Error", f"Export failed:\n{exc}")

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
        self._refresh_all_prices()
