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
    QToolButton,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
)

from core.database import (
    create_bundle_category,
    delete_bundle,
    delete_bundle_category,
    get_available_years,
    get_current_year_or_fallback,
    get_fees,
    get_selected_states,
    is_rural_zip,
    list_bundle_categories,
    list_bundles,
    load_bundle,
    rename_bundle_category,
    rename_bundle,
    set_bundle_category,
    save_bundle,
)
from core.exporter import (
    export_purchase_list_to_csv,
    export_purchase_list_to_docx,
    export_purchase_list_to_excel,
    export_purchase_list_to_pdf,
)


class BundlePickerDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Load Bundle")
        self.setMinimumSize(620, 360)
        self.selected_bundle_id = None
        self._bundle_payload_cache = {}
        self._description_cache = {}
        self._init_ui()
        self._refresh()

    def _init_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(10, 10, 10, 10)
        root.setSpacing(8)
        filter_row = QHBoxLayout()
        filter_row.addWidget(QLabel("Find Bundle:"))
        self.bundle_filter_edit = QLineEdit()
        self.bundle_filter_edit.setPlaceholderText("Search categories, bundle names, or preview descriptions…")
        self.bundle_filter_edit.textChanged.connect(self._apply_tree_filter)
        filter_row.addWidget(self.bundle_filter_edit, 1)
        root.addLayout(filter_row)

        self.bundle_tree = QTreeWidget()
        self.bundle_tree.setColumnCount(4)
        self.bundle_tree.setHeaderLabels(["Name", "# Items", "Created", "Last Updated"])
        self.bundle_tree.header().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.bundle_tree.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.bundle_tree.itemSelectionChanged.connect(self._preview_selected_bundle)
        self.bundle_tree.itemDoubleClicked.connect(lambda *_: self._load_selected())
        root.addWidget(self.bundle_tree, 1)

        self.preview_label = QLabel("Bundle Preview")
        self.preview_label.setStyleSheet("font-weight: 600;")
        root.addWidget(self.preview_label)
        self.preview_table = QTableWidget(0, 3)
        self.preview_table.setHorizontalHeaderLabels(["HCPCS Code", "Description", "Quantity"])
        self.preview_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.preview_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.preview_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.preview_table.setAlternatingRowColors(True)
        root.addWidget(self.preview_table, 1)

        btns = QHBoxLayout()
        manage_btn = QToolButton()
        manage_btn.setText("Manage")
        manage_btn.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        manage_btn.setProperty("role", "ghost")
        manage_menu = QMenu(manage_btn)
        manage_menu.addAction("Create Category…", self._create_category)
        manage_menu.addAction("Rename Category…", self._rename_category)
        manage_menu.addAction("Delete Category…", self._delete_category)
        manage_menu.addSeparator()
        manage_menu.addAction("Move Bundle…", self._move_bundle)
        manage_menu.addAction("Rename Bundle…", self._rename_selected)
        manage_menu.addAction("Delete Bundle…", self._delete_selected)
        manage_btn.setMenu(manage_menu)
        load_btn = QPushButton("Load Bundle")
        cancel_btn = QPushButton("Cancel")
        load_btn.setProperty("role", "primary")
        cancel_btn.setProperty("role", "ghost")
        load_btn.clicked.connect(self._load_selected)
        cancel_btn.clicked.connect(self.reject)
        btns.addWidget(manage_btn)
        btns.addStretch()
        btns.addWidget(cancel_btn)
        btns.addWidget(load_btn)
        root.addLayout(btns)

    def _refresh(self):
        current_bundle_id = self._current_bundle_id()
        self.bundle_tree.clear()
        self._bundle_payload_cache = {}
        categories = list_bundle_categories()
        bundles = list_bundles()
        cat_nodes = {}
        uncategorized = QTreeWidgetItem(["Uncategorized"])
        uncategorized.setData(0, Qt.ItemDataRole.UserRole, {"kind": "category", "id": None})
        self.bundle_tree.addTopLevelItem(uncategorized)
        cat_nodes[None] = uncategorized
        for category in categories:
            node = QTreeWidgetItem([category["name"]])
            node.setData(0, Qt.ItemDataRole.UserRole, {"kind": "category", "id": category["id"]})
            self.bundle_tree.addTopLevelItem(node)
            cat_nodes[category["id"]] = node
        for bundle in bundles:
            parent = cat_nodes.get(bundle.get("category_id"), uncategorized)
            node = QTreeWidgetItem([
                bundle["name"],
                str(bundle.get("item_count", 0)),
                bundle.get("created_at", "") or "",
                bundle.get("updated_at", "") or "",
            ])
            node.setData(0, Qt.ItemDataRole.UserRole, {"kind": "bundle", "id": bundle["id"]})
            parent.addChild(node)
        self.bundle_tree.expandAll()
        if current_bundle_id is not None:
            match = self._find_bundle_item(current_bundle_id)
            if match:
                self.bundle_tree.setCurrentItem(match)
        elif bundles:
            first = self._find_bundle_item(bundles[0]["id"])
            if first:
                self.bundle_tree.setCurrentItem(first)
        else:
            self.preview_table.setRowCount(0)
            self.preview_label.setText("Bundle Preview")
        self._apply_tree_filter()

    def _current_bundle_id(self):
        item = self.bundle_tree.currentItem()
        if item is None:
            return None
        payload = item.data(0, Qt.ItemDataRole.UserRole) or {}
        if payload.get("kind") != "bundle":
            return None
        return payload.get("id")

    def _current_category_id(self):
        item = self.bundle_tree.currentItem()
        if item is None:
            return None
        payload = item.data(0, Qt.ItemDataRole.UserRole) or {}
        if payload.get("kind") == "category":
            return payload.get("id")
        parent = item.parent()
        if parent:
            parent_data = parent.data(0, Qt.ItemDataRole.UserRole) or {}
            return parent_data.get("id")
        return None

    def _find_bundle_item(self, bundle_id):
        def walk(node):
            for i in range(node.childCount()):
                child = node.child(i)
                data = child.data(0, Qt.ItemDataRole.UserRole) or {}
                if data.get("kind") == "bundle" and data.get("id") == bundle_id:
                    return child
            return None

        for i in range(self.bundle_tree.topLevelItemCount()):
            node = self.bundle_tree.topLevelItem(i)
            found = walk(node)
            if found:
                return found
        return None

    def _rename_selected(self):
        bundle_id = self._current_bundle_id()
        if bundle_id is None:
            return
        current = self.bundle_tree.currentItem().text(0)
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
        name = self.bundle_tree.currentItem().text(0)
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

    def _preview_selected_bundle(self):
        bundle_id = self._current_bundle_id()
        if bundle_id is None:
            self.preview_table.setRowCount(0)
            self.preview_label.setText("Bundle Preview")
            return
        self._populate_preview(bundle_id)

    def _apply_tree_filter(self):
        term = (self.bundle_filter_edit.text() or "").strip().lower()

        def bundle_matches(item):
            data = item.data(0, Qt.ItemDataRole.UserRole) or {}
            if data.get("kind") != "bundle":
                return False
            if not term:
                return True
            bundle_id = data.get("id")
            if term in (item.text(0) or "").lower():
                return True
            if bundle_id not in self._bundle_payload_cache:
                self._bundle_payload_cache[bundle_id] = load_bundle(bundle_id) or {}
            payload = self._bundle_payload_cache[bundle_id]
            for entry in (payload or {}).get("items", []):
                code = (entry.get("hcpcs_code") or "").lower()
                desc = (entry.get("description") or "").lower()
                if term in code or term in desc:
                    return True
            return False

        for i in range(self.bundle_tree.topLevelItemCount()):
            category_item = self.bundle_tree.topLevelItem(i)
            category_name_match = term in (category_item.text(0) or "").lower() if term else True
            visible_children = 0
            for c in range(category_item.childCount()):
                bundle_item = category_item.child(c)
                visible = category_name_match or bundle_matches(bundle_item)
                bundle_item.setHidden(not visible)
                if visible:
                    visible_children += 1
            category_visible = category_name_match or visible_children > 0
            category_item.setHidden(not category_visible)

    def _create_category(self):
        name, ok = QInputDialog.getText(self, "New Category", "Category name:")
        if not ok:
            return
        name = name.strip()
        if not name:
            return
        try:
            create_bundle_category(name)
            self._refresh()
        except Exception as exc:
            QMessageBox.critical(self, "Category Error", str(exc))

    def _rename_category(self):
        current_id = self._current_category_id()
        if current_id is None:
            QMessageBox.information(self, "Select Category", "Select a category to rename.")
            return
        item = self.bundle_tree.currentItem()
        if item is not None and (item.data(0, Qt.ItemDataRole.UserRole) or {}).get("kind") == "bundle":
            item = item.parent()
        current_name = item.text(0) if item else ""
        new_name, ok = QInputDialog.getText(self, "Rename Category", "New category name:", text=current_name)
        if not ok:
            return
        new_name = new_name.strip()
        if not new_name:
            return
        try:
            rename_bundle_category(current_id, new_name)
            self._refresh()
        except Exception as exc:
            QMessageBox.critical(self, "Category Error", str(exc))

    def _delete_category(self):
        current_id = self._current_category_id()
        if current_id is None:
            QMessageBox.information(self, "Select Category", "Select a category to delete.")
            return
        ans = QMessageBox.question(
            self,
            "Delete Category",
            "Delete this category? Bundles will be moved to Uncategorized.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
            QMessageBox.StandardButton.Cancel,
        )
        if ans != QMessageBox.StandardButton.Yes:
            return
        delete_bundle_category(current_id)
        self._refresh()

    def _move_bundle(self):
        bundle_id = self._current_bundle_id()
        if bundle_id is None:
            QMessageBox.information(self, "Select Bundle", "Select a bundle to move.")
            return
        categories = list_bundle_categories()
        options = ["Uncategorized"] + [c["name"] for c in categories]
        chosen, ok = QInputDialog.getItem(self, "Move Bundle", "Move to category:", options, editable=False)
        if not ok:
            return
        category_id = None
        for category in categories:
            if category["name"] == chosen:
                category_id = category["id"]
                break
        set_bundle_category(bundle_id, category_id)
        self._refresh()

    def _populate_preview(self, bundle_id):
        if bundle_id not in self._bundle_payload_cache:
            self._bundle_payload_cache[bundle_id] = load_bundle(bundle_id) or {}
        payload = self._bundle_payload_cache[bundle_id]
        items = payload.get("items", []) if payload else []
        bundle_name = (payload or {}).get("name") or ""
        self.preview_label.setText(f"Bundle Preview — {bundle_name} ({len(items)} item{'s' if len(items) != 1 else ''})")
        self.preview_table.setRowCount(len(items))
        for row, entry in enumerate(items):
            code = (entry.get("hcpcs_code") or "").upper()
            description = (entry.get("description") or "").strip()
            if code and not description:
                description = self._lookup_description(code)
            qty = max(1, int(entry.get("quantity", 1) or 1))
            self.preview_table.setItem(row, 0, QTableWidgetItem(code))
            self.preview_table.setItem(row, 1, QTableWidgetItem(description))
            self.preview_table.setItem(row, 2, QTableWidgetItem(str(qty)))

    def _lookup_description(self, code):
        code = (code or "").strip().upper()
        if not code:
            return ""
        if code in self._description_cache:
            return self._description_cache[code]
        records = get_fees(hcpcs_code=code)
        description = ""
        for rec in records:
            if (rec.get("hcpcs_code") or "").upper() == code and rec.get("description"):
                description = rec["description"]
                break
        self._description_cache[code] = description
        return description


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
        save_bundle_btn = QPushButton("Save as Bundle")
        load_bundle_btn = QPushButton("Load Bundle")
        export_btn = QPushButton("Export")
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
        filters = "Word Documents (*.docx);;PDF Files (*.pdf);;Excel Files (*.xlsx);;CSV Files (*.csv)"
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
