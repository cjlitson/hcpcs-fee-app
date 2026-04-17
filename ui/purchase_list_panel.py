from PyQt6.QtCore import Qt, QSize, pyqtSignal, QStringListModel, QTimer
from PyQt6.QtGui import QFont, QKeySequence, QShortcut
from PyQt6.QtWidgets import (
    QCompleter,
    QDialog,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMenu,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QToolButton,
    QVBoxLayout,
    QWidget,
    QInputDialog,
)

from core.config import get_config_value, set_config_value
from core.database import (
    delete_bundle,
    get_fees,
    is_rural_zip,
    list_bundles,
    load_bundle,
    save_bundle,
)
from ui.purchase_list_dialog import BundlePickerDialog

PURCHASE_COL_HCPCS = 0
PURCHASE_COL_DESCRIPTION = 1
PURCHASE_COL_QTY = 2
PURCHASE_COL_UNIT_PRICE = 3
PURCHASE_COL_LINE_TOTAL = 4
PURCHASE_COL_DELETE = 5
PURCHASE_TABLE_COLUMN_COUNT = 6
QUICK_ADD_SUGGEST_DEBOUNCE_MS = 140
SCORE_PREFIX_MATCH = 0
SCORE_PREFIX_MISS_PENALTY = 10
SCORE_CONTAINS_MISS_PENALTY = 5
ROW_DELETE_BUTTON_SIZE = QSize(30, 30)
PURCHASE_ROW_HEIGHT = 52

_QUICK_ADD_TOOLTIP = (
    "Quick Add HCPCS\n"
    "\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n"
    "\u2022 Type a code and press Enter to add it to the list\n"
    "\u2022 Autocomplete suggestions appear as you type\n"
    "\u2022 Arrow keys navigate suggestions; Enter confirms\n"
    "\u2022 Use the \u2715 button on each row to remove it\n"
    "\u2022 Select rows in results, then use Add Selected (shown near Purchase List in the top toolbar)"
)


class QtyStepWidget(QWidget):
    """Compact − N + stepper used in the purchase table Qty column."""

    valueChanged = pyqtSignal(int)
    _MIN = 1
    _MAX = 9999

    def __init__(self, value=1, parent=None):
        super().__init__(parent)
        self._value = max(self._MIN, min(self._MAX, int(value)))

        lay = QHBoxLayout(self)
        lay.setContentsMargins(4, 0, 4, 0)
        lay.setSpacing(3)

        self._minus = QPushButton("\u2212")   # −
        self._val_label = QLabel(str(self._value))
        self._val_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._val_label.setMinimumWidth(26)
        f = self._val_label.font()
        f.setBold(True)
        self._val_label.setFont(f)
        self._plus = QPushButton("\u002B")    # +

        for btn in (self._minus, self._plus):
            btn.setProperty("role", "stepper")
            btn.setFixedSize(QSize(26, 26))
            btn.setCursor(Qt.CursorShape.PointingHandCursor)

        lay.addStretch()
        lay.addWidget(self._minus)
        lay.addWidget(self._val_label)
        lay.addWidget(self._plus)
        lay.addStretch()

        self._minus.clicked.connect(self._decrement)
        self._plus.clicked.connect(self._increment)

    # ------------------------------------------------------------------
    def _decrement(self):
        if self._value > self._MIN:
            self._value -= 1
            self._val_label.setText(str(self._value))
            self.valueChanged.emit(self._value)

    def _increment(self):
        if self._value < self._MAX:
            self._value += 1
            self._val_label.setText(str(self._value))
            self.valueChanged.emit(self._value)

    # Public API (matches QSpinBox subset used by this panel)
    def value(self) -> int:
        return self._value

    def setValue(self, v: int) -> None:
        self._value = max(self._MIN, min(self._MAX, int(v)))
        self._val_label.setText(str(self._value))


class PurchaseListPanel(QWidget):
    count_changed = pyqtSignal(int)

    def __init__(self, parent=None, year_combo=None, state_combo=None, zip_edit=None,
                 add_callback=None, remove_callback=None):
        super().__init__(parent)
        self._main_year_combo = year_combo  # Main window's year combo (for reference)
        self._state_combo = state_combo
        self._zip_edit = zip_edit
        self._add_callback = add_callback
        self._remove_callback = remove_callback
        self._bundle_name = None
        self._quick_add_btn = None
        self._bundles_btn = None
        self._generate_btn = None
        self._clear_btn = None
        self._add_transfer_btn = None
        self._remove_transfer_btn = None
        self._quick_add_suggestions = {}
        self._quick_add_model = QStringListModel(self)
        self._quick_add_completer = None
        self._quick_add_suggest_timer = QTimer(self)
        self._quick_add_suggest_timer.setSingleShot(True)
        self._quick_add_suggest_timer.setInterval(QUICK_ADD_SUGGEST_DEBOUNCE_MS)
        self._quick_add_suggest_timer.timeout.connect(self._update_quick_add_suggestions)
        self._pending_quick_add_text = ""
        self._init_ui()
        self.refresh_context_state()

    def _init_ui(self):
        self.setObjectName("purchaseListPanel")
        root = QVBoxLayout(self)
        root.setContentsMargins(10, 10, 10, 10)
        root.setSpacing(8)

        header_card = QFrame()
        header_card.setObjectName("purchaseHeaderCard")
        header_card_layout = QVBoxLayout(header_card)
        header_card_layout.setContentsMargins(10, 8, 10, 8)
        header_card_layout.setSpacing(4)

        header = QHBoxLayout()
        header.setSpacing(6)

        self.title_label = QLabel("Purchase List (0)")
        self.title_label.setStyleSheet("font-weight: bold; font-size: 14px;")
        header.addWidget(self.title_label)
        header.addStretch()

        self.bundle_label = QLabel("Bundle: \u2014")
        header.addWidget(self.bundle_label)
        header_card_layout.addLayout(header)

        context = QHBoxLayout()
        self.pricing_context_label = QLabel("")
        self.pricing_context_label.setStyleSheet("font-style: italic;")
        context.addWidget(self.pricing_context_label)
        context.addStretch()
        header_card_layout.addLayout(context)

        self.selection_requirement_label = QLabel("")
        self.selection_requirement_label.setWordWrap(True)
        self.selection_requirement_label.setStyleSheet("font-size: 11px; font-weight: 600; color: #cc0000;")
        header_card_layout.addWidget(self.selection_requirement_label)
        root.addWidget(header_card)

        controls = QHBoxLayout()
        controls.setSpacing(6)
        controls.addWidget(QLabel("Quick Add:"))
        # Info icon with instructional tooltip (replaces inline instructions block)
        info_lbl = QLabel("\u24d8")  # ⓘ
        info_lbl.setObjectName("quickAddInfoIcon")
        info_lbl.setToolTip(_QUICK_ADD_TOOLTIP)
        controls.addWidget(info_lbl)
        self.quick_add_edit = QLineEdit()
        self.quick_add_edit.setPlaceholderText("e.g. L5301")
        self.quick_add_edit.returnPressed.connect(self._quick_add_from_input)
        self.quick_add_edit.textChanged.connect(self._queue_quick_add_suggestions)
        self._quick_add_completer = QCompleter(self._quick_add_model, self.quick_add_edit)
        self._quick_add_completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self._quick_add_completer.setFilterMode(Qt.MatchFlag.MatchContains)
        self._quick_add_completer.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
        self.quick_add_edit.setCompleter(self._quick_add_completer)
        controls.addWidget(self.quick_add_edit, 1)
        self._quick_add_btn = QPushButton("Add")
        self._quick_add_btn.setProperty("role", "primary")
        self._quick_add_btn.setMinimumWidth(72)
        self._quick_add_btn.clicked.connect(self._quick_add_from_input)
        controls.addWidget(self._quick_add_btn)
        root.addLayout(controls)

        self.table = QTableWidget(0, PURCHASE_TABLE_COLUMN_COUNT)
        self.table.setHorizontalHeaderLabels(
            ["HCPCS Code", "Description", "Qty", "Unit Price", "Line Total", ""]
        )
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.table.horizontalHeader().setSectionResizeMode(PURCHASE_COL_DELETE, QHeaderView.ResizeMode.Fixed)
        self.table.horizontalHeader().setSectionResizeMode(PURCHASE_COL_DESCRIPTION, QHeaderView.ResizeMode.Stretch)
        self.table.horizontalHeader().setSectionsMovable(True)
        self.table.setHorizontalScrollMode(QTableWidget.ScrollMode.ScrollPerPixel)
        self.table.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QTableWidget.SelectionMode.ExtendedSelection)
        self.table.setAlternatingRowColors(True)
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table.horizontalHeader().sectionMoved.connect(self._save_table_layout_preferences)
        self.table.horizontalHeader().sectionResized.connect(self._save_table_layout_preferences)
        self.table.setWordWrap(True)
        self.table.verticalHeader().setDefaultSectionSize(PURCHASE_ROW_HEIGHT)
        self.table.verticalHeader().hide()
        root.addWidget(self.table, 1)

        totals_card = QFrame()
        totals_card.setObjectName("purchaseSummaryCard")
        totals = QHBoxLayout(totals_card)
        totals.setContentsMargins(10, 6, 10, 6)
        totals.addWidget(QLabel("Summary"))
        totals.addStretch()
        totals.addWidget(QLabel("Total Items:"))
        self.total_items_label = QLabel("0")
        self.total_items_label.setStyleSheet("font-weight: 600;")
        totals.addWidget(self.total_items_label)
        totals.addSpacing(12)
        totals.addWidget(QLabel("Estimated Total:"))
        self.grand_total_label = QLabel("$0.00")
        self.grand_total_label.setStyleSheet("font-weight: bold;")
        totals.addWidget(self.grand_total_label)
        root.addWidget(totals_card)

        btns = QHBoxLayout()
        self._bundles_btn = QToolButton()
        self._bundles_btn.setText("Bundles")
        self._bundles_btn.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        bundles_menu = QMenu(self._bundles_btn)
        bundles_menu.addAction("Save Current Bundle", self._save_bundle)
        bundles_menu.addAction("Load / Manage Bundles…", self._load_bundle)
        self._bundles_btn.setMenu(bundles_menu)
        self._generate_btn = QPushButton("Generate Document")
        self._clear_btn = QPushButton("Clear")
        self._bundles_btn.setProperty("role", "ghost")
        self._generate_btn.setProperty("role", "primary")
        self._clear_btn.setProperty("role", "ghost")
        self._generate_btn.clicked.connect(self._generate_document)
        self._clear_btn.clicked.connect(self._clear_list)
        btns.addWidget(self._bundles_btn)
        btns.addStretch()
        btns.addWidget(self._generate_btn)
        btns.addWidget(self._clear_btn)
        root.addLayout(btns)

        QShortcut(QKeySequence("Ctrl+Shift+C"), self, activated=self._copy_to_clipboard)
        if self._main_year_combo:
            self._main_year_combo.currentIndexChanged.connect(self.refresh_context_state)
        if self._state_combo:
            self._state_combo.currentIndexChanged.connect(self.refresh_context_state)
        if self._zip_edit:
            self._zip_edit.textChanged.connect(self.refresh_prices)
        self._restore_table_layout_preferences()

    def _effective_year(self):
        """Return the main toolbar's explicitly selected year for purchase-list pricing."""
        return self._main_year_combo.currentData() if self._main_year_combo else None

    def _state_abbr(self):
        return self._state_combo.currentData() if self._state_combo else None

    def _is_ready_for_pricing(self):
        return self._effective_year() is not None and self._state_abbr() is not None

    def context_requirement_message(self):
        return "Select one specific Year and one specific State to use Purchase List pricing."

    def is_context_ready(self):
        return self._is_ready_for_pricing()

    def refresh_context_state(self, *_args):
        year = self._effective_year()
        if year is None:
            self.pricing_context_label.setText("Pricing requires a specific CMS year selection.")
        else:
            self.pricing_context_label.setText(f"Pricing is based on CMS {year} Fee Schedule")

        ready = self._is_ready_for_pricing()
        message = "" if ready else self.context_requirement_message()
        self.selection_requirement_label.setText(message)
        self.selection_requirement_label.setVisible(bool(message))

        for widget in (
            self.quick_add_edit,
            self._quick_add_btn,
            self._bundles_btn,
            self._generate_btn,
        ):
            if widget is not None:
                widget.setEnabled(ready)
        self.refresh_prices()

    def _zip_code(self):
        return self._zip_edit.text().strip() if self._zip_edit else ""

    def _is_rural(self):
        zip5 = self._zip_code()
        year = self._effective_year()
        if not year or len(zip5) != 5 or not zip5.isdigit():
            return False
        return is_rural_zip(year, zip5)

    def add_code(self, hcpcs_code, description_hint=""):
        if not self._is_ready_for_pricing():
            QMessageBox.information(self, "Purchase List Filter Required", self.context_requirement_message())
            return
        code = (hcpcs_code or "").strip().upper()
        if not code:
            return
        for row in range(self.table.rowCount()):
            existing = self.table.item(row, PURCHASE_COL_HCPCS)
            if existing and existing.text().upper() == code:
                spin = self.table.cellWidget(row, PURCHASE_COL_QTY)
                if isinstance(spin, QtyStepWidget):
                    spin.setValue(spin.value() + 1)
                return
        row = self.table.rowCount()
        self.table.insertRow(row)
        self.table.setItem(row, PURCHASE_COL_HCPCS, QTableWidgetItem(code))
        description_text = (description_hint or "").strip()
        if not description_text:
            description_text = self._lookup_description(code)
        self.table.setItem(row, PURCHASE_COL_DESCRIPTION, QTableWidgetItem(description_text))
        qty_spin = QtyStepWidget()
        qty_spin.valueChanged.connect(self.refresh_prices)
        self.table.setCellWidget(row, PURCHASE_COL_QTY, qty_spin)
        self.table.setItem(row, PURCHASE_COL_UNIT_PRICE, QTableWidgetItem("\u2014"))
        self.table.setItem(row, PURCHASE_COL_LINE_TOTAL, QTableWidgetItem("\u2014"))
        self.table.setCellWidget(row, PURCHASE_COL_DELETE, self._make_row_delete_button())
        self.refresh_prices()

    def _quick_add_from_input(self):
        if not self._is_ready_for_pricing():
            QMessageBox.information(self, "Purchase List Filter Required", self.context_requirement_message())
            return
        code = self._resolve_quick_add_code()
        if not code:
            return
        recs = get_fees(hcpcs_code=code)
        exact = [r for r in recs if (r.get("hcpcs_code") or "").upper() == code]
        if not exact:
            QMessageBox.warning(self, "Code Not Found", f"HCPCS code '{code}' was not found.")
            return
        desc = exact[0].get("description") or ""
        self.add_code(code, desc)
        self.quick_add_edit.clear()

    def _resolve_quick_add_code(self):
        typed = (self.quick_add_edit.text() or "").strip()
        typed_upper = typed.upper()
        popup = self._quick_add_completer.popup() if self._quick_add_completer else None
        if popup is not None and popup.isVisible():
            idx = popup.currentIndex()
            if idx.isValid():
                label = str(idx.data() or "")
                return self._quick_add_suggestions.get(label, label).upper()
        if typed in self._quick_add_suggestions:
            return self._quick_add_suggestions[typed].upper()
        first_suggestion = None
        first_prefix_match = None
        for _label, code in self._quick_add_suggestions.items():
            normalized_code = (code or "").upper()
            if not normalized_code:
                continue
            if first_suggestion is None:
                first_suggestion = normalized_code
            if normalized_code == typed_upper:
                return normalized_code
            if typed_upper and first_prefix_match is None and normalized_code.startswith(typed_upper):
                first_prefix_match = normalized_code
        if first_prefix_match:
            return first_prefix_match
        if first_suggestion:
            return first_suggestion
        return self._quick_add_suggestions.get(typed, typed).upper()

    def _queue_quick_add_suggestions(self, text):
        self._pending_quick_add_text = text or ""
        self._quick_add_suggest_timer.start()

    def _update_quick_add_suggestions(self, text=None):
        if text is None:
            text = self._pending_quick_add_text
        term = (text or "").strip()
        if not term:
            self._quick_add_suggestions = {}
            self._quick_add_model.setStringList([])
            return
        records = get_fees(
            state_abbr=self._state_abbr(),
            year=self._effective_year(),
            hcpcs_code=term,
            keyword=term,
        )
        if not records:
            records = get_fees(hcpcs_code=term, keyword=term)
        normalized = term.upper()
        scored = []
        seen = set()
        for rec in records:
            code = (rec.get("hcpcs_code") or "").upper()
            if not code or code in seen:
                continue
            seen.add(code)
            desc = (rec.get("description") or "").strip()
            starts = SCORE_PREFIX_MATCH if code.startswith(normalized) else SCORE_PREFIX_MISS_PENALTY
            contains = SCORE_PREFIX_MATCH if normalized in code else SCORE_CONTAINS_MISS_PENALTY
            scored.append(((starts, contains, code), code, desc))
        scored.sort(key=lambda entry: entry[0])
        suggestions = []
        mapping = {}
        for _, code, desc in scored[:20]:
            label = f"{code} — {desc}" if desc else code
            suggestions.append(label)
            mapping[label] = code
        self._quick_add_suggestions = mapping
        self._quick_add_model.setStringList(suggestions)

    def _make_row_delete_button(self):
        holder = QWidget()
        holder_layout = QHBoxLayout(holder)
        holder_layout.setContentsMargins(0, 0, 0, 0)
        holder_layout.setSpacing(0)
        holder_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)

        btn = QPushButton("\u2715")  # ✕  MULTIPLICATION X — crisp on all platforms
        btn.setToolTip("Remove this line item")
        btn.setAccessibleName("Delete purchase list row")
        btn.setProperty("deleteAction", "true")
        f = QFont()
        f.setPointSize(11)
        f.setBold(True)
        btn.setFont(f)
        btn.setFixedSize(ROW_DELETE_BUTTON_SIZE)
        btn.clicked.connect(self._remove_row_for_sender)
        holder_layout.addWidget(btn)
        return holder

    def _remove_row_for_sender(self):
        btn = self.sender()
        if not isinstance(btn, QPushButton):
            return
        for row in range(self.table.rowCount()):
            delete_cell = self.table.cellWidget(row, PURCHASE_COL_DELETE)
            if delete_cell is btn:
                self._remove_row(row)
                return
            if isinstance(delete_cell, QWidget) and delete_cell.findChild(QPushButton) is btn:
                self._remove_row(row)
                return

    def _remove_row(self, row):
        if row < 0 or row >= self.table.rowCount():
            return
        self.table.removeRow(row)
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
        if not hasattr(self, "table") or not hasattr(self, "grand_total_label"):
            return
        if not self._is_ready_for_pricing():
            for row in range(self.table.rowCount()):
                unit_item = QTableWidgetItem("—")
                line_item = QTableWidgetItem("—")
                unit_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                line_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                self.table.setItem(row, PURCHASE_COL_UNIT_PRICE, unit_item)
                self.table.setItem(row, PURCHASE_COL_LINE_TOTAL, line_item)
            self.grand_total_label.setText("$0.00")
            self._update_header()
            return
        total = 0.0
        for row in range(self.table.rowCount()):
            code_item = self.table.item(row, PURCHASE_COL_HCPCS)
            if not code_item:
                continue
            code = code_item.text().strip().upper()
            price = self._lookup_price(code)
            qty_widget = self.table.cellWidget(row, PURCHASE_COL_QTY)
            qty = qty_widget.value() if isinstance(qty_widget, QtyStepWidget) else 1
            line_total = None if price is None else float(price) * qty
            unit_txt = "—" if price is None else f"${float(price):,.2f}"
            line_txt = "—" if line_total is None else f"${line_total:,.2f}"
            if line_total is not None:
                total += line_total
            unit_item = QTableWidgetItem(unit_txt)
            line_item = QTableWidgetItem(line_txt)
            unit_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            line_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            self.table.setItem(row, PURCHASE_COL_UNIT_PRICE, unit_item)
            self.table.setItem(row, PURCHASE_COL_LINE_TOTAL, line_item)
        self.grand_total_label.setText(f"${total:,.2f}")
        self._update_header()

    def _update_header(self):
        count = self.table.rowCount()
        self.title_label.setText(f"Purchase List ({count})")
        if hasattr(self, "total_items_label"):
            self.total_items_label.setText(str(count))
        self.count_changed.emit(count)

    def _collect_items(self):
        items = []
        for row in range(self.table.rowCount()):
            code_item = self.table.item(row, PURCHASE_COL_HCPCS)
            desc_item = self.table.item(row, PURCHASE_COL_DESCRIPTION)
            qty_widget = self.table.cellWidget(row, PURCHASE_COL_QTY)
            if not code_item:
                continue
            code = code_item.text().strip().upper()
            qty = qty_widget.value() if isinstance(qty_widget, QtyStepWidget) else 1
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
            self.table.setItem(row, PURCHASE_COL_HCPCS, QTableWidgetItem(code))
            self.table.setItem(row, PURCHASE_COL_DESCRIPTION, QTableWidgetItem(description_text))
            qty_spin = QtyStepWidget(value=max(1, int(item.get("quantity", 1) or 1)))
            qty_spin.valueChanged.connect(self.refresh_prices)
            self.table.setCellWidget(row, PURCHASE_COL_QTY, qty_spin)
            self.table.setItem(row, PURCHASE_COL_UNIT_PRICE, QTableWidgetItem("\u2014"))
            self.table.setItem(row, PURCHASE_COL_LINE_TOTAL, QTableWidgetItem("\u2014"))
            self.table.setCellWidget(row, PURCHASE_COL_DELETE, self._make_row_delete_button())
        self.refresh_prices()

    def _generate_document(self):
        if not self._is_ready_for_pricing():
            QMessageBox.information(self, "Purchase List Filter Required", self.context_requirement_message())
            return
        items = self._collect_items()
        if not items:
            QMessageBox.information(self, "No Items", "No purchase list items selected.")
            return
        from ui.generate_document_dialog import GenerateDocumentDialog
        dlg = GenerateDocumentDialog(purchase_items=items, parent=self)
        dlg.exec()

    def _copy_to_clipboard(self):
        items = self._collect_items()
        if not items:
            return
        headers = ["HCPCS Code", "Description", "Quantity", "Unit Price", "Line Total"]
        lines = ["\t".join(headers)]
        for item in items:
            unit = item.get("unit_price")
            line = item.get("line_total")
            lines.append("\t".join([
                str(item.get("hcpcs_code", "")),
                str(item.get("description", "")),
                str(item.get("quantity", 1)),
                "" if unit is None else f"{unit:.2f}",
                "" if line is None else f"{line:.2f}",
            ]))
        from PyQt6.QtWidgets import QApplication
        QApplication.clipboard().setText("\n".join(lines))
        QMessageBox.information(self, "Copied", "Purchase list copied to clipboard.")

    def remove_selected_items(self):
        """Remove all currently selected rows from the purchase list."""
        selected_rows = sorted(
            {idx.row() for idx in self.table.selectionModel().selectedRows()},
            reverse=True,
        )
        for row in selected_rows:
            self.table.removeRow(row)
        if selected_rows:
            self.refresh_prices()
        return len(selected_rows)

    # Keep for backward compatibility with any external callers
    def remove_checked_items(self):
        return self.remove_selected_items()

    def select_all_items(self):
        self.table.selectAll()

    def deselect_all_items(self):
        self.table.clearSelection()

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
        self.bundle_label.setText("Bundle: \u2014")
        self.refresh_prices()

    def _table_layout_key(self):
        return "purchase_list_table_layout_v5"

    def _legacy_table_layout_key(self):
        return "purchase_list_table_layout_v4"

    def _restore_table_layout_preferences(self):
        header = self.table.horizontalHeader()
        state = get_config_value(self._table_layout_key(), "")
        if not state:
            legacy_state = get_config_value(self._legacy_table_layout_key(), "")
            if legacy_state:
                state = legacy_state
                set_config_value(self._table_layout_key(), legacy_state)
        if not state:
            self.table.setColumnWidth(PURCHASE_COL_HCPCS, 100)
            self.table.setColumnWidth(PURCHASE_COL_DESCRIPTION, 200)
            self.table.setColumnWidth(PURCHASE_COL_QTY, 90)
            self.table.setColumnWidth(PURCHASE_COL_UNIT_PRICE, 88)
            self.table.setColumnWidth(PURCHASE_COL_LINE_TOTAL, 88)
            self.table.setColumnWidth(PURCHASE_COL_DELETE, 38)
            return
        try:
            import base64
            from PyQt6.QtCore import QByteArray
            raw = base64.b64decode(state.encode("ascii"))
            header.restoreState(QByteArray(raw))
        except Exception:
            pass

    def _save_table_layout_preferences(self, *_args):
        try:
            import base64
            encoded = base64.b64encode(bytes(self.table.horizontalHeader().saveState())).decode("ascii")
            set_config_value(self._table_layout_key(), encoded)
        except Exception:
            pass
