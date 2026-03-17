"""Browse HCPCS Groups dialog.

Provides a two-panel dialog:
  Left  — list of all HCPCS Level II prefix groups
  Right — table of codes in the selected group, scoped to the parent
          window's current Year / State filters.
"""

from PyQt6.QtWidgets import (
    QDialog, QHBoxLayout, QVBoxLayout, QWidget,
    QLabel, QLineEdit, QListWidget, QListWidgetItem,
    QTableWidget, QTableWidgetItem, QHeaderView,
    QPushButton,
)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QColor, QFont


class GroupBrowserDialog(QDialog):
    """Browse HCPCS codes organised by CMS Level II prefix group."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Browse HCPCS Groups")
        self.setMinimumSize(1000, 600)

        # Pull current filters from parent MainWindow when available
        self._year = None
        self._state = None
        self._is_rural = False
        if hasattr(parent, "_query_year"):
            self._year = parent._query_year()
        if hasattr(parent, "state_combo"):
            self._state = parent.state_combo.currentData()
        if hasattr(parent, "_is_rural"):
            self._is_rural = parent._is_rural()

        self._init_ui()

    # ------------------------------------------------------------------ UI --

    def _init_ui(self):
        layout = QHBoxLayout(self)
        layout.setSpacing(8)

        # ---- Left panel: group list ------------------------------------
        left = QVBoxLayout()
        left.addWidget(QLabel("<b>HCPCS Groups</b>"))

        self.group_list = QListWidget()
        self.group_list.setMinimumWidth(240)

        from core.hcpcs_groups import HCPCS_GROUPS
        for prefix, (short, desc) in HCPCS_GROUPS.items():
            item = QListWidgetItem(f"{prefix} — {short}")
            item.setData(Qt.ItemDataRole.UserRole, prefix)
            item.setToolTip(desc)
            self.group_list.addItem(item)

        self.group_list.currentItemChanged.connect(self._on_group_selected)
        left.addWidget(self.group_list, 1)

        left_widget = QWidget()
        left_widget.setLayout(left)
        left_widget.setMaximumWidth(280)
        layout.addWidget(left_widget)

        # ---- Right panel: code table -----------------------------------
        right = QVBoxLayout()

        # Filter within group
        filter_row = QHBoxLayout()
        filter_row.addWidget(QLabel("Filter:"))
        self.filter_edit = QLineEdit()
        self.filter_edit.setPlaceholderText("Search within group…")
        self.filter_edit.textChanged.connect(self._on_filter_changed)
        filter_row.addWidget(self.filter_edit)
        right.addLayout(filter_row)

        # Results table — same columns as main window
        self.table = QTableWidget(0, 7)
        self.table.setHorizontalHeaderLabels([
            "HCPCS Code", "Description", "State", "Year",
            "Allowable ($)", "Modifier", "Source",
        ])
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setAlternatingRowColors(True)
        self.table.setSortingEnabled(True)
        self.table.setStyleSheet(
            "QTableWidget::item:selected { background-color: #003366; color: white; }"
            "QTableWidget::item:selected:!active { background-color: #4a7ab5; color: white; }"
            "QTableWidget::item:hover { background-color: #e0e8f0; }"
        )
        right.addWidget(self.table, 1)

        self.status_label = QLabel("Select a group to browse.")
        self.status_label.setStyleSheet("color: #555; font-style: italic;")
        right.addWidget(self.status_label)

        # Close button
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        right.addWidget(close_btn, alignment=Qt.AlignmentFlag.AlignRight)

        right_widget = QWidget()
        right_widget.setLayout(right)
        layout.addWidget(right_widget, 1)

    # --------------------------------------------------------------- Slots --

    def _on_group_selected(self, current, _previous):
        if not current:
            return
        prefix = current.data(Qt.ItemDataRole.UserRole)
        self._load_group(prefix)

    def _on_filter_changed(self):
        current = self.group_list.currentItem()
        if current:
            self._load_group(current.data(Qt.ItemDataRole.UserRole))

    # -------------------------------------------------------------- Helpers --

    def _load_group(self, prefix: str):
        from core.database import get_fees
        from core.hcpcs_groups import HCPCS_GROUPS

        filter_text = self.filter_edit.text().strip() or None

        records = get_fees(
            state_abbr=self._state,
            year=self._year,
            hcpcs_group=prefix,
            keyword=filter_text,
        )

        self.table.setSortingEnabled(False)
        self.table.setRowCount(len(records))

        link_color = QColor("#0066CC")
        link_font = QFont()
        link_font.setUnderline(True)

        for row_i, r in enumerate(records):
            if self._is_rural:
                chosen = r.get("allowable_r") or r.get("allowable_nr") or r.get("allowable")
            else:
                chosen = r.get("allowable_nr") or r.get("allowable")

            values = [
                r.get("hcpcs_code", ""),
                r.get("description", ""),
                r.get("state_abbr", ""),
                str(r.get("year", "")),
                "—" if chosen is None else f"{chosen:,.2f}",
                r.get("modifier", "") or "",
                r.get("data_source", "") or "",
            ]
            for col_i, v in enumerate(values):
                item = QTableWidgetItem(str(v))
                if col_i == 0:
                    item.setForeground(link_color)
                    item.setFont(link_font)
                self.table.setItem(row_i, col_i, item)

        self.table.setSortingEnabled(True)

        short = HCPCS_GROUPS.get(prefix, ("Unknown",))[0]
        self.status_label.setText(
            f"Showing {len(records):,} codes in group {prefix} — {short}"
        )
