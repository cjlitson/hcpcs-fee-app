from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QScrollArea, QWidget, QCheckBox, QLineEdit, QMessageBox, QFrame
)
from PyQt6.QtCore import Qt
from core.database import get_selected_states, save_selected_states

ALL_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "DC": "District of Columbia", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
    "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine",
    "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska",
    "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico",
    "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas",
    "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
}


class StateSelectorDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Manage States")
        self.setMinimumSize(440, 560)
        self._checkboxes = {}
        self._init_ui()
        self._load_current()

    def _init_ui(self):
        layout = QVBoxLayout(self)

        layout.addWidget(QLabel(
            "Select the states you want to track and sync data for:"
        ))

        # Search bar
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Filter states…")
        self.search_edit.textChanged.connect(self._filter)
        layout.addWidget(self.search_edit)

        # Select all / none
        sel_row = QHBoxLayout()
        all_btn = QPushButton("Select All")
        none_btn = QPushButton("Select None")
        all_btn.clicked.connect(self._select_all)
        none_btn.clicked.connect(self._select_none)
        sel_row.addWidget(all_btn)
        sel_row.addWidget(none_btn)
        sel_row.addStretch()
        layout.addLayout(sel_row)

        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        layout.addWidget(sep)

        # Scrollable checkbox list
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        self._list_widget = QWidget()
        self._list_layout = QVBoxLayout(self._list_widget)
        self._list_layout.setSpacing(2)

        for abbr, name in sorted(ALL_STATES.items(), key=lambda x: x[1]):
            cb = QCheckBox(f"{name} ({abbr})")
            cb.setProperty("abbr", abbr)
            self._checkboxes[abbr] = cb
            self._list_layout.addWidget(cb)

        self._list_layout.addStretch()
        scroll.setWidget(self._list_widget)
        layout.addWidget(scroll, 1)

        sep2 = QFrame()
        sep2.setFrameShape(QFrame.Shape.HLine)
        layout.addWidget(sep2)

        # Buttons
        btn_row = QHBoxLayout()
        cancel_btn = QPushButton("Cancel")
        save_btn = QPushButton("Save")
        save_btn.setStyleSheet(
            "background-color: #003366; color: white; padding: 6px 16px; font-weight: bold;"
        )
        cancel_btn.clicked.connect(self.reject)
        save_btn.clicked.connect(self._save)
        btn_row.addStretch()
        btn_row.addWidget(cancel_btn)
        btn_row.addWidget(save_btn)
        layout.addLayout(btn_row)

    def _load_current(self):
        selected = {abbr for abbr, _ in get_selected_states()}
        for abbr, cb in self._checkboxes.items():
            cb.setChecked(abbr in selected)

    def _filter(self, text):
        text = text.lower()
        for abbr, cb in self._checkboxes.items():
            name = ALL_STATES[abbr].lower()
            cb.setVisible(not text or text in name or text in abbr.lower())

    def _select_all(self):
        for cb in self._checkboxes.values():
            if cb.isVisible():
                cb.setChecked(True)

    def _select_none(self):
        for cb in self._checkboxes.values():
            if cb.isVisible():
                cb.setChecked(False)

    def _save(self):
        selected = [
            (abbr, ALL_STATES[abbr])
            for abbr, cb in self._checkboxes.items()
            if cb.isChecked()
        ]
        if not selected:
            QMessageBox.warning(
                self, "No States Selected",
                "Please select at least one state to track.",
            )
            return
        save_selected_states(selected)
        self.accept()

    def get_selected(self):
        """Return list of (abbr, name) for checked states."""
        return [
            (abbr, ALL_STATES[abbr])
            for abbr, cb in self._checkboxes.items()
            if cb.isChecked()
        ]
