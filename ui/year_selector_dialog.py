from datetime import date

from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QCheckBox, QMessageBox, QFrame
)
from core.cms_downloader import SUPPORTED_YEARS, discover_available_cms_years
from core.database import get_selected_years, save_selected_years, get_default_selected_years


def _default_selected_years():
    """Return the default set of years to select on first run (current + last 3)."""
    return get_default_selected_years()


class YearSelectorDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Manage Years")
        self.setMinimumWidth(360)
        self._checkboxes = {}
        self._init_ui()
        self._load_current()

    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel(
            "Select the years you want to track and sync data for.\n"
            "Default shows current year + last 3; add more as needed."
        ))

        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        layout.addWidget(sep)

        # Best-effort: discover which years CMS currently publishes.
        # If scraping fails, available_years will be empty and all years stay enabled.
        try:
            available_years = discover_available_cms_years()
        except Exception:
            available_years = set()

        for year in sorted(SUPPORTED_YEARS):
            if available_years and year not in available_years:
                cb = QCheckBox(f"{year}  (not currently available on CMS)")
                cb.setEnabled(False)
            else:
                cb = QCheckBox(str(year))
            self._checkboxes[year] = cb
            layout.addWidget(cb)

        sep2 = QFrame()
        sep2.setFrameShape(QFrame.Shape.HLine)
        layout.addWidget(sep2)

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
        saved = get_selected_years()
        # If nothing saved yet, default to current year + last 3
        defaults = saved if saved else _default_selected_years()
        for year, cb in self._checkboxes.items():
            if cb.isEnabled():
                cb.setChecked(year in defaults)

    def _save(self):
        selected = [year for year, cb in self._checkboxes.items() if cb.isChecked()]
        if not selected:
            QMessageBox.warning(
                self, "No Years Selected",
                "Please select at least one year to track.",
            )
            return
        save_selected_years(selected)
        self.accept()

    def get_selected(self):
        """Return list of checked years."""
        return [year for year, cb in self._checkboxes.items() if cb.isChecked()]
