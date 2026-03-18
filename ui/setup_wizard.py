from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QGridLayout,
    QPushButton, QLabel, QScrollArea, QWidget, QCheckBox,
    QMessageBox, QFrame, QStackedWidget,
)
from PyQt6.QtCore import Qt

from core.cms_downloader import ALL_STATES, SUPPORTED_YEARS, discover_available_cms_years
from core.database import (
    get_selected_states, save_selected_states,
    get_selected_years, save_selected_years,
    get_default_selected_years, set_preference,
)

_BTN_STYLE = (
    "background-color: #003366; color: white; "
    "padding: 6px 18px; font-weight: bold;"
)


class SetupWizard(QDialog):
    """Single multi-page first-run wizard (Welcome → States → Years)."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Setup — VA HCPCS Fee Schedule Manager")
        self.setMinimumSize(520, 480)
        if parent is not None:
            self._center_on_parent(parent)

        self._state_checkboxes: dict[str, QCheckBox] = {}
        self._year_checkboxes: dict[int, QCheckBox] = {}

        self._stack = QStackedWidget()
        self._stack.addWidget(self._build_welcome_page())
        self._stack.addWidget(self._build_states_page())
        self._stack.addWidget(self._build_years_page())

        main_layout = QVBoxLayout(self)
        main_layout.addWidget(self._stack, 1)

        self._nav_layout = QHBoxLayout()
        self._back_btn = QPushButton("← Back")
        self._next_btn = QPushButton("Next →")
        self._next_btn.setStyleSheet(_BTN_STYLE)
        self._back_btn.clicked.connect(self._go_back)
        self._next_btn.clicked.connect(self._go_next)
        self._nav_layout.addStretch()
        self._nav_layout.addWidget(self._back_btn)
        self._nav_layout.addWidget(self._next_btn)
        main_layout.addLayout(self._nav_layout)

        self._update_nav()

    # ---------------------------------------------------------------- pages --

    def _build_welcome_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(24, 24, 24, 16)
        layout.setSpacing(12)

        title = QLabel("Welcome to VA HCPCS Fee Schedule Manager")
        title.setStyleSheet("font-size: 16px; font-weight: bold; color: #003366;")
        title.setWordWrap(True)
        layout.addWidget(title)

        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        layout.addWidget(sep)

        body = QLabel(
            "To get started:\n\n"
            "  1. Select the states you want to track and sync data for.\n"
            "  2. Select the years you need.\n"
            "  3. Click \"Sync from CMS\" to download the latest fee schedules,\n"
            "     OR use File → Import CSV to load an existing file.\n\n"
            "Data is stored locally in a SQLite database — no network connection\n"
            "is required after the initial sync.\n\n"
            "Tip: Enter a 5-digit ZIP code in the toolbar to automatically see\n"
            "rural (R) or non-rural (NR) allowables, similar to PDAC lookup."
        )
        body.setWordWrap(True)
        body.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
        layout.addWidget(body, 1)

        sep2 = QFrame()
        sep2.setFrameShape(QFrame.Shape.HLine)
        layout.addWidget(sep2)

        # ---- Desktop shortcut option (Windows .exe only) ----------------
        from core.shortcut import can_create_shortcut, shortcut_exists
        self._shortcut_check = QCheckBox("Create a desktop shortcut for this application")
        if can_create_shortcut():
            self._shortcut_check.setChecked(not shortcut_exists())
        else:
            self._shortcut_check.setChecked(False)
            self._shortcut_check.setEnabled(False)
            self._shortcut_check.setToolTip(
                "Desktop shortcuts can only be created when running as the installed .exe on Windows."
            )
        layout.addWidget(self._shortcut_check)

        return page

    def _build_states_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(24, 16, 24, 8)
        layout.setSpacing(8)

        title = QLabel("Step 1 of 2 — Select States to Track")
        title.setStyleSheet("font-size: 14px; font-weight: bold; color: #003366;")
        layout.addWidget(title)

        layout.addWidget(QLabel("Select the states you want to track and sync data for:"))

        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        layout.addWidget(sep)

        # Select All / None helpers
        sel_row = QHBoxLayout()
        all_btn = QPushButton("Select All")
        none_btn = QPushButton("Select None")
        all_btn.clicked.connect(self._states_select_all)
        none_btn.clicked.connect(self._states_select_none)
        sel_row.addWidget(all_btn)
        sel_row.addWidget(none_btn)
        sel_row.addStretch()
        layout.addLayout(sel_row)

        # Checkbox grid inside a scroll area
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        grid_widget = QWidget()
        grid = QGridLayout(grid_widget)
        grid.setSpacing(2)

        sorted_states = sorted(ALL_STATES.items(), key=lambda x: x[1])
        cols = 3
        for idx, (abbr, name) in enumerate(sorted_states):
            cb = QCheckBox(f"{name} ({abbr})")
            cb.setChecked(False)
            self._state_checkboxes[abbr] = cb
            grid.addWidget(cb, idx // cols, idx % cols)

        grid_widget.setLayout(grid)
        scroll.setWidget(grid_widget)
        layout.addWidget(scroll, 1)

        # Pre-populate from existing DB selection (re-run scenario)
        selected = {abbr for abbr, _ in get_selected_states()}
        for abbr, cb in self._state_checkboxes.items():
            cb.setChecked(abbr in selected)

        return page

    def _build_years_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(24, 16, 24, 8)
        layout.setSpacing(8)

        title = QLabel("Step 2 of 2 — Select Years to Track")
        title.setStyleSheet("font-size: 14px; font-weight: bold; color: #003366;")
        layout.addWidget(title)

        layout.addWidget(QLabel(
            "Select the years you want to track and sync data for.\n"
            "Default shows current year + last 3; add more as needed."
        ))

        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        layout.addWidget(sep)

        # Best-effort CMS availability check
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
            self._year_checkboxes[year] = cb
            layout.addWidget(cb)

        sep2 = QFrame()
        sep2.setFrameShape(QFrame.Shape.HLine)
        layout.addWidget(sep2)

        # Pre-populate defaults
        saved = get_selected_years()
        defaults = saved if saved else get_default_selected_years()
        for year, cb in self._year_checkboxes.items():
            if cb.isEnabled():
                cb.setChecked(year in defaults)

        layout.addStretch(1)
        return page

    # ----------------------------------------------------------- navigation --

    def _update_nav(self):
        idx = self._stack.currentIndex()
        last = self._stack.count() - 1
        self._back_btn.setVisible(idx > 0)
        if idx == last:
            self._next_btn.setText("Finish")
            self._next_btn.setStyleSheet(_BTN_STYLE)
        else:
            self._next_btn.setText("Next →")
            self._next_btn.setStyleSheet(_BTN_STYLE)

    def _go_back(self):
        idx = self._stack.currentIndex()
        if idx > 0:
            self._stack.setCurrentIndex(idx - 1)
            self._update_nav()

    def _go_next(self):
        idx = self._stack.currentIndex()
        last = self._stack.count() - 1
        if idx < last:
            if idx == 1 and not self._validate_states():
                return
            self._stack.setCurrentIndex(idx + 1)
            self._update_nav()
        else:
            self._finish()

    # ------------------------------------------------------- helper actions --

    def _states_select_all(self):
        for cb in self._state_checkboxes.values():
            cb.setChecked(True)

    def _states_select_none(self):
        for cb in self._state_checkboxes.values():
            cb.setChecked(False)

    def _validate_states(self) -> bool:
        selected = [abbr for abbr, cb in self._state_checkboxes.items() if cb.isChecked()]
        if not selected:
            QMessageBox.warning(
                self, "No States Selected",
                "Please select at least one state to track.",
            )
            return False
        return True

    def _finish(self):
        # Validate years
        selected_years = [year for year, cb in self._year_checkboxes.items() if cb.isChecked()]
        if not selected_years:
            QMessageBox.warning(
                self, "No Years Selected",
                "Please select at least one year to track.",
            )
            return

        # Save states
        selected_states = [
            (abbr, ALL_STATES[abbr])
            for abbr, cb in self._state_checkboxes.items()
            if cb.isChecked()
        ]
        save_selected_states(selected_states)
        save_selected_years(selected_years)
        set_preference("first_run_done", "1")

        # Create desktop shortcut if requested
        if self._shortcut_check.isChecked():
            from core.shortcut import create_desktop_shortcut
            create_desktop_shortcut()

        self.accept()

    # ---------------------------------------------------------------- utils --

    def _center_on_parent(self, parent):
        """Center this dialog on the parent window after it is shown."""
        parent_geo = parent.geometry()
        self.move(
            parent_geo.center().x() - self.minimumWidth() // 2,
            parent_geo.center().y() - self.minimumHeight() // 2,
        )
