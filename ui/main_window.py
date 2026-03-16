from datetime import date

from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QComboBox, QLineEdit, QTableWidget,
    QTableWidgetItem, QHeaderView, QStatusBar, QMessageBox,
    QDialog, QTextEdit, QSizePolicy, QFrame, QCheckBox,
    QProgressDialog,
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QAction, QFont

from core.database import (
    get_fees, get_selected_states, get_available_years, get_import_log,
    get_preference, set_preference, get_selected_years, save_selected_years,
    is_rural_zip, get_current_year_or_fallback,
)
from core.cms_downloader import download_cms_fees, SUPPORTED_YEARS
from ui.import_dialog import ImportDialog
from ui.export_dialog import ExportDialog
from ui.state_selector_dialog import StateSelectorDialog
from ui.year_selector_dialog import YearSelectorDialog


class SyncWorker(QThread):
    progress = pyqtSignal(str)
    finished = pyqtSignal(int)
    error = pyqtSignal(str)

    def __init__(self, years, states):
        super().__init__()
        self.years = years
        self.states = states

    def run(self):
        try:
            total = 0
            for year in self.years:
                count = download_cms_fees(
                    year,
                    self.states,
                    progress_callback=lambda msg: self.progress.emit(msg),
                )
                total += count
            self.finished.emit(total)
        except Exception as e:
            self.error.emit(str(e))


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("VA HCPCS Fee Schedule Manager")
        self.setMinimumSize(1200, 700)
        self._records = []
        self._sync_worker = None
        self._progress_dlg = None
        self._init_ui()
        self._init_menu()
        self._check_first_run()
        self._refresh_filters()
        self._apply_filters()

    # ------------------------------------------------------------------ UI --

    def _init_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(6)

        # ---- Toolbar row ----
        toolbar = QHBoxLayout()

        sync_btn = QPushButton("⚌  Sync from CMS")
        sync_btn.setStyleSheet(
            "background-color: #003366; color: white; padding: 6px 14px; font-weight: bold; font-size: 13px;"
        )
        sync_btn.setToolTip("Download latest CMS DMEPOS fee schedules for your tracked states")
        sync_btn.clicked.connect(self._sync_cms)
        toolbar.addWidget(sync_btn)

        toolbar.addSpacing(16)

        # Year filter
        toolbar.addWidget(QLabel("Year:"))
        self.year_combo = QComboBox()
        self.year_combo.setMinimumWidth(90)
        self.year_combo.currentIndexChanged.connect(self._on_year_changed)
        toolbar.addWidget(self.year_combo)

        toolbar.addSpacing(8)

        # State filter
        toolbar.addWidget(QLabel("State:"))
        self.state_combo = QComboBox()
        self.state_combo.setMinimumWidth(130)
        self.state_combo.currentIndexChanged.connect(self._apply_filters)
        toolbar.addWidget(self.state_combo)

        toolbar.addSpacing(8)

        # ZIP code input for rural/non-rural determination
        toolbar.addWidget(QLabel("ZIP:"))
        self.zip_edit = QLineEdit()
        self.zip_edit.setPlaceholderText("5-digit ZIP")
        self.zip_edit.setMaximumWidth(80)
        self.zip_edit.setToolTip(
            "Enter a 5-digit ZIP code to automatically select rural (R) or non-rural (NR) allowable.\n"
            "Leave blank to default to non-rural (NR)."
        )
        self.zip_edit.textChanged.connect(self._on_zip_changed)
        toolbar.addWidget(self.zip_edit)

        self.rural_label = QLabel("")
        self.rural_label.setMinimumWidth(80)
        toolbar.addWidget(self.rural_label)

        toolbar.addSpacing(8)

        # HCPCS code search
        toolbar.addWidget(QLabel("HCPCS:"))
        self.code_edit = QLineEdit()
        self.code_edit.setPlaceholderText("e.g. E0601")
        self.code_edit.setMaximumWidth(100)
        self.code_edit.returnPressed.connect(self._apply_filters)
        toolbar.addWidget(self.code_edit)

        toolbar.addSpacing(8)

        # Keyword search
        toolbar.addWidget(QLabel("Keyword:"))
        self.keyword_edit = QLineEdit()
        self.keyword_edit.setPlaceholderText("Description keyword…")
        self.keyword_edit.setMaximumWidth(200)
        self.keyword_edit.returnPressed.connect(self._apply_filters)
        toolbar.addWidget(self.keyword_edit)

        search_btn = QPushButton("Search")
        search_btn.clicked.connect(self._apply_filters)
        toolbar.addWidget(search_btn)

        clear_btn = QPushButton("Clear")
        clear_btn.clicked.connect(self._clear_filters)
        toolbar.addWidget(clear_btn)

        toolbar.addStretch()

        export_btn = QPushButton("Export…")
        export_btn.setStyleSheet(
            "background-color: #005A9C; color: white; padding: 6px 14px; font-weight: bold;"
        )
        export_btn.clicked.connect(self._export)
        toolbar.addWidget(export_btn)

        root.addLayout(toolbar)

        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        root.addWidget(sep)

        # ---- Results table ----
        self.table = QTableWidget(0, 7)
        self.table.setHorizontalHeaderLabels([
            "HCPCS Code", "Description", "State", "Year",
            "Allowable ($)", "Modifier", "Source",
        ])
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.table.horizontalHeader().setDefaultSectionSize(100)
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setAlternatingRowColors(True)
        self.table.setSortingEnabled(True)
        self.table.doubleClicked.connect(self._on_row_double_clicked)
        self.table.setToolTip("Double-click a row to view historical NR/R data for that HCPCS code.")
        root.addWidget(self.table, 1)

        # ---- Status bar ----
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self._set_status("Ready.")

    def _init_menu(self):
        menubar = self.menuBar()

        # File
        file_menu = menubar.addMenu("&File")

        import_action = QAction("&Import CSV…", self)
        import_action.setShortcut("Ctrl+I")
        import_action.triggered.connect(self._import_csv)
        file_menu.addAction(import_action)

        file_menu.addSeparator()

        log_action = QAction("View Import &Log", self)
        log_action.triggered.connect(self._show_import_log)
        file_menu.addAction(log_action)

        file_menu.addSeparator()

        exit_action = QAction("E&xit", self)
        exit_action.setShortcut("Ctrl+Q")
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        # Settings
        settings_menu = menubar.addMenu("&Settings")

        states_action = QAction("&Manage States…", self)
        states_action.triggered.connect(self._manage_states)
        settings_menu.addAction(states_action)

        years_action = QAction("Manage &Years…", self)
        years_action.triggered.connect(self._manage_years)
        settings_menu.addAction(years_action)

        # Developer Tools
        dev_menu = menubar.addMenu("&Developer Tools")
        sql_action = QAction("&SQL Publisher…", self)
        sql_action.triggered.connect(self._open_sql_publisher)
        dev_menu.addAction(sql_action)

        # Help
        help_menu = menubar.addMenu("&Help")

        about_action = QAction("&About", self)
        about_action.triggered.connect(self._show_about)
        help_menu.addAction(about_action)

    # --------------------------------------------------------------- Slots --

    def _check_first_run(self):
        if get_preference("first_run_done") != "1":
            QMessageBox.information(
                self,
                "Welcome to VA HCPCS Fee Schedule Manager",
                "Welcome!\n\n"
                "To get started:\n"
                "  1. Go to Settings → Manage States and select the states you track.\n"
                "  2. Go to Settings → Manage Years and select the years you need.\n"
                "  3. Click \"Sync from CMS\" to download the latest fee schedules,\n"
                "     OR use File → Import CSV to load an existing file.\n\n"
                "Data is stored locally in a SQLite database — no network connection\n"
                "is required after the initial sync.\n\n"
                "Tip: Enter a 5-digit ZIP code in the toolbar to automatically see\n"
                "rural (R) or non-rural (NR) allowables, similar to PDAC lookup.",
            )
            set_preference("first_run_done", "1")
            dlg = StateSelectorDialog(self)
            dlg.exec()
            year_dlg = YearSelectorDialog(self)
            year_dlg.exec()
            self._refresh_filters()

    def _refresh_filters(self):
        """Reload year and state combos from the database."""
        self.year_combo.blockSignals(True)
        self.state_combo.blockSignals(True)

        # Years
        prev_year = self.year_combo.currentData()
        self.year_combo.clear()
        self.year_combo.addItem("All Years", None)
        for y in get_available_years():
            self.year_combo.addItem(str(y), y)

        # Default to current year (or most recent year present in DB)
        target_year = get_current_year_or_fallback()
        if target_year is not None:
            idx = self.year_combo.findData(target_year)
            if idx >= 0:
                self.year_combo.setCurrentIndex(idx)
        elif prev_year is not None:
            idx = self.year_combo.findData(prev_year)
            if idx >= 0:
                self.year_combo.setCurrentIndex(idx)

        # States
        prev_state = self.state_combo.currentData()
        self.state_combo.clear()
        self.state_combo.addItem("All States", None)
        for abbr, name in get_selected_states():
            self.state_combo.addItem(f"{name} ({abbr})", abbr)
        if prev_state is not None:
            idx = self.state_combo.findData(prev_state)
            if idx >= 0:
                self.state_combo.setCurrentIndex(idx)

        self.year_combo.blockSignals(False)
        self.state_combo.blockSignals(False)

    def _effective_year(self):
        """Return the year that should drive rural ZIP determination.

        "All Years" → current year or most recent in DB.
        Specific year → that year.
        """
        y = self.year_combo.currentData()
        if y is not None:
            return y
        return get_current_year_or_fallback()

    def _is_rural(self):
        """Return True if the entered ZIP code is rural for the effective year."""
        zip5 = self.zip_edit.text().strip()
        if len(zip5) != 5 or not zip5.isdigit():
            return False
        eff_year = self._effective_year()
        if eff_year is None:
            return False
        return is_rural_zip(eff_year, zip5)

    def _on_zip_changed(self, text):
        """Update rural label and refresh display when ZIP input changes."""
        zip5 = text.strip()
        if not zip5:
            self.rural_label.setText("")
            self._apply_filters()
            return
        if len(zip5) == 5 and zip5.isdigit():
            rural = self._is_rural()
            if rural:
                self.rural_label.setText("🏘 Rural (R)")
                self.rural_label.setStyleSheet("color: #006600; font-weight: bold;")
            else:
                self.rural_label.setText("🏙 Non-Rural (NR)")
                self.rural_label.setStyleSheet("color: #003366; font-weight: bold;")
            self._apply_filters()
        else:
            self.rural_label.setText("")

    def _on_year_changed(self):
        """Re-evaluate rural label when year changes (rural ZIP sets are year-scoped)."""
        self._on_zip_changed(self.zip_edit.text())

    def _query_year(self):
        """Return the year to pass to get_fees().

        "All Years" with no specific year selected shows current (or fallback) year only.
        """
        y = self.year_combo.currentData()
        if y is not None:
            return y
        # "All Years" → show current year only (or fallback)
        return get_current_year_or_fallback()

    def _apply_filters(self):
        year = self._query_year()
        state = self.state_combo.currentData()
        code = self.code_edit.text().strip() or None
        keyword = self.keyword_edit.text().strip() or None

        self._records = get_fees(
            state_abbr=state,
            year=year,
            hcpcs_code=code,
            keyword=keyword,
        )
        self._populate_table(self._records)
        self._set_status(f"{len(self._records):,} records found.")

    def _clear_filters(self):
        self.year_combo.setCurrentIndex(0)
        self.state_combo.setCurrentIndex(0)
        self.code_edit.clear()
        self.keyword_edit.clear()
        self.zip_edit.clear()
        self._apply_filters()

    def _populate_table(self, records):
        is_rural = self._is_rural()
        self.table.setSortingEnabled(False)
        self.table.setRowCount(len(records))
        for row_i, r in enumerate(records):
            # Compute chosen allowable based on rural flag
            if is_rural:
                chosen = r.get("allowable_r") or r.get("allowable_nr") or r.get("allowable")
            else:
                chosen = r.get("allowable_nr") or r.get("allowable")

            values = [
                r.get("hcpcs_code", ""),
                r.get("description", ""),
                r.get("state_abbr", ""),
                str(r.get("year", "")),
                "#N/A" if chosen is None else f"{chosen:,.2f}",
                r.get("modifier", "") or "",
                r.get("data_source", "") or "",
            ]
            for col_i, v in enumerate(values):
                item = QTableWidgetItem(str(v))
                if col_i == 0:
                    # Store the record index so double-click works even after sorting
                    item.setData(Qt.ItemDataRole.UserRole, row_i)
                if col_i == 4 and chosen is None:
                    item.setForeground(Qt.GlobalColor.darkRed)
                self.table.setItem(row_i, col_i, item)
        self.table.setSortingEnabled(True)

    def _on_row_double_clicked(self, index):
        """Open historical detail dialog for the double-clicked HCPCS row."""
        row = index.row()
        # Read the original record index from UserRole (survives table sorting)
        first_item = self.table.item(row, 0)
        if first_item is None:
            return
        rec_idx = first_item.data(Qt.ItemDataRole.UserRole)
        if rec_idx is None or rec_idx < 0 or rec_idx >= len(self._records):
            return
        record = self._records[rec_idx]
        dlg = _HcpcsHistoryDialog(record, self)
        dlg.exec()

    def _import_csv(self):
        dlg = ImportDialog(self)
        dlg.import_complete.connect(self._on_import_done)
        dlg.exec()

    def _on_import_done(self, count):
        self._refresh_filters()
        self._apply_filters()
        self._set_status(f"Imported {count:,} records.")

    def _export(self):
        if not self._records:
            QMessageBox.information(self, "No Data", "No records to export. Apply filters first.")
            return
        dlg = ExportDialog(self._records, self, is_rural=self._is_rural())
        dlg.exec()

    def _manage_states(self):
        dlg = StateSelectorDialog(self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            self._refresh_filters()
            self._apply_filters()

    def _manage_years(self):
        dlg = YearSelectorDialog(self)
        dlg.exec()

    def _open_sql_publisher(self):
        from ui.dev_tools_dialog import DevToolsDialog
        dlg = DevToolsDialog(current_records=self._records, parent=self)
        dlg.exec()

    def _sync_cms(self):
        selected = get_selected_states()
        if not selected:
            QMessageBox.warning(
                self, "No States Selected",
                "Please go to Settings → Manage States and select at least one state before syncing.",
            )
            return

        state_abbrs = [abbr for abbr, _ in selected]

        # Ask which years
        dlg = _SyncYearsDialog(state_abbrs, self)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return

        years = dlg.selected_years()
        if not years:
            return

        # Build a modal progress dialog parented to main window
        self._progress_dlg = QProgressDialog("Starting sync…", None, 0, 0, self)
        self._progress_dlg.setWindowTitle("Syncing from CMS")
        self._progress_dlg.setWindowModality(Qt.WindowModality.WindowModal)
        self._progress_dlg.setMinimumWidth(500)
        self._progress_dlg.setMinimumDuration(0)
        self._progress_dlg.setValue(0)
        # Centre over main window
        geo = self.geometry()
        dlg_w, dlg_h = 500, 120
        self._progress_dlg.setGeometry(
            geo.x() + (geo.width() - dlg_w) // 2,
            geo.y() + (geo.height() - dlg_h) // 2,
            dlg_w, dlg_h,
        )
        self._progress_dlg.show()
        self._progress_dlg.raise_()
        self._progress_dlg.activateWindow()

        self._set_status("Syncing from CMS…")
        self._sync_worker = SyncWorker(years, state_abbrs)
        self._sync_worker.progress.connect(self._on_sync_progress)
        self._sync_worker.finished.connect(self._on_sync_done)
        self._sync_worker.error.connect(self._on_sync_error)
        self._sync_worker.start()

    def _on_sync_progress(self, msg):
        self._set_status(msg)
        if self._progress_dlg:
            self._progress_dlg.setLabelText(msg)
            self._progress_dlg.raise_()

    def _on_sync_done(self, count):
        if self._progress_dlg:
            self._progress_dlg.close()
            self._progress_dlg = None
        self._refresh_filters()
        self._apply_filters()
        self._set_status(f"CMS sync complete — {count:,} records imported.")
        QMessageBox.information(
            self, "Sync Complete",
            f"Successfully imported {count:,} records from CMS.",
        )

    def _on_sync_error(self, msg):
        if self._progress_dlg:
            self._progress_dlg.close()
            self._progress_dlg = None
        self._set_status("Sync failed.")
        QMessageBox.critical(self, "Sync Error", msg)

    def _show_import_log(self):
        dlg = _ImportLogDialog(self)
        dlg.exec()

    def _show_about(self):
        QMessageBox.about(
            self,
            "About VA HCPCS Fee Schedule Manager",
            "<b>VA HCPCS Fee Schedule Manager</b><br><br>"
            "A standalone Windows desktop application for VA staff to manage,<br>"
            "view, filter, and export CMS DMEPOS HCPCS fee schedule data.<br><br>"
            "Tip: Enter a ZIP code in the toolbar to automatically display rural (R)<br>"
            "or non-rural (NR) allowable amounts, similar to PDAC fee lookup.<br><br>"
            "Data source: <a href='https://www.cms.gov/medicare/payment/fee-schedules/dmepos'>"
            "CMS DMEPOS Fee Schedule</a>",
        )

    def _set_status(self, msg):
        self.status_bar.showMessage(msg)


# ----------------------------------------------------------------- Helpers --

class _SyncYearsDialog(QDialog):
    """Simple dialog to pick which year(s) to sync."""

    def __init__(self, state_abbrs, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Sync from CMS")
        self.setMinimumWidth(340)
        self._checks = {}
        layout = QVBoxLayout(self)

        states_label = QLabel(
            f"Syncing data for: <b>{', '.join(sorted(state_abbrs))}</b>"
        )
        states_label.setWordWrap(True)
        layout.addWidget(states_label)
        layout.addWidget(QLabel("Select year(s) to download:"))

        for year in SUPPORTED_YEARS:
            cb = QCheckBox(str(year))
            layout.addWidget(cb)
            self._checks[year] = cb

        saved_years = get_selected_years()
        # If nothing saved, default to current + last 3
        from core.database import get_default_selected_years
        defaults = saved_years if saved_years else get_default_selected_years()
        for year, cb in self._checks.items():
            cb.setChecked(year in defaults)

        btn_row = QHBoxLayout()
        cancel_btn = QPushButton("Cancel")
        sync_btn = QPushButton("Sync")
        sync_btn.setStyleSheet(
            "background-color: #003366; color: white; padding: 6px 16px; font-weight: bold;"
        )
        cancel_btn.clicked.connect(self.reject)
        sync_btn.clicked.connect(self.accept)
        btn_row.addStretch()
        btn_row.addWidget(cancel_btn)
        btn_row.addWidget(sync_btn)
        layout.addLayout(btn_row)

    def selected_years(self):
        return [y for y, cb in self._checks.items() if cb.isChecked()]


class _HcpcsHistoryDialog(QDialog):
    """Shows historical NR/R values for a specific HCPCS code across all years."""

    def __init__(self, record, parent=None):
        super().__init__(parent)
        hcpcs = record.get("hcpcs_code", "")
        state = record.get("state_abbr", "")
        modifier = record.get("modifier") or ""
        self.setWindowTitle(f"History: {hcpcs} — {state}")
        self.setMinimumSize(700, 400)
        layout = QVBoxLayout(self)

        desc = record.get("description", "")
        header = QLabel(
            f"<b>HCPCS:</b> {hcpcs}  <b>State:</b> {state}"
            + (f"  <b>Modifier:</b> {modifier}" if modifier else "")
            + (f"<br><small>{desc}</small>" if desc else "")
        )
        header.setWordWrap(True)
        layout.addWidget(header)

        # Fetch historical records for this HCPCS/state across all years
        from core.database import get_fees
        hist = get_fees(state_abbr=state, hcpcs_code=hcpcs)
        if modifier:
            hist = [r for r in hist if (r.get("modifier") or "") == modifier]
        # Sort by year desc
        hist.sort(key=lambda r: r.get("year", 0), reverse=True)

        table = QTableWidget(len(hist), 5)
        table.setHorizontalHeaderLabels(["Year", "NR ($)", "R ($)", "Modifier", "Source"])
        table.horizontalHeader().setDefaultSectionSize(100)
        table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeMode.Stretch)
        table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        table.setAlternatingRowColors(True)

        for i, r in enumerate(hist):
            nr = r.get("allowable_nr") or r.get("allowable")
            rv = r.get("allowable_r")
            table.setItem(i, 0, QTableWidgetItem(str(r.get("year", ""))))
            table.setItem(i, 1, QTableWidgetItem("#N/A" if nr is None else f"{nr:,.2f}"))
            table.setItem(i, 2, QTableWidgetItem("#N/A" if rv is None else f"{rv:,.2f}"))
            table.setItem(i, 3, QTableWidgetItem(r.get("modifier") or ""))
            table.setItem(i, 4, QTableWidgetItem(r.get("data_source") or ""))

        layout.addWidget(table)

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        layout.addWidget(close_btn, alignment=Qt.AlignmentFlag.AlignRight)


class _ImportLogDialog(QDialog):
    """Shows the import history log."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Import Log")
        self.setMinimumSize(700, 400)
        layout = QVBoxLayout(self)

        log = get_import_log()
        table = QTableWidget(len(log), 5)
        table.setHorizontalHeaderLabels(["File", "Source", "Records", "States", "Imported At"])
        table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)

        for row_i, entry in enumerate(log):
            table.setItem(row_i, 0, QTableWidgetItem(entry.get("file_name", "")))
            table.setItem(row_i, 1, QTableWidgetItem(entry.get("source", "")))
            table.setItem(row_i, 2, QTableWidgetItem(str(entry.get("record_count", ""))))
            table.setItem(row_i, 3, QTableWidgetItem(entry.get("states", "")))
            table.setItem(row_i, 4, QTableWidgetItem(entry.get("imported_at", "")))

        layout.addWidget(table)

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        layout.addWidget(close_btn, alignment=Qt.AlignmentFlag.AlignRight)
