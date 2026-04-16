import sys
import tempfile
from datetime import date, datetime
from pathlib import Path

from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QPushButton, QLabel, QComboBox, QLineEdit, QTableWidget,
    QTableWidgetItem, QHeaderView, QStatusBar, QMessageBox,
    QDialog, QTextEdit, QSizePolicy, QFrame, QCheckBox,
    QProgressDialog, QMenu, QApplication, QScrollArea, QFileDialog,
    QSplitter,
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer, QSize
from PyQt6.QtGui import QAction, QFont, QColor, QIcon, QPixmap, QShortcut, QKeySequence

from core.config import get_config_value, set_config_value
from core.database import (
    get_fees, get_selected_states, get_available_years, get_import_log,
    get_preference, set_preference, get_auto_selected_years,
    is_rural_zip, get_current_year_or_fallback,
)
from core.cms_downloader import download_cms_fees, SUPPORTED_YEARS
from ui.import_dialog import ImportDialog
from ui.state_selector_dialog import StateSelectorDialog
from ui.purchase_list_panel import PurchaseListPanel

PURCHASE_PANEL_LEFT_RATIO = 2 / 3
MAIN_COL_SELECT = 0
MAIN_COL_HCPCS = 1
MAIN_COL_DESC = 2
MAIN_COL_STATE = 3
MAIN_COL_YEAR = 4
MAIN_COL_ALLOWABLE = 5
MAIN_COL_MODIFIER = 6
MAIN_COL_SOURCE = 7
STARTUP_LOG_FILENAME = "HCPCSFeeApp_startup.log"


def _asset(name: str) -> Path:
    """Return the absolute path to *name* inside the ``assets/`` folder.

    Works in both development mode and when frozen by PyInstaller.
    """
    base = Path(getattr(sys, "_MEIPASS", Path(__file__).parent.parent))
    return base / "assets" / name


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


class CmsNewFileCheckWorker(QThread):
    """Background thread that checks all auto-selected years for newer CMS files.

    Emits ``newer_available`` when at least one year has a newer file on CMS
    than the last synced URL stored in preferences.  Only years that have
    previously been synced (``cms_synced_source_url_{year}`` is set) will
    trigger a network probe, so first-time installs make no requests.
    """

    newer_available = pyqtSignal()

    def run(self):
        try:
            from core.cms_downloader import has_newer_cms_file_available
            from core.database import get_auto_selected_years
            for year in get_auto_selected_years():
                if has_newer_cms_file_available(year):
                    self.newer_available.emit()
                    return
        except Exception:
            pass


class MainWindow(QMainWindow):
    def __init__(self, splash=None):
        super().__init__()
        self._splash = splash
        self._initial_load_started = False
        self._first_run_check_started = False
        self.setWindowTitle("VA HCPCS Fee Schedule Manager")
        self.setMinimumSize(1200, 700)
        # Set the window / taskbar icon from the assets folder.
        icon_path = _asset("wsnc_map.png")
        if icon_path.exists():
            self.setWindowIcon(QIcon(str(icon_path)))
        self._records = []
        self._sync_worker = None
        self._progress_dlg = None
        self._purchase_list_panel_visible = False
        self._cms_notification_shown = False  # Track if CMS notification has been shown this session
        self._cms_check_worker = None  # Background CMS new-file check thread
        # Debounce timer for live search (HCPCS + Keyword fields)
        self._search_timer = QTimer()
        self._search_timer.setSingleShot(True)
        self._search_timer.setInterval(250)
        self._search_timer.timeout.connect(self._apply_filters)

        # Wrap initialization in try-except to ensure splash closes on error
        try:
            self._splash_update(30, "Building user interface…")
            self._init_ui()
            self._init_menu()
            self._apply_theme(bool(get_config_value("dark_mode_enabled", False)))
            self._splash_update(55, "Loading year and state filters…")
            self._refresh_filters()
            self._splash_update(70, "Restoring saved preferences…")
            self._restore_filter_preferences()
            self._splash_update(85, "Loading fee records…")
            self._set_status("Loading fee records…")
            # Initial data load is started from showEvent, after first paint.

            # Background update check
            self._update_worker = None
            self._start_update_check()
            QTimer.singleShot(0, self._warn_if_pending_update_file)
            # CMS new-file check is started from _check_first_run, after first-run
            # wizard logic has run, to avoid a modal dialog appearing before the
            # wizard and to avoid blocking the main thread with network calls.
        except Exception as e:
            # Close splash and show error if initialization fails
            if self._splash is not None:
                self._splash.close()
                self._splash = None
            # Re-raise to prevent partially-initialized window from being used
            raise RuntimeError(f"Failed to initialize main window: {e}") from e

    def showEvent(self, event):
        super().showEvent(event)
        self._start_initial_load_when_visible()

    # ------------------------------------------------------------------ UI --

    def _splash_update(self, pct: int, message: str) -> None:
        """Forward a progress update to the splash screen if one is attached."""
        if self._splash is not None:
            try:
                self._splash.set_progress(pct, message)
            except Exception:
                pass  # Never let splash failures crash startup

    def _write_startup_breadcrumb(self, message: str) -> None:
        """Append a startup breadcrumb to a temp log file for frozen-startup diagnosis."""
        try:
            log_path = Path(tempfile.gettempdir()) / STARTUP_LOG_FILENAME
            with log_path.open("a", encoding="utf-8") as fh:
                fh.write(f"[{datetime.now().isoformat(timespec='seconds')}] {message}\n")
        except Exception:
            pass

    def _init_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(6)
        self._light_theme_qss = (
            "QWidget { background: #FFFFFF; color: #202124; }"
            "QLineEdit, QComboBox { border: 1px solid #C9CED6; border-radius: 3px; padding: 3px 6px; height: 22px; background: #FFFFFF; color: #202124; }"
            "QDateEdit { border: 1px solid #C9CED6; border-radius: 3px; padding: 3px 6px; height: 22px; background: #FFFFFF; color: #202124; }"
            "QPushButton { height: 24px; border-radius: 3px; padding: 3px 10px; border: 1px solid #AAB2BF; background: #F8F9FB; color: #202124; }"
            "QPushButton:hover { background-color: #EAF0F8; }"
            "QLabel { background: transparent; }"
            "QMenuBar { background: #FFFFFF; color: #202124; border-bottom: 1px solid #D8DDE6; }"
            "QMenu { background: #FFFFFF; color: #202124; border: 1px solid #D8DDE6; }"
            "QMenu::item:selected { background: #EAF0F8; color: #202124; }"
            "QHeaderView::section { background: #EEF2F7; color: #202124; padding: 4px; }"
            "QTableWidget { selection-background-color: #003366; selection-color: white; }"
            "QTableWidget::item:hover { background-color: #E8F0F8; }"
        )
        self._dark_theme_qss = (
            "QWidget { background: #1E1E1E; color: #D4D4D4; }"
            "QLineEdit, QComboBox { background: #2D2D2D; color: #D4D4D4; border: 1px solid #3E3E3E; border-radius: 3px; padding: 3px 6px; height: 22px; selection-background-color: #264F78; selection-color: #FFFFFF; }"
            "QDateEdit { background: #2D2D2D; color: #D4D4D4; border: 1px solid #3E3E3E; border-radius: 3px; padding: 3px 6px; height: 22px; }"
            "QTextEdit { background: #2D2D2D; color: #D4D4D4; border: 1px solid #3E3E3E; border-radius: 3px; padding: 4px 6px; selection-background-color: #264F78; selection-color: #FFFFFF; }"
            "QPushButton { height: 24px; border-radius: 3px; padding: 3px 10px; border: 1px solid #3E3E3E; background: #2D2D2D; color: #D4D4D4; }"
            "QPushButton:hover { background-color: #383838; border-color: #505050; }"
            "QPushButton:pressed { background-color: #252525; }"
            "QLabel { background: transparent; color: #D4D4D4; }"
            "QMenuBar { background: #1E1E1E; color: #D4D4D4; border-bottom: 1px solid #3E3E3E; }"
            "QMenu { background: #252525; color: #D4D4D4; border: 1px solid #3E3E3E; }"
            "QMenu::item:selected { background: #37373D; color: #FFFFFF; }"
            "QStatusBar { background: #1E1E1E; color: #D4D4D4; border-top: 1px solid #3E3E3E; }"
            "QHeaderView::section { background: #2D2D2D; color: #D4D4D4; border: 1px solid #3E3E3E; padding: 4px; }"
            "QTableWidget { background: #1E1E1E; alternate-background-color: #252525; gridline-color: #3E3E3E; color: #D4D4D4; selection-background-color: #264F78; selection-color: #FFFFFF; }"
            "QTableWidget::item { color: #D4D4D4; }"
            "QTableWidget::item:selected { background: #264F78; color: #FFFFFF; }"
            "QTableWidget::item:hover { background-color: #2A2A2A; color: #E6E6E6; }"
            "QComboBox QAbstractItemView { background: #2D2D2D; color: #D4D4D4; selection-background-color: #264F78; selection-color: #FFFFFF; }"
            "QSpinBox { background: #2D2D2D; color: #D4D4D4; border: 1px solid #3E3E3E; border-radius: 3px; padding: 3px; height: 22px; }"
        )

        # ---- Update notification bar (hidden by default) ----
        bar_style = (
            "background-color: #FFF3CD;"
            "border: 1px solid #FFECB5;"
            "border-radius: 4px;"
            "color: #664D03;"
            "font-size: 12px;"
        )
        self._update_bar_widget = QWidget()
        self._update_bar_widget.setObjectName("updateBarWidget")
        self._update_bar_widget.setStyleSheet(f"#updateBarWidget {{ {bar_style} }}")
        update_bar_layout = QHBoxLayout(self._update_bar_widget)
        update_bar_layout.setContentsMargins(12, 6, 12, 6)
        update_bar_layout.setSpacing(10)

        self.update_bar = QLabel()
        self.update_bar.setOpenExternalLinks(True)
        self.update_bar.setWordWrap(True)
        self.update_bar.setStyleSheet("background: transparent; border: none;")
        update_bar_layout.addWidget(self.update_bar, 1)

        self._update_now_btn = QPushButton("⬇  Update Now")
        self._update_now_btn.setStyleSheet(
            "QPushButton {"
            "  background-color: #0D6EFD; color: white;"
            "  padding: 4px 12px; border-radius: 4px; font-size: 12px;"
            "  border: none;"
            "}"
            "QPushButton:hover { background-color: #0B5ED7; }"
        )
        self._update_now_btn.setVisible(False)
        self._update_now_btn.clicked.connect(self._on_update_now)
        update_bar_layout.addWidget(self._update_now_btn)

        self._update_bar_widget.hide()
        root.addWidget(self._update_bar_widget)

        # ---- Toolbar (two rows) ----
        toolbar_card = QWidget()
        toolbar_card.setObjectName("toolbarCard")
        toolbar_card.setStyleSheet(
            "#toolbarCard { background-color: #F5F6F8; border: 1px solid #D8DDE6; border-radius: 6px; }"
            "#toolbarCard QLabel { background: transparent; color: #202124; font-size: 12px; }"
        )
        toolbar_container = QVBoxLayout(toolbar_card)
        toolbar_container.setContentsMargins(8, 6, 8, 6)
        toolbar_container.setSpacing(6)

        # ---- Row 1: Sync | Year | State | ZIP ----
        row1 = QHBoxLayout()
        row1.setSpacing(8)

        sync_btn = QPushButton("⚌  Sync from CMS")
        sync_btn.setStyleSheet(
            "background-color: #003366; color: white; padding: 4px 12px; font-weight: 600; font-size: 12px; height: 24px;"
        )
        sync_btn.setToolTip("Download latest CMS DMEPOS fee schedules for your tracked states")
        sync_btn.clicked.connect(self._sync_cms)
        row1.addWidget(sync_btn)

        row1.addSpacing(10)

        # Year filter
        row1.addWidget(QLabel("Year:"))
        self.year_combo = QComboBox()
        self.year_combo.setMinimumWidth(85)
        self.year_combo.currentIndexChanged.connect(self._on_year_changed)
        row1.addWidget(self.year_combo)

        # Label showing effective year (updated whenever year combo changes)
        self.year_view_label = QLabel("")
        self.year_view_label.setStyleSheet("color: #666666; font-style: italic; font-size: 11px;")
        self.year_view_label.setMinimumWidth(150)
        row1.addWidget(self.year_view_label)

        row1.addSpacing(6)

        # State filter
        row1.addWidget(QLabel("State:"))
        self.state_combo = QComboBox()
        self.state_combo.setMinimumWidth(260)
        self.state_combo.currentIndexChanged.connect(self._apply_filters)
        self.state_combo.currentIndexChanged.connect(self._save_filter_preferences)
        row1.addWidget(self.state_combo)

        row1.addSpacing(6)

        # ZIP code input for rural/non-rural determination
        row1.addWidget(QLabel("ZIP:"))
        self.zip_edit = QLineEdit()
        self.zip_edit.setPlaceholderText("5-digit ZIP")
        self.zip_edit.setMaximumWidth(75)
        self.zip_edit.setToolTip(
            "Enter a 5-digit ZIP code to automatically select rural (R) or non-rural (NR) allowable.\n"
            "Leave blank to default to non-rural (NR)."
        )
        self.zip_edit.textChanged.connect(self._on_zip_changed)
        row1.addWidget(self.zip_edit)

        self.rural_label = QLabel("No ZIP (default NR)")
        self.rural_label.setStyleSheet("color: #666666; font-size: 10px;")
        self.rural_label.setMinimumWidth(140)
        row1.addWidget(self.rural_label)

        row1.addStretch()
        toolbar_container.addLayout(row1)

        # ---- Row 2: Group | HCPCS | Keyword | Search | Clear | Export ----
        row2 = QHBoxLayout()
        row2.setSpacing(8)

        # HCPCS Group filter
        row2.addWidget(QLabel("Group:"))
        self.group_combo = QComboBox()
        self.group_combo.setMinimumWidth(200)
        self.group_combo.addItem("All Groups", None)
        from core.hcpcs_groups import get_group_choices
        from core.database import get_available_hcpcs_prefixes
        available_prefixes = get_available_hcpcs_prefixes()
        for prefix, label in get_group_choices(only_prefixes=available_prefixes or None):
            self.group_combo.addItem(label, prefix)
        self.group_combo.currentIndexChanged.connect(self._apply_filters)
        self.group_combo.currentIndexChanged.connect(self._save_filter_preferences)
        row2.addWidget(self.group_combo)
        row2.addSpacing(6)

        # HCPCS code search (debounced)
        row2.addWidget(QLabel("HCPCS:"))
        self.code_edit = QLineEdit()
        self.code_edit.setPlaceholderText("e.g. E0601")
        self.code_edit.setMaximumWidth(100)
        self.code_edit.textChanged.connect(self._on_search_text_changed)
        self.code_edit.returnPressed.connect(self._apply_filters)
        row2.addWidget(self.code_edit)

        row2.addSpacing(6)

        # Keyword search (debounced)
        row2.addWidget(QLabel("Keyword:"))
        self.keyword_edit = QLineEdit()
        self.keyword_edit.setPlaceholderText("Description keyword…")
        self.keyword_edit.setMinimumWidth(170)
        self.keyword_edit.textChanged.connect(self._on_search_text_changed)
        self.keyword_edit.returnPressed.connect(self._apply_filters)
        row2.addWidget(self.keyword_edit)

        row2.addSpacing(4)

        search_btn = QPushButton("Search")
        search_btn.clicked.connect(self._apply_filters)
        row2.addWidget(search_btn)

        clear_btn = QPushButton("Clear")
        clear_btn.clicked.connect(self._clear_filters)
        row2.addWidget(clear_btn)

        row2.addStretch()

        export_btn = QPushButton("Export")
        export_btn.setStyleSheet(
            "background-color: #005A9C; color: white; padding: 4px 12px; font-weight: 600; height: 24px;"
        )
        export_btn.clicked.connect(self._export)
        row2.addWidget(export_btn)

        purchase_btn = QPushButton("Purchase List (0)")
        purchase_btn.setStyleSheet(
            "background-color: #005A9C; color: white; padding: 4px 12px; font-weight: 600; height: 24px;"
        )
        purchase_btn.setCheckable(True)
        purchase_btn.toggled.connect(self._toggle_purchase_list_panel)
        self._purchase_btn = purchase_btn
        row2.addWidget(purchase_btn)

        toolbar_container.addLayout(row2)
        root.addWidget(toolbar_card)

        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        root.addWidget(sep)

        # ---- Results table ----
        self.table = QTableWidget(0, 8)
        self.table.setHorizontalHeaderLabels([
            "", "HCPCS Code", "Description", "State", "Year",
            "Allowable ($)", "Modifier", "Source",
        ])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.table.horizontalHeader().setSectionsMovable(True)
        self.table.horizontalHeader().setDefaultSectionSize(110)
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setAlternatingRowColors(True)
        self.table.setSortingEnabled(True)
        # Ensure selected-row text (including the blue hyperlink in col 0) is
        # always visible by forcing white text on the selection highlight.
        self.table.setStyleSheet(
            "QTableWidget::item:selected { background-color: #003366; color: white; }"
            "QTableWidget::item:selected:!active { background-color: #4a7ab5; color: white; }"
            "QTableWidget::item:hover { background-color: #e0e8f0; }"
        )
        self.table.cellClicked.connect(self._on_cell_clicked)
        self.table.doubleClicked.connect(self._on_row_double_clicked)
        self.table.setToolTip("Click HCPCS code to view history. Right-click for copy options.")
        self.table.setHorizontalScrollMode(QTableWidget.ScrollMode.ScrollPerPixel)
        self.table.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.table.horizontalHeader().sectionMoved.connect(self._save_main_table_layout_preferences)
        self.table.horizontalHeader().sectionResized.connect(self._save_main_table_layout_preferences)
        # Context menu
        self.table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self._on_table_context_menu)
        self.splitter = QSplitter(Qt.Orientation.Horizontal)
        self.splitter.addWidget(self.table)
        middle_controls = QWidget()
        middle_layout = QVBoxLayout(middle_controls)
        middle_layout.setContentsMargins(4, 4, 4, 4)
        middle_layout.setSpacing(6)
        middle_layout.addStretch()
        add_btn = QPushButton("►")
        add_btn.setStyleSheet("font-weight: bold; font-size: 13px; background-color: #003366; color: white; min-height: 28px;")
        add_btn.setToolTip("Add selected items to Purchase List (Ctrl+Right)")
        add_btn.clicked.connect(self._add_checked_from_main)
        middle_layout.addWidget(add_btn)
        remove_btn = QPushButton("◄")
        remove_btn.setStyleSheet("font-weight: bold; font-size: 13px; background-color: #003366; color: white; min-height: 28px;")
        remove_btn.setToolTip("Remove selected items from Purchase List (Ctrl+Left)")
        remove_btn.clicked.connect(self._remove_checked_from_purchase)
        middle_layout.addWidget(remove_btn)
        self._add_btn = add_btn
        self._remove_btn = remove_btn
        self._add_btn.hide()
        self._remove_btn.hide()
        middle_layout.addStretch()
        self.splitter.addWidget(middle_controls)
        self._write_startup_breadcrumb("MainWindow._init_ui: creating PurchaseListPanel")
        try:
            self._purchase_list_panel = PurchaseListPanel(
                self,
                year_combo=self.year_combo,
                state_combo=self.state_combo,
                zip_edit=self.zip_edit,
            )
        except Exception as e:
            self._write_startup_breadcrumb(f"MainWindow._init_ui: PurchaseListPanel creation failed: {e}")
            raise
        self._write_startup_breadcrumb("MainWindow._init_ui: created PurchaseListPanel")
        self._purchase_list_panel.count_changed.connect(self._update_purchase_button_label)
        self._purchase_list_panel.hide()
        self.splitter.addWidget(self._purchase_list_panel)
        self.splitter.setStretchFactor(0, 4)
        self.splitter.setStretchFactor(1, 0)
        self.splitter.setStretchFactor(2, 2)
        self.splitter.setSizes([1000, 120, 0])
        root.addWidget(self.splitter, 1)

        self.year_combo.currentIndexChanged.connect(self._refresh_purchase_list_prices_if_visible)
        self.state_combo.currentIndexChanged.connect(self._refresh_purchase_list_prices_if_visible)
        self.zip_edit.textChanged.connect(self._refresh_purchase_list_prices_if_visible)
        QShortcut(QKeySequence("Ctrl+Right"), self, activated=self._add_checked_from_main)
        QShortcut(QKeySequence("Ctrl+Left"), self, activated=self._remove_checked_from_purchase)
        QShortcut(
            QKeySequence("Ctrl+P"),
            self,
            activated=lambda: self._set_purchase_list_panel_visible(not self._purchase_list_panel_visible),
        )
        self._restore_main_table_layout_preferences()

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

        backup_action = QAction("Create &Backup…", self)
        backup_action.triggered.connect(self._create_backup)
        file_menu.addAction(backup_action)

        restore_action = QAction("&Restore from Backup…", self)
        restore_action.triggered.connect(self._restore_backup)
        file_menu.addAction(restore_action)

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

        settings_menu.addSeparator()

        db_path_action = QAction("Change &Database Path…", self)
        db_path_action.triggered.connect(self._change_db_path)
        settings_menu.addAction(db_path_action)

        shortcut_action = QAction("Create &Desktop Shortcut", self)
        shortcut_action.triggered.connect(self._create_desktop_shortcut)
        settings_menu.addAction(shortcut_action)

        # View
        view_menu = menubar.addMenu("&View")

        browse_groups_action = QAction("Browse HCPCS &Groups…", self)
        browse_groups_action.triggered.connect(self._browse_groups)
        view_menu.addAction(browse_groups_action)

        purchase_list_action = QAction("&Purchase List", self)
        purchase_list_action.setCheckable(True)
        purchase_list_action.toggled.connect(self._toggle_purchase_list_panel)
        self._purchase_list_action = purchase_list_action
        view_menu.addAction(purchase_list_action)
        dark_mode_action = QAction("Dark Mode", self)
        dark_mode_action.setCheckable(True)
        dark_mode_action.setChecked(bool(get_config_value("dark_mode_enabled", False)))
        dark_mode_action.toggled.connect(self._toggle_dark_mode)
        self._dark_mode_action = dark_mode_action
        view_menu.addAction(dark_mode_action)

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
        features_action = QAction("&Feature Guide", self)
        features_action.triggered.connect(self._show_feature_guide)
        help_menu.addAction(features_action)

    # --------------------------------------------------------------- Slots --

    def _load_initial_data(self):
        """Load initial fee records and close splash when done."""
        if self._initial_load_started:
            return
        self._initial_load_started = True
        try:
            self._apply_filters()
            self._splash_update(100, "Ready!")
        except Exception as e:
            # Log the error but don't crash - just show empty table
            print(f"Error loading initial data: {e}")
        finally:
            # Always close splash, even if there's an error
            if self._splash is not None:
                self._splash.close()
                self._splash = None
            # After splash closes, trigger first-run check
            # Use a small delay to ensure splash closing animation completes
            if not self._first_run_check_started:
                self._first_run_check_started = True
                QTimer.singleShot(100, self._check_first_run)

    def _start_initial_load_when_visible(self):
        """Start initial data load only after the main window is visible."""
        if self._initial_load_started:
            return
        QTimer.singleShot(0, self._load_initial_data)

    def _check_first_run(self):
        if get_preference("first_run_done") != "1":
            from ui.setup_wizard import SetupWizard
            wizard = SetupWizard(self)
            wizard.exec()
            self._refresh_filters()
            self._apply_filters()
        # Start background CMS new-file check now that first-run logic is done.
        self._start_cms_check()

    def _start_cms_check(self):
        """Start a background CMS new-file check after first-run logic completes."""
        if self._cms_notification_shown:
            return
        app = QApplication.instance()
        if app and app.platformName().lower() == "offscreen":
            return
        self._cms_check_worker = CmsNewFileCheckWorker()
        self._cms_check_worker.newer_available.connect(self._on_cms_newer_available)
        self._cms_check_worker.finished.connect(self._cms_check_worker.deleteLater)
        self._cms_check_worker.start()

    def _on_cms_newer_available(self):
        """Show the CMS new-file dialog once a background check finds a newer file."""
        if self._cms_notification_shown:
            return
        self._cms_notification_shown = True
        ans = QMessageBox.question(
            self,
            "New CMS File Available",
            "A newer CMS fee file appears to be available. Would you like to sync now?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.Yes,
        )
        if ans == QMessageBox.StandardButton.Yes:
            self._sync_cms()

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

        self._update_year_view_label()
        self._refresh_group_combo()

    def _refresh_group_combo(self):
        """Reload the group combo to only show groups that have data in the database."""
        if not hasattr(self, "group_combo"):
            return
        from core.hcpcs_groups import get_group_choices
        from core.database import get_available_hcpcs_prefixes
        prev_group = self.group_combo.currentData()
        self.group_combo.blockSignals(True)
        self.group_combo.clear()
        self.group_combo.addItem("All Groups", None)
        available_prefixes = get_available_hcpcs_prefixes()
        for prefix, label in get_group_choices(only_prefixes=available_prefixes or None):
            self.group_combo.addItem(label, prefix)
        if prev_group is not None:
            idx = self.group_combo.findData(prev_group)
            if idx >= 0:
                self.group_combo.setCurrentIndex(idx)
        self.group_combo.blockSignals(False)

    def _update_year_view_label(self):
        """Update the year view label to show the effective year being displayed."""
        if not hasattr(self, "year_view_label"):
            return
        y = self.year_combo.currentData()
        if y is not None:
            self.year_view_label.setText(f"(Showing year: {y})")
        else:
            fallback = get_current_year_or_fallback()
            current_year = date.today().year
            if fallback is None:
                self.year_view_label.setText("(No data — sync from CMS)")
                self.year_view_label.setStyleSheet("color: #CC3300; font-style: italic; font-size: 11px;")
            elif fallback == current_year:
                self.year_view_label.setText(f"(Showing current year: {current_year})")
                self.year_view_label.setStyleSheet("color: #555555; font-style: italic; font-size: 11px;")
            else:
                self.year_view_label.setText(
                    f"(Latest available: {fallback} — no data for {current_year})"
                )
                self.year_view_label.setStyleSheet("color: #AA5500; font-style: italic; font-size: 11px;")

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

    def _on_year_changed(self):
        """Re-evaluate rural label when year changes (rural ZIP sets are year-scoped)."""
        self._update_year_view_label()
        self._save_filter_preferences()
        self._on_zip_changed(self.zip_edit.text())

    def _sync_rural_label(self):
        """Update the rural/non-rural label to match the current ZIP field.

        Unlike ``_on_zip_changed``, this never triggers filter application,
        making it safe to call during startup or preference restoration.
        """
        zip5 = self.zip_edit.text().strip()
        if not zip5:
            self.rural_label.setText("No ZIP (default NR)")
            self.rural_label.setStyleSheet("color: #666666; font-size: 11px;")
        elif len(zip5) == 5 and zip5.isdigit():
            rural = self._is_rural()
            if rural:
                self.rural_label.setText(f"ZIP {zip5} → Rural (R)")
                self.rural_label.setStyleSheet("color: #006600; font-weight: bold; font-size: 11px;")
            else:
                self.rural_label.setText(f"ZIP {zip5} → Non-Rural (NR)")
                self.rural_label.setStyleSheet("color: #003366; font-weight: bold; font-size: 11px;")
        else:
            self.rural_label.setText("")
            self.rural_label.setStyleSheet("color: #666666; font-size: 11px;")

    def _on_zip_changed(self, text):
        """Update rural label and refresh display when ZIP input changes."""
        self._sync_rural_label()
        zip5 = text.strip()
        if not zip5 or (len(zip5) == 5 and zip5.isdigit()):
            self._save_filter_preferences()
            self._apply_filters()

    def _query_year(self):
        """Return the year to pass to get_fees().

        "All Years" with no specific year selected shows current (or fallback) year only.
        """
        y = self.year_combo.currentData()
        if y is not None:
            return y
        # "All Years" → show current year only (or fallback)
        return get_current_year_or_fallback()

    def _on_search_text_changed(self):
        """Restart the debounce timer when HCPCS or Keyword text changes."""
        self._search_timer.start()

    def _apply_filters(self):
        year = self._query_year()
        state = self.state_combo.currentData()
        code = self.code_edit.text().strip() or None
        keyword = self.keyword_edit.text().strip() or None
        group = self.group_combo.currentData() if hasattr(self, "group_combo") else None

        self._records = get_fees(
            state_abbr=state,
            year=year,
            hcpcs_code=code,
            keyword=keyword,
            hcpcs_group=group,
        )
        self._populate_table(self._records)
        self._set_status(f"{len(self._records):,} records found.")
        self._save_filter_preferences()

    def _clear_filters(self):
        self.year_combo.setCurrentIndex(0)
        self.state_combo.setCurrentIndex(0)
        self.group_combo.setCurrentIndex(0)
        self.code_edit.clear()
        self.keyword_edit.clear()
        self.zip_edit.clear()
        self._apply_filters()

    # ------------------------------------------------- Filter persistence ---

    def _save_filter_preferences(self):
        """Persist current filter values to user preferences."""
        try:
            set_preference("filter_year", str(self.year_combo.currentData() or ""))
            set_preference("filter_state", str(self.state_combo.currentData() or ""))
            set_preference("filter_group", str(self.group_combo.currentData() or ""))
            set_preference("filter_zip", self.zip_edit.text().strip())
            set_preference("filter_hcpcs", self.code_edit.text().strip())
            set_preference("filter_keyword", self.keyword_edit.text().strip())
        except Exception:
            pass  # Persistence is best-effort; don't crash the UI

    def _restore_filter_preferences(self):
        """Restore previously saved filter values from user preferences."""
        try:
            # Block all filter-widget signals so that restoring saved values does
            # not trigger _apply_filters (or start the debounce timer) before the
            # window is visible.  We update the derived labels manually below.
            _widgets = [
                self.year_combo, self.state_combo, self.group_combo,
                self.zip_edit, self.code_edit, self.keyword_edit,
            ]
            for w in _widgets:
                w.blockSignals(True)
            try:
                saved_year = get_preference("filter_year", "")
                if saved_year:
                    try:
                        y = int(saved_year)
                        idx = self.year_combo.findData(y)
                        if idx >= 0:
                            self.year_combo.setCurrentIndex(idx)
                    except ValueError:
                        pass

                saved_state = get_preference("filter_state", "")
                if saved_state:
                    idx = self.state_combo.findData(saved_state)
                    if idx >= 0:
                        self.state_combo.setCurrentIndex(idx)

                saved_group = get_preference("filter_group", "")
                if saved_group and hasattr(self, "group_combo"):
                    idx = self.group_combo.findData(saved_group)
                    if idx >= 0:
                        self.group_combo.setCurrentIndex(idx)

                saved_zip = get_preference("filter_zip", "")
                if saved_zip:
                    self.zip_edit.setText(saved_zip)

                saved_hcpcs = get_preference("filter_hcpcs", "")
                if saved_hcpcs:
                    self.code_edit.setText(saved_hcpcs)

                saved_keyword = get_preference("filter_keyword", "")
                if saved_keyword:
                    self.keyword_edit.setText(saved_keyword)
            finally:
                for w in _widgets:
                    w.blockSignals(False)

            # Refresh derived labels now that all values are in place.
            self._update_year_view_label()
            self._sync_rural_label()
        except Exception:
            pass  # Preference restore is best-effort

    def _populate_table(self, records):
        is_rural = self._is_rural()
        self.table.setSortingEnabled(False)
        self.table.setRowCount(len(records))
        link_color = QColor("#0066CC")
        link_font = QFont()
        link_font.setUnderline(True)
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
                "—" if chosen is None else f"{chosen:,.2f}",
                r.get("modifier", "") or "",
                r.get("data_source", "") or "",
            ]
            self.table.setItem(row_i, MAIN_COL_SELECT, self._checkbox_item(False))
            for col_i, v in enumerate(values):
                item = QTableWidgetItem(str(v))
                if col_i == 0:
                    # Style as hyperlink; store original record index for click handling
                    item.setForeground(link_color)
                    item.setFont(link_font)
                    item.setToolTip("Click to view history for this HCPCS code")
                    item.setData(Qt.ItemDataRole.UserRole, row_i)
                if col_i == 4 and chosen is None:
                    item.setForeground(Qt.GlobalColor.darkGray)
                self.table.setItem(row_i, col_i + 1, item)
        self.table.setSortingEnabled(True)

    def _on_cell_clicked(self, row, col):
        """Open history dialog when the HCPCS code cell (column 0) is clicked."""
        if col == MAIN_COL_HCPCS:
            self._open_history_for_row(row)

    def _on_row_double_clicked(self, index):
        """Open historical detail dialog for the double-clicked HCPCS row."""
        row = index.row()
        if index.column() != MAIN_COL_HCPCS:
            self._open_history_for_row(row)

    def _open_history_for_row(self, row):
        """Open the history dialog for the given table row."""
        first_item = self.table.item(row, MAIN_COL_HCPCS)
        if first_item is None:
            return
        rec_idx = first_item.data(Qt.ItemDataRole.UserRole)
        if rec_idx is None or rec_idx < 0 or rec_idx >= len(self._records):
            return
        record = self._records[rec_idx]
        dlg = _HcpcsHistoryDialog(record, self)
        dlg.exec()

    # ---------------------------------------------------- Context menu ------

    def _on_table_context_menu(self, pos):
        """Show right-click context menu on main grid with copy actions."""
        row = self.table.rowAt(pos.y())
        if row < 0:
            return
        first_item = self.table.item(row, MAIN_COL_HCPCS)
        if first_item is None:
            return

        menu = QMenu(self)

        def copy_text(col):
            item = self.table.item(row, col)
            if item:
                QApplication.clipboard().setText(item.text())

        def copy_row_csv():
            vals = []
            for c in range(1, self.table.columnCount()):
                item = self.table.item(row, c)
                v = item.text() if item else ""
                vals.append(f'"{v}"')
            QApplication.clipboard().setText(",".join(vals))

        menu.addAction("Copy HCPCS", lambda: copy_text(MAIN_COL_HCPCS))
        menu.addAction("Copy Description", lambda: copy_text(MAIN_COL_DESC))
        menu.addAction("Copy Effective Allowable", lambda: copy_text(MAIN_COL_ALLOWABLE))
        menu.addSeparator()
        selected_code = self.table.item(row, MAIN_COL_HCPCS).text()
        menu.addAction("Add to Purchase List", lambda code=selected_code: self._open_purchase_list_builder(code))
        menu.addSeparator()
        menu.addAction("Copy Row as CSV", copy_row_csv)
        menu.exec(self.table.viewport().mapToGlobal(pos))

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
        from ui.export_dialog import ExportDialog
        zip_code = self.zip_edit.text().strip()
        dlg = ExportDialog(self._records, self, is_rural=self._is_rural(), zip_code=zip_code)
        dlg.exec()

    def _manage_states(self):
        dlg = StateSelectorDialog(self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            self._refresh_filters()
            self._apply_filters()

    def _browse_groups(self):
        from ui.group_browser_dialog import GroupBrowserDialog
        dlg = GroupBrowserDialog(self)
        dlg.exec()

    def _create_desktop_shortcut(self):
        from core.shortcut import can_create_shortcut, create_desktop_shortcut, shortcut_exists
        if not can_create_shortcut():
            QMessageBox.information(
                self, "Desktop Shortcut",
                "Desktop shortcuts can only be created when running as the installed .exe on Windows.",
            )
            return
        if shortcut_exists():
            ans = QMessageBox.question(
                self, "Desktop Shortcut",
                "A desktop shortcut already exists. Replace it?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
                QMessageBox.StandardButton.Cancel,
            )
            if ans != QMessageBox.StandardButton.Yes:
                return
        ok = create_desktop_shortcut()
        if ok:
            QMessageBox.information(self, "Desktop Shortcut", "Desktop shortcut created successfully.")
        else:
            QMessageBox.warning(self, "Desktop Shortcut", "Failed to create desktop shortcut.")

    def _change_db_path(self):
        from core.config import get_data_dir, set_data_dir
        from pathlib import Path
        current = str(get_data_dir())
        folder = QFileDialog.getExistingDirectory(self, "Select Database Folder", current)
        if not folder:
            return
        new_path = Path(folder)
        if new_path == get_data_dir():
            return
        ans = QMessageBox.question(
            self, "Change Database Path",
            f"Change the database folder to:\n{folder}\n\n"
            "The app will use this location the next time it starts.\n"
            "Your existing data will NOT be moved automatically.\n\nContinue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
            QMessageBox.StandardButton.Cancel,
        )
        if ans == QMessageBox.StandardButton.Yes:
            set_data_dir(new_path)
            QMessageBox.information(
                self, "Database Path Changed",
                f"Database folder set to:\n{folder}\n\nRestart the app for the change to take effect.",
            )

    def _open_sql_publisher(self):
        from ui.dev_tools_dialog import DevToolsDialog
        dlg = DevToolsDialog(current_records=self._records, parent=self)
        dlg.exec()

    def _open_purchase_list_builder(self, initial_code=None):
        self._set_purchase_list_panel_visible(True)
        if initial_code:
            self._purchase_list_panel.add_code(initial_code)

    @staticmethod
    def _checkbox_item(checked=False):
        item = QTableWidgetItem("")
        item.setFlags(
            Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable
        )
        item.setCheckState(Qt.CheckState.Checked if checked else Qt.CheckState.Unchecked)
        return item

    def _select_all_main_rows(self):
        for row in range(self.table.rowCount()):
            item = self.table.item(row, MAIN_COL_SELECT)
            if item:
                item.setCheckState(Qt.CheckState.Checked)

    def _deselect_all_main_rows(self):
        for row in range(self.table.rowCount()):
            item = self.table.item(row, MAIN_COL_SELECT)
            if item:
                item.setCheckState(Qt.CheckState.Unchecked)

    def _add_checked_from_main(self):
        added = 0
        for row in range(self.table.rowCount()):
            check_item = self.table.item(row, MAIN_COL_SELECT)
            code_item = self.table.item(row, MAIN_COL_HCPCS)
            desc_item = self.table.item(row, MAIN_COL_DESC)
            if not check_item or check_item.checkState() != Qt.CheckState.Checked or not code_item:
                continue
            self._purchase_list_panel.add_code(code_item.text(), desc_item.text() if desc_item else "")
            check_item.setCheckState(Qt.CheckState.Unchecked)
            added += 1
        if added:
            self._set_purchase_list_panel_visible(True)
            self._set_status(f"Added {added} item(s) to purchase list.")
        else:
            QMessageBox.information(
                self,
                "No Selection",
                "Select at least one item using the checkboxes before adding.",
            )

    def _remove_checked_from_purchase(self):
        removed = self._purchase_list_panel.remove_checked_items()
        if removed:
            self._set_status(f"Removed {removed} item(s) from purchase list.")
        else:
            QMessageBox.information(
                self,
                "No Selection",
                "Select at least one item using the checkboxes before removing.",
            )

    def _toggle_purchase_list_panel(self, checked):
        self._set_purchase_list_panel_visible(bool(checked))

    def _set_purchase_list_panel_visible(self, visible):
        if visible:
            self._purchase_list_panel.show()
            left = max(1, int(self.splitter.width() * PURCHASE_PANEL_LEFT_RATIO))
            right = max(320, self.splitter.width() - left - 120)
            self.splitter.setSizes([left, 120, right])
            self._purchase_list_panel.refresh_prices()
        else:
            # Completely hide the purchase list and middle controls
            self._purchase_list_panel.hide()
            self.splitter.setSizes([self.splitter.width(), 0, 0])
        if getattr(self, "_add_btn", None):
            self._add_btn.setVisible(visible)
        if getattr(self, "_remove_btn", None):
            self._remove_btn.setVisible(visible)
        self._purchase_list_panel_visible = visible
        if getattr(self, "_purchase_btn", None):
            self._purchase_btn.blockSignals(True)
            self._purchase_btn.setChecked(visible)
            self._purchase_btn.setStyleSheet(
                "background-color: #005A9C; color: white; padding: 4px 12px; font-weight: 600; height: 24px;"
                if not visible
                else "background-color: #003366; color: white; padding: 4px 12px; font-weight: 600; height: 24px; border: 1px solid #002244;"
            )
            self._purchase_btn.blockSignals(False)
        if getattr(self, "_purchase_list_action", None):
            self._purchase_list_action.blockSignals(True)
            self._purchase_list_action.setChecked(visible)
            self._purchase_list_action.blockSignals(False)

    def _toggle_dark_mode(self, enabled):
        self._apply_theme(bool(enabled))
        set_config_value("dark_mode_enabled", bool(enabled))

    def _apply_theme(self, dark_enabled):
        app = QApplication.instance()
        if app is not None:
            app.setStyleSheet(self._dark_theme_qss if dark_enabled else self._light_theme_qss)
        if getattr(self, "_dark_mode_action", None):
            self._dark_mode_action.blockSignals(True)
            self._dark_mode_action.setChecked(bool(dark_enabled))
            self._dark_mode_action.blockSignals(False)

    def _main_table_layout_key(self):
        return "main_table_layout_v1"

    def _restore_main_table_layout_preferences(self):
        state = get_config_value(self._main_table_layout_key(), "")
        if not state:
            self.table.setColumnWidth(MAIN_COL_SELECT, 36)
            self.table.setColumnWidth(MAIN_COL_HCPCS, 110)
            self.table.setColumnWidth(MAIN_COL_DESC, 420)
            self.table.setColumnWidth(MAIN_COL_STATE, 90)
            self.table.setColumnWidth(MAIN_COL_YEAR, 80)
            self.table.setColumnWidth(MAIN_COL_ALLOWABLE, 120)
            self.table.setColumnWidth(MAIN_COL_MODIFIER, 100)
            self.table.setColumnWidth(MAIN_COL_SOURCE, 120)
            return
        try:
            import base64
            from PyQt6.QtCore import QByteArray
            raw = base64.b64decode(state.encode("ascii"))
            self.table.horizontalHeader().restoreState(QByteArray(raw))
        except Exception:
            pass

    def _save_main_table_layout_preferences(self, *_args):
        try:
            import base64
            encoded = base64.b64encode(bytes(self.table.horizontalHeader().saveState())).decode("ascii")
            set_config_value(self._main_table_layout_key(), encoded)
        except Exception:
            pass

    def _refresh_purchase_list_prices_if_visible(self, *_args):
        if self._purchase_list_panel_visible:
            self._purchase_list_panel.refresh_prices()

    def _update_purchase_button_label(self, count):
        if getattr(self, "_purchase_btn", None):
            self._purchase_btn.setText(f"Purchase List ({count})")

    def _create_backup(self):
        from core.backup import create_backup
        default_name = f"hcpcs_backup_{date.today().isoformat()}.zip"
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Create Backup",
            default_name,
            "Backup ZIP Files (*.zip)",
        )
        if not path:
            return
        try:
            backup_path = create_backup(path)
            p = Path(backup_path)
            size_kb = p.stat().st_size / 1024
            QMessageBox.information(
                self,
                "Backup Created",
                f"Backup created successfully.\n\nLocation:\n{backup_path}\nSize: {size_kb:,.1f} KB",
            )
        except Exception as exc:
            QMessageBox.critical(self, "Backup Error", f"Failed to create backup:\n{exc}")

    def _restore_backup(self):
        from core.backup import inspect_backup, restore_backup
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Restore from Backup",
            "",
            "Backup ZIP Files (*.zip)",
        )
        if not path:
            return
        try:
            manifest = inspect_backup(path)
            counts = manifest.get("record_counts", {})
            preview = (
                f"Backup date: {manifest.get('backup_date', 'Unknown')}\n"
                f"App version: {manifest.get('app_version', 'Unknown')}\n"
                f"Fee records: {counts.get('hcpcs_fees', 0):,}\n"
                f"Bundles: {counts.get('purchase_list_bundles', 0):,}\n"
                f"Selected states: {counts.get('selected_states', 0):,}\n"
                f"Import log entries: {counts.get('import_log', 0):,}\n\n"
                "Warning: restoring will overwrite your current app data."
            )
            ans = QMessageBox.question(
                self,
                "Confirm Restore",
                preview,
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
                QMessageBox.StandardButton.Cancel,
            )
            if ans != QMessageBox.StandardButton.Yes:
                return
            restore_backup(path)
            QMessageBox.information(
                self,
                "Restore Complete",
                "Backup restored successfully.\nPlease restart the app for changes to take effect.",
            )
        except Exception as exc:
            QMessageBox.critical(self, "Restore Error", f"Failed to restore backup:\n{exc}")

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
        dlg = _AboutDialog(self)
        dlg.exec()

    def _show_feature_guide(self):
        dlg = QDialog(self)
        dlg.setWindowTitle("VA HCPCS Fee Schedule Manager - User Guide")
        dlg.resize(900, 700)
        layout = QVBoxLayout(dlg)
        layout.setContentsMargins(0, 0, 0, 10)

        body = QTextEdit()
        body.setReadOnly(True)
        body.setHtml("""
            <style>
                h1 { color: #003366; margin-top: 10px; margin-bottom: 10px; }
                h2 { color: #005A9C; margin-top: 15px; margin-bottom: 8px; font-size: 16px; }
                h3 { color: #333; margin-top: 10px; margin-bottom: 5px; font-size: 14px; }
                p { margin: 5px 0; }
                ul { margin: 5px 0 10px 20px; }
                li { margin: 3px 0; }
                .section { margin-bottom: 15px; }
                .tip { background-color: #EEF2F7; padding: 8px; border-left: 3px solid #005A9C; margin: 10px 0; }
                .shortcut { font-family: monospace; background-color: #E8F0F8; padding: 2px 6px; border-radius: 3px; }
            </style>

            <h1>VA HCPCS Fee Schedule Manager - Comprehensive User Guide</h1>

            <div class="section">
                <h2>1. Getting Started</h2>
                <h3>First-Time Setup</h3>
                <p>When you first launch the app, a Setup Wizard will guide you through:</p>
                <ul>
                    <li><b>State Selection:</b> Choose which states you need to track (e.g., CA, TX, FL)</li>
                    <li><b>Data Sync:</b> Option to download the latest CMS DMEPOS fee schedules</li>
                </ul>

                <h3>Application Layout</h3>
                <ul>
                    <li><b>Top Toolbar (Row 1):</b> Sync from CMS, Year filter, State filter, ZIP code entry</li>
                    <li><b>Top Toolbar (Row 2):</b> HCPCS Group filter, HCPCS code search, Keyword search, Export button, Purchase List button</li>
                    <li><b>Main Table:</b> Displays fee schedule records with columns for HCPCS code, description, state, year, allowable amount, modifier, and source</li>
                    <li><b>Status Bar:</b> Shows record counts and operation status</li>
                </ul>
            </div>

            <div class="section">
                <h2>2. Searching and Filtering Data</h2>

                <h3>Year Filter</h3>
                <p>Select a specific year or "All Years" to view data. The app shows the most current year by default.</p>

                <h3>State Filter</h3>
                <p>Filter by state abbreviation (e.g., CA, TX). Only states you've selected in Settings → Manage States will appear.</p>

                <h3>ZIP Code Lookup</h3>
                <p>Enter a 5-digit ZIP code to automatically determine rural (R) or non-rural (NR) allowable amounts:</p>
                <ul>
                    <li>The app uses CMS's rural ZIP designation files</li>
                    <li>Rural status is year-specific and updates based on selected year</li>
                    <li>Displayed allowable amounts automatically reflect the appropriate rural/non-rural value</li>
                </ul>
                <div class="tip">
                    <b>Tip:</b> This mimics the PDAC fee lookup behavior - just enter the ZIP and see the correct allowable amount.
                </div>

                <h3>HCPCS Group Filter</h3>
                <p>Filter by equipment category (e.g., "A4 - Surgical Supplies", "E0 - Durable Medical Equipment").
                Only groups with data in your database will appear.</p>

                <h3>HCPCS Code Search</h3>
                <p>Type a partial or complete HCPCS code (e.g., "E0601" or "E06") to find specific items.
                Search is debounced for smooth typing.</p>

                <h3>Keyword Search</h3>
                <p>Search by description keywords (e.g., "wheelchair", "oxygen", "prosthetic"). Searches are case-insensitive.</p>

                <h3>Clearing Filters</h3>
                <p>Click the <b>Clear</b> button to reset all filters to defaults.</p>
            </div>

            <div class="section">
                <h2>3. Viewing Historical Data</h2>
                <p>Click any HCPCS code (blue hyperlink) in the main table to open the History dialog showing:</p>
                <ul>
                    <li>All years of data for that code and state</li>
                    <li>Non-rural (NR) and Rural (R) allowable amounts</li>
                    <li>Effective allowable based on your entered ZIP code</li>
                    <li>HCPCS group classification</li>
                    <li>Option to compare with another state side-by-side</li>
                </ul>
            </div>

            <div class="section">
                <h2>4. Purchase List Feature</h2>
                <p>The Purchase List lets you build a shopping cart of HCPCS codes for procurement or documentation.</p>

                <h3>Opening the Purchase List</h3>
                <ul>
                    <li>Click the <b>Purchase List (0)</b> button in the toolbar</li>
                    <li>Press <span class="shortcut">Ctrl+P</span> keyboard shortcut</li>
                    <li>Select View → Purchase List from the menu</li>
                </ul>

                <h3>Adding Items</h3>
                <ul>
                    <li><b>From Main Table:</b> Check the boxes next to items, then click the <b>►</b> button or press <span class="shortcut">Ctrl+Right</span></li>
                    <li><b>Quick Add:</b> Type an HCPCS code directly in the Purchase List's input field and press Enter</li>
                    <li><b>Right-Click Menu:</b> Right-click any row in the main table and select "Add to Purchase List"</li>
                </ul>

                <h3>Removing Items</h3>
                <ul>
                    <li>Check items in the Purchase List panel</li>
                    <li>Click the <b>◄</b> button or press <span class="shortcut">Ctrl+Left</span></li>
                </ul>

                <h3>Saving & Loading Bundles</h3>
                <ul>
                    <li><b>Save Bundle:</b> Save your current purchase list with a name for reuse</li>
                    <li><b>Load Bundle:</b> Restore a previously saved bundle. Hover over bundles to see preview</li>
                    <li><b>Categories:</b> Organize bundles into categories (right-click to manage)</li>
                    <li><b>Rename/Delete:</b> Right-click bundles or categories to rename or delete</li>
                </ul>

                <h3>Generating Documents</h3>
                <p>Click <b>Generate Document</b> to create a procurement worksheet:</p>
                <ul>
                    <li>Opens a form showing all items with quantities and prices</li>
                    <li>Add vendor information and contact details</li>
                    <li>Copy data to clipboard for pasting into emails or forms</li>
                    <li>Generate and attach to email directly from the app</li>
                    <li>Export to Word document (.docx)</li>
                </ul>

                <h3>Exporting Purchase Lists</h3>
                <ul>
                    <li><b>CSV:</b> For Excel or database import</li>
                    <li><b>Excel:</b> Formatted .xlsx file with proper columns</li>
                    <li><b>PDF:</b> Professional printable document</li>
                </ul>
                <p>Press <span class="shortcut">Ctrl+Shift+C</span> to copy the purchase list table to clipboard.</p>
            </div>

            <div class="section">
                <h2>5. Syncing CMS Data</h2>
                <p>Keep your fee schedules up to date by syncing from CMS.gov:</p>

                <h3>Manual Sync</h3>
                <ol>
                    <li>Click <b>⚌ Sync from CMS</b> button in the toolbar</li>
                    <li>Review the year selection (current year + previous 2 years by default)</li>
                    <li>Click <b>Sync</b> to download</li>
                    <li>Progress dialog shows download and import status</li>
                </ol>

                <h3>Automatic Notifications</h3>
                <p>On startup, the app checks if newer CMS files are available and prompts you to sync (shown once per session).</p>

                <div class="tip">
                    <b>Note:</b> CMS typically publishes quarterly updates. Sync regularly to ensure accurate allowable amounts.
                </div>
            </div>

            <div class="section">
                <h2>6. Importing Custom Data</h2>
                <p>Import fee schedules from CSV files (File → Import CSV):</p>
                <ul>
                    <li>Supports CMS format and custom CSV formats</li>
                    <li>Auto-detects columns for HCPCS, description, state, year, and allowables</li>
                    <li>Can import rural ZIP designation files</li>
                    <li>View import history in File → View Import Log</li>
                </ul>
            </div>

            <div class="section">
                <h2>7. Exporting Data</h2>
                <p>Export current search results (must have data visible first):</p>
                <ul>
                    <li><b>CSV:</b> For Excel or database import</li>
                    <li><b>Excel (.xlsx):</b> Formatted spreadsheet with proper columns</li>
                    <li><b>PDF:</b> Professional printable report with metadata</li>
                </ul>
                <p>Exports include your current ZIP code and rural status if specified.</p>
            </div>

            <div class="section">
                <h2>8. Backup & Restore</h2>

                <h3>Creating Backups</h3>
                <p>File → Create Backup creates a .zip file containing:</p>
                <ul>
                    <li>All fee schedule records</li>
                    <li>Purchase list bundles and categories</li>
                    <li>Selected states and preferences</li>
                    <li>Import history</li>
                </ul>

                <h3>Restoring Backups</h3>
                <p>File → Restore from Backup:</p>
                <ol>
                    <li>Select a .zip backup file</li>
                    <li>Preview shows what will be restored</li>
                    <li>Confirm to overwrite current data</li>
                    <li>Restart the app after restore</li>
                </ol>
            </div>

            <div class="section">
                <h2>9. Settings & Configuration</h2>

                <h3>Manage States</h3>
                <p>Settings → Manage States:</p>
                <ul>
                    <li>Check states you need to track</li>
                    <li>Only selected states appear in filters and sync operations</li>
                    <li>Changes refresh immediately</li>
                </ul>

                <h3>Database Location</h3>
                <p>Settings → Change Database Path to store data in a different location (e.g., network drive, different folder).</p>

                <h3>Dark Mode</h3>
                <p>View → Dark Mode switches between light and dark themes. Preference is saved automatically.</p>

                <h3>Desktop Shortcut</h3>
                <p>Settings → Create Desktop Shortcut (Windows .exe only) adds a shortcut to your desktop.</p>
            </div>

            <div class="section">
                <h2>10. Developer Tools</h2>
                <p>Developer Tools → SQL Publisher allows publishing data to SQL Server or Databricks databases for enterprise integration.</p>
            </div>

            <div class="section">
                <h2>11. Keyboard Shortcuts</h2>
                <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse; width: 100%;">
                    <tr style="background-color: #EEF2F7;">
                        <th>Shortcut</th>
                        <th>Action</th>
                    </tr>
                    <tr>
                        <td><span class="shortcut">Ctrl+I</span></td>
                        <td>Import CSV</td>
                    </tr>
                    <tr>
                        <td><span class="shortcut">Ctrl+Q</span></td>
                        <td>Exit application</td>
                    </tr>
                    <tr>
                        <td><span class="shortcut">Ctrl+P</span></td>
                        <td>Toggle Purchase List panel</td>
                    </tr>
                    <tr>
                        <td><span class="shortcut">Ctrl+Right</span></td>
                        <td>Add checked items to Purchase List</td>
                    </tr>
                    <tr>
                        <td><span class="shortcut">Ctrl+Left</span></td>
                        <td>Remove checked items from Purchase List</td>
                    </tr>
                    <tr>
                        <td><span class="shortcut">Ctrl+Shift+C</span></td>
                        <td>Copy Purchase List table to clipboard</td>
                    </tr>
                    <tr>
                        <td><span class="shortcut">Enter</span></td>
                        <td>Execute search / Submit forms</td>
                    </tr>
                </table>
            </div>

            <div class="section">
                <h2>12. Tips & Best Practices</h2>
                <ul>
                    <li><b>Regular Syncs:</b> Check for CMS updates quarterly (January, April, July, October)</li>
                    <li><b>Use ZIP Codes:</b> Always enter a ZIP when looking up allowables for accurate rural/non-rural pricing</li>
                    <li><b>Save Bundles:</b> Create bundles for frequently ordered equipment combinations</li>
                    <li><b>Browse Groups:</b> View → Browse HCPCS Groups to explore available equipment categories</li>
                    <li><b>Right-Click Menus:</b> Right-click rows for quick actions like copying data or adding to Purchase List</li>
                    <li><b>Create Backups:</b> Back up before major changes or before upgrading the app</li>
                    <li><b>Column Customization:</b> Drag column headers to reorder, resize columns by dragging edges</li>
                    <li><b>Sorting:</b> Click column headers to sort data ascending/descending</li>
                </ul>
            </div>

            <div class="section">
                <h2>13. Troubleshooting</h2>

                <h3>No Data Appears</h3>
                <ul>
                    <li>Check if you've selected states in Settings → Manage States</li>
                    <li>Run Sync from CMS to download fee schedules</li>
                    <li>Verify year filter includes data years</li>
                </ul>

                <h3>Sync Fails</h3>
                <ul>
                    <li>Check internet connection</li>
                    <li>CMS website may be temporarily unavailable - try again later</li>
                    <li>Check File → View Import Log for error details</li>
                </ul>

                <h3>App Updates</h3>
                <p>When updates are available, a notification bar appears at the top with download link.
                Click "Update Now" for automatic installation (when running as .exe).</p>
            </div>

            <div class="section" style="margin-top: 20px; padding: 10px; background-color: #F5F6F8; border-radius: 6px;">
                <p><b>Data Source:</b> CMS DMEPOS Fee Schedule (<a href="https://www.cms.gov/medicare/payment/fee-schedules/dmepos">cms.gov</a>)</p>
                <p><b>Developed by:</b> WSNC Impact Team</p>
                <p><b>Support:</b> For questions or issues, contact your VA IT support team.</p>
            </div>
        """)
        layout.addWidget(body)

        # Button row
        btn_row = QHBoxLayout()
        btn_row.setContentsMargins(10, 0, 10, 0)
        btn_row.addStretch()
        close_btn = QPushButton("Close")
        close_btn.setDefault(True)
        close_btn.setMinimumWidth(100)
        close_btn.clicked.connect(dlg.accept)
        btn_row.addWidget(close_btn)
        layout.addLayout(btn_row)

        dlg.exec()

    def _start_update_check(self):
        """Start a background thread to check for app updates."""
        from core.update_checker import UpdateCheckWorker
        self._update_worker = UpdateCheckWorker()
        self._update_worker.update_available.connect(self._on_update_available)
        self._update_worker.finished.connect(self._update_worker.deleteLater)
        self._update_worker.start()

    def _on_update_available(self, version, url):
        """Show the update notification bar when a new version is found."""
        # Validate URL is a GitHub URL to prevent injection from unexpected API responses
        if not url.startswith("https://github.com/"):
            from core.version import RELEASES_URL
            url = RELEASES_URL
        self._update_pending_version = version
        self._update_pending_url = url
        self.update_bar.setText(
            f'🔔 <b>Update available!</b> Version {version} is ready. '
            f'<a href="{url}" style="color: #0D6EFD;">Download now</a>'
        )
        # Show the "Update Now" button only when running as a frozen exe
        self._update_now_btn.setVisible(getattr(sys, "frozen", False))
        self._update_bar_widget.show()

    def _on_update_now(self):
        """Download and apply the update in-place, or fall back to browser."""
        import webbrowser
        url = getattr(self, "_update_pending_url", None)
        version = getattr(self, "_update_pending_version", "?")

        # Only attempt self-update when running as a frozen exe
        if not getattr(sys, "frozen", False):
            if url:
                webbrowser.open(url)
            return

        from core.version import get_latest_release_asset_url

        asset_url = get_latest_release_asset_url("HCPCSFeeApp.exe")
        if not asset_url:
            # No direct asset URL — open the releases page instead
            if url:
                webbrowser.open(url)
            return

        # Show an indeterminate progress dialog while downloading
        dlg = QProgressDialog(
            f"Downloading version {version}…", "Cancel", 0, 0, self
        )
        dlg.setWindowTitle("Updating…")
        dlg.setWindowModality(Qt.WindowModality.ApplicationModal)
        dlg.setMinimumDuration(0)
        dlg.setValue(0)

        cancelled = False

        def _on_cancel():
            nonlocal cancelled
            cancelled = True

        dlg.canceled.connect(_on_cancel)

        def _progress(downloaded, total):
            if cancelled:
                return
            if total and total > 0:
                dlg.setMaximum(total)
                dlg.setValue(downloaded)
            QApplication.processEvents()

        try:
            from core.self_updater import download_update, apply_update
            new_exe = download_update(asset_url, progress_callback=_progress)
            dlg.close()

            if cancelled:
                try:
                    new_exe.unlink()
                except Exception:
                    pass
                return

            reply = QMessageBox.question(
                self,
                "Apply Update",
                f"Version {version} has been downloaded.\n\n"
                "The app will restart to complete the update.\n\nContinue?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            )
            if reply == QMessageBox.StandardButton.Yes:
                apply_update(new_exe)  # does not return — calls sys.exit(0)
            else:
                # User declined — clean up the downloaded file
                try:
                    new_exe.unlink()
                except Exception:
                    pass

        except Exception as exc:
            dlg.close()
            QMessageBox.warning(
                self,
                "Update Failed",
                f"Automatic update failed:\n{exc}\n\n"
                "Please download the update manually.",
            )
            if url:
                webbrowser.open(url)

    def _warn_if_pending_update_file(self):
        if not getattr(sys, "frozen", False):
            return
        pending = Path(sys.executable).parent / "HCPCSFeeApp_new.exe"
        if not pending.exists():
            return
        from core.self_updater import UPDATE_LOG_FILENAME
        log_path = Path(tempfile.gettempdir()) / UPDATE_LOG_FILENAME
        QMessageBox.warning(
            self,
            "Incomplete Update Detected",
            "A previous update did not fully apply.\n\n"
            "Please close the app and rename:\n"
            "HCPCSFeeApp_new.exe -> HCPCSFeeApp.exe\n"
            "in the application folder.\n\n"
            f"Update log: {log_path}",
        )

    def _set_status(self, msg):
        self.status_bar.showMessage(msg)


# ----------------------------------------------------------------- Helpers --

class _AboutDialog(QDialog):
    """Rich About dialog displaying the WSNC map banner and app details."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("About VA HCPCS Fee Schedule Manager")
        self.setFixedWidth(620)
        layout = QVBoxLayout(self)
        layout.setSpacing(10)
        layout.setContentsMargins(0, 0, 0, 16)

        # ---- WSNC map banner ------------------------------------------------
        map_path = _asset("wsnc_map.png")
        if map_path.exists():
            banner_label = QLabel()
            pix = QPixmap(str(map_path)).scaledToWidth(
                620, Qt.TransformationMode.SmoothTransformation
            )
            banner_label.setPixmap(pix)
            banner_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            layout.addWidget(banner_label)
        else:
            title_lbl = QLabel("WSNC Impact Team")
            title_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
            title_lbl.setStyleSheet(
                "background:#003366; color:white; font-size:14px;"
                "font-weight:bold; padding:16px;"
            )
            layout.addWidget(title_lbl)

        # ---- App icon + title row -------------------------------------------
        header = QHBoxLayout()
        header.setContentsMargins(16, 4, 16, 0)
        icon_path = _asset("wsnc_map.png")
        if icon_path.exists():
            icon_lbl = QLabel()
            icon_lbl.setPixmap(
                QPixmap(str(icon_path)).scaled(
                    48, 48,
                    Qt.AspectRatioMode.KeepAspectRatio,
                    Qt.TransformationMode.SmoothTransformation,
                )
            )
            header.addWidget(icon_lbl)
        from core.version import APP_VERSION
        app_title = QLabel(f"<b>VA HCPCS Fee Schedule Manager</b> v{APP_VERSION}")
        app_title.setStyleSheet("font-size:14px; color:#003366;")
        header.addWidget(app_title, 1)
        layout.addLayout(header)

        # ---- Description text -----------------------------------------------
        body = QLabel(
            "A standalone Windows desktop application for VA staff to manage,<br>"
            "view, filter, and export CMS DMEPOS HCPCS fee schedule data.<br><br>"
            "<b>Tip:</b> Enter a ZIP code in the toolbar to automatically display<br>"
            "rural (R) or non-rural (NR) allowable amounts, similar to PDAC fee lookup.<br><br>"
            "Data source: <a href='https://www.cms.gov/medicare/payment/fee-schedules/dmepos'>"
            "CMS DMEPOS Fee Schedule</a><br><br>"
            "Developed by the <b>WSNC Impact Team</b>"
        )
        body.setContentsMargins(16, 0, 16, 0)
        body.setWordWrap(True)
        body.setOpenExternalLinks(True)
        body.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextBrowserInteraction
        )
        layout.addWidget(body)

        # ---- Close button ---------------------------------------------------
        btn_row = QHBoxLayout()
        btn_row.setContentsMargins(16, 0, 16, 0)
        btn_row.addStretch()
        close_btn = QPushButton("Close")
        close_btn.setDefault(True)
        close_btn.clicked.connect(self.accept)
        btn_row.addWidget(close_btn)
        layout.addLayout(btn_row)


class _SyncYearsDialog(QDialog):
    """Simple dialog to confirm which year(s) to sync (auto-determined)."""

    def __init__(self, state_abbrs, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Sync from CMS")
        self.setMinimumWidth(340)
        self._auto_years = get_auto_selected_years()
        layout = QVBoxLayout(self)

        states_label = QLabel(
            f"Syncing data for: <b>{', '.join(sorted(state_abbrs))}</b>"
        )
        states_label.setWordWrap(True)
        layout.addWidget(states_label)

        from core.database import get_current_quarter
        current_quarter = get_current_quarter()
        current_year = self._auto_years[0]

        info_text = (
            f"<p>The app will automatically sync:</p>"
            f"<ul>"
            f"<li><b>{current_year} Q{current_quarter}</b> (current year, most recent quarter)</li>"
            f"<li><b>{current_year - 1} Q4</b> (last full year)</li>"
            f"<li><b>{current_year - 2} Q4</b> (2 years ago)</li>"
            f"</ul>"
        )
        info_label = QLabel(info_text)
        info_label.setWordWrap(True)
        layout.addWidget(info_label)

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
        return self._auto_years


class _HcpcsHistoryDialog(QDialog):
    """Modernized history dialog showing NR/R/Effective across years for a HCPCS code.

    Features:
    - Summary card header (HCPCS, State, Description, ZIP / rural status)
    - Primary state table: Year, NR ($), R ($), Effective ($), Modifier, Source
    - Optional comparison state table (no Effective column; ZIP not applicable cross-state)
    """

    def __init__(self, record, parent=None):
        super().__init__(parent)
        hcpcs = record.get("hcpcs_code", "")
        state = record.get("state_abbr", "")
        modifier = record.get("modifier") or ""
        desc = record.get("description", "")

        # Grab ZIP from parent MainWindow (if available) for Effective computation
        self._zip5 = ""
        if hasattr(parent, "zip_edit"):
            self._zip5 = parent.zip_edit.text().strip()

        self.setWindowTitle(f"History: {hcpcs} — {state}")
        self.setMinimumSize(860, 520)

        layout = QVBoxLayout(self)
        layout.setSpacing(8)

        # ---- Summary card ----
        card = QFrame()
        card.setFrameShape(QFrame.Shape.StyledPanel)
        card.setStyleSheet(
            "QFrame { background: #eef2f7; border: 1px solid #c0c8d8; border-radius: 6px; }"
        )
        card_layout = QGridLayout(card)
        card_layout.setContentsMargins(12, 8, 12, 8)
        card_layout.setHorizontalSpacing(16)

        def _bold(text):
            lbl = QLabel(f"<b>{text}</b>")
            return lbl

        def _val(text):
            lbl = QLabel(str(text) if text else "—")
            lbl.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
            return lbl

        card_layout.addWidget(_bold("HCPCS:"), 0, 0)
        card_layout.addWidget(_val(hcpcs), 0, 1)
        card_layout.addWidget(_bold("State:"), 0, 2)
        card_layout.addWidget(_val(state), 0, 3)
        if modifier:
            card_layout.addWidget(_bold("Modifier:"), 0, 4)
            card_layout.addWidget(_val(modifier), 0, 5)

        desc_label = QLabel(str(desc) if desc else "—")
        desc_label.setWordWrap(True)
        desc_label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        card_layout.addWidget(_bold("Description:"), 1, 0)
        card_layout.addWidget(desc_label, 1, 1, 1, 5)

        # Group info
        from core.hcpcs_groups import get_group_for_code
        group_info = get_group_for_code(hcpcs)
        if group_info:
            prefix, short_name, _group_desc = group_info
            card_layout.addWidget(_bold("Group:"), 2, 0)
            card_layout.addWidget(_val(f"{prefix} — {short_name}"), 2, 1, 1, 5)

        # ZIP / rural status
        zip_text = self._zip5 if self._zip5 else "—"
        card_layout.addWidget(_bold("ZIP:"), 3, 0)
        card_layout.addWidget(_val(zip_text), 3, 1)

        if self._zip5 and len(self._zip5) == 5 and self._zip5.isdigit():
            # Show rural status for the most recent year with data (best proxy)
            from core.database import get_available_years
            avail = get_available_years()
            rural_note = "—"
            if avail:
                rural = is_rural_zip(avail[0], self._zip5)
                rural_note = f"{'Rural (R)' if rural else 'Non-Rural (NR)'} (as of {avail[0]})"
            card_layout.addWidget(_bold("Rural Status:"), 3, 2)
            card_layout.addWidget(_val(rural_note), 3, 3, 1, 3)
        else:
            card_layout.addWidget(_bold("Rural Status:"), 3, 2)
            card_layout.addWidget(_val("No ZIP — defaulting to NR"), 3, 3, 1, 3)

        layout.addWidget(card)

        # ---- Fetch all historical records for this HCPCS/state ----
        hist = get_fees(state_abbr=state, hcpcs_code=hcpcs)
        if modifier:
            hist = [r for r in hist if (r.get("modifier") or "") == modifier]
        hist.sort(key=lambda r: r.get("year", 0), reverse=True)

        # ---- Primary state section label ----
        prim_label = QLabel(f"<b>State: {state}</b>")
        prim_label.setStyleSheet("font-size: 13px; margin-top: 4px;")
        layout.addWidget(prim_label)

        # ---- Primary table ----
        primary_table = self._build_primary_table(hist)
        layout.addWidget(primary_table)

        # ---- Comparison state section ----
        comp_row = QHBoxLayout()
        comp_row.addWidget(QLabel("Compare to state:"))
        self._comp_combo = QComboBox()
        self._comp_combo.setMinimumWidth(160)
        self._comp_combo.addItem("— None —", None)
        # Populate with all available states that have data for this HCPCS
        from core.database import get_selected_states
        for abbr, sname in get_selected_states():
            if abbr != state:
                self._comp_combo.addItem(f"{sname} ({abbr})", abbr)
        comp_row.addWidget(self._comp_combo)
        comp_row.addWidget(
            QLabel("<small><i>Effective ($) not shown for comparison state "
                   "(ZIP rural applies to primary state only)</i></small>")
        )
        comp_row.addStretch()
        layout.addLayout(comp_row)

        # Container for the comparison section (label + table)
        self._comp_container = QWidget()
        self._comp_layout = QVBoxLayout(self._comp_container)
        self._comp_layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self._comp_container)

        self._hcpcs = hcpcs
        self._modifier = modifier
        self._comp_combo.currentIndexChanged.connect(self._update_comparison)

        # ---- Close button ----
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        layout.addWidget(close_btn, alignment=Qt.AlignmentFlag.AlignRight)

    # ---- Helpers ----

    @staticmethod
    def _fmt(val):
        """Format a dollar amount or return '—' if None."""
        if val is None:
            return "—"
        try:
            return f"{float(val):,.2f}"
        except (TypeError, ValueError):
            return "—"

    def _build_primary_table(self, hist):
        """Build the primary state table with Year, NR, R, Effective, Modifier, Source."""
        columns = ["Year", "NR ($)", "R ($)", "Effective ($)", "Modifier", "Source"]
        table = QTableWidget(len(hist), len(columns))
        table.setHorizontalHeaderLabels(columns)
        table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        table.setAlternatingRowColors(True)
        table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        table.horizontalHeader().setDefaultSectionSize(110)
        table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeMode.Stretch)
        table.setSortingEnabled(False)

        right_align = Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter

        for i, r in enumerate(hist):
            year = r.get("year", 0)
            nr = r.get("allowable_nr") or r.get("allowable")
            rv = r.get("allowable_r")
            # Effective: prefer R if rural for that year, else NR; fallback NR
            rural = (
                is_rural_zip(year, self._zip5)
                if self._zip5 and len(self._zip5) == 5 and self._zip5.isdigit()
                else False
            )
            if rural and rv is not None:
                effective = rv
            else:
                effective = nr

            row_data = [
                (str(year), Qt.AlignmentFlag.AlignCenter),
                (self._fmt(nr), right_align),
                (self._fmt(rv), right_align),
                (self._fmt(effective), right_align),
                (r.get("modifier") or "—", Qt.AlignmentFlag.AlignCenter),
                (r.get("data_source") or "—", Qt.AlignmentFlag.AlignLeft),
            ]
            for col_i, (val, align) in enumerate(row_data):
                item = QTableWidgetItem(val)
                item.setTextAlignment(align)
                table.setItem(i, col_i, item)

        return table

    def _build_comparison_table(self, hist_comp, comp_state):
        """Build the comparison state table: Year, NR ($), R ($), Modifier, Source."""
        columns = ["Year", "NR ($)", "R ($)", "Modifier", "Source"]
        table = QTableWidget(len(hist_comp), len(columns))
        table.setHorizontalHeaderLabels(columns)
        table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        table.setAlternatingRowColors(True)
        table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        table.horizontalHeader().setDefaultSectionSize(110)
        table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeMode.Stretch)
        table.setSortingEnabled(False)

        right_align = Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter

        for i, r in enumerate(hist_comp):
            year = r.get("year", 0)
            nr = r.get("allowable_nr") or r.get("allowable")
            rv = r.get("allowable_r")
            row_data = [
                (str(year), Qt.AlignmentFlag.AlignCenter),
                (self._fmt(nr), right_align),
                (self._fmt(rv), right_align),
                (r.get("modifier") or "—", Qt.AlignmentFlag.AlignCenter),
                (r.get("data_source") or "—", Qt.AlignmentFlag.AlignLeft),
            ]
            for col_i, (val, align) in enumerate(row_data):
                item = QTableWidgetItem(val)
                item.setTextAlignment(align)
                table.setItem(i, col_i, item)
        return table

    def _update_comparison(self):
        """Rebuild the comparison section when the combo selection changes."""
        # Clear previous comparison widgets
        while self._comp_layout.count():
            item = self._comp_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        comp_state = self._comp_combo.currentData()
        if not comp_state:
            return

        hist_comp = get_fees(state_abbr=comp_state, hcpcs_code=self._hcpcs)
        if self._modifier:
            hist_comp = [r for r in hist_comp if (r.get("modifier") or "") == self._modifier]
        hist_comp.sort(key=lambda r: r.get("year", 0), reverse=True)

        section_label = QLabel(f"<b>Comparison — State: {comp_state}</b>")
        section_label.setStyleSheet(
            "font-size: 13px; color: #003366; padding-top: 6px;"
        )
        self._comp_layout.addWidget(section_label)

        if hist_comp:
            table = self._build_comparison_table(hist_comp, comp_state)
            self._comp_layout.addWidget(table)
        else:
            self._comp_layout.addWidget(
                QLabel(f"<i>No data available for {comp_state}.</i>")
            )


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
