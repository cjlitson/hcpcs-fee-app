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
from ui.main_export_dialog import MainExportDialog
from ui.state_selector_dialog import StateSelectorDialog
from ui.purchase_list_panel import PurchaseListPanel

PURCHASE_PANEL_LEFT_RATIO = 2 / 3
PURCHASE_PANEL_MIN_WIDTH_PX = 360
TRANSFER_RAIL_WIDTH_PX = 72
TRANSFER_RAIL_BUTTON_SIZE = QSize(40, 34)
PREFERRED_RESULTS_PANEL_MIN_WIDTH_PX = 420
RESULTS_PANEL_MIN_WIDTH_PX = 300
MAIN_COL_SELECT = 0
MAIN_COL_HCPCS = 1
MAIN_COL_DESC = 2
MAIN_COL_STATE = 3
MAIN_COL_YEAR = 4
MAIN_COL_ALLOWABLE = 5
MAIN_COL_MODIFIER = 6
MAIN_COL_SOURCE = 7
STARTUP_LOG_FILENAME = "HCPCSFeeApp_startup.log"
USER_GUIDE_DARK_LINK_COLOR = "#8CC8FF"
ZIP_EDIT_MAX_WIDTH_PX = 96


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
        self.setMinimumSize(1080, 680)
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
        central.setObjectName("appShell")
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(12, 12, 12, 10)
        root.setSpacing(10)
        self._light_theme_qss = (
            "QWidget { background: #FFFFFF; color: #202124; }"
            "QWidget#appShell { background: #F3F5F8; }"
            "QFrame#updateBannerCard, QFrame#filterCard, QFrame#resultsCard, QFrame#footerStrip, QWidget#purchaseListPanel, QFrame#purchaseHeaderCard, QFrame#purchaseContextCard, QFrame#purchaseSummaryCard { background: #FFFFFF; border: 1px solid #D8DDE6; border-radius: 10px; }"
            "QFrame#updateBannerCard { background: #FFF8E1; border-color: #F2D8A7; }"
            "QFrame#transferRail { background: #F3F6FB; border: 1px solid #D8DDE6; border-radius: 8px; }"
            "QFrame#footerStrip { border-radius: 8px; }"
            "QLineEdit, QComboBox { border: 1px solid #C9CED6; border-radius: 6px; padding: 4px 8px; min-height: 24px; background: #FFFFFF; color: #202124; }"
            "QDateEdit { border: 1px solid #C9CED6; border-radius: 6px; padding: 4px 8px; min-height: 24px; background: #FFFFFF; color: #202124; }"
            "QPushButton { min-height: 28px; border-radius: 6px; padding: 4px 12px; border: 1px solid #AAB2BF; background: #F8F9FB; color: #202124; font-weight: 600; }"
            "QPushButton:hover { background-color: #EAF0F8; }"
            "QPushButton[role='primary'] { background: #0D6EFD; color: #FFFFFF; border: 1px solid #0D6EFD; }"
            "QPushButton[role='primary']:hover { background: #0B5ED7; border-color: #0A58CA; }"
            "QPushButton[role='accent'] { background: #005A9C; color: #FFFFFF; border: 1px solid #004B82; }"
            "QPushButton[role='accent']:hover { background: #004D85; }"
            "QPushButton[role='ghost'] { background: #FFFFFF; color: #2E3A48; border: 1px solid #C9CED6; }"
            "QPushButton[deleteAction='true'] { background: #FFFFFF; color: #B42318; border: 1px solid #F2C7C4; padding: 0; border-radius: 6px; font-size: 13px; font-weight: 700; }"
            "QPushButton[deleteAction='true']:hover { background: #FFF1F0; border-color: #E6938E; color: #8B1108; }"
            "QToolButton { min-height: 28px; border-radius: 6px; padding: 4px 12px; border: 1px solid #AAB2BF; background: #F8F9FB; color: #202124; font-weight: 600; }"
            "QToolButton:hover { background-color: #EAF0F8; }"
            "QToolButton[role='ghost'] { background: #FFFFFF; color: #2E3A48; border: 1px solid #C9CED6; }"
            "QPushButton[role='rail'] { background: #003366; color: #FFFFFF; border: 1px solid #002244; min-width: 40px; min-height: 34px; padding: 0; border-radius: 8px; font-size: 14px; font-weight: 700; }"
            "QPushButton[role='toggle'][active='true'] { background: #003366; color: #FFFFFF; border: 1px solid #002244; }"
            "QPushButton[role='toggle'][active='false'] { background: #005A9C; color: #FFFFFF; border: 1px solid #004B82; }"
            "QLabel#ruralPill { border: 1px solid #C9CED6; border-radius: 11px; background: #F2F4F7; color: #344054; padding: 2px 10px; font-size: 11px; font-weight: 600; }"
            "QLabel#ruralPill[ruralState='rural'] { background: #EAF8EF; border-color: #A8D5B9; color: #146C2E; }"
            "QLabel#ruralPill[ruralState='non_rural'] { background: #EAF2FF; border-color: #B9CCF2; color: #0E4AA6; }"
            "QLabel#ruralPill[ruralState='invalid'] { background: #F5F5F5; border-color: #D0D5DD; color: #667085; }"
            "QLabel[subtle='true'] { color: #667085; font-size: 11px; }"
            "QLabel { background: transparent; }"
            "QMenuBar { background: #FFFFFF; color: #202124; border-bottom: 1px solid #D8DDE6; }"
            "QMenu { background: #FFFFFF; color: #202124; border: 1px solid #D8DDE6; }"
            "QMenu::item:selected { background: #EAF0F8; color: #202124; }"
            "QHeaderView::section { background: #EEF2F7; color: #202124; padding: 6px; border: none; border-bottom: 1px solid #D8DDE6; font-weight: 600; }"
            "QAbstractItemView { selection-background-color: #0D6EFD; selection-color: #FFFFFF; }"
            "QAbstractItemView::item:selected { background-color: #0D6EFD; color: #FFFFFF; }"
            "QAbstractItemView::item:selected:!active { background-color: #4A7AB5; color: #FFFFFF; }"
            "QTableWidget { selection-background-color: #0D6EFD; selection-color: #FFFFFF; border: 1px solid #D8DDE6; border-radius: 8px; }"
            "QTableCornerButton::section { background: #EEF2F7; border: 1px solid #D8DDE6; }"
            "QTableWidget::item:selected { background-color: #0D6EFD; color: #FFFFFF; }"
            "QTableWidget::item:selected:!active { background-color: #4A7AB5; color: #FFFFFF; }"
            "QTableWidget::item:hover { background-color: #E8F0F8; }"
            "QScrollBar:vertical { background: #EDF2F7; width: 14px; border: 1px solid #D0D7E2; margin: 0px; }"
            "QScrollBar::handle:vertical { background: #7E8FA8; border-radius: 5px; min-height: 26px; margin: 2px; }"
            "QScrollBar::handle:vertical:hover { background: #5F7390; }"
            "QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { background: none; height: 0px; }"
            "QScrollBar:horizontal { background: #EDF2F7; height: 14px; border: 1px solid #D0D7E2; margin: 0px; }"
            "QScrollBar::handle:horizontal { background: #7E8FA8; border-radius: 5px; min-width: 26px; margin: 2px; }"
            "QScrollBar::handle:horizontal:hover { background: #5F7390; }"
            "QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal { background: none; width: 0px; }"
            "QStatusBar { background: #F3F5F8; color: #2E3A48; border-top: 1px solid #D8DDE6; }"
        )
        self._dark_theme_qss = (
            "QWidget { background: #1E1E1E; color: #D4D4D4; }"
            "QWidget#appShell { background: #181B20; }"
            "QFrame#updateBannerCard, QFrame#filterCard, QFrame#resultsCard, QFrame#footerStrip, QWidget#purchaseListPanel, QFrame#purchaseHeaderCard, QFrame#purchaseContextCard, QFrame#purchaseSummaryCard { background: #22262C; border: 1px solid #353C46; border-radius: 10px; }"
            "QFrame#updateBannerCard { background: #3A2F1B; border-color: #6B5632; }"
            "QFrame#transferRail { background: #1C2128; border: 1px solid #353C46; border-radius: 8px; }"
            "QLineEdit, QComboBox { background: #2D2D2D; color: #D4D4D4; border: 1px solid #3E3E3E; border-radius: 6px; padding: 4px 8px; min-height: 24px; selection-background-color: #264F78; selection-color: #FFFFFF; }"
            "QDateEdit { background: #2D2D2D; color: #D4D4D4; border: 1px solid #3E3E3E; border-radius: 6px; padding: 4px 8px; min-height: 24px; }"
            "QTextEdit { background: #2D2D2D; color: #D4D4D4; border: 1px solid #3E3E3E; border-radius: 3px; padding: 4px 6px; selection-background-color: #264F78; selection-color: #FFFFFF; }"
            "QPushButton { min-height: 28px; border-radius: 6px; padding: 4px 12px; border: 1px solid #3E3E3E; background: #2D2D2D; color: #D4D4D4; font-weight: 600; }"
            "QPushButton:hover { background-color: #383838; border-color: #505050; }"
            "QPushButton:pressed { background-color: #252525; }"
            "QPushButton:disabled { background: #252525; color: #5A5A5A; border-color: #333333; }"
            "QPushButton[role='primary'] { background: #1A73E8; color: #FFFFFF; border: 1px solid #1A73E8; }"
            "QPushButton[role='primary']:hover { background: #1869D2; border-color: #1869D2; }"
            "QPushButton[role='accent'] { background: #0B5A8C; color: #FFFFFF; border: 1px solid #0A4D77; }"
            "QPushButton[role='accent']:hover { background: #0A4D77; }"
            "QPushButton[role='ghost'] { background: #232830; color: #D4D4D4; border: 1px solid #434B57; }"
            "QPushButton[deleteAction='true'] { background: #232830; color: #FF938B; border: 1px solid #7B3A35; padding: 0; border-radius: 6px; font-size: 13px; font-weight: 700; }"
            "QPushButton[deleteAction='true']:hover { background: #3A2525; border-color: #A55650; color: #FFB8B2; }"
            "QToolButton { min-height: 28px; border-radius: 6px; padding: 4px 12px; border: 1px solid #3E3E3E; background: #2D2D2D; color: #D4D4D4; font-weight: 600; }"
            "QToolButton:hover { background-color: #383838; border-color: #505050; }"
            "QToolButton[role='ghost'] { background: #232830; color: #D4D4D4; border: 1px solid #434B57; }"
            "QPushButton[role='rail'] { background: #0B5A8C; color: #FFFFFF; border: 1px solid #083E61; min-width: 40px; min-height: 34px; padding: 0; border-radius: 8px; font-size: 14px; font-weight: 700; }"
            "QPushButton[role='toggle'][active='true'] { background: #0A4D77; color: #FFFFFF; border: 1px solid #083E61; }"
            "QPushButton[role='toggle'][active='false'] { background: #0B5A8C; color: #FFFFFF; border: 1px solid #0A4D77; }"
            "QLabel#ruralPill { border: 1px solid #434B57; border-radius: 11px; background: #232830; color: #C5CED8; padding: 2px 10px; font-size: 11px; font-weight: 600; }"
            "QLabel#ruralPill[ruralState='rural'] { background: #183626; border-color: #2D6A46; color: #8EE7AB; }"
            "QLabel#ruralPill[ruralState='non_rural'] { background: #1B2E4D; border-color: #2D4E82; color: #A7C7FF; }"
            "QLabel#ruralPill[ruralState='invalid'] { background: #2A2D33; border-color: #3A414D; color: #9BA8B7; }"
            "QLabel[subtle='true'] { color: #9BA8B7; font-size: 11px; }"
            "QLabel { background: transparent; color: #D4D4D4; }"
            "QMenuBar { background: #1E1E1E; color: #D4D4D4; border-bottom: 1px solid #3E3E3E; }"
            "QMenu { background: #252525; color: #D4D4D4; border: 1px solid #3E3E3E; }"
            "QMenu::item:selected { background: #37373D; color: #FFFFFF; }"
            "QStatusBar { background: #1E1E1E; color: #D4D4D4; border-top: 1px solid #3E3E3E; }"
            "QHeaderView::section { background: #2D2D2D; color: #D4D4D4; border: none; border-bottom: 1px solid #3E3E3E; padding: 6px; font-weight: 600; }"
            "QAbstractItemView { selection-background-color: #2A7FD4; selection-color: #FFFFFF; }"
            "QAbstractItemView::item:selected { background: #2A7FD4; color: #FFFFFF; }"
            "QAbstractItemView::item:selected:!active { background-color: #335A8A; color: #FFFFFF; }"
            "QTableWidget { background: #1E1E1E; alternate-background-color: #252525; gridline-color: #3E3E3E; color: #D4D4D4; selection-background-color: #2A7FD4; selection-color: #FFFFFF; border: 1px solid #3E3E3E; border-radius: 8px; }"
            "QTableCornerButton::section { background: #2D2D2D; border: 1px solid #3E3E3E; }"
            "QTableWidget::item { color: #D4D4D4; }"
            "QTableWidget::item:selected { background: #2A7FD4; color: #FFFFFF; }"
            "QTableWidget::item:selected:!active { background-color: #335A8A; color: #FFFFFF; }"
            "QTableWidget::item:hover { background-color: #2A2A2A; color: #E6E6E6; }"
            # Combo box dropdown button and popup
            "QComboBox::drop-down { border-left: 1px solid #3E3E3E; width: 18px; background: #2D2D2D; }"
            "QComboBox::down-arrow { border-left: 4px solid transparent; border-right: 4px solid transparent; border-top: 5px solid #A0A0A0; width: 0px; height: 0px; margin-right: 4px; }"
            "QComboBox QAbstractItemView { background: #2D2D2D; color: #D4D4D4; selection-background-color: #264F78; selection-color: #FFFFFF; border: 1px solid #3E3E3E; }"
            "QSpinBox { background: #2D2D2D; color: #D4D4D4; border: 1px solid #3E3E3E; border-radius: 3px; padding: 3px; height: 22px; }"
            "QSpinBox::up-button, QSpinBox::down-button { background: #3A3A3A; border: 1px solid #3E3E3E; width: 14px; }"
            # Checkbox indicators inside item views (tables)
            "QAbstractItemView::indicator { width: 14px; height: 14px; border-radius: 2px; }"
            "QAbstractItemView::indicator:unchecked { background: #2D2D2D; border: 1px solid #606060; }"
            "QAbstractItemView::indicator:checked { background: #264F78; border: 1px solid #4A9EFF; }"
            "QAbstractItemView::indicator:indeterminate { background: #3A3A3A; border: 1px solid #606060; }"
            # Scrollbars
            "QScrollBar:vertical { background: #181C22; width: 14px; border: 1px solid #2D333D; margin: 0px; }"
            "QScrollBar::handle:vertical { background: #6C7B8F; border-radius: 5px; min-height: 26px; margin: 2px; }"
            "QScrollBar::handle:vertical:hover { background: #85A0C2; }"
            "QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { background: none; height: 0px; }"
            "QScrollBar:horizontal { background: #181C22; height: 14px; border: 1px solid #2D333D; margin: 0px; }"
            "QScrollBar::handle:horizontal { background: #6C7B8F; border-radius: 5px; min-width: 26px; margin: 2px; }"
            "QScrollBar::handle:horizontal:hover { background: #85A0C2; }"
            "QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal { background: none; width: 0px; }"
            # Splitter handle
            "QSplitter::handle { background: #3E3E3E; }"
            # Frame/groupbox borders
            "QFrame { border-color: #3E3E3E; }"
            "QGroupBox { border: 1px solid #3E3E3E; border-radius: 4px; color: #D4D4D4; }"
        )

        self._build_update_banner(root)
        self._build_filter_panel(root)
        self._build_results_region(root)
        self._build_footer_strip(root)

        self.year_combo.currentIndexChanged.connect(self._sync_purchase_list_context)
        self.state_combo.currentIndexChanged.connect(self._sync_purchase_list_context)
        self.zip_edit.textChanged.connect(self._sync_purchase_list_context)
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

    @staticmethod
    def _styled_button(text: str, role: str = "ghost") -> QPushButton:
        btn = QPushButton(text)
        btn.setProperty("role", role)
        return btn

    def _build_update_banner(self, root_layout):
        self._update_bar_widget = QFrame()
        self._update_bar_widget.setObjectName("updateBannerCard")
        update_bar_layout = QHBoxLayout(self._update_bar_widget)
        update_bar_layout.setContentsMargins(12, 8, 12, 8)
        update_bar_layout.setSpacing(10)

        banner_icon = QLabel("🔔")
        banner_icon.setStyleSheet("font-size: 14px;")
        update_bar_layout.addWidget(banner_icon)

        self.update_bar = QLabel()
        self.update_bar.setOpenExternalLinks(True)
        self.update_bar.setWordWrap(True)
        update_bar_layout.addWidget(self.update_bar, 1)

        self._update_now_btn = self._styled_button("Update Now", "primary")
        self._update_now_btn.setVisible(False)
        self._update_now_btn.clicked.connect(self._on_update_now)
        update_bar_layout.addWidget(self._update_now_btn)

        self._update_bar_widget.hide()
        root_layout.addWidget(self._update_bar_widget)

    def _build_filter_panel(self, root_layout):
        toolbar_card = QFrame()
        self._toolbar_card = toolbar_card
        toolbar_card.setObjectName("filterCard")
        toolbar_container = QVBoxLayout(toolbar_card)
        toolbar_container.setContentsMargins(12, 10, 12, 10)
        toolbar_container.setSpacing(10)

        row1 = QHBoxLayout()
        row1.setSpacing(10)

        sync_btn = self._styled_button("⚌  Sync from CMS", "primary")
        sync_btn.setToolTip("Download latest CMS DMEPOS fee schedules for your tracked states")
        sync_btn.clicked.connect(self._sync_cms)
        sync_btn.setMinimumWidth(148)
        row1.addWidget(sync_btn, 0)

        row1.addWidget(QLabel("Year:"))
        self.year_combo = QComboBox()
        self.year_combo.setMinimumWidth(85)
        self.year_combo.currentIndexChanged.connect(self._on_year_changed)
        row1.addWidget(self.year_combo, 0)

        self.year_view_label = QLabel("")
        self.year_view_label.setStyleSheet("color: #666666; font-style: italic; font-size: 11px;")
        self.year_view_label.setMinimumWidth(150)
        row1.addWidget(self.year_view_label, 0)

        row1.addWidget(QLabel("State:"))
        self.state_combo = QComboBox()
        self.state_combo.setMinimumWidth(280)
        self.state_combo.currentIndexChanged.connect(self._apply_filters)
        self.state_combo.currentIndexChanged.connect(self._save_filter_preferences)
        row1.addWidget(self.state_combo, 1)

        row1.addWidget(QLabel("ZIP:"))
        self.zip_edit = QLineEdit()
        self.zip_edit.setPlaceholderText("5-digit ZIP")
        self.zip_edit.setMaximumWidth(ZIP_EDIT_MAX_WIDTH_PX)
        self.zip_edit.setToolTip(
            "Enter a 5-digit ZIP code to automatically select rural (R) or non-rural (NR) allowable.\n"
            "Leave blank to default to non-rural (NR)."
        )
        self.zip_edit.textChanged.connect(self._on_zip_changed)
        row1.addWidget(self.zip_edit, 0)

        self.rural_label = QLabel("No ZIP • NR")
        self.rural_label.setObjectName("ruralPill")
        self.rural_label.setProperty("ruralState", "default")
        self.rural_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.rural_label.setMinimumWidth(112)
        row1.addWidget(self.rural_label, 0)
        row1.addStretch(1)
        toolbar_container.addLayout(row1)

        row2 = QHBoxLayout()
        row2.setSpacing(10)
        row2.addWidget(QLabel("HCPCS Group:"))
        self.group_combo = QComboBox()
        self.group_combo.setMinimumWidth(210)
        self.group_combo.addItem("All Groups", None)
        from core.hcpcs_groups import get_group_choices
        from core.database import get_available_hcpcs_prefixes
        available_prefixes = get_available_hcpcs_prefixes()
        for prefix, label in get_group_choices(only_prefixes=available_prefixes or None):
            self.group_combo.addItem(label, prefix)
        self.group_combo.currentIndexChanged.connect(self._apply_filters)
        self.group_combo.currentIndexChanged.connect(self._save_filter_preferences)
        row2.addWidget(self.group_combo, 0)
        row2.addWidget(QLabel("HCPCS Code:"))
        self.code_edit = QLineEdit()
        self.code_edit.setPlaceholderText("e.g. E0601")
        self.code_edit.setMaximumWidth(130)
        self.code_edit.textChanged.connect(self._on_search_text_changed)
        self.code_edit.returnPressed.connect(self._apply_filters)
        row2.addWidget(self.code_edit, 0)
        row2.addWidget(QLabel("Keyword:"))
        self.keyword_edit = QLineEdit()
        self.keyword_edit.setPlaceholderText("Description keyword…")
        self.keyword_edit.setMinimumWidth(260)
        self.keyword_edit.textChanged.connect(self._on_search_text_changed)
        self.keyword_edit.returnPressed.connect(self._apply_filters)
        row2.addWidget(self.keyword_edit, 1)

        search_btn = self._styled_button("Search", "primary")
        search_btn.clicked.connect(self._apply_filters)
        row2.addWidget(search_btn)

        clear_btn = self._styled_button("Clear", "ghost")
        clear_btn.clicked.connect(self._clear_filters)
        row2.addWidget(clear_btn)
        export_btn = self._styled_button("Export", "ghost")
        export_btn.clicked.connect(self._export)
        row2.addWidget(export_btn)

        purchase_btn = self._styled_button("Purchase List (0)", "toggle")
        purchase_btn.setProperty("active", "false")
        purchase_btn.setCheckable(True)
        purchase_btn.toggled.connect(self._toggle_purchase_list_panel)
        self._purchase_btn = purchase_btn
        row2.addWidget(purchase_btn)
        row2.addStretch()

        toolbar_container.addLayout(row2)
        root_layout.addWidget(toolbar_card)

    def _build_results_region(self, root_layout):
        results_card = QFrame()
        results_card.setObjectName("resultsCard")
        results_layout = QVBoxLayout(results_card)
        results_layout.setContentsMargins(12, 10, 12, 12)
        results_layout.setSpacing(8)

        heading_row = QHBoxLayout()
        heading = QLabel("Fee Schedule Results")
        heading.setStyleSheet("font-size: 14px; font-weight: 700;")
        heading_row.addWidget(heading)
        hint = QLabel("Click HCPCS for history, or right-click for copy actions")
        hint.setProperty("subtle", True)
        heading_row.addStretch()
        heading_row.addWidget(hint)
        results_layout.addLayout(heading_row)

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
        self.table.cellClicked.connect(self._on_cell_clicked)
        self.table.doubleClicked.connect(self._on_row_double_clicked)
        self.table.setToolTip("Click HCPCS code to view history. Right-click for copy options.")
        self.table.setHorizontalScrollMode(QTableWidget.ScrollMode.ScrollPerPixel)
        self.table.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.table.horizontalHeader().sectionMoved.connect(self._save_main_table_layout_preferences)
        self.table.horizontalHeader().sectionResized.connect(self._save_main_table_layout_preferences)
        self.table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self._on_table_context_menu)

        self.splitter = QSplitter(Qt.Orientation.Horizontal)
        self.splitter.setHandleWidth(8)
        self.splitter.addWidget(self.table)
        self.table.setMinimumWidth(520)
        middle_controls = QFrame()
        middle_controls.setObjectName("transferRail")
        middle_controls.setFixedWidth(TRANSFER_RAIL_WIDTH_PX)
        middle_layout = QVBoxLayout(middle_controls)
        middle_layout.setContentsMargins(6, 10, 6, 10)
        middle_layout.setSpacing(8)
        middle_layout.addStretch()

        add_btn = self._styled_button("►", "rail")
        add_btn.setToolTip("Add selected items to Purchase List (Ctrl+Right)")
        add_btn.setFixedSize(TRANSFER_RAIL_BUTTON_SIZE)
        add_btn.clicked.connect(self._add_checked_from_main)
        middle_layout.addWidget(add_btn)

        remove_btn = self._styled_button("◄", "rail")
        remove_btn.setToolTip("Remove selected items from Purchase List (Ctrl+Left)")
        remove_btn.setFixedSize(TRANSFER_RAIL_BUTTON_SIZE)
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
            self._purchase_list_panel.setObjectName("purchaseListPanel")
        except Exception as e:
            self._write_startup_breadcrumb(f"MainWindow._init_ui: PurchaseListPanel creation failed: {e}")
            raise
        self._write_startup_breadcrumb("MainWindow._init_ui: created PurchaseListPanel")
        self._purchase_list_panel.count_changed.connect(self._update_purchase_button_label)
        self._purchase_list_panel.setMinimumWidth(PURCHASE_PANEL_MIN_WIDTH_PX)
        self._purchase_list_panel.hide()
        self.splitter.addWidget(self._purchase_list_panel)
        self.splitter.setStretchFactor(0, 4)
        self.splitter.setStretchFactor(1, 0)
        self.splitter.setStretchFactor(2, 2)
        self.splitter.setSizes([900, TRANSFER_RAIL_WIDTH_PX, 0])
        results_layout.addWidget(self.splitter, 1)
        root_layout.addWidget(results_card, 1)

    def _build_footer_strip(self, root_layout):
        footer = QFrame()
        footer.setObjectName("footerStrip")
        footer_layout = QHBoxLayout(footer)
        footer_layout.setContentsMargins(10, 6, 10, 6)
        footer_layout.setSpacing(8)
        footer_label = QLabel("Shortcuts: Ctrl+P toggle purchase list • Ctrl+Right add • Ctrl+Left remove")
        footer_label.setProperty("subtle", True)
        footer_layout.addWidget(footer_label)
        footer_layout.addStretch()
        root_layout.addWidget(footer)

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
        state = "default"
        if not zip5:
            self.rural_label.setText("No ZIP • NR")
        elif len(zip5) == 5 and zip5.isdigit():
            rural = self._is_rural()
            if rural:
                self.rural_label.setText(f"{zip5} • Rural (R)")
                state = "rural"
            else:
                self.rural_label.setText(f"{zip5} • Non-Rural (NR)")
                state = "non_rural"
        else:
            self.rural_label.setText("Invalid ZIP")
            state = "invalid"
        self.rural_label.setProperty("ruralState", state)
        self.rural_label.style().unpolish(self.rural_label)
        self.rural_label.style().polish(self.rural_label)

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
        zip_code = self.zip_edit.text().strip()
        dlg = MainExportDialog(self._records, self, is_rural=self._is_rural(), zip_code=zip_code)
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
            if not self._purchase_list_panel.is_context_ready():
                QMessageBox.information(
                    self,
                    "Purchase List Filter Required",
                    self._purchase_list_panel.context_requirement_message(),
                )
                return
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
        if not self._purchase_list_panel.is_context_ready():
            QMessageBox.information(
                self,
                "Purchase List Filter Required",
                self._purchase_list_panel.context_requirement_message(),
            )
            return
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
            usable_width = max(1, self.splitter.width() - TRANSFER_RAIL_WIDTH_PX)
            left = max(PREFERRED_RESULTS_PANEL_MIN_WIDTH_PX, int(usable_width * PURCHASE_PANEL_LEFT_RATIO))
            right = max(PURCHASE_PANEL_MIN_WIDTH_PX, usable_width - left)
            # Guard for tight/non-maximized windows where minimums plus integer
            # rounding can overrun usable width.
            if right >= usable_width:
                right = max(1, usable_width - RESULTS_PANEL_MIN_WIDTH_PX)
            if left + right > usable_width:
                left = max(RESULTS_PANEL_MIN_WIDTH_PX, usable_width - right)
            self.splitter.setSizes([left, TRANSFER_RAIL_WIDTH_PX, right])
            self._purchase_list_panel.refresh_context_state()
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
            self._purchase_btn.setProperty("active", "true" if visible else "false")
            self._purchase_btn.style().unpolish(self._purchase_btn)
            self._purchase_btn.style().polish(self._purchase_btn)
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

    def _sync_purchase_list_context(self, *_args):
        if getattr(self, "_purchase_list_panel", None):
            self._purchase_list_panel.refresh_context_state()

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
        dark_enabled = bool(get_config_value("dark_mode_enabled", False))
        if dark_enabled:
            body.setStyleSheet(
                "QTextEdit { background: #1E1E1E; color: #E6E6E6; border: 1px solid #3E3E3E; }"
            )
            guide_style = """
                h1 { color: __DARK_LINK_COLOR__; margin-top: 10px; margin-bottom: 10px; }
                h2 { color: #4DA3FF; margin-top: 15px; margin-bottom: 8px; font-size: 16px; }
                h3 { color: #E6E6E6; margin-top: 10px; margin-bottom: 5px; font-size: 14px; }
                p, ul, li, ol { color: #E6E6E6; }
                p { margin: 5px 0; }
                ul { margin: 5px 0 10px 20px; }
                li { margin: 3px 0; }
                a { color: __DARK_LINK_COLOR__; }
                .section { margin-bottom: 15px; }
                .tip { background-color: #2A2A2A; padding: 8px; border-left: 3px solid #4DA3FF; margin: 10px 0; color: #E6E6E6; }
                .shortcut { font-family: monospace; background-color: #2F3E53; padding: 2px 6px; border-radius: 3px; color: #E6E6E6; }
                .footer-note { margin-top: 20px; padding: 10px; background-color: #2A2A2A; border: 1px solid #3E3E3E; border-radius: 6px; }
                .footer-note p { color: #E6E6E6; }
                .footer-note a { color: __DARK_LINK_COLOR__; }
                .shortcut-table { border-collapse: collapse; width: 100%; }
                .shortcut-table th, .shortcut-table td { border: 1px solid #3E3E3E; padding: 5px; }
                .shortcut-table th { background-color: #2F3E53; color: #E6E6E6; }
            """.replace("__DARK_LINK_COLOR__", USER_GUIDE_DARK_LINK_COLOR)
        else:
            body.setStyleSheet(
                "QTextEdit { background: #FFFFFF; color: #202124; border: 1px solid #C9CED6; }"
            )
            guide_style = """
                h1 { color: #003366; margin-top: 10px; margin-bottom: 10px; }
                h2 { color: #005A9C; margin-top: 15px; margin-bottom: 8px; font-size: 16px; }
                h3 { color: #333; margin-top: 10px; margin-bottom: 5px; font-size: 14px; }
                p { margin: 5px 0; }
                ul { margin: 5px 0 10px 20px; }
                li { margin: 3px 0; }
                .section { margin-bottom: 15px; }
                .tip { background-color: #EEF2F7; padding: 8px; border-left: 3px solid #005A9C; margin: 10px 0; }
                .shortcut { font-family: monospace; background-color: #E8F0F8; padding: 2px 6px; border-radius: 3px; }
                .footer-note { margin-top: 20px; padding: 10px; background-color: #F5F6F8; border-radius: 6px; }
                .shortcut-table { border-collapse: collapse; width: 100%; }
                .shortcut-table th, .shortcut-table td { border: 1px solid #C9CED6; padding: 5px; }
                .shortcut-table th { background-color: #EEF2F7; }
            """
        body.setHtml(f"""
            <style>
                {guide_style}
            </style>

            <h1>VA HCPCS Fee Schedule Manager — User Guide</h1>

            <div class="section">
                <h2>1. Getting Started</h2>
                <h3>First-Time Setup</h3>
                <p>When you first launch the app, a Setup Wizard guides you through:</p>
                <ul>
                    <li><b>State Selection:</b> Choose which states you need to track (e.g., CA, TX, FL)</li>
                    <li><b>Data Sync:</b> Option to download the latest CMS DMEPOS fee schedules immediately</li>
                </ul>

                <h3>Application Layout</h3>
                <ul>
                    <li><b>Top Toolbar (Row 1):</b> Sync from CMS, Year filter, State filter, ZIP code entry</li>
                    <li><b>Top Toolbar (Row 2):</b> HCPCS Group filter, HCPCS code search, Keyword search, Export button, Purchase List button</li>
                    <li><b>Main Table:</b> HCPCS code, description, state, year, allowable amount, modifier, and source</li>
                    <li><b>Status Bar:</b> Record count and operation status</li>
                </ul>
            </div>

            <div class="section">
                <h2>2. Searching and Filtering Data</h2>

                <h3>Year Filter</h3>
                <p>Select a specific year or "All Years". The most recent year with data is selected by default.</p>

                <h3>State Filter</h3>
                <p>Filter by state abbreviation (e.g., CA, TX). Only states enabled in Settings → Manage States appear here.</p>

                <h3>ZIP Code Lookup</h3>
                <p>Enter a 5-digit ZIP code to automatically determine rural (R) or non-rural (NR) allowable amounts:</p>
                <ul>
                    <li>Uses CMS rural ZIP designation files bundled with the app</li>
                    <li>Rural status is year-specific and updates when you change the year filter</li>
                    <li>The main table automatically shows the correct allowable for your ZIP context</li>
                </ul>
                <div class="tip">
                    <b>Tip:</b> This mirrors PDAC fee lookup behavior — enter the ZIP and the correct allowable displays immediately.
                </div>

                <h3>HCPCS Group Filter</h3>
                <p>Narrow results to an equipment category (e.g., "E0 - Durable Medical Equipment"). Only groups with local data appear.</p>

                <h3>HCPCS Code Search</h3>
                <p>Type a partial or complete HCPCS code (e.g., "E0601" or "E06"). Search is debounced for smooth typing.</p>

                <h3>Keyword Search</h3>
                <p>Search description text (e.g., "wheelchair", "oxygen"). Case-insensitive.</p>

                <h3>Clearing Filters</h3>
                <p>Click <b>Clear</b> to reset all filters to defaults.</p>
            </div>

            <div class="section">
                <h2>3. Viewing Historical Data</h2>
                <p>Click any HCPCS code (blue link) in the main table to open the History dialog:</p>
                <ul>
                    <li>All years of data for that code and state</li>
                    <li>Non-rural (NR) and Rural (R) allowable amounts</li>
                    <li>Effective allowable based on your entered ZIP code</li>
                    <li>HCPCS group classification</li>
                    <li>Side-by-side state comparison option</li>
                </ul>
            </div>

            <div class="section">
                <h2>4. Purchase List</h2>
                <p>The Purchase List lets you build a working list of HCPCS codes with quantities and pricing for procurement or documentation.</p>

                <h3>Opening</h3>
                <ul>
                    <li>Click the <b>Purchase List (0)</b> button in the toolbar</li>
                    <li>Press <span class="shortcut">Ctrl+P</span></li>
                    <li>View → Purchase List</li>
                </ul>

                <h3>Year and State Context</h3>
                <p>The Purchase List prices items against a specific CMS year and state. The selected year is shown in the panel header.
                If the year does not match the current CMS release year, the header label is displayed in <b style="color: #C0392B;">red</b> as a reminder to verify your pricing context before generating documents.</p>

                <h3>Adding Items</h3>
                <ul>
                    <li><b>From Main Table:</b> Check items, then click <b>►</b> or press <span class="shortcut">Ctrl+Right</span></li>
                    <li><b>Quick Add:</b> Type an HCPCS code directly in the Purchase List input field and press Enter</li>
                    <li><b>Right-Click Menu:</b> Right-click any row in the main table → "Add to Purchase List"</li>
                </ul>

                <h3>Removing Items</h3>
                <ul>
                    <li>Click the <b>🗑</b> icon on any purchase-list row for quick single-item removal</li>
                    <li>Check items in the Purchase List, then click <b>◄</b> or press <span class="shortcut">Ctrl+Left</span></li>
                </ul>

                <h3>Saving and Loading Bundles</h3>
                <ul>
                    <li>Use the <b>Bundles</b> menu in the panel footer to save or load bundles</li>
                    <li><b>Load / Manage Bundles:</b> Restore a saved bundle and preview item descriptions</li>
                    <li><b>Manage</b> menu in the loader supports category and bundle create/rename/delete/move</li>
                </ul>

                <h3>Generating Worksheet Documents</h3>
                <p>Click <b>Generate Document</b> to open the procurement worksheet builder:</p>
                <ul>
                    <li>Shows all items with quantities and CMS allowable prices</li>
                    <li>Enter vendor information, contact details, and delivery destination</li>
                    <li><b>Deliver To:</b> Choose from standard delivery options or select <b>Other</b> and type a custom destination</li>
                    <li>Copy the completed table to clipboard for pasting into emails or procurement forms</li>
                    <li>Export to Word document (.docx) for a formatted printable worksheet</li>
                    <li>Attach to email directly from the dialog</li>
                </ul>

                <h3>Exporting the Purchase List</h3>
                <ul>
                    <li><b>CSV:</b> For Excel or database import</li>
                    <li><b>Excel (.xlsx):</b> Formatted spreadsheet</li>
                    <li><b>PDF:</b> Printable document</li>
                </ul>
                <p>Press <span class="shortcut">Ctrl+Shift+C</span> to copy the Purchase List table to clipboard.</p>
            </div>

            <div class="section">
                <h2>5. Syncing CMS Data</h2>
                <p>Keep fee schedules current by syncing directly from CMS.gov:</p>

                <h3>Manual Sync</h3>
                <ol>
                    <li>Click <b>⚌ Sync from CMS</b> in the toolbar</li>
                    <li>Review the year selection (current year + previous years)</li>
                    <li>Click <b>Sync</b> to download and import</li>
                    <li>A progress dialog shows download and import status</li>
                </ol>

                <h3>Startup Check</h3>
                <p>On startup the app checks whether newer CMS files may be available and shows a one-time prompt per session if an update is detected.
                The check is non-blocking — the app loads normally regardless of the result.</p>

                <div class="tip">
                    <b>Note:</b> CMS typically publishes quarterly updates (January, April, July, October). Sync regularly to ensure accurate allowable amounts.
                </div>
            </div>

            <div class="section">
                <h2>6. Importing CSV Data</h2>
                <p>Import fee schedules from CSV files via File → Import CSV:</p>
                <ul>
                    <li>Supports CMS DMEPOS format and custom CSV layouts</li>
                    <li>Auto-detects columns for HCPCS code, description, state, year, and allowables</li>
                    <li>Can import rural ZIP designation files</li>
                    <li>View import history in File → View Import Log</li>
                </ul>
            </div>

            <div class="section">
                <h2>7. Exporting Search Results</h2>
                <p>Export what is currently visible in the main results table:</p>
                <ul>
                    <li><b>CSV:</b> For spreadsheet or database import</li>
                    <li><b>Excel (.xlsx):</b> Formatted spreadsheet with proper columns</li>
                    <li><b>PDF:</b> Printable report with filter metadata</li>
                </ul>
                <p>Exports include your current ZIP code and rural status context when specified.</p>
                <div class="tip">
                    <b>Tip:</b> Apply your filters first — exports capture exactly what is shown in the table.
                </div>
            </div>

            <div class="section">
                <h2>8. Backup and Restore</h2>

                <h3>Creating Backups</h3>
                <p>File → Create Backup produces a .zip file containing:</p>
                <ul>
                    <li>All fee schedule records</li>
                    <li>Purchase List bundles and categories</li>
                    <li>Selected states and preferences</li>
                    <li>Import history</li>
                </ul>

                <h3>Restoring Backups</h3>
                <ol>
                    <li>File → Restore from Backup</li>
                    <li>Select a .zip backup file</li>
                    <li>Preview what will be restored</li>
                    <li>Confirm to overwrite current data</li>
                    <li>Restart the app after restore completes</li>
                </ol>
            </div>

            <div class="section">
                <h2>9. Settings and Configuration</h2>

                <h3>Manage States</h3>
                <p>Settings → Manage States: check the states you need. Only selected states appear in filters and sync operations.</p>

                <h3>Database Location</h3>
                <p>Settings → Change Database Path to store data in a different location (e.g., network drive).</p>

                <h3>Dark Mode</h3>
                <p>View → Dark Mode toggles light/dark themes. Preference is saved automatically.</p>

                <h3>Desktop Shortcut</h3>
                <p>Settings → Create Desktop Shortcut (Windows .exe only) adds or refreshes the desktop shortcut.</p>
            </div>

            <div class="section">
                <h2>10. App Updates</h2>
                <p>When a new release is available, an update banner appears at the top of the main window.</p>
                <ul>
                    <li><b>Download now</b> link opens the GitHub release page in your browser.</li>
                    <li><b>Update Now</b> button (visible when running the installed .exe) starts the automatic in-place update:
                        <ol>
                            <li>Downloads the new exe next to the current one.</li>
                            <li>Launches a detached helper script and exits the app.</li>
                            <li>The helper waits for the app to fully close, removes the old exe, renames the new one into place, and relaunches it.</li>
                            <li>Each step is logged to <span class="shortcut">%TEMP%\HCPCSFeeApp_update.log</span> for diagnostics.</li>
                            <li>If replacement fails, the log contains step-by-step manual recovery instructions.</li>
                        </ol>
                    </li>
                </ul>
                <div class="tip">
                    <b>Manual update fallback:</b> download the latest <b>HCPCSFeeApp-Setup.zip</b> from the GitHub releases page and run <b>Install.bat</b>.
                </div>
            </div>

            <div class="section">
                <h2>11. Developer Tools</h2>
                <p>Developer Tools → SQL Publisher allows publishing data to SQL Server or Databricks databases for enterprise integration.
                Requires additional drivers, credentials, and environment access.</p>
            </div>

            <div class="section">
                <h2>12. Keyboard Shortcuts</h2>
                <table class="shortcut-table">
                    <tr>
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
                <h2>13. Tips and Best Practices</h2>
                <ul>
                    <li><b>Regular Syncs:</b> Check for CMS updates quarterly (January, April, July, October)</li>
                    <li><b>Use ZIP Codes:</b> Always enter a ZIP when looking up allowables for accurate rural/non-rural pricing</li>
                    <li><b>Check the Year Context:</b> The Purchase List year label turns red when the selected year may not be the current CMS year — verify before generating documents</li>
                    <li><b>Save Bundles:</b> Create bundles for frequently ordered equipment combinations</li>
                    <li><b>Browse Groups:</b> View → Browse HCPCS Groups to explore available equipment categories</li>
                    <li><b>Right-Click Menus:</b> Right-click rows for quick actions (copy, add to Purchase List, etc.)</li>
                    <li><b>Create Backups:</b> Back up before major data changes or before updating the app</li>
                    <li><b>Column Customization:</b> Drag column headers to reorder; drag edges to resize</li>
                    <li><b>Sorting:</b> Click any column header to sort ascending/descending</li>
                </ul>
            </div>

            <div class="section">
                <h2>14. Troubleshooting</h2>

                <h3>No Data Appears</h3>
                <ul>
                    <li>Confirm states are enabled in Settings → Manage States</li>
                    <li>Run Sync from CMS to download fee schedules</li>
                    <li>Check the year filter includes years with data</li>
                </ul>

                <h3>Sync Fails</h3>
                <ul>
                    <li>Verify your internet connection</li>
                    <li>The CMS website may be temporarily unavailable — try again later</li>
                    <li>Check File → View Import Log for error details</li>
                </ul>

                <h3>Update Now Did Not Complete</h3>
                <ul>
                    <li>Open <span class="shortcut">%TEMP%\HCPCSFeeApp_update.log</span> for a step-by-step log of what happened</li>
                    <li>If the log contains a failure message, follow the MANUAL RECOVERY INSTRUCTIONS section in the log</li>
                    <li>You can always update manually: download the latest <b>HCPCSFeeApp-Setup.zip</b> and run <b>Install.bat</b></li>
                </ul>
            </div>

            <div class="section footer-note">
                <p><b>Data Source:</b> CMS DMEPOS Fee Schedule (<a href="https://www.cms.gov/medicare/payment/fee-schedules/dmepos">cms.gov</a>)</p>
                <p><b>Developed by:</b> WSNC IMPACT Team</p>
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
        if bool(get_config_value("dark_mode_enabled", False)):
            card.setStyleSheet(
                "QFrame { background: #252525; border: 1px solid #3E3E3E; border-radius: 6px; }"
            )
        else:
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
        label_color = "#6699CC" if bool(get_config_value("dark_mode_enabled", False)) else "#003366"
        section_label.setStyleSheet(
            f"font-size: 13px; color: {label_color}; padding-top: 6px;"
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
