"""Developer Tools → SQL Publisher dialog."""

from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QTabWidget, QWidget,
    QLabel, QLineEdit, QComboBox, QPushButton, QCheckBox,
    QRadioButton, QButtonGroup, QGroupBox, QScrollArea,
    QMessageBox, QProgressBar, QSizePolicy, QFrame,
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal

from core.database import (
    get_preference, set_preference, get_fees, _get_conn,
)


# ----------------------------------------------------------------- Worker --

class _PublishWorker(QThread):
    progress = pyqtSignal(str)
    finished = pyqtSignal(int)
    error = pyqtSignal(str)

    def __init__(self, conn, db_type, records, table_name, mode, schema):
        super().__init__()
        self._conn = conn
        self._db_type = db_type
        self._records = records
        self._table_name = table_name
        self._mode = mode
        self._schema = schema

    def run(self):
        try:
            from core.sql_publisher import ensure_table_exists, publish_records
            self.progress.emit("Ensuring table exists…")
            ensure_table_exists(self._conn, self._db_type, self._table_name, self._schema)
            self.progress.emit(f"Publishing {len(self._records):,} records…")
            count = publish_records(
                self._conn,
                self._db_type,
                self._records,
                self._table_name,
                self._mode,
                self._schema,
            )
            self.finished.emit(count)
        except Exception as exc:
            self.error.emit(str(exc))


# ----------------------------------------------------------------- Dialog --

class DevToolsDialog(QDialog):
    """Developer Tools → SQL Publisher dialog."""

    def __init__(self, current_records=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Developer Tools — SQL Publisher")
        self.setMinimumSize(560, 580)
        self._current_records = current_records or []
        self._conn = None
        self._db_type = "sqlserver"
        self._worker = None

        layout = QVBoxLayout(self)
        layout.setSpacing(8)

        self._tabs = QTabWidget()
        layout.addWidget(self._tabs)

        self._build_connection_tab()
        self._build_publish_tab()
        # Apply initial visibility NOW — after both tabs are built so that
        # _on_db_type_changed can safely reference self._publish_btn.
        self._on_db_type_changed(0)

        # Close button
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        btn_row.addWidget(close_btn)
        layout.addLayout(btn_row)

        self._load_preferences()

    # ---------------------------------------------------------- Tab 1: Connection --

    def _build_connection_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(10)

        # DB type
        type_row = QHBoxLayout()
        type_row.addWidget(QLabel("Database type:"))
        self._db_type_combo = QComboBox()
        self._db_type_combo.addItem("SQL Server", "sqlserver")
        self._db_type_combo.addItem("Databricks", "databricks")
        self._db_type_combo.currentIndexChanged.connect(self._on_db_type_changed)
        type_row.addWidget(self._db_type_combo)
        type_row.addStretch()
        layout.addLayout(type_row)

        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        layout.addWidget(sep)

        # --- SQL Server fields ---
        self._sqlserver_group = QGroupBox("SQL Server Connection")
        ss_layout = self._form_layout()

        self._ss_server = QLineEdit()
        self._ss_server.setPlaceholderText("e.g. myserver.database.windows.net")
        ss_layout.addRow("Server:", self._ss_server)

        self._ss_database = QLineEdit()
        self._ss_database.setPlaceholderText("e.g. my_database")
        ss_layout.addRow("Database:", self._ss_database)

        self._ss_schema = QLineEdit("dbo")
        self._ss_schema.setPlaceholderText("e.g. dbo")
        ss_layout.addRow("Schema:", self._ss_schema)

        self._ss_windows_auth = QCheckBox("Use Windows Authentication")
        self._ss_windows_auth.toggled.connect(self._on_windows_auth_toggled)
        ss_layout.addRow("", self._ss_windows_auth)

        self._ss_username = QLineEdit()
        self._ss_username.setPlaceholderText("Username")
        ss_layout.addRow("Username:", self._ss_username)

        self._ss_password = QLineEdit()
        self._ss_password.setEchoMode(QLineEdit.EchoMode.Password)
        self._ss_password.setPlaceholderText("Password (not saved)")
        ss_layout.addRow("Password:", self._ss_password)

        self._sqlserver_group.setLayout(ss_layout)
        layout.addWidget(self._sqlserver_group)

        # --- Databricks fields ---
        self._databricks_group = QGroupBox("Databricks Connection")
        db_layout = self._form_layout()

        self._db_host = QLineEdit()
        self._db_host.setPlaceholderText("e.g. adb-1234567890.1.azuredatabricks.net")
        db_layout.addRow("Server Hostname:", self._db_host)

        self._db_http_path = QLineEdit()
        self._db_http_path.setPlaceholderText("/sql/1.0/warehouses/…")
        db_layout.addRow("HTTP Path:", self._db_http_path)

        self._db_token = QLineEdit()
        self._db_token.setEchoMode(QLineEdit.EchoMode.Password)
        self._db_token.setPlaceholderText("Access token (not saved)")
        db_layout.addRow("Access Token:", self._db_token)

        self._db_catalog = QLineEdit("hive_metastore")
        db_layout.addRow("Catalog:", self._db_catalog)

        self._db_schema = QLineEdit("default")
        db_layout.addRow("Schema:", self._db_schema)

        self._databricks_group.setLayout(db_layout)
        layout.addWidget(self._databricks_group)

        # --- Test + Save row ---
        action_row = QHBoxLayout()
        self._test_btn = QPushButton("Test Connection")
        self._test_btn.clicked.connect(self._test_connection)
        action_row.addWidget(self._test_btn)

        self._conn_status_label = QLabel("")
        action_row.addWidget(self._conn_status_label)
        action_row.addStretch()

        self._save_pref_check = QCheckBox("Save connection settings")
        self._save_pref_check.setChecked(True)
        action_row.addWidget(self._save_pref_check)
        layout.addLayout(action_row)

        layout.addStretch()
        self._tabs.addTab(tab, "Connection")

        # NOTE: Initial visibility is applied AFTER _build_publish_tab() so that
        # _on_db_type_changed can safely reference self._publish_btn.

    def _form_layout(self):
        """Return a QFormLayout with standard settings."""
        from PyQt6.QtWidgets import QFormLayout
        fl = QFormLayout()
        fl.setLabelAlignment(Qt.AlignmentFlag.AlignRight)
        fl.setSpacing(6)
        return fl

    def _on_db_type_changed(self, _index=None):
        db_type = self._db_type_combo.currentData()
        self._db_type = db_type
        self._sqlserver_group.setVisible(db_type == "sqlserver")
        self._databricks_group.setVisible(db_type == "databricks")
        # Reset connection state when type changes
        self._conn = None
        self._publish_btn.setEnabled(False)
        self._conn_status_label.setText("")

    def _on_windows_auth_toggled(self, checked):
        self._ss_username.setEnabled(not checked)
        self._ss_password.setEnabled(not checked)

    def _test_connection(self):
        self._conn_status_label.setText("Testing…")
        self._test_btn.setEnabled(False)
        try:
            conn = self._make_connection()
            from core.sql_publisher import test_connection
            test_connection(conn)
            # Close any previous connection
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass  # Best-effort cleanup; ignore close errors
            self._conn = conn
            self._conn_status_label.setText("✔ Connected")
            self._conn_status_label.setStyleSheet("color: green;")
            self._publish_btn.setEnabled(True)
            if self._save_pref_check.isChecked():
                self._save_preferences()
        except Exception as exc:
            self._conn_status_label.setText(f"✘ {exc}")
            self._conn_status_label.setStyleSheet("color: red;")
            self._publish_btn.setEnabled(False)
        finally:
            self._test_btn.setEnabled(True)

    def _make_connection(self):
        from core.sql_publisher import get_sqlserver_connection, get_databricks_connection
        if self._db_type == "sqlserver":
            server = self._ss_server.text().strip()
            database = self._ss_database.text().strip()
            use_win = self._ss_windows_auth.isChecked()
            username = self._ss_username.text().strip()
            password = self._ss_password.text()
            if not server or not database:
                raise ValueError("Server and Database are required.")
            if not use_win and (not username or not password):
                raise ValueError("Username and Password are required (or use Windows Authentication).")
            return get_sqlserver_connection(server, database, username, password, use_win)
        else:
            host = self._db_host.text().strip()
            http_path = self._db_http_path.text().strip()
            token = self._db_token.text().strip()
            catalog = self._db_catalog.text().strip() or "hive_metastore"
            schema = self._db_schema.text().strip() or "default"
            if not host or not http_path or not token:
                raise ValueError("Server Hostname, HTTP Path, and Access Token are required.")
            return get_databricks_connection(host, http_path, token, catalog, schema)

    # ---------------------------------------------------------- Tab 2: Publish --

    def _build_publish_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(10)

        # Table name
        tbl_row = QHBoxLayout()
        tbl_row.addWidget(QLabel("Table Name:"))
        self._table_name_edit = QLineEdit("hcpcs_fees")
        self._table_name_edit.setMaximumWidth(240)
        tbl_row.addWidget(self._table_name_edit)
        tbl_row.addStretch()
        layout.addLayout(tbl_row)

        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        layout.addWidget(sep)

        # --- Data Scope ---
        scope_group = QGroupBox("Data Scope")
        scope_layout = QVBoxLayout(scope_group)

        self._scope_bg = QButtonGroup(self)

        self._scope_all = QRadioButton("All records in local database")
        self._scope_all.setChecked(True)
        self._scope_bg.addButton(self._scope_all, 0)
        scope_layout.addWidget(self._scope_all)

        self._scope_filtered = QRadioButton(
            f"Currently filtered records ({len(self._current_records):,} rows)"
        )
        self._scope_bg.addButton(self._scope_filtered, 1)
        scope_layout.addWidget(self._scope_filtered)

        self._scope_custom = QRadioButton("Select by state / year")
        self._scope_bg.addButton(self._scope_custom, 2)
        scope_layout.addWidget(self._scope_custom)

        # Scroll area for state/year checkboxes
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setMaximumHeight(160)
        scroll.setVisible(False)
        self._scope_custom_widget = scroll

        inner = QWidget()
        inner_layout = QHBoxLayout(inner)
        inner_layout.setAlignment(Qt.AlignmentFlag.AlignTop)

        # States column
        self._state_checks = {}
        state_col = QVBoxLayout()
        state_col.addWidget(QLabel("<b>States</b>"))
        for abbr in self._get_distinct_states():
            cb = QCheckBox(abbr)
            cb.setChecked(True)
            state_col.addWidget(cb)
            self._state_checks[abbr] = cb
        state_col.addStretch()
        inner_layout.addLayout(state_col)

        # Years column
        self._year_checks = {}
        year_col = QVBoxLayout()
        year_col.addWidget(QLabel("<b>Years</b>"))
        for yr in self._get_distinct_years():
            cb = QCheckBox(str(yr))
            cb.setChecked(True)
            year_col.addWidget(cb)
            self._year_checks[yr] = cb
        year_col.addStretch()
        inner_layout.addLayout(year_col)

        scroll.setWidget(inner)
        scope_layout.addWidget(scroll)

        self._scope_custom.toggled.connect(scroll.setVisible)

        layout.addWidget(scope_group)

        # --- Mode ---
        mode_group = QGroupBox("Write Mode")
        mode_layout = QVBoxLayout(mode_group)

        self._mode_bg = QButtonGroup(self)
        self._mode_merge = QRadioButton("Merge (upsert) — matched on hcpcs_code + state + year + modifier")
        self._mode_merge.setChecked(True)
        self._mode_bg.addButton(self._mode_merge, 0)
        mode_layout.addWidget(self._mode_merge)

        self._mode_replace = QRadioButton("Replace (delete + insert) — deletes matching state/year scope, then inserts")
        self._mode_bg.addButton(self._mode_replace, 1)
        mode_layout.addWidget(self._mode_replace)

        layout.addWidget(mode_group)

        # --- Progress + Publish ---
        self._progress_label = QLabel("")
        self._progress_label.setWordWrap(True)
        layout.addWidget(self._progress_label)

        pub_row = QHBoxLayout()
        pub_row.addStretch()
        self._publish_btn = QPushButton("Publish")
        self._publish_btn.setEnabled(False)
        self._publish_btn.setStyleSheet(
            "background-color: #003366; color: white; padding: 6px 20px; font-weight: bold;"
        )
        self._publish_btn.clicked.connect(self._publish)
        pub_row.addWidget(self._publish_btn)
        layout.addLayout(pub_row)

        layout.addStretch()
        self._tabs.addTab(tab, "Publish")

    # ---------------------------------------------------------------- Publish --

    def _publish(self):
        if self._conn is None:
            QMessageBox.warning(self, "No Connection", "Please test a connection on the Connection tab first.")
            return

        mode = "merge" if self._mode_merge.isChecked() else "replace"
        table_name = self._table_name_edit.text().strip() or "hcpcs_fees"

        if mode == "replace":
            ans = QMessageBox.warning(
                self,
                "Confirm Replace",
                "This will DELETE existing records for the selected states/years, then insert fresh data. Continue?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
                QMessageBox.StandardButton.Cancel,
            )
            if ans != QMessageBox.StandardButton.Yes:
                return

        records = self._get_scope_records()
        if not records:
            QMessageBox.information(self, "No Records", "No records match the selected scope.")
            return

        schema = self._get_current_schema()

        self._publish_btn.setEnabled(False)
        self._progress_label.setText("Starting publish…")

        self._worker = _PublishWorker(self._conn, self._db_type, records, table_name, mode, schema)
        self._worker.progress.connect(self._progress_label.setText)
        self._worker.finished.connect(self._on_publish_done)
        self._worker.error.connect(self._on_publish_error)
        self._worker.start()

    def _get_current_schema(self):
        if self._db_type == "sqlserver":
            return self._ss_schema.text().strip() or "dbo"
        else:
            return self._db_schema.text().strip() or "default"

    def _on_publish_done(self, count):
        self._publish_btn.setEnabled(True)
        server = self._get_server_label()
        table = self._table_name_edit.text().strip() or "hcpcs_fees"
        msg = f"{count:,} records pushed to [{table}] on {server}"
        self._progress_label.setText(f"✔ {msg}")
        self._progress_label.setStyleSheet("color: green;")
        QMessageBox.information(self, "Publish Complete", msg)

    def _on_publish_error(self, msg):
        self._publish_btn.setEnabled(True)
        self._progress_label.setText(f"✘ Error: {msg}")
        self._progress_label.setStyleSheet("color: red;")
        QMessageBox.critical(self, "Publish Error", msg)

    def _get_server_label(self):
        if self._db_type == "sqlserver":
            return self._ss_server.text().strip()
        return self._db_host.text().strip()

    # ---------------------------------------------------------- Scope helpers --

    def _get_scope_records(self):
        scope_id = self._scope_bg.checkedId()
        if scope_id == 1:
            # Currently filtered
            return self._current_records
        elif scope_id == 2:
            # Custom state/year selection
            states = [abbr for abbr, cb in self._state_checks.items() if cb.isChecked()]
            years = [yr for yr, cb in self._year_checks.items() if cb.isChecked()]
            if not states or not years:
                return []
            records = []
            for state in states:
                for year in years:
                    records.extend(get_fees(state_abbr=state, year=year))
            return records
        else:
            # All records
            return get_fees()

    def _get_distinct_states(self):
        try:
            conn = _get_conn()
            rows = conn.execute(
                "SELECT DISTINCT state_abbr FROM hcpcs_fees ORDER BY state_abbr"
            ).fetchall()
            conn.close()
            return [r["state_abbr"] for r in rows]
        except Exception:
            return []

    def _get_distinct_years(self):
        try:
            conn = _get_conn()
            rows = conn.execute(
                "SELECT DISTINCT year FROM hcpcs_fees ORDER BY year"
            ).fetchall()
            conn.close()
            return [r["year"] for r in rows]
        except Exception:
            return []

    # ---------------------------------------------------- Preferences --

    def _load_preferences(self):
        db_type = get_preference("sql_db_type", "sqlserver")
        idx = self._db_type_combo.findData(db_type)
        if idx >= 0:
            self._db_type_combo.setCurrentIndex(idx)

        # SQL Server
        self._ss_server.setText(get_preference("sql_server", ""))
        self._ss_database.setText(get_preference("sql_database", ""))
        self._ss_schema.setText(get_preference("sql_ss_schema", "dbo"))
        self._ss_username.setText(get_preference("sql_username", ""))
        self._ss_windows_auth.setChecked(get_preference("sql_windows_auth", "0") == "1")

        # Databricks
        self._db_host.setText(get_preference("sql_databricks_host", ""))
        self._db_http_path.setText(get_preference("sql_databricks_path", ""))
        self._db_catalog.setText(get_preference("sql_databricks_catalog", "hive_metastore"))
        self._db_schema.setText(get_preference("sql_databricks_schema", "default"))

        # Table name
        self._table_name_edit.setText(get_preference("sql_table_name", "hcpcs_fees"))

    def _save_preferences(self):
        set_preference("sql_db_type", self._db_type_combo.currentData())
        set_preference("sql_server", self._ss_server.text().strip())
        set_preference("sql_database", self._ss_database.text().strip())
        set_preference("sql_ss_schema", self._ss_schema.text().strip())
        set_preference("sql_username", self._ss_username.text().strip())
        set_preference("sql_windows_auth", "1" if self._ss_windows_auth.isChecked() else "0")
        set_preference("sql_databricks_host", self._db_host.text().strip())
        set_preference("sql_databricks_path", self._db_http_path.text().strip())
        set_preference("sql_databricks_catalog", self._db_catalog.text().strip())
        set_preference("sql_databricks_schema", self._db_schema.text().strip())
        set_preference("sql_table_name", self._table_name_edit.text().strip())
        # NOTE: Passwords/tokens are intentionally NOT saved
