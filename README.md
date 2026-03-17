# VA HCPCS Fee Schedule Manager

A standalone Windows desktop application for VA staff to manage, view, filter, and export CMS DMEPOS HCPCS fee schedule data. No installation required — runs as a single `.exe` file.

---

## Features

- **Auto-download** CMS DMEPOS fee schedules for user-selected states (2024 through the current calendar year), with live URL scraping and 24-hour cache
- **Smart file detection** — selects the main DMEPOS schedule file (`DMEPOS*.csv`) by name pattern, excluding auxiliary datasets (Rural ZIP, Former CBA, PEN schedules)
- **Quarterly replace** — each CMS sync replaces prior data for the same year/state so quarterly updates stay current without duplicates
- **Import** existing VISN-format CSV files
- **Filter** by state, year, HCPCS code, and description keyword
- **Export** to CSV, Excel (.xlsx), or PDF
- **SQLite database** — all data stored locally, no server needed
- **State management** — select any of the 50 states + DC to track
- **Year management** — select which fiscal years to track
- **Import log** — track what data has been loaded and when
- **Developer Tools** — SQL Publisher dialog for direct database queries and Databricks/ODBC publishing

---

## Requirements

- Python 3.11 or higher
- Windows 10/11 (for .exe build)

---

## Run from Source

```bash
git clone https://github.com/cjlitson/hcpcs-fee-app.git
cd hcpcs-fee-app
pip install -r requirements.txt
python main.py
```

---

## Build the Windows .exe

Double-click `build.bat` or run from command prompt:

```bat
build.bat
```

Output: `dist\HCPCSFeeApp.exe` — copy this single file anywhere and run it. No installation needed.

> **Note:** The build includes hidden imports for `pyodbc`, `databricks.sql`, and `databricks.sql.client` to ensure the Developer Tools / SQL Publisher feature works correctly in the bundled `.exe`. These are loaded lazily at runtime and would otherwise be missed by PyInstaller's static analysis.

---

## Usage

1. **First Launch** — A welcome dialog will appear. Select your tracked states and years.
2. **Manage States** — Go to `Settings → Manage States`, check the states you want to track, and save.
3. **Manage Years** — Go to `Settings → Manage Years` to select which fiscal years to include.
4. **Sync Data** — Click `Sync from CMS` to auto-download the latest CMS DMEPOS fee schedules for your selected states. The downloader scrapes live URLs from the CMS website and falls back to known URL templates if needed. Only years up to the current calendar year are offered; years not currently detected on CMS are shown as disabled in the Manage Years dialog.
5. **Import CSV** — Use `File → Import CSV` to load an existing VISN 22 CSV or a manually downloaded CMS file (`.csv`).
6. **Filter** — Use the toolbar to filter by state, year, HCPCS code, or description keyword.
7. **Export** — Click `Export...` to save filtered results as CSV, Excel, or PDF.
8. **Developer Tools** — Use `Developer Tools → SQL Publisher` to run direct SQL queries or publish data to a Databricks or ODBC endpoint.

---

## CMS Download Strategy

When syncing from CMS, the downloader uses a multi-step strategy:

1. Check a 24-hour local cache for previously discovered download URLs.
2. Scrape the [CMS DMEPOS page](https://www.cms.gov/medicare/payment/fee-schedules/dmepos) for current ZIP links, following year-specific sub-pages as needed.
3. Fall back to hardcoded quarterly URL templates (e.g. `dme26-a.zip` through `dme26-d.zip`, and no-hyphen variants `dme26a.zip`).
4. Select the main DMEPOS fee-schedule **CSV** file from the ZIP by name pattern (CSV-only):
   - **Tier 1** — files whose name starts with `DMEPOS` and ends with `.csv` (e.g. `DMEPOS26_JAN.csv`).
   - **Tier 2** — files containing `dmepos` that do not match auxiliary-dataset keywords (`rural`, `zip code`, `cba`, `pen`, `back`, `fad`, `former`, `schedule file`).
   - **Tier 3** — fallback to the largest remaining non-documentation `.csv` file (legacy heuristic).
   - Files matching documentation keywords (`readme`, `layout`, `codebook`, etc.) are always excluded.
   - `.txt` files are never selected — the CSV grid format is self-describing with named column headers and is more resilient to CMS format changes.

   The UI status bar shows which internal file was selected from the archive.
5. **Quarterly replace** — for each `(state, year)` the sync deletes existing `cms_download` rows and inserts the freshly parsed records. If the parse yields 0 records the delete is skipped and an error is shown, protecting existing data.

**Supported years:** The app only offers years from 2024 through the current calendar year. Years not currently detected on the CMS page are shown as disabled (greyed out) in the Manage Years dialog.

If all download attempts fail, a clear error message is shown with a link to manually download the file from CMS.

---

## Data Source

CMS DMEPOS Fee Schedule:  
https://www.cms.gov/medicare/payment/fee-schedules/dmepos

---

## Project Structure

```
hcpcs-fee-app/
├── main.py                          # App entry point
├── requirements.txt                 # Python dependencies
├── build.bat                        # Windows .exe build script
├── .github/
│   └── workflows/
│       └── build.yml                # GitHub Actions CI/CD build
├── .gitignore
├── ui/
│   ├── main_window.py               # Main window + sync worker
│   ├── state_selector_dialog.py     # State management dialog
│   ├── year_selector_dialog.py      # Year management dialog
│   ├── import_dialog.py             # CSV import wizard
│   ├── export_dialog.py             # Export options dialog
│   └── dev_tools_dialog.py          # Developer Tools / SQL Publisher
├── core/
│   ├── database.py                  # SQLite operations + preferences
│   ├── importer.py                  # CSV parser (VISN + CMS grid formats, auto-delimiter)
│   ├── cms_downloader.py            # CMS auto-download (scrape + cache + fallback)
│   └── exporter.py                  # CSV / Excel / PDF export
├── models/
│   └── schema.sql                   # Database schema reference
└── data/
    └── hcpcs_fees.db                # Auto-created SQLite database (gitignored)
```
