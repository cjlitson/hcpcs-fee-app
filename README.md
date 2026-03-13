# VA HCPCS Fee Schedule Manager

A standalone Windows desktop application for VA staff to manage, view, filter, and export CMS DMEPOS HCPCS fee schedule data. No installation required — runs as a single `.exe` file.

---

## Features

- **Auto-download** CMS DMEPOS fee schedules for user-selected states (2024–2026)
- **Import** existing VISN-format CSV files
- **Filter** by state, year, HCPCS code, and description keyword
- **Export** to CSV, Excel (.xlsx), or PDF
- **SQLite database** — all data stored locally, no server needed
- **State management** — select any of the 50 states + DC to track
- **Import log** — track what data has been loaded and when

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

---

## Usage

1. **First Launch** — A welcome dialog will appear. Click OK to open the State Manager.
2. **Manage States** — Go to `Settings → Manage States`, check the states you want to track, and save.
3. **Sync Data** — Click `Sync from CMS` to auto-download the latest CMS DMEPOS fee schedules for your selected states.
4. **Import CSV** — Use `File → Import CSV` to load your existing VISN 22 CSV file.
5. **Filter** — Use the sidebar or search bar to filter by state, year, or HCPCS code.
6. **Export** — Click `Export...` to save filtered results as CSV, Excel, or PDF.

---

## Data Source

CMS DMEPOS Fee Schedule:  
https://www.cms.gov/medicare/payment/fee-schedules/dmepos

---

## Project Structure

```
hcpcs-fee-app/
├── main.py                     # App entry point
├── requirements.txt            # Python dependencies
├── build.bat                   # Windows .exe build script
├── .gitignore
├── ui/
│   ├── main_window.py          # Main window
│   ├── state_selector_dialog.py # State management dialog
│   ├── import_dialog.py        # CSV import wizard
│   └── export_dialog.py        # Export options dialog
├── core/
│   ├── database.py             # SQLite operations
│   ├── importer.py             # CSV parser (VISN + CMS formats)
│   ├── cms_downloader.py       # CMS auto-download
│   └── exporter.py             # CSV / Excel / PDF export
├── models/
│   └── schema.sql              # Database schema reference
└── data/
    └── hcpcs_fees.db           # Auto-created SQLite database (gitignored)
```
