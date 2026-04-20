# VA HCPCS Fee Schedule Manager (HCPCSFeeApp)

VA HCPCS Fee Schedule Manager is a Windows desktop application for VA teams to search, compare, export, and generate worksheet documents from CMS DMEPOS HCPCS fee schedule data.

It is designed for day-to-day fee lookup and procurement workflows while keeping data local to each user profile.

Current release: **v2.0.0** (Relaunch release line)

---

## What the app does

### CMS sync and local data
- Sync fee schedule data directly from CMS sources for selected states and years.
- Import CSVs (including CMS/manual files) when needed.
- Store data locally in SQLite for fast filtering and offline lookup after sync.

### Search, filtering, and ZIP pricing
- Filter by year, state, HCPCS group, HCPCS code, and keyword.
- Enter a 5-digit ZIP code to automatically apply rural (R) vs non-rural (NR) allowable pricing.
- View effective allowables in the main table based on current ZIP context.

### History and comparison lookup
- Open HCPCS history from the main results table.
- Review multi-year pricing history.
- Compare the same HCPCS code across states.

### Purchase List workflow
- Open the Purchase List panel, select one or more results rows, then use **Add N item(s) to List**.
- Add HCPCS items from right-click actions or quick-add code entry with autocomplete suggestions.
- Remove individual line items with the row delete button, or remove selected purchase-list rows via `Ctrl+Left`.
- Save/load bundles for recurring workflows.
- Keep pricing tied to a specific selected CMS year and state.

### Exports and document generation
- Export main search results to CSV, Excel (.xlsx), or PDF.
- Generate Purchase List worksheet-style documents (Word/PDF oriented workflow).
- Copy workflow-friendly output for downstream communication or procurement steps.

### App experience
- Dark mode support.
- Startup splash/loading progress.
- Backup/restore utilities.
- Desktop shortcut creation.
- In-app update notification and update action when running the installed executable.

---

## Windows installation and updates

### Recommended install method
1. Download **`HCPCSFeeApp-Setup.zip`** from [GitHub Releases](https://github.com/cjlitson/hcpcs-fee-app/releases).
2. Extract the ZIP.
3. Run **`Install.bat`**.
4. Launch from the desktop shortcut.

Install target: `Documents\HCPCSFeeApp\` (per-user install). No administrator rights are required.

### Update behavior
- **In-app update:** when a newer release is detected, the app shows an update banner with **Update Now**. Clicking it launches `HCPCSFeeAppUpdater.exe` and exits early. The updater then owns the workflow (download, wait-for-exit, replace, relaunch), shows a dedicated progress window during long operations, and logs every step to `%TEMP%\HCPCSFeeApp_update.log`.
- **Manual update:** download the latest `HCPCSFeeApp-Setup.zip` and run `Install.bat` again.
- Always launch from the desktop shortcut after updating.

---

## Basic workflow (screen summary)

### 1) Main window
- Unified top bar: Sync from CMS, year/state filters, ZIP input, HCPCS group/code/keyword filters, Export, and Purchase List.
- Results table: HCPCS code, description, state/year, allowables, modifiers, source.

### 2) History dialog
- Open by selecting/clicking a code from results.
- Shows year-over-year values and supports state comparison.

### 3) Purchase List panel
- Build a selected-row workflow list with quantities and pricing.
- Save/load bundles and generate documents.

### 4) Export dialog
- Export visible results as CSV, Excel, or PDF.

### 5) Generate Document dialog
- Create worksheet-ready output for Purchase List scenarios.

---

## Notes and limitations

- Windows desktop app (primary supported environment: Windows 10/11).
- Internet is required for CMS sync and GitHub release/update checks.
- The standalone `.exe` is not code-signed; some systems may show SmartScreen warnings.
- CMS website/file-structure changes can require app updates to keep sync automation current.
- SQL Publisher/Databricks tooling requires additional drivers, credentials, and environment access.

---

## Data source

CMS DMEPOS Fee Schedule:
https://www.cms.gov/medicare/payment/fee-schedules/dmepos

---

## Release notes

- [v2.0.0 — Relaunch Release](docs/releases/v2.0.0.md)

Legacy pre-relaunch release notes:
- [v1.2.4 — Test Release for Updater Progress Window Validation](docs/releases/v1.2.4.md)
- [v1.2.3 — Test Release for Updater Validation](docs/releases/v1.2.3.md)
- [v1.2.2 — Updater Handoff Reliability + Clear-Filter UX Refinement](docs/releases/v1.2.2.md)
- [v1.2.1 — Updater Flow Validation and UI Refinement Alignment](docs/releases/v1.2.1.md)
- [v1.2.0 — UI Workflow Refinement and Documentation Refresh](docs/releases/v1.2.0.md)
- [v1.1.3 — Public Release for Updater Validation](docs/releases/v1.1.3.md)
- [v1.1.2 — Updater Reliability and Release-Prep Polish](docs/releases/v1.1.2.md)
- [v1.1.1 — Reliability, Export, Purchase List, and Dark Mode refresh](docs/releases/v1.1.1.md)
- [v1.0.0 — Initial Release](docs/releases/v1.0.0.md)
