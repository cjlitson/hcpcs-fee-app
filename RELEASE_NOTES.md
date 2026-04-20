# HCPCS Fee App — v2.0.0 Relaunch Release Notes

## Release summary

VA HCPCS Fee Schedule Manager v2.0.0 is the public relaunch baseline for a fresh release line.  
This release aligns version metadata and release-facing documentation across the repository and clarifies the current updater workflow for end users.

## What’s included

- New relaunch version line beginning at **v2.0.0**
- Consistent app/version metadata updates for runtime and Windows package details
- README/install/update guidance refreshed for current behavior
- Relaunch release documentation added at `docs/releases/v2.0.0.md`
- Updater guidance aligned to the standalone `HCPCSFeeAppUpdater.exe` handoff model with visible progress and `%TEMP%\HCPCSFeeApp_update.log` diagnostics

## Install / update

- Fresh install: download `HCPCSFeeApp-Setup.zip`, extract, run `Install.bat`, launch from desktop shortcut
- In-app update: click **Update Now** when prompted from the installed app
- Manual fallback: rerun installer from the latest setup ZIP

## Suggested GitHub release body (copy/paste)

🚀 **HCPCS Fee App v2.0.0 — Public Relaunch**

This release starts a fresh public release line for VA HCPCS Fee Schedule Manager.

### Highlights
- Fresh relaunch baseline at **v2.0.0**
- Version metadata aligned across app + Windows packaging resources
- Docs refreshed repo-wide for install/update/release guidance
- Updater behavior clarified:
  - `Update Now` launches `HCPCSFeeAppUpdater.exe`
  - Progress is shown during long-running download/apply steps
  - Diagnostic log path: `%TEMP%\HCPCSFeeApp_update.log`

### Install
1. Download `HCPCSFeeApp-Setup.zip`
2. Extract ZIP
3. Run `Install.bat`
4. Launch from desktop shortcut

