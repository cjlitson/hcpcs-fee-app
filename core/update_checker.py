"""Background worker that checks for app updates on GitHub."""

from PyQt6.QtCore import QThread, pyqtSignal


class UpdateCheckWorker(QThread):
    """Runs check_for_update() in a background thread.

    Signals:
        update_available(str, str): Emitted with (version, download_url) if a newer release exists.

    If the app is up to date or the check fails, no signal is emitted.
    """

    update_available = pyqtSignal(str, str)

    def run(self):
        from core.version import check_for_update
        result = check_for_update()
        if result:
            version, url = result
            self.update_available.emit(version, url)
