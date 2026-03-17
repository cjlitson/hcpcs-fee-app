import sys
from pathlib import Path
from PyQt6.QtWidgets import QApplication, QSplashScreen
from PyQt6.QtGui import QFont, QPixmap, QColor, QPainter, QLinearGradient, QIcon
from PyQt6.QtCore import Qt, QTimer, QRect
from core.database import init_db
from ui.main_window import MainWindow


def _asset(name: str) -> Path:
    """Return the absolute path to *name* inside the ``assets/`` folder.

    Works both in development (source tree) and when frozen by PyInstaller
    (where ``sys._MEIPASS`` points to the extracted bundle directory).
    """
    base = Path(getattr(sys, "_MEIPASS", Path(__file__).parent))
    return base / "assets" / name


class _ProgressSplash(QSplashScreen):
    """Splash screen with the VISN 22 Impact Team logo, step label, and
    animated progress bar.

    Call ``set_progress(pct, message)`` (0–100) to update the display.
    ``QApplication.processEvents()`` is called automatically so the repaint
    is visible even before the event loop starts.
    """

    _SPLASH_W = 560
    _SPLASH_H = 340

    def __init__(self):
        pix = QPixmap(self._SPLASH_W, self._SPLASH_H)
        pix.fill(QColor("#003366"))
        super().__init__(pix, Qt.WindowType.WindowStaysOnTopHint)
        self._pct = 0
        self._step_msg = "Starting up…"
        # Pre-load the logo so drawContents never blocks on I/O.
        logo_path = _asset("wsnc_map.png")
        self._logo = QPixmap(str(logo_path)) if logo_path.exists() else QPixmap()

    # ------------------------------------------------------------------
    def set_progress(self, pct: int, message: str) -> None:
        """Update the progress bar and step message, then force a repaint."""
        self._pct = max(0, min(100, pct))
        self._step_msg = message
        self.repaint()
        QApplication.processEvents()

    # ------------------------------------------------------------------
    def drawContents(self, painter: QPainter) -> None:  # noqa: N802
        """Paint logo, step label, progress bar, and percentage text."""
        w = self._SPLASH_W
        h = self._SPLASH_H

        # ---- background gradient ----------------------------------------
        grad = QLinearGradient(0, 0, 0, h)
        grad.setColorAt(0.0, QColor("#004080"))
        grad.setColorAt(1.0, QColor("#001f4d"))
        painter.fillRect(0, 0, w, h, grad)

        # ---- VISN 22 Impact Team logo (top section) ----------------------
        logo_area_h = 180
        if not self._logo.isNull():
            scaled = self._logo.scaled(
                w - 20, logo_area_h,
                Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation,
            )
            logo_x = (w - scaled.width()) // 2
            painter.drawPixmap(logo_x, 10, scaled)
        else:
            # Fallback text title when the image asset is missing
            title_font = QFont("Segoe UI", 15, QFont.Weight.Bold)
            painter.setFont(title_font)
            painter.setPen(QColor("#FFFFFF"))
            painter.drawText(
                QRect(20, 20, w - 40, 50),
                Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignVCenter,
                "VA HCPCS Fee Schedule Manager",
            )
            sub_font = QFont("Segoe UI", 9)
            painter.setFont(sub_font)
            painter.setPen(QColor("#7AADDD"))
            painter.drawText(
                QRect(20, 70, w - 40, 24),
                Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignVCenter,
                "VISN 22 Impact Team",
            )

        # ---- app sub-title ----------------------------------------------
        sub_font = QFont("Segoe UI", 9)
        painter.setFont(sub_font)
        painter.setPen(QColor("#7AADDD"))
        painter.drawText(
            QRect(20, logo_area_h + 14, w - 40, 22),
            Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignVCenter,
            "VA HCPCS Fee Schedule Manager  •  DMEPOS Lookup Tool",
        )

        # ---- step message -----------------------------------------------
        msg_font = QFont("Segoe UI", 9)
        painter.setFont(msg_font)
        painter.setPen(QColor("#CCDDEE"))
        painter.drawText(
            QRect(20, logo_area_h + 40, w - 40, 22),
            Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignVCenter,
            self._step_msg,
        )

        # ---- progress bar track -----------------------------------------
        bar_x = 30
        bar_y = logo_area_h + 72
        bar_w = w - 60
        bar_h = 16
        painter.setPen(QColor("#0A2A50"))
        painter.setBrush(QColor("#0A2A50"))
        painter.drawRoundedRect(QRect(bar_x, bar_y, bar_w, bar_h), 5, 5)

        # ---- progress bar fill ------------------------------------------
        if self._pct > 0:
            fill_w = max(bar_h, int(bar_w * self._pct / 100))
            fill_grad = QLinearGradient(bar_x, 0, bar_x + fill_w, 0)
            fill_grad.setColorAt(0.0, QColor("#3399FF"))
            fill_grad.setColorAt(1.0, QColor("#66BBFF"))
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(fill_grad)
            painter.drawRoundedRect(QRect(bar_x, bar_y, fill_w, bar_h), 5, 5)

        # ---- percentage text --------------------------------------------
        pct_font = QFont("Segoe UI", 8)
        painter.setFont(pct_font)
        painter.setPen(QColor("#8AB8D8"))
        painter.drawText(
            QRect(bar_x, bar_y + bar_h + 6, bar_w, 18),
            Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignVCenter,
            f"{self._pct}%",
        )


def main():
    app = QApplication(sys.argv)
    app.setApplicationName("VA HCPCS Fee Schedule Manager")
    app.setOrganizationName("VISN 22 Impact Team")
    app.setOrganizationDomain("va.gov")
    app.setFont(QFont("Segoe UI", 10))

    # ---- App icon (taskbar, window title bar, Alt-Tab, .exe) -------------
    icon_path = _asset("app_icon.png")
    if icon_path.exists():
        app.setWindowIcon(QIcon(str(icon_path)))

    splash = _ProgressSplash()
    splash.show()
    app.processEvents()

    # ---- Step 1: database -----------------------------------------------
    splash.set_progress(10, "Initializing database…")
    init_db()

    # ---- Step 2–5: build main window (it updates the splash internally) --
    splash.set_progress(25, "Building user interface…")
    window = MainWindow(splash=splash)
    from core.version import APP_VERSION
    window.setWindowTitle(f"VA HCPCS Fee Schedule Manager  v{APP_VERSION}")

    # ---- Center on the same screen the splash used ----------------------
    splash_screen = splash.screen()
    if splash_screen is not None:
        screen_geo = splash_screen.availableGeometry()
        win_geo = window.frameGeometry()
        win_geo.moveCenter(screen_geo.center())
        window.move(win_geo.topLeft())

    # ---- Close splash BEFORE showing the main window --------------------
    splash.close()
    window.show()

    # Defer the first-run check until after the event loop starts so the
    # main window is fully painted before the wizard appears.
    QTimer.singleShot(0, window._check_first_run)

    sys.exit(app.exec())

if __name__ == "__main__":
    main()