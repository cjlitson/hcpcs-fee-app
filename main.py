import sys
from PyQt6.QtWidgets import QApplication, QSplashScreen
from PyQt6.QtGui import QFont, QPixmap, QColor, QPainter, QLinearGradient
from PyQt6.QtCore import Qt, QTimer, QRect
from core.database import init_db
from ui.main_window import MainWindow


class _ProgressSplash(QSplashScreen):
    """Splash screen with a live step-label and animated progress bar.

    Call ``set_progress(pct, message)`` (0–100) to update the display.
    ``QApplication.processEvents()`` is called automatically so the repaint
    is visible even before the event loop starts.
    """

    _SPLASH_W = 520
    _SPLASH_H = 260

    def __init__(self):
        pix = QPixmap(self._SPLASH_W, self._SPLASH_H)
        pix.fill(QColor("#003366"))
        super().__init__(pix, Qt.WindowType.WindowStaysOnTopHint)
        self._pct = 0
        self._step_msg = "Starting up…"

    # ------------------------------------------------------------------
    def set_progress(self, pct: int, message: str) -> None:
        """Update the progress bar and step message, then force a repaint."""
        self._pct = max(0, min(100, pct))
        self._step_msg = message
        self.repaint()
        QApplication.processEvents()

    # ------------------------------------------------------------------
    def drawContents(self, painter: QPainter) -> None:  # noqa: N802
        """Paint title, step label, progress bar, and percentage text."""
        w = self._SPLASH_W
        h = self._SPLASH_H

        # ---- background gradient (subtle depth) -------------------------
        grad = QLinearGradient(0, 0, 0, h)
        grad.setColorAt(0.0, QColor("#004080"))
        grad.setColorAt(1.0, QColor("#001f4d"))
        painter.fillRect(0, 0, w, h, grad)

        # ---- app title ---------------------------------------------------
        title_font = QFont("Segoe UI", 15, QFont.Weight.Bold)
        painter.setFont(title_font)
        painter.setPen(QColor("#FFFFFF"))
        painter.drawText(
            QRect(20, 28, w - 40, 48),
            Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignVCenter,
            "VA HCPCS Fee Schedule Manager",
        )

        # ---- subtitle / version tag -------------------------------------
        sub_font = QFont("Segoe UI", 9)
        painter.setFont(sub_font)
        painter.setPen(QColor("#7AADDD"))
        painter.drawText(
            QRect(20, 76, w - 40, 24),
            Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignVCenter,
            "DMEPOS Fee Schedule Lookup Tool",
        )

        # ---- step message -----------------------------------------------
        msg_font = QFont("Segoe UI", 9)
        painter.setFont(msg_font)
        painter.setPen(QColor("#CCDDEE"))
        painter.drawText(
            QRect(20, 118, w - 40, 24),
            Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignVCenter,
            self._step_msg,
        )

        # ---- progress bar track -----------------------------------------
        bar_x, bar_y, bar_w, bar_h = 30, 154, w - 60, 16
        painter.setPen(QColor("#0A2A50"))
        painter.setBrush(QColor("#0A2A50"))
        painter.drawRoundedRect(QRect(bar_x, bar_y, bar_w, bar_h), 5, 5)

        # ---- progress bar fill ------------------------------------------
        if self._pct > 0:
            fill_w = max(bar_h, int(bar_w * self._pct / 100))  # min width = height so corners round properly
            fill_grad = QLinearGradient(bar_x, 0, bar_x + fill_w, 0)
            fill_grad.setColorAt(0.0, QColor("#3399FF"))
            fill_grad.setColorAt(1.0, QColor("#66BBFF"))
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(fill_grad)
            painter.drawRoundedRect(QRect(bar_x, bar_y, fill_w, bar_h), 5, 5)

        # ---- percentage text below bar ----------------------------------
        pct_font = QFont("Segoe UI", 8)
        painter.setFont(pct_font)
        painter.setPen(QColor("#8AB8D8"))
        painter.drawText(
            QRect(bar_x, bar_y + bar_h + 6, bar_w, 18),
            Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignVCenter,
            f"{self._pct}%",
        )

        # ---- bottom tagline ----------------------------------------------
        tag_font = QFont("Segoe UI", 7)
        painter.setFont(tag_font)
        painter.setPen(QColor("#4466AA"))
        painter.drawText(
            QRect(20, h - 22, w - 40, 18),
            Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignVCenter,
            "WSNC IMPACT Team",
        )


def main():
    app = QApplication(sys.argv)
    app.setApplicationName("VA HCPCS Fee Schedule Manager")
    app.setOrganizationName("WSNC IMPACT Team")
    app.setOrganizationDomain("va.gov")
    app.setFont(QFont("Segoe UI", 10))

    splash = _ProgressSplash()
    splash.show()
    app.processEvents()

    # ---- Step 1: database -----------------------------------------------
    splash.set_progress(10, "Initializing database…")
    init_db()

    # ---- Step 2–5: build main window (it updates the splash internally) --
    splash.set_progress(25, "Building user interface…")
    window = MainWindow(splash=splash)

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