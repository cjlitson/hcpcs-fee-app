import sys
from PyQt6.QtWidgets import QApplication, QSplashScreen, QLabel
from PyQt6.QtGui import QFont, QPixmap, QColor
from PyQt6.QtCore import Qt, QTimer
from core.database import init_db
from ui.main_window import MainWindow

def _make_splash(app):
    """Create and return a simple splash screen with a loading message."""
    # Build a pixmap for the splash (dark VA blue background, white text)
    pix = QPixmap(480, 200)
    pix.fill(QColor("#003366"))

    splash = QSplashScreen(pix, Qt.WindowType.WindowStaysOnTopHint)
    splash.setFont(QFont("Segoe UI", 12))

    # Title
    splash.showMessage(
        "VA HCPCS Fee Schedule Manager\n\nLoading, please wait…",
        Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignBottom,
        QColor("white"),
    )
    splash.show()
    app.processEvents()
    return splash

def main():
    app = QApplication(sys.argv)
    app.setApplicationName("VA HCPCS Fee Schedule Manager")
    app.setOrganizationName("WSNC IMPACT Team")
    app.setOrganizationDomain("va.gov")
    app.setFont(QFont("Segoe UI", 10))

    splash = _make_splash(app)

    # Initialise the database while the splash is visible
    init_db()
    app.processEvents()

    window = MainWindow()

    # Center the main window on the same screen the splash used so that
    # multi-monitor setups don't produce a split-screen startup experience.
    splash_screen = splash.screen()
    if splash_screen is not None:
        screen_geo = splash_screen.availableGeometry()
        win_geo = window.frameGeometry()
        win_geo.moveCenter(screen_geo.center())
        window.move(win_geo.topLeft())

    # Close the splash BEFORE showing the main window so they never overlap.
    splash.close()
    window.show()

    # Defer the first-run check until after the event loop starts so the
    # main window is fully painted before the wizard appears.
    QTimer.singleShot(0, window._check_first_run)

    sys.exit(app.exec())

if __name__ == "__main__":
    main()