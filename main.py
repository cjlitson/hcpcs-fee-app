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

    # Show the main window then close the splash after a short delay so
    # users can see it transition rather than an abrupt switch.
    window.show()
    QTimer.singleShot(400, splash.close)

    sys.exit(app.exec())

if __name__ == "__main__":
    main()