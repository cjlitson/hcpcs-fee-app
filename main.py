import sys
from PyQt6.QtWidgets import QApplication
from PyQt6.QtGui import QFont
from core.database import init_db
from ui.main_window import MainWindow


def main():
    init_db()
    app = QApplication(sys.argv)
    app.setApplicationName("VA HCPCS Fee Schedule Manager")
    app.setOrganizationName("VA")

    font = QFont("Segoe UI", 10)
    app.setFont(font)

    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
