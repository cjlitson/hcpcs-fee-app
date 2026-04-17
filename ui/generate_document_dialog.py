from __future__ import annotations

from datetime import date

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QApplication,
    QComboBox,
    QDateEdit,
    QDialog,
    QFileDialog,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
)

from core.document_generator import generate_purchase_document_docx
from core.email_helper import open_email_with_attachment
from core.vendor_store import list_saved_vendors, save_vendor_name


class GenerateDocumentDialog(QDialog):
    def __init__(self, purchase_items=None, parent=None):
        super().__init__(parent)
        self.purchase_items = purchase_items or []
        self.setWindowTitle("Generate Document")
        self.resize(980, 700)
        self._init_ui()

    def _init_ui(self):
        root = QVBoxLayout(self)
        root.setSpacing(8)
        form = QGridLayout()
        form.setVerticalSpacing(6)
        form.setHorizontalSpacing(8)

        self.veteran_last_name_edit = QLineEdit()
        self.veteran_last_name_edit.setFixedHeight(24)
        self.last4_edit = QLineEdit()
        self.last4_edit.setMaxLength(4)
        self.last4_edit.setFixedHeight(24)
        self.consult_date_edit = QDateEdit()
        self.consult_date_edit.setCalendarPopup(True)
        self.consult_date_edit.setDate(date.today())
        self.consult_date_edit.setFixedHeight(24)

        self.deliver_to_combo = QComboBox()
        self.deliver_to_combo.setFixedHeight(24)
        self.deliver_to_combo.addItems([
            "Veteran",
            "Prosthetics",
            "Other",
        ])

        self.deliver_to_other_edit = QLineEdit()
        self.deliver_to_other_edit.setFixedHeight(24)
        self.deliver_to_other_edit.setPlaceholderText("Specify delivery destination…")
        self.deliver_to_other_edit.setVisible(False)
        self.deliver_to_combo.currentTextChanged.connect(self._on_deliver_to_changed)

        # Vendor row with inline save button
        vendor_container = QHBoxLayout()
        vendor_container.setSpacing(6)
        self.vendor_combo = QComboBox()
        self.vendor_combo.setEditable(True)
        self.vendor_combo.setFixedHeight(24)
        self.vendor_combo.addItems(list_saved_vendors())
        vendor_container.addWidget(self.vendor_combo, 1)
        save_vendor_btn = QPushButton("Save")
        save_vendor_btn.setFixedHeight(24)
        save_vendor_btn.setFixedWidth(60)
        save_vendor_btn.setToolTip("Save this vendor to the quick-select list")
        save_vendor_btn.clicked.connect(self._save_vendor)
        vendor_container.addWidget(save_vendor_btn)

        # Format selector
        self.format_combo = QComboBox()
        self.format_combo.setFixedHeight(24)
        self.format_combo.addItem("Word Document (.docx)", "docx")
        self.format_combo.addItem("Excel Spreadsheet (.xlsx)", "xlsx")
        self.format_combo.addItem("PDF Document (.pdf)", "pdf")

        form.addWidget(QLabel("Veteran Last Name"), 0, 0)
        form.addWidget(self.veteran_last_name_edit, 0, 1)
        form.addWidget(QLabel("Last 4"), 0, 2)
        form.addWidget(self.last4_edit, 0, 3)
        form.addWidget(QLabel("Consult Date"), 1, 0)
        form.addWidget(self.consult_date_edit, 1, 1)
        deliver_to_container = QHBoxLayout()
        deliver_to_container.setSpacing(6)
        deliver_to_container.addWidget(self.deliver_to_combo)
        deliver_to_container.addWidget(self.deliver_to_other_edit, 1)
        form.addWidget(QLabel("Deliver To"), 1, 2)
        form.addLayout(deliver_to_container, 1, 3)
        form.addWidget(QLabel("Vendor"), 2, 0)
        form.addLayout(vendor_container, 2, 1, 1, 3)
        form.addWidget(QLabel("Format"), 3, 0)
        form.addWidget(self.format_combo, 3, 1)
        root.addLayout(form)

        self.items_table = QTableWidget(0, 4)
        self.items_table.setHorizontalHeaderLabels(["HCPCS Code", "Quantity", "Cost", "Description / Details"])
        self.items_table.verticalHeader().setVisible(False)
        self.items_table.horizontalHeader().setStretchLastSection(True)
        root.addWidget(self.items_table, 1)
        self._populate_items()

        root.addWidget(QLabel("Additional Comments"))
        self.comments_edit = QTextEdit()
        self.comments_edit.setPlaceholderText("Enter any additional notes for the generated document.")
        self.comments_edit.setMaximumHeight(80)
        root.addWidget(self.comments_edit)

        btns = QHBoxLayout()
        copy_btn = QPushButton("Copy Data")
        copy_btn.setFixedHeight(26)
        generate_btn = QPushButton("Generate Document")
        generate_btn.setFixedHeight(26)
        email_btn = QPushButton("Generate + Attach to Email")
        email_btn.setFixedHeight(26)
        cancel_btn = QPushButton("Cancel")
        cancel_btn.setFixedHeight(26)
        copy_btn.clicked.connect(self._copy_data)
        generate_btn.clicked.connect(lambda: self._generate(attach_to_email=False))
        email_btn.clicked.connect(lambda: self._generate(attach_to_email=True))
        cancel_btn.clicked.connect(self.reject)
        btns.addWidget(copy_btn)
        btns.addStretch()
        btns.addWidget(email_btn)
        btns.addWidget(generate_btn)
        btns.addWidget(cancel_btn)
        root.addLayout(btns)

    def _populate_items(self):
        for item in self.purchase_items:
            row = self.items_table.rowCount()
            self.items_table.insertRow(row)
            code_item = QTableWidgetItem(str(item.get("hcpcs_code", "")))
            code_item.setFlags(code_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.items_table.setItem(row, 0, code_item)
            self.items_table.setItem(row, 1, QTableWidgetItem(str(item.get("quantity", 1))))
            cost = item.get("unit_price")
            self.items_table.setItem(row, 2, QTableWidgetItem("" if cost is None else f"{float(cost):.2f}"))
            self.items_table.setItem(row, 3, QTableWidgetItem(str(item.get("description", ""))))

    def _on_deliver_to_changed(self, text: str):
        self.deliver_to_other_edit.setVisible(text == "Other")

    def _save_vendor(self):
        vendor = self.vendor_combo.currentText().strip()
        vendors = save_vendor_name(vendor)
        self.vendor_combo.blockSignals(True)
        self.vendor_combo.clear()
        self.vendor_combo.addItems(vendors)
        self.vendor_combo.setEditText(vendor)
        self.vendor_combo.blockSignals(False)
        if vendor:
            QMessageBox.information(self, "Vendor Saved", f"Saved vendor '{vendor}'.")

    def _collect_payload(self):
        items = []
        for row in range(self.items_table.rowCount()):
            code = (self.items_table.item(row, 0).text() if self.items_table.item(row, 0) else "").strip()
            qty_raw = (self.items_table.item(row, 1).text() if self.items_table.item(row, 1) else "1").strip()
            cost_raw = (self.items_table.item(row, 2).text() if self.items_table.item(row, 2) else "0").strip()
            desc = (self.items_table.item(row, 3).text() if self.items_table.item(row, 3) else "").strip()
            try:
                qty = max(1, int(float(qty_raw)))
            except ValueError:
                qty = 1
            try:
                cost = max(0.0, float(cost_raw.replace("$", "").replace(",", "")))
            except ValueError:
                cost = 0.0
            items.append({
                "hcpcs_code": code,
                "quantity": qty,
                "cost": cost,
                "description": desc,
            })

        return {
            "veteran_last_name": self.veteran_last_name_edit.text().strip(),
            "last4": self.last4_edit.text().strip(),
            "consult_date": self.consult_date_edit.date().toString("yyyy-MM-dd"),
            "deliver_to": (
                self.deliver_to_other_edit.text().strip()
                if self.deliver_to_combo.currentText() == "Other"
                else self.deliver_to_combo.currentText().strip()
            ),
            "vendor": self.vendor_combo.currentText().strip(),
            "comments": self.comments_edit.toPlainText().strip(),
            "items": items,
        }

    def _copy_data(self):
        payload = self._collect_payload()
        lines = [
            f"Veteran Last Name\t{payload['veteran_last_name']}",
            f"Last 4\t{payload['last4']}",
            f"Consult Date\t{payload['consult_date']}",
            f"Deliver To\t{payload['deliver_to']}",
            f"Vendor\t{payload['vendor']}",
            "",
            "HCPCS Code\tQuantity\tCost\tDescription / Details",
        ]
        for item in payload["items"]:
            lines.append(
                f"{item['hcpcs_code']}\t{item['quantity']}\t{item['cost']:.2f}\t{item['description']}"
            )
        if payload["comments"]:
            lines.extend(["", "Additional Comments", payload["comments"]])
        QApplication.clipboard().setText("\n".join(lines))
        QMessageBox.information(self, "Copied", "Document data copied to clipboard.")

    def _generate(self, attach_to_email: bool):
        if attach_to_email:
            warning = QMessageBox.warning(
                self,
                "PHI Encryption Required",
                "This attachment may contain PHI. You must encrypt the email before sending.\n\nContinue?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
                QMessageBox.StandardButton.Cancel,
            )
            if warning != QMessageBox.StandardButton.Yes:
                return

        selected_format = self.format_combo.currentData()
        ext_map = {
            "docx": ("Word Documents (*.docx)", "hcpcs_document.docx"),
            "xlsx": ("Excel Files (*.xlsx)", "hcpcs_document.xlsx"),
            "pdf": ("PDF Files (*.pdf)", "hcpcs_document.pdf"),
        }
        filter_text, default_name = ext_map.get(selected_format, ext_map["docx"])

        path, _ = QFileDialog.getSaveFileName(self, "Save Generated Document", default_name, filter_text)
        if not path:
            return

        payload = self._collect_payload()
        try:
            if selected_format == "docx":
                from core.document_generator import generate_purchase_document_docx
                generate_purchase_document_docx(path, payload)
            elif selected_format == "xlsx":
                from core.document_generator import generate_purchase_document_excel
                generate_purchase_document_excel(path, payload)
            elif selected_format == "pdf":
                from core.document_generator import generate_purchase_document_pdf
                generate_purchase_document_pdf(path, payload)
        except Exception as exc:
            QMessageBox.critical(self, "Generate Error", f"Document generation failed:\n{exc}")
            return

        if attach_to_email:
            ok, msg = open_email_with_attachment(
                path,
                subject="HCPCS Generated Document",
                body="Please ensure email encryption is enabled before sending this PHI-containing attachment.",
            )
            if not ok:
                QMessageBox.warning(
                    self,
                    "Email Attachment",
                    f"Document generated at:\n{path}\n\n{msg}\nPlease attach it manually.",
                )
                return

        QMessageBox.information(self, "Generate Complete", f"Document generated successfully:\n{path}")
        self.accept()
