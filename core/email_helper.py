from __future__ import annotations

import os
import subprocess
from pathlib import Path


def open_email_with_attachment(file_path: str, subject: str, body: str) -> tuple[bool, str]:
    path = Path(file_path)
    if not path.exists():
        return False, "Generated file was not found."

    if os.name != "nt":
        return False, "Automatic email attachment is only supported on Windows with Outlook installed."

    escaped_path = str(path).replace("'", "''")
    escaped_subject = (subject or "").replace("'", "''")
    escaped_body = (body or "").replace("'", "''")
    script = (
        "$outlook = New-Object -ComObject Outlook.Application; "
        "$mail = $outlook.CreateItem(0); "
        f"$mail.Subject = '{escaped_subject}'; "
        f"$mail.Body = '{escaped_body}'; "
        f"$mail.Attachments.Add('{escaped_path}'); "
        "$mail.Display();"
    )
    try:
        subprocess.run(
            ["powershell", "-NoProfile", "-Command", script],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return True, ""
    except Exception:
        return False, "Could not open Outlook with the attachment automatically."
