from __future__ import annotations

import os
import subprocess
import base64
from pathlib import Path


def open_email_with_attachment(file_path: str, subject: str, body: str) -> tuple[bool, str]:
    path = Path(file_path)
    if not path.exists():
        return False, "Generated file was not found."

    if os.name != "nt":
        return False, "Automatic email attachment is only supported on Windows with Outlook installed."

    script = (
        "$outlook = New-Object -ComObject Outlook.Application; "
        "$mail = $outlook.CreateItem(0); "
        "$mail.Subject = $env:HCPCS_MAIL_SUBJECT; "
        "$mail.Body = $env:HCPCS_MAIL_BODY; "
        "$mail.Attachments.Add($env:HCPCS_MAIL_ATTACHMENT); "
        "$mail.Display();"
    )
    encoded = base64.b64encode(script.encode("utf-16le")).decode("ascii")
    try:
        env = os.environ.copy()
        env["HCPCS_MAIL_SUBJECT"] = subject or ""
        env["HCPCS_MAIL_BODY"] = body or ""
        env["HCPCS_MAIL_ATTACHMENT"] = str(path)
        subprocess.run(
            ["powershell", "-NoProfile", "-EncodedCommand", encoded],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
        )
        return True, ""
    except Exception:
        return False, "Could not open Outlook with the attachment automatically."
