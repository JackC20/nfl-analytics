"""
Email PNG charts, shown inline in the message body so they open straight to
the pictures on a phone.

Sends through Gmail with an app password (Google account -> Security ->
2-Step Verification -> App passwords). The password is read from:
  - inside Databricks: the secret nfl/gmail_app_password
  - on your laptop:    the GMAIL_APP_PASSWORD environment variable
"""

from __future__ import annotations

import html
import os
import smtplib
import ssl
from email.message import EmailMessage
from email.utils import make_msgid
from pathlib import Path

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 465  # implicit TLS
SECRET_SCOPE = "nfl"
SECRET_KEY = "gmail_app_password"


def app_password() -> str:
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        from databricks.sdk.runtime import dbutils

        return dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY)
    password = os.environ.get("GMAIL_APP_PASSWORD")
    if not password:
        raise SystemExit("Set GMAIL_APP_PASSWORD to send email from your laptop.")
    return password


def send_charts(images: list[Path], to: str, subject: str, intro: str = "", sender: str | None = None) -> None:
    """Email the images inline, in order. Sends from `sender`, or from `to` itself."""
    sender = sender or to
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = to
    msg.set_content(intro or subject)  # plain-text fallback for clients that block HTML

    # Each image is referenced from the HTML by a Content-ID, so it renders in
    # the body instead of as an attachment to open.
    cids = [make_msgid(domain="nfl-charts") for _ in images]
    body = "".join(
        f'<p style="margin:0 0 16px"><img src="cid:{cid[1:-1]}" alt="{html.escape(path.stem)}" '
        f'style="width:100%;max-width:1000px;height:auto"></p>'
        for cid, path in zip(cids, images)
    )
    lead = f'<p style="font-family:sans-serif;color:#52514e">{html.escape(intro)}</p>' if intro else ""
    msg.add_alternative(f"<html><body>{lead}{body}</body></html>", subtype="html")

    html_part = msg.get_payload()[1]
    for cid, path in zip(cids, images):
        html_part.add_related(
            path.read_bytes(), "image", "png", cid=cid, disposition="inline", filename=path.name,
        )

    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ssl.create_default_context(), timeout=30) as smtp:
        smtp.login(sender, app_password())
        smtp.send_message(msg)
    print(f"Emailed {len(images)} chart(s) to {to}")
