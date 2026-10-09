"""Approved-recipient SMTP delivery using standard-library TLS and mail APIs."""

from __future__ import annotations

from email.message import EmailMessage
import re
import smtplib
import ssl
from typing import Any

from gridsentinel.core.exceptions import ConfigurationError, InfrastructureError, ValidationError


class NotificationService:
    """Send only to deployment-approved recipients when explicitly configured.

    SMTP does not support a general transactional idempotency guarantee. A stable
    Message-ID helps downstream deduplication but a lost acknowledgment may
    duplicate delivery. Notification delivery is never required to commit telemetry.
    """

    def __init__(self, config: Any) -> None:
        self.config = config

    def send(self, job: dict[str, Any]) -> dict[str, Any]:
        """Use configured SMTP TLS; reject destination and header injection."""
        if not self.config.get_bool("notifications", "enabled", False):
            raise ConfigurationError("Notification delivery has not been configured")
        recipients = self.config.get_list("notifications", "approved_recipients", [])
        sender = self.config.get_required("notifications", "sender")
        if not 1 <= len(recipients) <= 20 or any(not re.fullmatch(r"[^\s@<>]+@[^\s@<>]+", item) for item in [sender, *recipients]):
            raise ConfigurationError("Approved SMTP sender/recipients are invalid")
        payload = job["payload"]
        if "recipients" in payload or "to" in payload:
            raise ValidationError("Notification recipients are controlled by deployment configuration")
        subject, body = payload.get("subject"), payload.get("body")
        if not isinstance(subject, str) or not 1 <= len(subject) <= 128 or any(c in subject for c in "\r\n"):
            raise ValidationError("Invalid notification subject")
        if not isinstance(body, str) or len(body.encode()) > 16384:
            raise ValidationError("Invalid notification body")
        message = EmailMessage()
        message["From"], message["To"], message["Subject"] = sender, ", ".join(recipients), subject
        message["Message-ID"] = "<" + job["job_id"] + "@gridsentinel.local>"
        message.set_content(body)
        context = ssl.create_default_context()
        host, port = self.config.get_required("notifications", "smtp_host"), self.config.get_int("notifications", "smtp_port", 587)
        try:
            with smtplib.SMTP(host, port, timeout=15) as client:
                client.ehlo()
                client.starttls(context=context)
                client.ehlo()
                username = self.config.get("notifications", "username", "")
                if username:
                    client.login(username, self.config.get_required("notifications", "password"))
                refused = client.send_message(message)
                if refused:
                    raise InfrastructureError("SMTP did not accept all configured recipients")
        except smtplib.SMTPAuthenticationError as error:
            raise ConfigurationError("SMTP authentication failed") from error
        except (OSError, smtplib.SMTPException) as error:
            raise InfrastructureError("SMTP delivery is unavailable") from error
        return {"message_id": str(message["Message-ID"]), "accepted_recipient_count": len(recipients)}
