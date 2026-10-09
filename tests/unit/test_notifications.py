"""Verify SMTP boundaries without contacting or sending mail to anyone."""

import ssl
import smtplib
import unittest
from unittest.mock import Mock, patch

from gridsentinel.core.exceptions import ConfigurationError, InfrastructureError, ValidationError
from gridsentinel.services.alerts import NotificationService


class NotificationTests(unittest.TestCase):
    def setUp(self):
        self.config = Mock()
        values = {"enabled": True, "approved_recipients": ["operations@example.invalid"],
                  "sender": "sentinel@example.invalid", "smtp_host": "smtp.example.invalid",
                  "smtp_port": 587, "username": ""}
        for name in ("get", "get_bool", "get_int", "get_list", "get_required"):
            getattr(self.config, name).side_effect = lambda section, option, *default: values.get(option, default[0] if default else None)
        self.service = NotificationService(self.config)
        self.job = {"job_id": "9c5d0313-e72c-44f3-a502-190cb49bf401",
                    "payload": {"subject": "Synthetic incident", "body": "Test evidence only"}}

    @patch("gridsentinel.services.alerts.smtplib.SMTP")
    def test_disabled_delivery_never_contacts_smtp(self, smtp):
        self.config.get_bool.side_effect = None
        self.config.get_bool.return_value = False
        with self.assertRaises(ConfigurationError):
            self.service.send(self.job)
        smtp.assert_not_called()

    @patch("gridsentinel.services.alerts.smtplib.SMTP")
    def test_destination_override_and_header_injection_are_rejected(self, smtp):
        for payload in ({"subject": "x", "body": "x", "to": "attacker@example.invalid"},
                        {"subject": "x\r\nBcc: attacker@example.invalid", "body": "x"}):
            with self.subTest(payload=payload), self.assertRaises(ValidationError):
                self.service.send({**self.job, "payload": payload})
        smtp.assert_not_called()

    @patch("gridsentinel.services.alerts.smtplib.SMTP")
    def test_verified_tls_and_configured_destinations(self, smtp):
        client = smtp.return_value.__enter__.return_value
        client.send_message.return_value = {}
        result = self.service.send(self.job)
        context = client.starttls.call_args.kwargs["context"]
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(context.check_hostname)
        message = client.send_message.call_args.args[0]
        self.assertEqual(message["To"], "operations@example.invalid")
        self.assertEqual(client.ehlo.call_count, 2)
        self.assertEqual(result["accepted_recipient_count"], 1)
        self.assertIn(self.job["job_id"], result["message_id"])

    @patch("gridsentinel.services.alerts.smtplib.SMTP")
    def test_transport_retry_and_authentication_failure_are_distinct(self, smtp):
        client = smtp.return_value.__enter__.return_value
        client.send_message.side_effect = OSError("connection lost")
        with self.assertRaises(InfrastructureError):
            self.service.send(self.job)
        client.send_message.side_effect = smtplib.SMTPAuthenticationError(535, b"rejected")
        with self.assertRaises(ConfigurationError):
            self.service.send(self.job)


if __name__ == "__main__":
    unittest.main()
