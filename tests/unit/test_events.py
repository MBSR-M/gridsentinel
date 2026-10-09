"""Canonical boundaries and durable connector handoff contracts."""
from datetime import datetime,timedelta,timezone
import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock
from gridsentinel.core.exceptions import ValidationError
from gridsentinel.domain.events import stamp,validate_event
from gridsentinel.connectors.adapters import FileConnector,HttpConnector,VendorNormalizer,handoff_batch,MySQLIncrementalConnector


def event() -> dict:
    now=datetime.now(timezone.utc)-timedelta(minutes=2)
    return {"schema_version":"1.0","event_id":"source-1","event_type":"METER_COMM_SUCCESS",
        "source_system":"test","tenant_id":"tenant","meter_id":"meter","event_time_utc":stamp(now),
        "received_at_utc":stamp(now+timedelta(seconds=1)),"ingested_at_utc":stamp(now+timedelta(seconds=2)),
        "correlation_id":"corr-1","trace_id":"trace-1","payload":{"status":"SUCCESS"}}


class EventTests(unittest.TestCase):
    def test_deterministic_dedup_and_immutable_payload(self) -> None:
        raw=event();a=validate_event(raw);b=validate_event(dict(reversed(list(raw.items()))))
        self.assertEqual(a.serialize(),b.serialize());self.assertEqual(a.deduplication_key,b.deduplication_key)
        a.payload["status"]="FAILED";self.assertEqual(a.payload["status"],"SUCCESS")
        raw["tenant_id"]="another";self.assertNotEqual(a.deduplication_key,validate_event(raw).deduplication_key)

    def test_timestamp_schema_and_finite_limits(self) -> None:
        for changes in ({"schema_version":"2.0"},{"event_time_utc":"2026-01-01T00:00:00"},
                        {"event_time_utc":"2026-01-01T01:00:00+01:00"},{"unknown":True},
                        {"payload":{"value":float("nan")}}, {"source_sequence":True}):
            raw=event();raw.update(changes)
            with self.subTest(changes=changes),self.assertRaises(ValidationError):validate_event(raw)
        with self.assertRaises(ValidationError):validate_event(event(),max_bytes=10)

    def test_future_clock_skew_and_late_legitimate_event(self) -> None:
        raw=event();raw["event_time_utc"]=stamp(datetime.now(timezone.utc)+timedelta(days=1))
        with self.assertRaises(ValidationError):validate_event(raw)
        raw=event();raw["event_time_utc"]="2020-01-01T00:00:00Z";validate_event(raw)

    def test_unknown_vendor_code_rejected(self) -> None:
        normalizer=VendorNormalizer("tenant","test",{"event_id":"id","meter_id":"meter","event_time_utc":"at","event_code":"code"},{"OK":"METER_COMM_SUCCESS"})
        raw={"id":"one","meter":"meter","at":event()["event_time_utc"],"code":"BAD"}
        with self.assertRaises(ValidationError):normalizer.normalize(raw)
        raw["code"]="OK";self.assertEqual(normalizer.normalize(raw)["event_type"],"METER_COMM_SUCCESS")

    def test_stream_file_resume_and_size(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"events.jsonl";path.write_text('{"a":1}\n{"a":2}\n',encoding="utf-8")
            self.assertEqual(list(FileConnector(path,max_records=1).records(1)),[(2,{"a":2})])
            with self.assertRaises(ValidationError):list(FileConnector(path,max_record_bytes=2).records())

    def test_checkpoint_never_advances_after_failed_handoff(self) -> None:
        publish=Mock(side_effect=ConnectionError("offline"));checkpoint=Mock()
        with self.assertRaises(ConnectionError):handoff_batch(iter([(1,{})]),publish,checkpoint)
        checkpoint.assert_not_called()

    def test_http_host_and_mysql_identifiers(self) -> None:
        for url in ("http://source.test/a","https://evil.test/a","https://user:secret@source.test/a"):
            with self.assertRaises(ValidationError):HttpConnector(url,["source.test"])
        with self.assertRaises(ValidationError):MySQLIncrementalConnector(Mock(),"events;DROP",["id","at"],"at","id")
