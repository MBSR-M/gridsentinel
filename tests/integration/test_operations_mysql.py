"""Opt-in live transactional telemetry, workflow and ML registry verification."""
from datetime import datetime,timedelta,timezone
import os
from pathlib import Path
import secrets
import sys
import unittest
from unittest.mock import patch
import uuid
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/"scripts"))
from generate_meter_data import iter_episodes
from gridsentinel.core.configuration import ConfigManager
from gridsentinel.core.exceptions import AuthorizationError,ConflictError,ValidationError
from gridsentinel.domain.events import stamp
from gridsentinel.infrastructure.mysql import Database
from gridsentinel.services.audit import AuditService
from gridsentinel.services.auth import AuthService
from gridsentinel.processing.operations import OperationsService
from gridsentinel.ml.service import ModelService


@unittest.skipUnless(os.environ.get("GS_RUN_INTEGRATION")=="1","requires configured live MySQL")
class OperationsMySQLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        root=Path(__file__).resolve().parents[2]
        cls.config=ConfigManager(root/"config",overrides={"incident.minimum_cluster_meter_count":2,"incident.shared_gateway_failure_ratio":.5,"ml.minimum_training_samples":100})
        cls.database=Database(cls.config);cls.database.migrate(root/"database"/"migrations")
        cls.audit=AuditService(cls.database);cls.auth=AuthService(cls.database,cls.config,cls.audit)
        cls.tenant="telemetry-test-"+uuid.uuid4().hex[:12]
        password=secrets.token_urlsafe(32);cls.auth.bootstrap_admin("telemetry-verifier",password,cls.tenant)
        credentials=cls.auth.login("telemetry-verifier",password,cls.tenant,"127.0.0.1","telemetry-tests","suite-login")
        cls.principal=cls.auth.authenticate(credentials["access_token"])
        cls.operations=OperationsService(cls.database,cls.config,cls.audit);cls.models=ModelService(cls.database,cls.config,cls.audit)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.database.close()

    def setUp(self) -> None:
        self.meter="SYN-TEST-"+uuid.uuid4().hex[:12];self.now=datetime.now(timezone.utc)

    def event(self,kind: str="METER_COMM_SUCCESS",at: datetime|None=None,payload: dict|None=None,meter: str|None="default") -> dict:
        at=at or self.now-timedelta(minutes=1);event_id=uuid.uuid4().hex
        return {"schema_version":"1.0","event_id":event_id,"event_type":kind,"source_system":"synthetic_test",
            "tenant_id":self.tenant,"meter_id":self.meter if meter=="default" else meter,"event_time_utc":stamp(at),
            "received_at_utc":stamp(at+timedelta(seconds=1)),"ingested_at_utc":stamp(at+timedelta(seconds=2)),
            "correlation_id":event_id,"trace_id":event_id,"payload":payload or {}}

    def test_ingest_dedup_atomic_rollback_and_queries(self) -> None:
        row=self.event();first=self.operations.ingest(row);second=self.operations.ingest(row)
        self.assertFalse(first["duplicate"]);self.assertTrue(second["duplicate"])
        self.assertEqual(first["health"]["current_status"],"HEALTHY")
        with self.database.read() as connection:
            count=connection.fetch_one("SELECT COUNT(*) AS count FROM outbox_events WHERE tenant_id=%s AND aggregate_id=%s",(self.tenant,self.meter))["count"]
        self.assertEqual(count,2)
        failed=self.event()
        with patch.object(self.operations,"_outbox",side_effect=RuntimeError("injected crash")),self.assertRaises(RuntimeError):
            self.operations.ingest(failed)
        self.assertFalse(self.operations.ingest(failed)["duplicate"])
        self.assertEqual(self.operations.meter_detail(self.tenant,self.meter)["health"]["current_status"],"HEALTHY")
        self.assertEqual(self.operations.overview(self.tenant)["data_mode"],"synthetic")
        for kind in ("communication","regions","gateways","sla","data-quality"):
            self.assertIsInstance(self.operations.analytics(self.tenant,kind,{}),dict)
        self.assertTrue(self.operations.list_meters(self.tenant,{"q":self.meter})["items"])

    def test_late_event_uses_historical_topology_and_no_overlap(self) -> None:
        start=self.now-timedelta(hours=1)
        self.operations.ingest(self.event("TOPOLOGY_UPDATE",start,{"valid_from":stamp(start),"gateway_id":"SYN-GW-OLD"}))
        moved=self.now-timedelta(minutes=2)
        self.operations.ingest(self.event("TOPOLOGY_UPDATE",moved,{"valid_from":stamp(moved),"gateway_id":"SYN-GW-NEW"}))
        old=self.event("METER_COMM_FAILURE",self.now-timedelta(minutes=4));self.operations.ingest(old)
        with self.database.read() as connection:
            saved=connection.fetch_one("SELECT gateway_id FROM communication_events WHERE tenant_id=%s AND source_event_id=%s",(self.tenant,old["event_id"]))
        self.assertEqual(saved["gateway_id"],"SYN-GW-OLD")
        self.assertEqual(self.operations.meter_detail(self.tenant,self.meter)["health"]["gateway_id"],"SYN-GW-NEW")
        with self.assertRaises(ConflictError):self.operations.ingest(self.event("TOPOLOGY_UPDATE",start,{"valid_from":stamp(start),"gateway_id":"SYN-CONFLICT"}))

    def test_mdms_absence_cannot_mark_offline(self) -> None:
        self.operations.ingest(self.event())
        result=self.operations.ingest(self.event("INTERVAL_MISSING",payload={"interval_start_utc":stamp(self.now-timedelta(minutes=30)),"interval_end_utc":stamp(self.now),"estimation_flag":False}))
        self.assertEqual(result["health"]["current_status"],"DELAYED")

    def test_silent_meter_sweep_and_late_hour_recompute(self) -> None:
        old=self.now-timedelta(hours=5)
        success=self.event(at=old)
        # A worker resuming after a long pause sees the stored previously healthy
        # state, then expires it using the independently observed communication.
        self.operations.ingest(success)
        with self.database.transaction() as connection:
            connection.execute("UPDATE meter_current_health SET current_status='HEALTHY' WHERE tenant_id=%s AND meter_id=%s",(self.tenant,self.meter))
        result=self.operations.sweep(self.tenant)
        self.assertGreaterEqual(result["changed"],1)
        self.assertEqual(self.operations.meter_detail(self.tenant,self.meter)["health"]["current_status"],"OFFLINE")
        newest=self.event(at=self.now-timedelta(minutes=1));self.operations.ingest(newest)
        late=self.event(at=old+timedelta(seconds=1));self.operations.ingest(late)
        with self.database.read() as connection:
            row=connection.fetch_one("SELECT on_time_reads FROM meter_health_hourly WHERE tenant_id=%s AND meter_id=%s AND hour_start=%s",(self.tenant,self.meter,old.replace(tzinfo=None,minute=0,second=0,microsecond=0)))
        self.assertEqual(row["on_time_reads"],2)

    def test_gateway_incident_workflow_and_assignment_scope(self) -> None:
        gateway="SYN-GW-"+uuid.uuid4().hex[:8]
        for index in range(2):
            meter=self.meter+str(index)
            self.operations.ingest(self.event("TOPOLOGY_UPDATE",payload={"valid_from":stamp(self.now-timedelta(hours=1)),"gateway_id":gateway},meter=meter))
            result=self.operations.ingest(self.event("METER_COMM_FAILURE",payload={"gateway_id":gateway},meter=meter))
        incident=result["incident_id"];self.assertTrue(incident)
        self.operations.incident_action(self.principal,incident,"acknowledge",{},"ack")
        self.operations.incident_action(self.principal,incident,"assign",{"assigned_to":self.principal["user_id"]},"assign")
        rows=self.operations.list_incidents(self.tenant,{"assigned_to":self.principal["user_id"]})["items"]
        self.assertIn(incident,[row["incident_id"] for row in rows])
        self.assertFalse(self.operations.list_incidents(self.tenant,{"assigned_to":999999999})["items"])
        assigned={**self.principal,"incident_scope":"assigned","user_id":999999999}
        with self.assertRaises(AuthorizationError):self.operations.incident_action(assigned,incident,"resolve",{"resolution":"test"},"denied")
        self.operations.incident_action(self.principal,incident,"resolve",{"resolution":"Synthetic verification completed"},"resolve")
        self.assertEqual(self.operations.incident_detail(self.tenant,incident)["status"],"RESOLVED")
        self.assertTrue(self.audit.verify(self.tenant)["valid"])

    def test_model_training_approval_activation_and_corruption(self) -> None:
        run_id=str(uuid.uuid4());body={"episodes":list(iter_episodes(120)),"data_source":"synthetic","training_run_id":run_id}
        result=self.models.train(self.tenant,body,self.principal,"train");model=result["model_id"]
        self.assertTrue(self.models.train(self.tenant,body,self.principal,"repeat")["duplicate"])
        with self.assertRaises(ConflictError):self.models.model_action(self.principal,model,"activate",{},"denied")
        self.models.model_action(self.principal,model,"approve",{"reason":"Synthetic algorithm verification only"},"approve")
        self.models.model_action(self.principal,model,"activate",{},"activate")
        second=self.models.train(self.tenant,{"episodes":list(iter_episodes(120,seed=43)),"data_source":"synthetic"},self.principal,"train-next")["model_id"]
        self.models.model_action(self.principal,second,"approve",{"reason":"Synthetic rollback verification"},"approve-next")
        self.models.model_action(self.principal,second,"activate",{},"activate-next")
        self.models.model_action(self.principal,model,"rollback",{},"rollback")
        with self.database.read() as connection:
            active=connection.fetch_one("SELECT active_model_id FROM ml_model_deployments WHERE tenant_id=%s AND model_name='recovery_probability'",(self.tenant,))
        self.assertEqual(active["active_model_id"],model)
        features=next(iter_episodes())["features"]
        self.operations.ingest(self.event())
        prediction=self.models.infer(self.tenant,self.meter,features,"RF");self.assertTrue(prediction["available"])
        self.assertTrue(self.models.predictions(self.tenant,self.meter)["items"])
        with self.database.transaction() as connection:
            connection.execute("UPDATE ml_model_registry SET artifact_checksum=%s WHERE tenant_id=%s AND model_id=%s",("0"*64,self.tenant,model))
        self.assertFalse(self.models.infer(self.tenant,self.meter,features)["available"])
        with self.assertRaises(ValidationError):self.models.model_action(self.principal,model,"activate",{},"corrupt")
