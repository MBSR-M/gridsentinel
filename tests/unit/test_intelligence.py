"""Source-time health, evidence scoring and customer-approved SLA semantics."""
from datetime import datetime,timedelta,timezone
import unittest
from gridsentinel.intelligence.health import derive_health
from gridsentinel.intelligence.rules import correlate_gateway,rank_causes,evaluate_sla,anomaly_summary


class IntelligenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.now=datetime(2026,1,1,12,tzinfo=timezone.utc)

    def row(self,kind: str,minutes: int,id: str="event") -> dict:
        return {"event_type":kind,"event_time":self.now+timedelta(minutes=minutes),"event_id":id}

    def test_health_arrival_order_does_not_regress(self) -> None:
        rows=[self.row("METER_COMM_FAILURE",-3,"a"),self.row("METER_COMM_SUCCESS",-2,"b"),self.row("METER_COMM_SUCCESS",-1,"c")]
        first=derive_health(rows,self.now);second=derive_health(list(reversed(rows)),self.now)
        self.assertEqual(first,second);self.assertEqual(first.current_status,"HEALTHY")

    def test_offline_failure_recovery_and_decommission(self) -> None:
        failures=[self.row("METER_COMM_FAILURE",i-4,str(i)) for i in range(3)]
        self.assertEqual(derive_health(failures,self.now).current_status,"OFFLINE")
        self.assertEqual(derive_health(failures+[self.row("METER_COMM_SUCCESS",-1)],self.now).current_status,"RECOVERING")
        self.assertEqual(derive_health([],self.now,installation_status="DECOMMISSIONED").current_status,"DECOMMISSIONED")

    def test_mdms_absence_only_delays_delivery(self) -> None:
        health=derive_health([self.row("METER_COMM_SUCCESS",-1)],self.now,missing_intervals=20)
        self.assertEqual(health.current_status,"DELAYED");self.assertEqual(health.delivery_status,"MISSING")
        self.assertEqual(derive_health([],self.now,missing_intervals=20).current_status,"UNKNOWN")

    def test_silence_requires_observed_communication_and_cadence(self) -> None:
        self.assertEqual(derive_health([],self.now).current_status,"UNKNOWN")
        self.assertEqual(derive_health([],self.now,last_seen_at=self.now-timedelta(hours=4)).current_status,"OFFLINE")

    def test_gateway_counts_unique_and_unknown_population(self) -> None:
        self.assertFalse(correlate_gateway(["a","a"],0,1,.5)["is_shared_failure"])
        self.assertEqual(correlate_gateway(["a","b","a"],2,2,.7)["affected_meter_count"],2)
        self.assertTrue(correlate_gateway(["a","b"],2,2,.7)["is_shared_failure"])

    def test_rca_contradictions_lower_support_and_unknown(self) -> None:
        first=rank_causes({"gateway_alarm":True,"shared_gateway_failure":True})[0]
        second=rank_causes({"gateway_alarm":True,"shared_gateway_failure":True,"device_heartbeat_ok":True})[0]
        self.assertLess(second["evidence_score"],first["evidence_score"]);self.assertEqual(first["status"],"SUSPECTED")
        self.assertEqual(rank_causes({})[0]["cause"],"UNKNOWN")

    def test_sla_requires_approval_and_excludes_estimates(self) -> None:
        self.assertIsNone(evaluate_sla(None,[])["kpi"])
        contract={"approval_status":"APPROVED","formula":"INTERVAL_DELIVERY_BY_DEADLINE","deadline_utc":"2026-01-01T12:00:00Z","target_ratio":.9,"excluded_meter_ids":["excluded"],"expected_eligible_intervals":2}
        rows=[{"meter_id":"physical","mdms_received_at":"2026-01-01T11:00:00Z","quality_status":"VALID","estimated_flag":False},
            {"meter_id":"estimated","mdms_received_at":"2026-01-01T11:00:00Z","quality_status":"VALID","estimated_flag":True},
            {"meter_id":"excluded","mdms_received_at":None,"quality_status":"VALID","estimated_flag":False}]
        result=evaluate_sla(contract,rows);self.assertEqual(result["kpi"],.5);self.assertEqual(result["estimated_intervals"],1)
        contract["expected_eligible_intervals"]=4
        result=evaluate_sla(contract,rows);self.assertEqual(result["kpi"],.25);self.assertEqual(result["unobserved_intervals"],2)

    def test_anomaly_constant_baseline(self) -> None:
        self.assertFalse(anomaly_summary([1,1,1],1)["anomaly"]);self.assertTrue(anomaly_summary([1,1,1],2)["anomaly"])
