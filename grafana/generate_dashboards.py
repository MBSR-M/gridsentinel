"""Generate the nine checked-in dashboards from explicit approved-view queries."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

DATASOURCE = {"type": "mysql", "uid": "gs-reporting"}


def panel(identifier: int, title: str, kind: str, sql: str,
          x: int, y: int, width: int = 12, unit: str = "short") -> dict[str, Any]:
    """Build a native Grafana panel with a finite query and honest no-data state."""
    result = {"id": identifier, "title": title, "type": kind,
              "datasource": DATASOURCE, "gridPos": {"x": x, "y": y, "w": width, "h": 8},
              "targets": [{"refId": "A", "datasource": DATASOURCE, "rawQuery": True,
                           "rawSql": sql, "format": "time_series" if kind == "timeseries" else "table"}],
              "fieldConfig": {"defaults": {"unit": unit, "noValue": "Not observed",
                                            "color": {"mode": "palette-classic"}}, "overrides": []},
              "options": {}}
    if kind == "stat":
        result["options"] = {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                             "textMode": "auto", "colorMode": "value", "graphMode": "none"}
    elif kind == "piechart":
        result["options"] = {"pieType": "donut", "displayLabels": ["name", "percent"],
                             "reduceOptions": {"values": True, "calcs": ["lastNotNull"], "fields": ""},
                             "legend": {"displayMode": "table", "placement": "right"}}
    elif kind == "table":
        result["options"] = {"showHeader": True, "cellHeight": "sm"}
    elif kind == "timeseries":
        result["options"] = {"legend": {"displayMode": "list", "placement": "bottom"},
                             "tooltip": {"mode": "multi"}}
    return result


def definitions() -> list[tuple[str, str, str, list[dict[str, Any]]]]:
    """Return dashboards tied only to the nine approved reporting projections."""
    return [
        ("fleet-health", "Fleet Health", "Observed meter states. Communication score is based on stored observations; unknown meters remain visible.", [
            panel(1, "Registered meters", "stat", "SELECT SUM(meter_count) AS registered_meters FROM v_fleet_health", 0, 0),
            panel(2, "Active meters", "stat", "SELECT SUM(active_meter_count) AS active_meters FROM v_fleet_health", 12, 0),
            panel(3, "Health distribution", "piechart", "SELECT health_status,meter_count FROM v_fleet_health ORDER BY health_status", 0, 8),
            panel(4, "Observed health details", "table", "SELECT health_status,meter_count,communication_score,observed_at FROM v_fleet_health ORDER BY meter_count DESC LIMIT 20", 12, 8),
        ]),
        ("region-health", "Regional Operations", "Aggregate region health. No consumer mappings or meter coordinates are exposed.", [
            panel(1, "Meters by region and status", "table", "SELECT region_name,health_status,meter_count,communication_score,observed_at FROM v_region_health ORDER BY region_name,health_status LIMIT 500", 0, 0, 24),
            panel(2, "Offline meters by region", "table", "SELECT region_name,SUM(meter_count) AS offline_meters FROM v_region_health WHERE health_status='OFFLINE' GROUP BY region_name ORDER BY offline_meters DESC LIMIT 100", 0, 8),
            panel(3, "Regions with observations", "stat", "SELECT COUNT(DISTINCT region_name) AS regions FROM v_region_health", 12, 8),
        ]),
        ("delivery-latency", "HES and MDMS Delivery", "Latency from interval end to physical receipt. Estimated intervals are counted separately and excluded from latency.", [
            panel(1, "HES receipt latency", "timeseries", "SELECT observed_at AS time,source_system AS metric,mean_hes_latency_seconds AS value FROM v_delivery_latency WHERE $__timeFilter(observed_at) ORDER BY time LIMIT 10000", 0, 0, unit="s"),
            panel(2, "MDMS receipt latency", "timeseries", "SELECT observed_at AS time,source_system AS metric,mean_mdms_latency_seconds AS value FROM v_delivery_latency WHERE $__timeFilter(observed_at) ORDER BY time LIMIT 10000", 12, 0, unit="s"),
            panel(3, "Recorded estimated intervals", "timeseries", "SELECT observed_at AS time,SUM(estimated_intervals) AS estimated_intervals FROM v_delivery_latency WHERE $__timeFilter(observed_at) GROUP BY observed_at ORDER BY time LIMIT 10000", 0, 8),
            panel(4, "Recorded missing intervals", "timeseries", "SELECT observed_at AS time,SUM(observed_missing_intervals) AS missing_intervals FROM v_delivery_latency WHERE $__timeFilter(observed_at) GROUP BY observed_at ORDER BY time LIMIT 10000", 12, 8),
        ]),
        ("ingestion-operations", "Ingestion Operations", "Durable handoff checkpoints and MySQL outbox backlog. Missing telemetry is not represented as zero Kafka lag.", [
            panel(1, "Durable connector checkpoints", "table", "SELECT component,stream,last_handoff_at,watermark FROM v_ingestion_operations WHERE component<>'outbox' ORDER BY component,stream LIMIT 100", 0, 0, 24),
            panel(2, "Pending outbox events", "table", "SELECT stream,pending_events,published_events,last_handoff_at FROM v_ingestion_operations WHERE component='outbox' ORDER BY pending_events DESC LIMIT 100", 0, 8, 24),
        ]),
        ("queue-health", "Background Queue Health", "Durable job states in MySQL. These counts are not RabbitMQ queue-depth measurements. Exhausted jobs remain visible.", [
            panel(1, "Jobs by queue and state", "table", "SELECT queue,status,durable_jobs,max_attempt_number,expired_leases,oldest_created_at FROM v_queue_health ORDER BY queue,status LIMIT 100", 0, 0, 24),
            panel(2, "Exhausted jobs", "stat", "SELECT SUM(exhausted_jobs) AS exhausted_jobs FROM v_queue_health", 0, 8),
            panel(3, "Expired running leases", "stat", "SELECT SUM(expired_leases) AS expired_leases FROM v_queue_health", 12, 8),
        ]),
        ("platform-health", "Platform Health", "Persisted worker heartbeats. Broker metrics remain absent until a worker records them. Heartbeat age indicates stale observations.", [
            panel(1, "Service heartbeat status", "table", "SELECT service_name,instance_id,status,observed_at,heartbeat_age_seconds FROM v_platform_health ORDER BY service_name LIMIT 100", 0, 0, 24),
            panel(2, "Recorded consumer lag", "table", "SELECT service_name,instance_id,observed_kafka_lag,processed_events,invalid_events FROM v_platform_health WHERE observed_kafka_lag IS NOT NULL ORDER BY service_name LIMIT 100", 0, 8),
            panel(3, "Dropped buffered debug logs", "table", "SELECT service_name,instance_id,dropped_debug_logs FROM v_platform_health ORDER BY service_name LIMIT 100", 12, 8),
        ]),
        ("error-trends", "Application Error Trends", "Counts of sanitized warning/error records. Message bodies and stack traces are excluded from Grafana.", [
            panel(1, "Errors by service", "timeseries", "SELECT observed_at AS time,service_name AS metric,SUM(error_count) AS value FROM v_error_trends WHERE $__timeFilter(observed_at) GROUP BY observed_at,service_name ORDER BY time LIMIT 10000", 0, 0, 24),
            panel(2, "Exception groups", "table", "SELECT service_name,level,exception_type,SUM(error_count) AS recorded_errors FROM v_error_trends WHERE $__timeFilter(observed_at) GROUP BY service_name,level,exception_type ORDER BY recorded_errors DESC LIMIT 100", 0, 8, 24),
        ]),
        ("prediction-monitoring", "Recovery Prediction Monitoring", "Stored prediction volume, values and confidence. Synthetic evaluation is not proof of real-world accuracy.", [
            panel(1, "Prediction volume", "timeseries", "SELECT observed_at AS time,prediction_type AS metric,SUM(prediction_count) AS value FROM v_prediction_monitoring WHERE $__timeFilter(observed_at) GROUP BY observed_at,prediction_type ORDER BY time LIMIT 10000", 0, 0, 24),
            panel(2, "Prediction summaries", "table", "SELECT observed_at,prediction_type,model_id,prediction_count,mean_predicted_value,mean_confidence FROM v_prediction_monitoring WHERE $__timeFilter(observed_at) ORDER BY observed_at DESC LIMIT 500", 0, 8, 24),
        ]),
        ("incident-sla", "Incident and SLA Operations", "Observed acknowledgement/resolution latency. Contractual compliance is unknown without an approved customer SLA definition.", [
            panel(1, "Incident volume", "timeseries", "SELECT observed_at AS time,severity AS metric,SUM(incident_count) AS value FROM v_incident_sla WHERE $__timeFilter(observed_at) GROUP BY observed_at,severity ORDER BY time LIMIT 10000", 0, 0, 24),
            panel(2, "Observed response times", "table", "SELECT severity,status,SUM(incident_count) AS incidents,AVG(mean_ack_minutes) AS acknowledgement_minutes,AVG(mean_resolution_minutes) AS resolution_minutes,MAX(oldest_open_minutes) AS oldest_open_minutes FROM v_incident_sla WHERE $__timeFilter(observed_at) GROUP BY severity,status ORDER BY severity,status LIMIT 100", 0, 8, 24),
            panel(3, "Approved SLA contracts", "stat", "SELECT MAX(approved_sla_contracts) AS approved_sla_contracts FROM v_incident_sla", 0, 16),
            panel(4, "Suspected causes", "table", "SELECT suspected_root_cause,SUM(incident_count) AS incidents FROM v_incident_sla WHERE $__timeFilter(observed_at) GROUP BY suspected_root_cause ORDER BY incidents DESC LIMIT 20", 12, 16),
        ]),
    ]


def generate() -> None:
    """Write stable, editable-source dashboards without hosted libraries."""
    directory = Path(__file__).resolve().parent / "dashboards"
    directory.mkdir(parents=True, exist_ok=True)
    for slug, title, description, panels in definitions():
        dashboard = {"uid": "gs-" + slug, "title": "GridSentinel | " + title,
                     "description": description, "schemaVersion": 39, "version": 1,
                     "editable": False, "tags": ["gridsentinel", "pilot", "tenant-isolated"],
                     "timezone": "utc", "refresh": "30s", "time": {"from": "now-24h", "to": "now"},
                     "templating": {"list": []}, "annotations": {"list": []},
                     "panels": panels, "links": [{"title": "GridSentinel Admin", "type": "link", "url": "http://localhost:8080", "targetBlank": True}]}
        (directory / (slug + ".json")).write_text(json.dumps(dashboard, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    generate()
