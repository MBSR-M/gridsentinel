# Operational data dictionary

MySQL 8.4 tables use InnoDB, `utf8mb4_0900_as_cs`, case-sensitive identifiers and
UTC `DATETIME(6)` values. Public timestamps have an explicit `Z` suffix. Tenant IDs
scope primary/unique keys, foreign keys, source envelopes and all repository
queries. Foreign keys use matching unsigned identity IDs and composite tenant
relationships where applicable. These tables are not date-partitioned; no
unsupported partitioned foreign-key design is claimed.

| Table | Key and important fields | Purpose |
| --- | --- | --- |
| tenants | tenant_id | Isolated deployment/customer namespace |
| regions | tenant_id, region_id; parent_region_id | Geographic hierarchy without precise coordinates |
| meters | tenant_id, meter_id; installation_status, expected_communication_minutes | Asset registry and explicit/configured communication cadence |
| network_devices | tenant_id, device_id; status, last_event_at | Latest observed gateway/DCU condition; absent attributes remain null |
| network_links | link_id; parent/child, valid_from/to | Effective-dated device links |
| meter_topology_history | mapping_id; meter, gateway/DCU/carrier/region, valid_from/to | Historical asset association; ingestion locks a meter and rejects conflicting ranges |
| event_deduplication | tenant_id, namespace, idempotency_key | Durable source identity; SHA-256 of tenant/source/event ID |
| communication_events | tenant_id, event_id; source_event_id, event_time, received_at, evidence_json | Retained normalized communication/alarm facts; incident-relevant raw references |
| interval_delivery_status | tenant_id, meter_id, interval_start | Received/late/missing/estimated delivery facts and source-time corrections |
| meter_current_health | tenant_id, meter_id | Independent communication state and data-delivery indicators |
| meter_health_hourly | tenant_id, meter_id, hour_start | Recomputed observed communication successes/failures, late successes and missing interval facts |
| meter_health_daily | tenant_id, meter_id, day_start | Daily observed counts; contractual availability/outage duration remain null without an approved derivation |
| gateway_health_snapshots | tenant_id, gateway_id, window_start | Historical correlation population and concentration observations |
| incidents | incident_id; tenant_id/group unique | Operational case with suspected cause and explicit rule-support confidence |
| incident_evidence | evidence_id; incident/source reference unique | Preserved source-time evidence and ranked candidate explanations |
| incident_history | history_id; before/after, actor, correlation | Append-only case changes, including evidence corrections |
| incident_actions | action_id; actor, result | Acknowledgment, assignment, resolution and operator confirmation outcomes |
| connector_checkpoints | tenant_id, connector_id, source_shard | Resumable source watermark; checkpoint follows durable handoff |
| outbox_events | outbox_id; topic, partition_key, payload, published_at | Durable outgoing telemetry/job publication in the originating transaction |
| sla_contracts | tenant_id, contract_id; approval_status, definition_json | Explicitly approved customer delivery KPI definition |
| ml_feature_definitions | feature_name/version unique | Versioned feature metadata |
| ml_training_runs | training_run_id; dataset/validation boundaries, owner, metrics | Provenance, configuration snapshot and durable job idempotency |
| ml_model_registry | model_id; tenant/name/version unique | Safe JSON model artifact, integrity digest and approval |
| ml_model_deployments | tenant_id, model_name | Atomic active and previous model pointers |
| ml_predictions | prediction_id; tenant/entity/time indexed | Observed recovery probabilities, feature snapshot and explanation |
| ml_prediction_outcomes | tenant_id, prediction_id | Follow-up recovery and intervention evidence |
| ml_drift_metrics | drift_id; tenant/model/time indexed | Persisted reference-window PSI observations |

Identity, roles, sessions, API tokens, login throttling, audit heads/events and
anchors are defined in `001_foundation.sql`. Connector definitions, durable jobs
and application logs are defined in `004_platform.sql`. Full field types, keys
and constraints are the authoritative migration SQL under `database/migrations`.

## Canonical schema 1.0

Every envelope requires source event identity, event type, source system, tenant,
explicit UTC event/receipt/ingestion times, correlation and trace IDs, and an
object payload. Meter telemetry requires `meter_id`; network events require an
explicit gateway/DCU instead. Optional source sequence and raw payload reference
are bounded. Unknown envelope fields, unmapped event codes, non-finite numbers,
oversized payloads, invalid interval ranges and excessive future clock skew are
rejected before a transaction. Vendor mappings are explicit per-vendor INI data.

JSON serialization is deterministic. Deduplication binds tenant, source and
source event ID, independently of delivery order. `METER_SILENT` describes a
schedule observation after observed communication exceeded configured cadence
and grace; it is not a source timeout. Estimated intervals remain distinct from
physical receipts. MDMS missing data can make delivery `MISSING` and overall
health `DELAYED`; it cannot alone make communication `OFFLINE`.

## Transaction and replay behavior

One ingest transaction commits deduplication, source facts, source-time health,
hourly/daily correction, correlation evidence and outbox publication intents.
Rollback removes all these effects. Broker offsets/checkpoints advance after
that durable boundary. Replaying a committed source ID returns `duplicate=true`.
This is at-least-once delivery with idempotent effects; it does not claim
end-to-end exactly-once processing.

Late legitimate events are accepted. Health is recomputed in deterministic
event-time/sequence/source-ID order over the configured bounded recent run;
last observed communication is independently preserved beyond detailed-event
retention. The affected event hour and day are recomputed, and incident topology
joins use each source event time. A new topology mapping can close the preceding
open range; an overlap with an existing closed range is rejected. Historical
incident evidence remains available when the current association changes.

Source APIs are authorized HTTPS host endpoints with redirect refusal, timeouts
and response/page limits. Incremental MySQL extraction uses read-only source
credentials, an allowlisted identifier query and a timestamp-plus-ID watermark.
Naive source timestamps require an explicit configured source timezone. JSONL
and one-record-per-line CSV are streamed with record limits; multiline CSV
exports need conversion before this pilot adapter can accept them.

## SLA contract boundary

The implemented contractual formula identifier is
`INTERVAL_DELIVERY_BY_DEADLINE`. An approved definition must provide its UTC
deadline, customer target ratio, expected eligible interval count **after
exclusions**, accepted quality flags and explicit excluded meter IDs when
applicable. Estimated intervals never count as physically received. The
approved expected population includes missing/unobserved intervals, so a sparse
source table cannot inflate the denominator. No approved contract returns
`UNCONFIGURED` with null KPI/risk. Other formulas require implementation and
customer review. The pilot bounds direct SLA evaluation to 10,000 rows; wider
fleet windows require approved aggregated eligibility calculations.

## Retention and scale

Raw HES/MDMS histories remain in approved source systems. Normalized detail,
rolling state, bounded interval reconciliation and hourly/daily aggregates are
retained locally according to configured policy. Incident evidence, source
deduplication and audit retention need a coordinated customer replay/retention
policy. Deleting deduplication before a possible replay can repeat old effects.

Logical 10M-meter synthetic generation streams a bounded sample by default. The
local validation and MySQL benchmark artifacts measure one local process with
synthetic input; they are not broker throughput, production fleet capacity or
proof that this unpartitioned pilot schema supports 10M physical meters.
