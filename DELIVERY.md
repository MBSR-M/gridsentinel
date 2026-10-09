# Delivery scope

This is a working local pilot with real persistence and brokers. The master engineering request is not fully complete. Customer data contracts, a measured 100K-meter pilot, and production deployment acceptance remain required. The implementation deliberately labels synthetic data and unknown metrics.

| Phase | Working result and evidence | Remaining qualification |
| --- | --- | --- |
| 1 Foundation | Strict immutable configuration, redacted structured logging, exceptions, retry, bounded MySQL pool, five checksummed migrations, Docker startup, readiness and unit checks | Multi-host log transport, complete production privilege separation |
| 2 Identity | Users, roles, live grants, hashed sessions, refresh reuse detection, API scopes, CSRF, assignment restrictions, serialized audit; isolated and live security checks | Enterprise SSO, independent audit anchoring and external security review |
| 3 Ingestion | File/CSV/JSONL, approved HTTPS and ordered MySQL adapters, INI normalization, confirmed Kafka, transaction dedup, checkpoints, poison references and replay checks | Actual HES/MDMS/GIS/network protocol schemas and customer replay horizon; live source acceptance |
| 4 Operations | Event-time health/history, effective-dated topology, late recomputation, gateway correlation, supporting and contradictory RCA evidence, incident actions, separate communication/MDMS quality, configured SLA coverage | Customer cadence/SLA contract, authoritative topology/population, operator RCA outcome validation |
| 5 ML | Pure-Python historical baseline and logistic SGD, censoring, purged temporal evaluation, JSON artifact checksums, approval/activation/rollback, persisted predictions and drift API; live registry checks | Customer labels, vendor/region holdouts, scheduled drift population collection, shadow/canary acceptance, operational effectiveness studies |
| 6 UI | Thirteen permission-aware workspaces, persisted API observations, meter/incident details, governance, admin, audit, logs, sessions and tokens; browser navigation verified | Broader accessibility/device qualification, GIS map assets and customer GIS coordinates |
| 7 Observability | Nine Grafana dashboards with 27 executed SQL panels, separate authenticated login, nine tenant-bound SELECT-only views, worker heartbeats and queue/outbox counts; SMTP handler tested without sending mail | Approved SMTP endpoint and automatic incident routing, Grafana alert/contact-point policy, complete connector/broker resource telemetry |
| 8 Resilience | Real broker confirmations, duplicates, poison commit ordering, lease fencing, retry/DLQ, cache failure, transactional rollback, replay and isolated backup restoration; benchmark scripts and runbooks | Full broker/database outage chaos, distributed failover, authenticated concurrent load, retention/archival acceptance, production TLS/ACL/penetration qualification |

## Scale evidence

The demonstration processed 7,004 generated observations for 1,000 meters. A bounded sample can represent a logical fleet of 10 million IDs; that is not a ten-million-meter database or load test. The single-process committed MySQL benchmark measured about 39 unique observations/second with recomputation enabled. At 72 observations per meter per day, a 100K-meter fleet alone averages about 83 observations/second before bursts. The current measurement does not qualify that target. The architecture's 10M sizing is a planning model only.

## Integration inputs required

Provide source schemas and identities, approved endpoints/TLS/credentials, vendor code mappings, source timezone and checkpoint contract, authoritative effective-dated topology/populations, customer cadence and SLA denominators, retention/archival/replay policy, and approved recovery episode labels. Source workers remain disabled until configured. Unknown classifications, coverage and model availability remain explicit.

Only implemented job handlers execute. Model evaluation jobs, integration-specific retry and external audit anchors fail as unsupported rather than reporting invented success. Configuration is edited and deployed through INI files; the dashboard shows redacted configuration and connector metadata, not arbitrary live configuration mutation. Overview JSON export is implemented; a complete scheduled report catalog and retrieval/archive policy are not.

No third-party notifications were sent. No production deployment, public release, Git commit or pull request was created.
