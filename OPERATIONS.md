# Operations

Use the development Compose file pair consistently. Containers run the application as a non-root user, use read-only roots and explicit writable volumes, and retain database/broker state in named volumes. Migrations use checksums and a database lock; never edit an applied migration. Add a new numbered migration. If an existing container loses its service alias, recreating that service with the same Compose files restores DNS without deleting its volume. Never run `down -v` against data you need.

## Sources and recovery

Enable a named source in `config/connectors.ini` only after supplying its actual file, approved HTTPS endpoint, or read-only MySQL source configuration. Canonical JSONL/CSV and INI field mappings are implemented. HTTPS source requests reject redirects and unapproved hosts. MySQL extraction uses explicit watermark and source ID ordering; naive source timestamps require an explicit source timezone.

File checkpoints are physical line numbers advanced after confirmed Kafka publication. Invalid records go to a confirmed dead-letter reference before the checkpoint advances. Source shard identity must change when replacing a source file; reusing a checkpoint for different content is unsafe. `scripts/run_backfill.py --help` describes bounded replay. Kafka manual offsets advance after committed ingestion effects; poison records retain digest and broker coordinates in the dead-letter reference. Durable deduplication suppresses repeated effects. Deduplication entries are conservatively retained until an approved replay horizon is supplied.

Kafka topics can be provisioned with `python -m gridsentinel provision-topics`. Existing partition layouts are not silently changed. The local single-broker deployment is not highly available. Profile `pipelines` adds telemetry consumers and operational workers; `connectors` adds configured source polling workers. A metadata entry created in the dashboard does not enable a source worker: deployment INI configuration controls source execution.

## Jobs and notifications

MySQL stores job state and publication outboxes; RabbitMQ uses persistent messages, publisher confirms, manual acknowledgments, TTL retry tiers and a dead-letter queue. Leases include fencing generations; expired jobs can be recovered. Cancellation and manual retry are audited. Implemented handlers cover reports, exports, model training, incident evidence enrichment and approved-recipient SMTP. Unsupported job types fail visibly; deployment-specific integration retry, model evaluation and external audit-anchor handlers are not implemented.

SMTP is disabled by default. Configure an approved sender/recipient list and verified STARTTLS endpoint before enabling it. Job payloads cannot choose destinations. Stable Message-ID values help downstream duplicate suppression; SMTP acknowledgment loss can still duplicate mail. No notification was sent during development.

## Logs and retention

Each process emits structured stdout and bounded local spool files. The collector writes validated records idempotently into application_logs before removing a spool item. This shared-volume transport supports the local stack; multi-host deployments need an approved transport and backpressure policy. Monitor heartbeat error and spool-drop counters.

The scheduler deletes bounded batches for operational logs, communication facts, interval facts and hourly/daily aggregates according to INI policy. Audit, incident, model and deduplication archival are not automatically deleted. Define approved archival/retention and verify foreign-key effects before changing policies. Health silence is derived from expected cadence and grace; historical event-time aggregation is recomputed for late records. Customer SLA denominators must be configured explicitly; unknown coverage remains UNKNOWN.

## Grafana

Bind the approved reporting views to the deployment tenant, then start Grafana:

```powershell
py -3.12 scripts/provision_reporting.py --tenant demo --compose-file docker-compose.yml --compose-file docker-compose.dev.yml
docker compose -f docker-compose.yml -f docker-compose.dev.yml --profile observability up -d grafana
```

Open http://localhost:3000. The independent administrator is `gs_grafana_admin`; its generated password is in `.local/secrets/grafana_password`. Nine provisioned dashboards query 27 panels through a restricted MySQL reporting account. Grafana reads its password through a mounted secret file. Views are bound to one configured tenant; separate deployments need separate reporting identities/views. Never grant access to base identity or payload tables to support a chart.

## Backup and restore

```powershell
py -3.12 scripts/backup.py .local/backups/demo.sql.gz
py -3.12 scripts/restore.py --help
```

Backup streams mysqldump into an atomic compressed file and reads the mounted secret inside the container. Restore requires an explicitly named separate Compose project and a confirmation flag. Preserve broker replay history as well as SQL state. Do not claim an RPO/RTO until a timed restoration and post-restore audit/data verification have passed. Encrypt and access-control backup files under deployment policy.

Use `/api/v1/system/readiness` for database readiness and authenticated system metrics for actual service heartbeats, pool state, job counts and outbox backlog. Missing observations remain missing; dashboard values do not imply deployment capacity. For shutdown, use `docker compose -f docker-compose.yml -f docker-compose.dev.yml stop`; retain volumes.

The production override requires a Compose version supporting `!override`, verified certificates, deployment-specific broker TLS/CA configuration and approved API origins. Every application role, including migration and profile workers, enters production mode. It does not configure TLS servers or broker ACLs for the bundled development brokers. Set `GS_API_ALLOWED_ORIGINS` to the reviewed HTTPS domain; do not combine the development port override with production.

The isolated restore check can be repeated with `python scripts/verify_restore.py --tenant demo --expected-meters 1000` inside the recovery application's configured container. See VERIFICATION.md for the restoration already executed and its limits.

Frontend imports share a release version query so every page uses the same API session module. Update the HTML entry and all module release tags together when publishing a UI revision. The web server requests revalidation so a reload can pick up changed assets.
