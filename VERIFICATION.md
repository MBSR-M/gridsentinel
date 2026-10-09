# Verification evidence

Verified locally on Windows, Docker Desktop and Python 3.12 on 2026-10-08. These results cover the local pilot; they do not certify fleet scale or production security.

## Automated checks

The final host run discovered **144 tests: 143 passed, 1 skipped, no failures** in 28.432 seconds. All live-suite switches were enabled: MySQL identity/operations, HTTP workflow, Kafka/Redis, RabbitMQ, reporting and Grafana. The skipped Windows symlink test requires OS permission; a portable root-resolution check passed, and the original symlink check passed in Linux.

The application image ran **112 unit/security tests: 108 passed, 4 skipped, no failures** in 0.957 seconds on container Python 3.12.12. Its four skips are the deliberately disabled live MySQL security tests, which passed in the host live run. JavaScript syntax checks passed for every UI module. The development Nginx configuration passed `nginx -t`. The full production Compose override parses for all profiles; production TLS runtime is not qualified.

Security evidence includes denied permission paths, cross-tenant and CSRF rejection, live assignment scope with mixed roles, refresh-family reuse/revocation, expired/disabled accounts, API-token scope and issuance receipt secrecy, audit rollback and concurrent audit chain serialization. Socket checks cover malformed framing, duplicate keys/headers, size limits, URI/query limits, static-root containment, bounded admission and released capacity.

Live telemetry checks include duplicate suppression, transaction rollback/replay, old topology selection, interval absence without false communication outage, silence expiration, historical-hour recomputation, incident ownership/actions, model approval/activation/rollback, prediction storage and corrupted-artifact fallback. Broker checks include confirmed outbox publication, repeated Kafka records, poison dead-letter references before offset commit, persistent Rabbit delivery, retry TTL, terminal DLQ, lease recovery and stale-worker fencing. Redis namespace, expiration, corrupt-cache and connection-failure fallback were checked.

## Running deployment and browser

MySQL, Kafka, RabbitMQ, Redis, API and web UI reported healthy. Scheduler, background worker and log collector are running; Grafana is running and its MySQL datasource health check passed. The demo processed **7,004 generated events** for **1,000 meters**. Synthetic labels are visible in the overview.

The supplied local account was used to sign in successfully. All thirteen navigation workspaces rendered against the API. The meter detail displayed recorded communication history and effective-dated topology, with explicit model unavailability when no approved model is active. The default narrow layout and the desktop breakpoint were inspected. No horizontal overflow was observed at the tested desktop breakpoint. [Dashboard proof](docs/screenshots/overview.png) was captured from the actual running app. This is a smoke check, not a full accessibility/device certification.

Grafana's nine dashboards and all 27 panel queries executed. Anonymous access was denied. Its reporting account could read only nine approved views, could not read users/sessions/audit/raw facts/model registry/log tables, and excluded a second tenant.

## Backup restoration

A 3,860,027-byte compressed mysqldump was restored into a newly created, separate `gridsentinel-recovery` Compose project. Checksummed migrations validated, the restored `demo` tenant contained **1,000 meters and 5,804 communication facts**, and its **20-record audit chain verified**. The recovery MySQL container was stopped afterward; its separate volume is retained. The development database was not overwritten. This establishes local restoration behavior; it does not establish a production RPO/RTO, encrypted off-host backup policy, or full broker recovery procedure.

## Measurements

| Scope | Workload | Measured result | Limit |
| --- | --- | --- | --- |
| Canonical validation | 10,000 unique synthetic records | 4,160.87 records/s; p95 0.2231 ms; traced peak 361,815 bytes | No MySQL or broker work |
| Committed MySQL operations | 1,000 unique synthetic records, isolated tenant | 39.036 records/s; p95 35.8437 ms; p99 50.7974 ms; traced peak 3,077,132 bytes | One local process, state/history/aggregate transactions; no distributed pipeline qualification |
| Proxy HTTP liveness | 100 serial requests, 50 ms between requests | 100 successes; p50 3.9717 ms; p95 27.2116 ms; p99 45.3658 ms | Liveness only, excludes authenticated analytics and ingestion |

Raw measurements are in `artifacts/benchmark_ingestion.json`, `artifacts/benchmark_mysql.json`, and `artifacts/benchmark_api.json`. Memory values are Python tracemalloc peaks, not total system RSS. A bounded logical 10M fleet sample is in `artifacts/logical_10m_sample.jsonl`; its existence is not measured ten-million-meter capacity.

Single-broker failover, database failover, sustained authenticated concurrent loads, complete process/crash chaos, customer vendor feeds, complete production TLS, actual notification delivery, operational ML accuracy and a 100K/10M fleet acceptance test remain unverified. See [delivery scope](DELIVERY.md) for the complete remaining work.
