# Reporting database boundary

Migration005 installs safe empty reporting views with a sentinel tenant predicate.
Run `python scripts/provision_reporting.py` after migrations and bootstrap to bind
all nine views to the configured customer tenant and grant gs_reporter SELECT on
only those views. The script uses the mounted root secret inside the MySQL container;
the secret never appears in command arguments or output. An explicit `--tenant`
argument must be a validated identifier. Re-provisioning changes the isolation
boundary and is a trusted deployment-administrator operation.

Grafana has independent authentication and roles. Custom application RBAC does
not grant Grafana access. Anonymous access/signup/embedding are disabled in Compose;
Grafana listens on loopback for the development pilot. Grant Viewer access only to
approved operators. Production requires separately validated TLS/access proxy and
Grafana datasource TLS CA configuration. Grafana authentication/access isolation is
not a shared multi-tenant authorization system.

The views expose aggregate health, intervals, incidents, prediction summaries,
durable jobs, extraction checkpoints, and measured heartbeats. They omit user data,
audit rows, consumer mappings, meter locations, log text, raw payloads and model
artifacts. All view predicates bind the same deployment tenant. SQL SECURITY DEFINER
lets the reporter read approved projections without access to underlying tables.
The provisioning script resets reporter privileges to exactly these nine SELECT
grants, so a separate deployment is required for another customer.

Queue panels show durable MySQL job counts rather than claiming unmeasured RabbitMQ
queue depth. Kafka lag is the recorded consumer observation and remains NULL when
unavailable. Delivery charts exclude estimated readings from physical receipt
latency. Incident latency is an observation, not contractual SLA compliance; missing
approved contracts are visible. Prediction confidence is reported as stored and does
not imply accuracy on production data. Views are pilot aggregate queries; larger
fleets require measured query plans and materialized reporting aggregates.

Provisioning follows the official
[Grafana provisioning documentation](https://grafana.com/docs/grafana/latest/administration/provisioning/)
and [MySQL datasource configuration](https://grafana.com/docs/grafana/latest/datasources/mysql/configure/).
Mounted development passwords are generated with URL-safe characters; this avoids
the provider expansion ambiguity documented for passwords containing `$`.
