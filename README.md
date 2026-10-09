# GridSentinel Enterprise

A working local pilot for smart meter operations: tenant-scoped authentication, ingestion, meter health, shared outage evidence, incidents, SLA reporting, model training and approval, durable jobs, audit trails, a vanilla JavaScript dashboard, and Grafana. Python 3.12 uses the standard library and four runtime libraries: PyMySQL, kafka-python, pika, and redis.

The current demonstration contains 1,000 **synthetic** meters. This repository has **not** been qualified for 100,000 or 10 million meters, public production deployment, or customer-specific vendor protocols. See [delivery scope](DELIVERY.md), [verification](VERIFICATION.md), [architecture](ARCHITECTURE.md), and [security](SECURITY.md) before deployment.

## Start a local deployment

Use Docker Compose and Python 3.12. Run from the repository directory:

```powershell
py -3.12 scripts/init_secrets.py
docker compose -f docker-compose.yml -f docker-compose.dev.yml build api
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d mysql migrate
docker compose -f docker-compose.yml -f docker-compose.dev.yml run --rm api python scripts/bootstrap.py --username YOUR_USERNAME --demo
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --no-build
```

Bootstrap prompts for a password and creates the first administrator only when one does not already exist. Tenant ID defaults to `demo`; there is no built-in password. Secrets are generated in ignored `.local/secrets/` files. The optional `--demo` processes generated observations through the real ingestion service.

Open **http://localhost:8080**. The local account provisioned during development is `pilotadmin`, tenant `demo`; its generated password is in `.local/secrets/pilot_admin_password`. Keep this file private. This account is specific to the existing local database and is not recreated on a fresh installation.

Always use both Compose files for this development stack. `docker-compose.dev.yml` publishes broker/database ports on loopback for integration tests. The base Compose file exposes only the application proxy. [Operations](OPERATIONS.md) covers connectors, jobs, monitoring, backup and restore.

## Verification

```powershell
py -3.12 -m venv .venv
.venv/Scripts/python.exe -m pip install --no-deps -r requirements.txt
$env:PYTHONPATH='src'
.venv/Scripts/python.exe scripts/run_tests.py
```

Live tests require the running local stack and explicit switches:

```powershell
$env:GS_RUN_INTEGRATION='1'
$env:GS_MYSQL_PASSWORD_FILE='C:/mbsr/gridsentinel/.local/secrets/mysql_password'
$env:GS_RUN_INFRASTRUCTURE_INTEGRATION='1'
$env:GS_RUN_RABBITMQ_INTEGRATION='1'
$env:GS_RUN_HTTP_INTEGRATION='1'
$env:GS_TEST_ADMIN_USERNAME='pilotadmin'
$env:GS_TEST_ADMIN_PASSWORD_FILE='C:/mbsr/gridsentinel/.local/secrets/pilot_admin_password'
.venv/Scripts/python.exe scripts/run_tests.py --integration
```

Use your own account and absolute password-file path for another deployment. Integration fixtures use separate synthetic tenants. Reporting and Grafana tests also require provisioning described in [Operations](OPERATIONS.md).

## Data and measurements

```powershell
py -3.12 scripts/generate_meter_data.py --meters 10000000 --max-events 1000 --output .local/logical_10m_sample.jsonl
py -3.12 scripts/benchmark_ingestion.py --help
py -3.12 scripts/benchmark_api.py --requests 100 --output artifacts/benchmark_api.json
```

Logical fleet generation streams a bounded sample; it does not create ten million database rows. Benchmarks distinguish validation-only processing, committed MySQL transactions, and HTTP liveness. See the [data dictionary](DATA_DICTIONARY.md), [model card](MODEL_CARD.md), and raw JSON in `artifacts/`. Candidate models require explicit approval and activation; synthetic evaluation does not establish field accuracy.

Additional workers are available through Compose profiles `pipelines`, `connectors`, `ml`, and `observability`. Source connectors are disabled until their source configuration is supplied. No external notification is sent by default.
