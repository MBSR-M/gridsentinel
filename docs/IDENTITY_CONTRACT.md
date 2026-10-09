# Identity and foundation implementation contract

`ConfigManager(root, environment='development', overrides=None, environ=None)`
loads base.ini and component INIs, then environment INI, allowlisted variables,
then explicit overrides. Component INIs comprise the base layer. Runtime override
keys use `section.option` or nested mappings. Secret values support `_file` options
and allowlisted `GS_*_PASSWORD_FILE` variables. Direct overrides win before opening
lower-priority secret files. Snapshots are nested immutable mappings; diagnostics
redact secrets by default. Use `snapshot(redact=False)` only inside trusted workers.
Reload validates the entire replacement first and preserves already captured snapshots.

`Database(config)` exposes transaction/read context managers returning Connection
with execute, fetch_one and fetch_all. Queries use `%s` parameters. Transaction
scopes commit atomically or roll back; read scopes always roll back. `run_transaction`
retries entire transactions for only MySQL 1205/1213, never uncertain commit failures.
The finite connection pool bounds acquisition time and discards unhealthy connections.
TLS with a configured CA verifies the server certificate and hostname; production
INI requires TLS. PyMySQL is imported only when connecting.

`migrate(directory)` serializes migrations with a MySQL named lock and records
SHA-256 checksums. Applied migrations must never be edited. MySQL DDL commits
implicitly, so interrupted DDL requires idempotent restart or tested restore; no
transactional DDL rollback is claimed. Migration grammar excludes routines and
semicolons inside string literals/comments.

Identity tables all contain tenant_id. String IDs use utf8mb4_0900_as_cs;
foreign-key columns must match this collation and unsigned BIGINT user identifiers.
Principal maps contain id, user_id (alias), username, tenant_id, roles, permissions,
session_id; scoped API principals additionally carry api_token_id with session_id=None.
Permission decisions require explicit permissions, independent of role names.
Account permission changes take effect on the next request because MySQL is queried
for every credential validation; revoked sessions have no Redis cache grace period.

AuthService includes bootstrap_admin, login, authenticate, refresh, validate_csrf,
logout, list_sessions, revoke_session, list_users, create_user, update_user, roles,
permissions, create_role, create_api_token, list_api_tokens, revoke_api_token and
authenticate_api_token. User/role/token mutations receive principal, a validated
data dict, and correlation_id. List users supports limit/after_id. Refresh requires
the matching session CSRF token and rotates access, refresh and CSRF credentials.
Consumed refresh hashes remain durable until the absolute deadline for replay
detection. Replay revokes the entire token family. Account updates revoke sessions.
API tokens intersect explicit scopes with the account's current grants on every
request and reject disabled owners, expiry, tenant mismatch and durable revocation.

Audits append through the *same* connection as each mutation. Per-tenant exclusive
head-row locks serialize sequence assignment and hash updates. Verification checks
chain order, canonical digests, actual row columns, and the current head. Hash chains
alone cannot prevent a privileged administrator rewriting the database. External
anchor retention and production audit-table INSERT/SELECT-only grants remain
deployment obligations. Application methods expose no edit/delete audit operation.

Boundaries: initial deployments isolate one customer; regional scopes are not yet
persisted in identity tables. Field incident assignment is enforced in transport and
domain queries. PBKDF2 opaque-session authentication has no MFA/SSO/password reset
email provider. Password rules require 12–128 characters and nontrivial passphrases;
no dictionary breach-screening claim is made. Cookie/TLS/CSRF headers are enforced
by API/reverse proxy; identity alone is not a hardened web security boundary.

Verified on Python3.12.10 and MySQL8.4.11: 11 foundation unit tests, 7 security unit
tests and 9 live authentication/audit integration tests. Live tests cover rotation,
replay-family revocation, CSRF, cross-tenant denial, lockout, immediate revocation,
role escalation denial, API scope enforcement, audit-failure rollback, concurrent
chain sequencing, and row-column tamper detection. All tests use synthetic tenants.
