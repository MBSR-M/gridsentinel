# Security boundary

Authentication uses salted PBKDF2-HMAC-SHA256 password hashes, hashed opaque sessions and refresh tokens, refresh-family reuse detection, live permission checks, session revocation, and lockout. Browser sessions use HttpOnly access/refresh cookies and SameSite=Strict; mutations require CSRF and accepted origins. API tokens are scoped to their live owner's permissions, returned once, and stored hashed. Receipt replay redacts an issued token.

All protected API routes require server-side permissions. Tenant identity comes from the authenticated principal. FIELD_OPERATOR assignment restrictions continue to apply to mixed-role users. Database queries bind values; configured table/column identifiers use allowlists. Structured logs redact secret-like fields. Configuration snapshots redact secrets and secret-file paths.

Mutation idempotency receipts can become UNCERTAIN if a process fails between the domain transaction and receipt completion. Such receipts block automatic replay and require investigation. They are not a claim of atomic exactly-once effects across every service. Kafka and RabbitMQ use at-least-once delivery, durable effects, bounded retry, and explicit failure handling.

## Deployment boundary

The development stack uses loopback HTTP and plaintext broker connections. Production mode rejects missing verified TLS configurations for MySQL, Kafka, RabbitMQ and Redis. The production proxy needs deployment-managed certificates. Its configuration is a starting point; an end-to-end production TLS deployment has not been qualified here.

The standard-library HTTP server is behind Nginx on an internal network, bounds concurrent connections and JSON bodies, validates request framing, applies socket timeouts, and serves static files only within the configured root. Header parsing starts in the standard-library parser before the smaller aggregate limit is checked. Socket inactivity timeouts are not a total request deadline. Keep the API private and qualify the reverse proxy against the intended threat model.

The development MySQL application account has broad grants in its application database. Split migration, identity, ingestion, operational and reporting privileges before production. Grafana receives SELECT on nine tenant-bound views only; it cannot read identity tables or underlying event payloads. Grafana authentication is independent of application sessions.

## Audit and secrets

Sensitive changes append to a serialized per-tenant hash chain within the same database transaction. Concurrent append and rollback tests verify this property. No API edits or deletes audit records. A database administrator who can rewrite the chain and its head remains outside this protection. Approved external anchoring and independent archival must be implemented for stronger tamper evidence.

Secret files are excluded from Git and application images. Protect `.local/secrets` with host access controls; never commit credentials, SQL dumps, or private keys. Generated local credentials are demonstration credentials. Logging uses bounded local spool files and a collector; this is a single-host transport, and spool saturation may drop operational logs. Critical audit events use the separate database transaction path.

Production acceptance requires verified TLS, least-privilege accounts, broker ACLs, external audit retention, customer-approved SLA and topology contracts, vendor protocol validation, load/failover tests, backup restoration, and penetration testing. Enterprise SSO is not implemented.
