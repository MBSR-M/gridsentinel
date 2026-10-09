# Dependency inventory

Python runtime target: CPython 3.12 (host verification uses 3.12.10; container
pins 3.12.12). Runtime packages are installed with `--no-deps`:

| Package | Pin | License | Purpose |
|---|---|---|---|
| PyMySQL | 1.1.2 | MIT | Parameterized MySQL connections |
| kafka-python | 2.2.15 | Apache-2.0 | Kafka producers, consumers and topic provisioning |
| pika | 1.3.2 | BSD-3-Clause | RabbitMQ confirmed publishing and workers |
| redis-py (`redis`) | 6.4.0 | MIT | Disposable cache and rate limits |

The optional packaging tool setuptools is a build tool, not an application
runtime dependency. Running with `PYTHONPATH=src` needs no packaging tools.
No cryptography, ML, HTTP, UI, charting or mapping libraries are installed.
MySQL native-password authentication is confined to the isolated development
stack. Production MySQL connections require verified TLS; caching_sha2_password
over TLS avoids PyMySQL's optional RSA/cryptography dependency.

Infrastructure: MySQL 8.4.11 LTS, pinned by image digest (GPL-2.0/community); Apache Kafka 4.1.0
(Apache-2.0); RabbitMQ 4.1.4 (MPL-2.0); Redis 7.4.6 (RSALv2/SSPLv1);
Grafana OSS 12.2.0 (AGPL-3.0); Nginx 1.28.0 (BSD-2-Clause); Docker/Compose
(Apache-2.0 for engine/Compose; Docker Desktop has separate commercial terms).
Operators must assess redistribution and deployment licensing for their use.
Pins are reproducibility choices, not a claim of current security patch status.

The standard-library HTTP server is deliberately bounded but remains a pilot
application transport behind Nginx. A production security review may require
a separately approved HTTP server; no such additional dependency is installed.

Primary references: [PyMySQL](https://pymysql.readthedocs.io/),
[Kafka Python](https://kafka-python.readthedocs.io/),
[Pika](https://pika.readthedocs.io/),
[Redis Python](https://redis.readthedocs.io/),
[MySQL partitioning restrictions](https://dev.mysql.com/doc/refman/8.4/en/partitioning-limitations-storage-engines.html).
