"""Layered configuration with explicit environment and secret-file inputs."""

from __future__ import annotations

import configparser
import math
import os
import threading
from collections.abc import Callable, Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Any, TypeVar

from gridsentinel.core.exceptions import ConfigurationError

T = TypeVar("T")
ENV_ALLOWLIST = {
    "GS_MYSQL_HOST": ("mysql", "host"),
    "GS_MYSQL_PORT": ("mysql", "port"),
    "GS_MYSQL_DATABASE": ("mysql", "database"),
    "GS_MYSQL_USER": ("mysql", "user"),
    "GS_MYSQL_PASSWORD": ("mysql", "password"),
    "GS_MYSQL_PASSWORD_FILE": ("mysql", "password_file"),
    "GS_MYSQL_SSL_CA": ("mysql", "ssl_ca"),
    "GS_KAFKA_BOOTSTRAP_SERVERS": ("kafka", "bootstrap_servers"),
    "GS_KAFKA_SECURITY_PROTOCOL": ("kafka", "security_protocol"),
    "GS_KAFKA_SSL_CA": ("kafka", "ssl_ca"),
    "GS_RABBITMQ_HOST": ("rabbitmq", "host"),
    "GS_RABBITMQ_PORT": ("rabbitmq", "port"),
    "GS_RABBITMQ_USER": ("rabbitmq", "user"),
    "GS_RABBITMQ_PASSWORD": ("rabbitmq", "password"),
    "GS_RABBITMQ_PASSWORD_FILE": ("rabbitmq", "password_file"),
    "GS_RABBITMQ_SSL": ("rabbitmq", "ssl"),
    "GS_RABBITMQ_SSL_CA": ("rabbitmq", "ssl_ca"),
    "GS_REDIS_HOST": ("redis", "host"),
    "GS_REDIS_PORT": ("redis", "port"),
    "GS_REDIS_PASSWORD": ("redis", "password"),
    "GS_REDIS_PASSWORD_FILE": ("redis", "password_file"),
    "GS_REDIS_SSL": ("redis", "ssl"),
    "GS_REDIS_SSL_CA": ("redis", "ssl_ca"),
    "GS_API_HOST": ("api", "host"),
    "GS_API_PORT": ("api", "port"),
    "GS_API_TRUSTED_PROXY": ("api", "trusted_proxy"),
    "GS_API_ALLOWED_ORIGINS": ("api", "allowed_origins"),
    "GS_API_COOKIE_SECURE": ("security", "cookie_secure"),
    "GS_TENANT_ID": ("application", "tenant_id"),
    "GS_LOG_LEVEL": ("logging", "level"),
    "GS_GRAFANA_URL": ("grafana", "url"),
}
SENSITIVE_WORDS = ("password", "secret", "token", "private_key", "credential")


class ConfigManager:
    """Resolve overrides > allowlisted env > environment INI > base/modules.

    Component INIs augment the base layer in lexical order. Existing snapshots
    remain unchanged when reload replaces the active validated configuration.
    """

    def __init__(self, root: str | Path, environment: str = "development",
                 overrides: Mapping[str, Any] | None = None,
                 environ: Mapping[str, str] | None = None) -> None:
        self.root = Path(root).resolve()
        if environment not in {"development", "staging", "production", "test"}:
            raise ConfigurationError("Unknown configuration environment")
        self.environment = environment
        self._overrides = dict(overrides or {})
        self._environ = environ
        self._lock = threading.RLock()
        self._values: Mapping[str, Mapping[str, str]] = MappingProxyType({})
        self.reload()

    def reload(self) -> None:
        """Validate a fresh configuration before atomically replacing it."""
        parser = configparser.ConfigParser(interpolation=None, strict=True)
        files = [self.root / "base.ini"]
        if not files[0].is_file():
            raise ConfigurationError("Configuration base.ini is missing")
        environments = {"development", "staging", "production", "test", "base"}
        files.extend(sorted(path for path in self.root.glob("*.ini")
                            if path.stem not in environments))
        files.append(self.root / f"{self.environment}.ini")
        try:
            parser.read(files, encoding="utf-8")
        except (configparser.Error, OSError) as error:
            raise ConfigurationError("Unable to parse INI configuration") from error
        values = {section: dict(parser.items(section)) for section in parser.sections()}
        values.setdefault("application", {})["environment"] = self.environment
        env = os.environ if self._environ is None else self._environ
        for key, (section, option) in ENV_ALLOWLIST.items():
            if key in env:
                values.setdefault(section, {})[option] = str(env[key])
        for key, value in self._overrides.items():
            if isinstance(value, Mapping):
                values.setdefault(key, {}).update({str(k): str(v) for k, v in value.items()})
            elif "." in key:
                section, option = key.split(".", 1)
                values.setdefault(section, {})[option] = str(value)
            else:
                raise ConfigurationError("Overrides require section.option keys")
        for section, entries in values.items():
            for option, value in list(entries.items()):
                if option.endswith("_file") and value:
                    target = option[:-5]
                    if not any(word in target for word in SENSITIVE_WORDS):
                        continue
                    direct_override = f"{section}.{target}" in self._overrides or (
                        isinstance(self._overrides.get(section), Mapping)
                        and target in self._overrides[section])
                    env_direct = any(pair == (section, target) and name in env
                                     for name, pair in ENV_ALLOWLIST.items())
                    file_override = f"{section}.{option}" in self._overrides or (
                        isinstance(self._overrides.get(section), Mapping)
                        and option in self._overrides[section])
                    if direct_override or (env_direct and not file_override):
                        continue
                    secret_path = Path(value).expanduser()
                    if not secret_path.is_absolute():
                        secret_path = self.root / secret_path
                    try:
                        if secret_path.stat().st_size > 16384:
                            raise ConfigurationError("Secret file exceeds allowed size")
                        secret = secret_path.read_text(encoding="utf-8").rstrip("\r\n")
                    except OSError as error:
                        raise ConfigurationError(f"Secret file for {section}.{target} unavailable") from error
                    if not secret:
                        raise ConfigurationError(f"Secret file for {section}.{target} is empty")
                    entries[target] = secret
        candidate = MappingProxyType({s: MappingProxyType(v) for s, v in values.items()})
        self._validate(candidate)
        with self._lock:
            self._values = candidate

    def get(self, section: str, option: str, default: T | None = None,
            cast: Callable[[Any], T] = str) -> T | None:
        """Return an explicitly converted configured value or absent default."""
        value = self._values.get(section, {}).get(option)
        if value is None:
            return default
        try:
            if cast is bool:
                if value.lower() not in configparser.ConfigParser.BOOLEAN_STATES:
                    raise ValueError("invalid boolean")
                result = configparser.ConfigParser.BOOLEAN_STATES[value.lower()]
            else:
                result = cast(value)
            if isinstance(result, float) and not math.isfinite(result):
                raise ValueError("non-finite")
            return result
        except (TypeError, ValueError) as error:
            raise ConfigurationError(f"Malformed value for {section}.{option}") from error

    def get_int(self, section: str, option: str, default: int = 0) -> int:
        """Return an integer; malformed configured input is an error."""
        return self.get(section, option, default, int)  # type: ignore[return-value]

    def get_float(self, section: str, option: str, default: float = 0.0) -> float:
        """Return a finite floating-point value."""
        return self.get(section, option, default, float)  # type: ignore[return-value]

    def get_bool(self, section: str, option: str, default: bool = False) -> bool:
        """Return a strict INI boolean."""
        def convert(value: str) -> bool:
            if value.lower() not in configparser.ConfigParser.BOOLEAN_STATES:
                raise ValueError("invalid boolean")
            return configparser.ConfigParser.BOOLEAN_STATES[value.lower()]
        return self.get(section, option, default, convert)  # type: ignore[return-value]

    def get_list(self, section: str, option: str,
                 default: list[str] | None = None) -> list[str]:
        """Return a fresh comma-separated list."""
        value = self.get(section, option)
        return list(default or []) if value is None else [v.strip() for v in value.split(",") if v.strip()]

    def get_path(self, section: str, option: str,
                 default: str | Path | None = None) -> Path | None:
        """Resolve relative paths against the configuration directory."""
        value = self.get(section, option, default)
        if value is None:
            return None
        path = Path(value).expanduser()
        return path.resolve() if path.is_absolute() else (self.root / path).resolve()

    def get_required(self, section: str, option: str) -> str:
        """Return a nonempty required value or raise ConfigurationError."""
        value = self.get(section, option)
        if value is None or not value.strip():
            raise ConfigurationError(f"Missing required value {section}.{option}")
        return value

    def validate(self) -> None:
        """Validate cross-field constraints on the active snapshot."""
        self._validate(self._values)

    def snapshot(self, redact: bool = True) -> Mapping[str, Mapping[str, str]]:
        """Capture immutable values; redact credentials for diagnostics by default."""
        with self._lock:
            if not redact:
                return self._values
            return MappingProxyType({section: MappingProxyType({
                key: "[REDACTED]" if any(word in key for word in SENSITIVE_WORDS) else value
                for key, value in entries.items()}) for section, entries in self._values.items()})

    def _validate(self, values: Mapping[str, Mapping[str, str]]) -> None:
        ranges = {("mysql", "port"): (1, 65535), ("mysql", "pool_size"): (1, 256),
                  ("api", "port"): (1, 65535), ("security", "password_iterations"): (600000, 5000000),
                  ("security", "access_ttl_seconds"): (30, 3600),
                  ("security", "idle_timeout_seconds"): (60, 86400),
                  ("security", "absolute_ttl_seconds"): (300, 2592000),
                  ("security", "login_max_attempts"): (1, 100),
                  ("security", "lockout_seconds"): (1, 86400)}
        for (section, option), (low, high) in ranges.items():
            raw = values.get(section, {}).get(option)
            if raw is None:
                continue
            try:
                value = int(raw)
            except ValueError as error:
                raise ConfigurationError(f"Malformed value for {section}.{option}") from error
            if not low <= value <= high:
                raise ConfigurationError(f"Value outside allowed range for {section}.{option}")
        for section, entries in values.items():
            for option in ("debug", "enabled", "cookie_secure", "enable_auto_commit", "ssl", "require_tls"):
                if option in entries and entries[option].lower() not in configparser.ConfigParser.BOOLEAN_STATES:
                    raise ConfigurationError(f"Malformed boolean for {section}.{option}")
        if values.get("kafka", {}).get("enable_auto_commit", "false").lower() in ("true", "1", "yes", "on"):
            raise ConfigurationError("Kafka automatic offset commits are forbidden")
        if self.environment == "production":
            if values.get("application", {}).get("debug", "false").lower() in ("true", "1", "yes", "on"):
                raise ConfigurationError("Debug mode is forbidden in production")
            if values.get("security", {}).get("cookie_secure", "true").lower() in ("false", "0", "no", "off"):
                raise ConfigurationError("Secure cookies are required in production")
            if values.get("mysql", {}).get("require_tls", "false").lower() not in ("true", "1", "yes", "on"):
                raise ConfigurationError("MySQL TLS is required in production")
            if values.get("kafka", {}).get("security_protocol", "PLAINTEXT").upper() not in ("SSL", "SASL_SSL"):
                raise ConfigurationError("Kafka TLS is required in production")
            for service in ("rabbitmq", "redis"):
                if values.get(service, {}).get("ssl", "false").lower() not in ("true", "1", "yes", "on"):
                    raise ConfigurationError(f"{service} TLS is required in production")
