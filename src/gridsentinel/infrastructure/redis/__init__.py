"""Disposable tenant-aware caching with explicit outage fallback."""

from __future__ import annotations

import json
import logging
from typing import Any

LOGGER = logging.getLogger(__name__)


class RedisCache:
    """Cache failures return misses; authority always stays in MySQL."""

    def __init__(self, config: Any, client: Any = None) -> None:
        if client is None:
            import redis
            from redis.backoff import NoBackoff
            from redis.retry import Retry
            options: dict[str, Any] = {
                "host": config.get("redis", "host", "redis"),
                "port": config.get_int("redis", "port", 6379),
                "password": config.get("redis", "password", None),
                "socket_timeout": config.get_float("redis", "socket_timeout_seconds", 3),
                "socket_connect_timeout": config.get_float("redis", "socket_connect_timeout_seconds", 3),
                "decode_responses": True, "retry": Retry(NoBackoff(), 0),
            }
            if config.get_bool("redis", "ssl", False):
                options.update(ssl=True, ssl_ca_certs=config.get_required("redis", "ssl_ca"),
                               ssl_cert_reqs="required", ssl_check_hostname=True)
            client = redis.Redis(**options)
        self.client = client
        self.ttl = config.get_int("redis", "default_ttl_seconds", 300)
        self.max_value_bytes = config.get_int("redis", "max_value_bytes", 262144)
        if not 1 <= self.ttl <= 86400 or not 1 <= self.max_value_bytes <= 1048576:
            raise ValueError("Invalid Redis cache limits")

    @staticmethod
    def key(namespace: str, tenant_id: str, entity: str) -> str:
        """Prevent tenant/namespace separators from making ambiguous cache keys."""
        from urllib.parse import quote
        if namespace not in {"session", "meter:state", "gateway:health", "rate_limit", "dashboard", "job:lock"}:
            raise ValueError("Unknown cache namespace")
        if not isinstance(tenant_id, str) or not 1 <= len(tenant_id) <= 64 or not isinstance(entity, str) or not 1 <= len(entity) <= 512:
            raise ValueError("Invalid tenant or entity cache identifier")
        return f"gs:{namespace}:{quote(tenant_id, safe='')}:{quote(entity, safe='')}"

    def get(self, namespace: str, tenant_id: str, entity: str) -> Any:
        """Read validated JSON or return a miss when Redis is unavailable."""
        key = self.key(namespace, tenant_id, entity)
        try:
            value = self.client.get(key)
            if value is None:
                return None
            raw = value.encode("utf-8") if isinstance(value, str) else value
            if len(raw) > self.max_value_bytes:
                self.client.delete(key)
                return None
            def reject_constant(value: str) -> None:
                raise ValueError("Non-finite cached value")
            return json.loads(raw.decode("utf-8"), parse_constant=reject_constant)
        except Exception:
            LOGGER.warning("Redis cache read unavailable")
            return None

    def set(self, namespace: str, tenant_id: str, entity: str,
            value: Any, ttl: int | None = None) -> bool:
        """Every cache entry has a finite TTL; no critical lock uses this cache."""
        duration = self.ttl if ttl is None else ttl
        if type(duration) is not int or duration < 1 or duration > 86400:
            raise ValueError("Cache TTL must be between 1 and 86400 seconds")
        key = self.key(namespace, tenant_id, entity)
        raw = json.dumps(value, allow_nan=False)
        if len(raw.encode("utf-8")) > self.max_value_bytes:
            raise ValueError("Cache value exceeds configured limit")
        try:
            return bool(self.client.set(key, raw, ex=duration))
        except Exception:
            LOGGER.warning("Redis cache write unavailable")
            return False

    def invalidate(self, namespace: str, tenant_id: str, entity: str) -> bool:
        """Delete a cached value; sessions are strictly revalidated in MySQL."""
        key = self.key(namespace, tenant_id, entity)
        try:
            self.client.delete(key)
            return True
        except Exception:
            return False

    def rate_limit(self, tenant_id: str, identity: str, limit: int, window: int) -> bool | None:
        """Atomically increment a fixed window; None requests a local fallback."""
        script = "local n=redis.call('INCR',KEYS[1]); if n==1 then redis.call('EXPIRE',KEYS[1],ARGV[1]) end; return n"
        if type(limit) is not int or type(window) is not int or not 1 <= limit <= 1000000 or not 1 <= window <= 86400:
            raise ValueError("Invalid rate limit budget or window")
        key = self.key("rate_limit", tenant_id, identity)
        try:
            count = self.client.eval(script, 1, key, window)
            return int(count) <= limit
        except Exception:
            return None

    def health(self) -> bool:
        """Probe connectivity without treating cache availability as authority."""
        try:
            return bool(self.client.ping())
        except Exception:
            return False

    def close(self) -> None:
        """Close Redis connection resources."""
        self.client.close()
