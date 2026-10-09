"""Measure local liveness endpoint latency without exposing credentials."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from urllib.parse import urlsplit
from urllib.request import urlopen
from urllib.error import HTTPError


def main() -> None:
    """Report only measured requests; health latency is not analytics capacity."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8080/api/v1/system/health")
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--interval-ms", type=int, default=50, help="Delay between requests; default stays below the development proxy limit")
    parser.add_argument("--output", type=Path, default=Path(".local/api_benchmark.json"))
    args = parser.parse_args()
    if not 1 <= args.requests <= 1000 or urlsplit(args.url).hostname not in {"localhost", "127.0.0.1"}:
        parser.error("Use a local URL and 1–1000 requests")
    if not 0 <= args.interval_ms <= 1000:
        parser.error("Interval must be 0–1000 milliseconds")
    values = []
    failures = {}
    for _ in range(args.requests):
        started = time.perf_counter()
        try:
            with urlopen(args.url, timeout=10) as response:
                if not json.load(response)["success"]:
                    raise RuntimeError("Request failed")
            values.append((time.perf_counter() - started) * 1000)
        except HTTPError as error:
            key = str(error.code)
            failures[key] = failures.get(key, 0) + 1
        if args.interval_ms:
            time.sleep(args.interval_ms / 1000)
    values.sort()
    result = {"scope": "local HTTP liveness endpoint", "requests": args.requests,
              "successful_requests": len(values), "http_failures": failures, "interval_ms": args.interval_ms,
              "measured_at": datetime.now(timezone.utc).isoformat(),
              "latency_ms": {name: values[min(len(values)-1, int(len(values)*fraction))]
                             for name, fraction in (("p50", .5), ("p95", .95), ("p99", .99))} if values else {},
              "limitations": "Serial local requests; excludes authenticated analytics and fleet/broker ingestion."}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
