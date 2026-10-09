"""Load reproducible synthetic telemetry through the actual processing engine."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import time
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from generate_meter_data import iter_events, iter_episodes
from gridsentinel.core.exceptions import ConfigurationError
from gridsentinel.workers.runtime import Runtime


def seed(runtime: Runtime, meters: int = 1000) -> dict[str, int]:
    """Keep demo loading bounded and disable it in production configuration."""
    if runtime.config.environment == "production" or not 1 <= meters <= 10000:
        raise ConfigurationError("Demo loading requires development and 1–10000 meters")
    operations, _, _ = runtime.services()
    tenant = runtime.config.get("application", "tenant_id", "demo")
    started = time.monotonic()
    count = duplicates = 0
    for event in iter_events(meters, tenant_id=tenant):
        result = operations.ingest(event)
        count += 1
        duplicates += int(result.get("duplicate", False))
    dataset = runtime.root / ".local/demo_training.json"
    dataset.parent.mkdir(parents=True, exist_ok=True)
    dataset.write_text(json.dumps({"data_source": "synthetic", "horizon_hours": 12,
                                   "episodes": list(iter_episodes(400))}), encoding="utf-8")
    summary = {"meters": meters, "events": count, "duplicates": duplicates}
    print(json.dumps({"synthetic": True, **summary, "elapsed_seconds": time.monotonic() - started,
                      "training_dataset": ".local/demo_training.json"}))
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--meters", type=int, default=1000)
    args = parser.parse_args()
    runtime = Runtime("demo-seed")
    try:
        seed(runtime, args.meters)
    finally:
        runtime.close()
