"""Check expected tenant data and its audit chain after an isolated restore."""

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from gridsentinel.workers.runtime import Runtime


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--expected-meters", type=int, required=True)
    args = parser.parse_args()
    runtime = Runtime("restore-verification")
    try:
        runtime.database.migrate(runtime.root / "database/migrations")
        with runtime.database.read() as connection:
            meters = connection.fetch_one("SELECT COUNT(*) AS count FROM meters WHERE tenant_id=%s", (args.tenant,))["count"]
            events = connection.fetch_one("SELECT COUNT(*) AS count FROM communication_events WHERE tenant_id=%s", (args.tenant,))["count"]
        audit = runtime.audit.verify(args.tenant)
        result = {"tenant_id": args.tenant, "meter_count": meters, "communication_events": events,
                  "expected_meter_count": args.expected_meters, "audit": audit,
                  "passed": meters == args.expected_meters and audit["valid"]}
        print(json.dumps(result))
        if not result["passed"]:
            raise SystemExit(1)
    finally:
        runtime.close()


if __name__ == "__main__":
    main()
