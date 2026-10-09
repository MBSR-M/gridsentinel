"""Migrate, seed roles and provision the first administrator securely."""

import argparse
from getpass import getpass
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from gridsentinel.workers.runtime import Runtime


def main() -> None:
    """Use a terminal prompt or a restricted password file, never CLI plaintext."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--username", required=True)
    parser.add_argument("--tenant", default=None)
    parser.add_argument("--password-file", type=Path)
    parser.add_argument("--demo", action="store_true")
    args = parser.parse_args()
    runtime = Runtime("bootstrap")
    try:
        runtime.database.migrate(runtime.root / "database/migrations")
        password = args.password_file.read_text(encoding="utf-8").strip() if args.password_file else getpass("Administrator password (12+ characters): ")
        tenant = args.tenant or runtime.config.get("application", "tenant_id", "demo")
        result = runtime.auth.bootstrap_admin(args.username, password, tenant)
        print(f"Administrator provisioned: {result['username']}")
        if args.demo:
            from seed_demo import seed
            seed(runtime, 1000)
    finally:
        runtime.close()


if __name__ == "__main__":
    main()
