"""Generate local deployment secrets without printing their values."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import secrets


def initialize(directory: Path) -> None:
    """Create missing secret files; never overwrite an existing credential."""
    directory.mkdir(parents=True, exist_ok=True)
    for name in ("mysql_root_password", "mysql_password", "rabbitmq_password",
                 "redis_password", "grafana_password", "grafana_mysql_password"):
        path = directory / name
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            continue
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(secrets.token_urlsafe(36))
        print(f"Created {path.name}")
    if os.name == "nt":
        print("Restrict .local/secrets using your Windows account ACL before sharing this machine.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=Path(".local/secrets"))
    initialize(parser.parse_args().directory)
