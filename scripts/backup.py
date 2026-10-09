"""Stream a consistent MySQL backup using the local Docker secret mount."""

from __future__ import annotations

import argparse
import gzip
import os
from pathlib import Path
import shutil
import subprocess


def backup(destination: Path) -> None:
    """Write a compressed backup atomically without exposing credentials."""
    destination = destination.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".partial")
    command = ["docker", "compose", "exec", "-T", "mysql", "sh", "-c",
               'MYSQL_PWD="$(cat /run/secrets/mysql_root_password)" exec mysqldump -uroot --single-transaction --routines --triggers --no-tablespaces gridsentinel']
    try:
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
            with gzip.open(temporary, "wb") as handle:
                shutil.copyfileobj(process.stdout, handle, length=1024 * 1024)
            # mysqldump diagnostics contain no passwords because they are not arguments.
            diagnostics = process.stderr.read(8192)
            if process.wait() != 0:
                raise RuntimeError("Database backup failed; check Docker/MySQL availability")
        os.replace(temporary, destination)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    print(f"Backup saved: {destination.name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path)
    backup(parser.parse_args().destination)
