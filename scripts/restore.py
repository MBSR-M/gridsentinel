"""Restore a backup into an explicitly selected, isolated Compose project."""

import argparse
import gzip
from pathlib import Path
import shutil
import subprocess


def restore(source: Path, project: str, confirmed: bool) -> None:
    """Require explicit restore intent; never restore into the default project."""
    if not confirmed or not project or project == "gridsentinel":
        raise SystemExit("Use a separate recovery project and --confirm-restore")
    source = source.resolve(strict=True)
    command = ["docker", "compose", "-p", project, "exec", "-T", "mysql", "sh", "-c",
               'MYSQL_PWD="$(cat /run/secrets/mysql_root_password)" exec mysql -uroot gridsentinel']
    with gzip.open(source, "rb") as handle, subprocess.Popen(command, stdin=subprocess.PIPE) as process:
        shutil.copyfileobj(handle, process.stdin, length=1024 * 1024)
        process.stdin.close()
        if process.wait() != 0:
            raise SystemExit("Restore failed")
    print("Restore completed; run migrations validation and audit verification")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--project", required=True)
    parser.add_argument("--confirm-restore", action="store_true")
    args = parser.parse_args()
    restore(args.source, args.project, args.confirm_restore)
