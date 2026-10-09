"""Apply checksummed forward migrations to the configured MySQL database."""

from pathlib import Path
import os
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from gridsentinel.core.configuration import ConfigManager
from gridsentinel.infrastructure.mysql.database import Database


def main() -> None:
    """Run migrations and close every database connection."""
    root = Path(__file__).resolve().parents[1]
    config = ConfigManager(root / "config", os.environ.get("GS_ENVIRONMENT", "development"))
    database = Database(config)
    try:
        database.migrate(root / "database/migrations")
        print("Database migrations applied")
    finally:
        database.close()


if __name__ == "__main__":
    main()
