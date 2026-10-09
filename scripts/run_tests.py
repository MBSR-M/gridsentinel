"""Run all available unittest suites without silently discovering zero tests."""

import argparse
from pathlib import Path
import unittest


def main() -> None:
    """Discover each implemented suite; integration suites are explicit opt-in."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--integration", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1] / "tests"
    suite = unittest.TestSuite()
    directories = ["unit", "security", "recovery"]
    if args.integration:
        directories.append("integration")
    for name in directories:
        path = root / name
        if path.is_dir():
            suite.addTests(unittest.defaultTestLoader.discover(str(path), pattern="test_*.py", top_level_dir=str(root.parent)))
    if not suite.countTestCases():
        raise SystemExit("No tests discovered")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
