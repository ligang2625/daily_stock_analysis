#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Umbrella CLI for database maintenance operations (Phase 4).

Usage:
    python scripts/maintenance.py archive --days 5 --dry-run
    python scripts/maintenance.py diagnostics --format json
    python scripts/maintenance.py backup --historical-only
    python scripts/maintenance.py health --soft-fail
"""

import sys
from pathlib import Path

# Ensure project root is on sys.path
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        print("Available subcommands:")
        print("  archive       Run archive maintenance")
        print("  diagnostics   Run database diagnostics")
        print("  backup        Run database backup")
        print("  health        Check archive health")
        print("")
        print("Use 'python scripts/maintenance.py <subcommand> --help' for details.")
        return 1

    subcommand = sys.argv[1]
    # Pass remaining args to the subcommand module
    remaining = sys.argv[2:]

    if subcommand == 'archive':
        from src.maintenance.archive import main as archive_main
        return archive_main(remaining)
    elif subcommand == 'diagnostics':
        from src.maintenance.db_diagnostics import main as diagnostics_main
        return diagnostics_main(remaining)
    elif subcommand == 'backup':
        from src.maintenance.backup import main as backup_main
        return backup_main(remaining)
    elif subcommand == 'health':
        from src.maintenance.archive_health import main as health_main
        return health_main(remaining)
    elif subcommand in ('-h', '--help'):
        print(__doc__)
        print("Available subcommands:")
        print("  archive       Run archive maintenance")
        print("  diagnostics   Run database diagnostics")
        print("  backup        Run database backup")
        print("  health        Check archive health")
        return 0
    else:
        print(f"Unknown subcommand: {subcommand}", file=sys.stderr)
        print("Available: archive, diagnostics, backup, health", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
