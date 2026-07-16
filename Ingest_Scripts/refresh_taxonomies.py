#!/usr/bin/env python3

"""Refresh snapshots of Islandora taxonomies on demand.

This script retrieves the latest taxonomy terms from the authenticated Drupal
REST export and updates the local taxonomy CSV cache. It can also synchronize
the configured Google Sheet taxonomy tabs.

Usage:
    # Refresh the local cache only
    python3 refresh_taxonomies.py

    # Refresh the cache and synchronize the Google Sheet
    python3 refresh_taxonomies.py --sync_google_sheet
"""

# ---------------------------------------------------------------------------
# Modules
# ---------------------------------------------------------------------------

# Standard library imports
import argparse
import sys
import traceback
from datetime import datetime

# Local imports
from definitions import PROJECT_ROOT
from taxonomy_manager import TAXONOMY_MANAGER
from utilities import (
    ERROR_SYMBOL,
    create_directory,
    setup_logger,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOGGER_NAME = 'refresh_taxonomies'


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Refresh the local Islandora taxonomy cache and optionally "
            "synchronize the taxonomy Google Sheet."
        )
    )
    parser.add_argument(
        '--sync_google_sheet',
        action='store_true',
        help=(
            "Update the configured Google Sheet taxonomy tabs after "
            "refreshing the local cache."
        ),
    )

    return parser.parse_args()


def main() -> None:
    """Refresh the taxonomy cache and optionally synchronize Google Sheets."""
    logger = None
    log_path = None

    try:
        args = parse_arguments()

        log_dir = create_directory(
            PROJECT_ROOT / 'logs'
        )
        timestamp = datetime.now().strftime('%Y-%m-%d-%H%M%S')
        log_path = log_dir / f'{timestamp}_refresh_taxonomies.log'
        logger = setup_logger(LOGGER_NAME, log_path)

        TAXONOMY_MANAGER.logger = logger

        TAXONOMY_MANAGER.refresh(
            sync_google_sheet=args.sync_google_sheet,
        )

        print(f"Log saved to: {log_path}")

    except Exception:
        message = "Taxonomy refresh failed."

        if logger:
            logger.exception(message)

        print(f"\n{ERROR_SYMBOL} {message}")

        if log_path:
            print(f"See logs: {log_path}")
        else:
            traceback.print_exc()

        sys.exit(1)


if __name__ == '__main__':
    main()
