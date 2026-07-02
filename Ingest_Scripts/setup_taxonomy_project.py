#!/usr/bin/env python3

"""Generate a taxonomy remediation project from metadata exceptions.

This script analyzes metadata exception logs to identify unresolved taxonomy
terms requiring remediation. It groups repeated exceptions, summarizes their
frequency, removes record-specific information, and produces a standardized
project spreadsheet for mapping terms to controlled vocabulary values.

Usage:
    # Generate a taxonomy remediation project
    python3 setup_taxonomy_project.py \
        --batch_path /workbench/batches/example \
        --exceptions_log /workbench/batches/example/logs/metadata_exceptions.csv

    # Run interactively
    python3 setup_taxonomy_project.py
"""

# ---------------------------------------------------------------------------
# Modules
# ---------------------------------------------------------------------------

# Standard library imports
import argparse
from pathlib import Path

# Third-party imports
import pandas as pd

# Local imports
from utilities import (
    SUCCESS_SYMBOL,
    create_df,
    create_directory,
    df_to_csv,
    prompt_for_input,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FIELDS_WITH_DESCRIPTIONS = {
    'genre',
    'genre_japanese_prints',
    'source_collection',
}

GROUPING_COLUMNS = [
    'field',
    'value',
    'exception',
]

RECORD_COLUMNS = [
    'batch',
    'pid',
    'record_id',
]


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------

# --- Argument Parsing ---

def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments or prompt for required paths.

    Returns:
        Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Taxonomy Remediation Project Generator"
    )
    parser.add_argument(
        '-b',
        '--batch_path',
        type=str,
        help="Path to the batch directory containing logs/ and metadata/.",
    )
    parser.add_argument(
        '-e',
        '--exceptions_log',
        type=str,
        help="Path to the system exceptions log CSV file.",
    )
    args = parser.parse_args()

    if not args.batch_path:
        args.batch_path = prompt_for_input(
            "Enter the path to the Workbench batch directory: "
        )

    if not args.exceptions_log:
        args.exceptions_log = prompt_for_input(
            "Enter the path to the exceptions log: "
        )

    return args


# --- Data Processing ---

def process_taxonomy_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Filter, clean, and group unique taxonomy exceptions.

    Args:
        df: Raw metadata exceptions log DataFrame.

    Returns:
        Cleaned taxonomy remediation DataFrame with count, term_name, uri, and
        optional description columns.
    """
    # Filter rows for taxonomy exceptions
    df = df[df['exception'].str.contains('taxonomy', na=False)].copy()

    # Get counts for exceptions
    counts = (
        df.groupby(GROUPING_COLUMNS)
        .size()
        .reset_index(name='count')
    )

    # Remove duplicate rows, keeping the first instance
    df = df.drop(columns=RECORD_COLUMNS, errors='ignore')
    df = df.drop_duplicates().copy()

    # Merge counts back into the unique dataframe
    df = df.merge(
        counts,
        on=GROUPING_COLUMNS,
        how='left',
    )

    # Initialize additional columns
    df['term_name'] = ''
    df['uri'] = ''

    if df['field'].isin(FIELDS_WITH_DESCRIPTIONS).any():
        df['description'] = ''

    return df


# --- File Helpers ---

def get_output_path(batch_path: Path) -> Path:
    """Build the output path for the taxonomy project CSV.

    Args:
        batch_path: Workbench batch directory.

    Returns:
        Output path for the taxonomy remediation project CSV.
    """
    remediation_path = create_directory(batch_path / 'remediation')
    filename = f'{batch_path.name}_taxonomy_project.csv'

    return remediation_path / filename


# --- Main Workflow ---

def main() -> None:
    """Run the taxonomy remediation project generation workflow."""
    args = parse_arguments()

    df = create_df(args.exceptions_log)
    processed_df = process_taxonomy_metadata(df)

    batch_path = Path(args.batch_path)
    output_file = get_output_path(batch_path)

    df_to_csv(processed_df, output_file)

    print(
        f"{SUCCESS_SYMBOL} Remediation project file saved to: {output_file}"
    )


if __name__ == '__main__':
    main()
