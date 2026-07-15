#!/usr/bin/env python3

"""Merge multiple Google Sheets into one CSV by identifier.

This script reads two or more Google Sheets, merges rows using a shared
identifier column, and writes the combined CSV to the given Workbench batch
directory. It supports either preserving existing values or overwriting them
with later sheet values.

Usage:
    # Provide Google Sheet IDs directly
    python3 merge_batches.py \
        --batch_path /workbench/batches/example \
        --sheet_ids <sheet_id_1> <sheet_id_2> <sheet_id_3>

    # Provide Google Sheet IDs in a text file, one ID per line
    python3 merge_batches.py \
        --batch_path /workbench/batches/example \
        --sheet_ids_file sheet_ids.txt

    # Overwrite existing values with later sheet values
    python3 merge_batches.py \
        --batch_path /workbench/batches/example \
        --sheet_ids_file sheet_ids.txt \
        --overwrite_existing
"""

# ---------------------------------------------------------------------------
# Modules
# ---------------------------------------------------------------------------

# Standard library imports
import argparse
import logging
import re
import traceback
from datetime import datetime
from pathlib import Path

# Third-party imports
import pandas as pd

# Local imports
from definitions import GOOGLE_CREDENTIALS_FILE
from utilities import (
    ERROR_SYMBOL,
    SUCCESS_SYMBOL,
    LogRegistry,
    create_directory,
    get_merged_column_order,
    df_to_csv,
    prompt_for_input,
    read_google_sheet,
    setup_logger,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOGGER_NAME = LogRegistry.MERGE_BATCHES
DEFAULT_ID_COLUMN = 'id'


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------

# --- Argument Parsing ---

def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments and prompt for missing values.

    Returns:
        Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Merge multiple Google Sheets into one batch CSV."
    )
    parser.add_argument(
        '-b',
        '--batch_path',
        type=str,
        help="Path to the Workbench batch directory.",
    )
    parser.add_argument(
        '-s',
        '--sheet_ids',
        nargs='+',
        help="Google Sheet IDs to merge.",
    )
    parser.add_argument(
        '-f',
        '--sheet_ids_file',
        type=str,
        help="Full path to text file containing one Google Sheet ID per line.",
    )
    parser.add_argument(
        '--sheet_name',
        type=str,
        default=None,
        help="Sheet tab name to read. Defaults to the first tab.",
    )
    parser.add_argument(
        '-i',
        '--id_col',
        type=str,
        default=DEFAULT_ID_COLUMN,
        help=f"Identifier column used for merging. Default: {DEFAULT_ID_COLUMN}.",
    )
    parser.add_argument(
        '-c',
        '--credentials_file',
        type=str,
        default=GOOGLE_CREDENTIALS_FILE,
        help="Path to the Google service account credentials JSON.",
    )
    parser.add_argument(
        '--overwrite_existing',
        action='store_true',
        help="Overwrite existing values with non-empty values from later sheets.",
    )

    args = parser.parse_args()

    if not args.batch_path:
        args.batch_path = prompt_for_input(
            "Enter the path to the Workbench batch directory: "
        )

    if not args.sheet_ids and not args.sheet_ids_file:
        raw_ids = prompt_for_input(
            "Enter Google Sheet IDs to merge "
            "(separated by spaces, commas, or new lines): "
        )
        args.sheet_ids = parse_sheet_ids(raw_ids)

    return args


# --- Input Helpers ---

def parse_sheet_ids(raw_ids: str) -> list[str]:
    """Parse Google Sheet IDs from user input.

    Args:
        raw_ids: String containing one or more Google Sheet IDs, spearated by a
        space and/or a comma.

    Returns:
        Cleaned list of Google Sheet IDs.
    """
    return [
        sheet_id.strip()
        for sheet_id in re.split(r'[\s,]+', raw_ids)
        if sheet_id.strip()
    ]


def read_sheet_ids_file(sheet_ids_file: str | Path) -> list[str]:
    """Read Google Sheet IDs from a text file.

    Blank lines and lines beginning with ``#`` are ignored.

    Args:
        sheet_ids_file: Path to a text file containing Google Sheet IDs.

    Returns:
        Google Sheet IDs from the file.
    """
    sheet_ids_path = Path(sheet_ids_file)
    lines = sheet_ids_path.read_text(encoding='utf-8').splitlines()

    return [
        line.strip()
        for line in lines
        if line.strip() and not line.strip().startswith('#')
    ]


def resolve_sheet_ids(
    sheet_ids: list[str] | None,
    sheet_ids_file: str | Path | None,
) -> list[str]:
    """Resolve Google Sheet IDs from CLI arguments or a text file.

    Args:
        sheet_ids: Google Sheet IDs provided directly.
        sheet_ids_file: Text file containing Google Sheet IDs.

    Returns:
        Unique Google Sheet IDs, preserving first-seen order.

    Raises:
        ValueError: If fewer than two IDs are provided.
    """
    resolved_ids = []

    if sheet_ids:
        for value in sheet_ids:
            resolved_ids.extend(parse_sheet_ids(value))

    if sheet_ids_file:
        resolved_ids.extend(read_sheet_ids_file(sheet_ids_file))

    unique_ids = []
    seen = set()

    for sheet_id in resolved_ids:
        if sheet_id not in seen:
            unique_ids.append(sheet_id)
            seen.add(sheet_id)

    if len(unique_ids) < 2:
        raise ValueError("Provide at least two Google Sheet IDs to merge.")

    return unique_ids


# --- DataFrame Helpers ---

def validate_id_column(
    df: pd.DataFrame,
    id_col: str,
    source_label: str,
) -> pd.DataFrame:
    """Validate and normalize the identifier column in a DataFrame.

    Args:
        df: DataFrame to validate.
        id_col: Identifier column used for merging.
        source_label: Human-readable source label for errors.

    Returns:
        DataFrame with cleaned identifier values.

    Raises:
        ValueError: If the identifier column is missing, blank, or duplicated.
    """
    if id_col not in df.columns:
        raise ValueError(
            f"{source_label} is missing required column: {id_col}"
        )

    df = df.copy().fillna('')
    df[id_col] = df[id_col].astype(str).str.strip()

    blank_ids = df[id_col].eq('')

    if blank_ids.any():
        raise ValueError(
            f"{source_label} contains {int(blank_ids.sum())} blank "
            f"{id_col} value(s)."
        )

    duplicate_ids = df[id_col].value_counts()
    duplicate_ids = duplicate_ids[duplicate_ids > 1]

    if not duplicate_ids.empty:
        examples = ', '.join(str(value) for value in duplicate_ids.index[:10])
        raise ValueError(
            f"{source_label} contains duplicate {id_col} values. "
            f"Examples: {examples}"
        )

    return df


def merge_dataframes_by_id(
    dataframes: list[pd.DataFrame],
    id_col: str = DEFAULT_ID_COLUMN,
    overwrite_existing: bool = False,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """Merge DataFrames using a shared identifier column.

    Rows with new identifiers are appended to the merged DataFrame. When the
    same identifier appears in multiple DataFrames, later DataFrames either
    fill blank values in the existing row or overwrite existing values,
    depending on ``overwrite_existing``.

    Duplicate identifiers within a single DataFrame are rejected. Identifiers
    shared across multiple DataFrames are logged along with the merge behavior
    applied to them.

    Args:
        dataframes: DataFrames to merge.
        id_col: Identifier column used for merging.
        overwrite_existing: Whether later nonblank values should replace
            existing values.
        logger: Optional logger for merge details.

    Returns:
        Merged DataFrame with null values converted to empty strings.

    Raises:
        ValueError: If no DataFrames are provided, an identifier column is
            missing, or a DataFrame contains duplicate identifiers.
    """
    if not dataframes:
        raise ValueError("No DataFrames were provided for merging.")

    def get_dataframe_label(
        dataframe: pd.DataFrame,
        index: int,
    ) -> str:
        """Return a useful label for a source DataFrame."""
        return str(
            dataframe.attrs.get(
                'sheet_name',
                f'DataFrame {index + 1}',
            )
        )

    # Ensure identifiers are unique within each source
    for index, dataframe in enumerate(dataframes):
        source_label = get_dataframe_label(dataframe, index)

        if id_col not in dataframe.columns:
            raise ValueError(
                f"{source_label} is missing identifier column "
                f"'{id_col}'."
            )

        duplicate_mask = dataframe[id_col].duplicated(keep=False)

        if duplicate_mask.any():
            duplicate_ids = (
                dataframe.loc[duplicate_mask, id_col]
                .astype(str)
                .drop_duplicates()
                .tolist()
            )

            message = (
                f"{source_label} contains duplicate values in identifier "
                f"column '{id_col}': {', '.join(duplicate_ids)}"
            )

            if logger:
                logger.error(message)

            raise ValueError(message)

    first_label = get_dataframe_label(dataframes[0], 0)

    # Use the first DataFrame as the base and convert empty strings to pd.NA to
    # distinguish blank values from populated values during merging
    combined = (
        dataframes[0]
        .copy()
        .replace('', pd.NA)
        .set_index(id_col, drop=False)
    )

    if logger:
        logger.info(
            "Initialized merge with %s containing %d record(s).",
            first_label,
            len(combined),
        )

    # Merge each later DataFrame into the accumulated result in source order
    for index, incoming_df in enumerate(
        dataframes[1:],
        start=1,
    ):
        source_label = get_dataframe_label(
            incoming_df,
            index,
        )

        # Normalize blank values and use the identifier as the row index so
        # existing and new records can be identified efficiently
        incoming = (
            incoming_df
            .copy()
            .replace('', pd.NA)
            .set_index(id_col, drop=False)
        )

        # Separate records that already exist from records that must be added
        existing_ids = incoming.index.intersection(
            combined.index,
            sort=False,
        )
        new_ids = incoming.index.difference(
            combined.index,
            sort=False,
        )

        if logger:
            logger.info(
                "Merging %s: %d existing record(s), %d new record(s).",
                source_label,
                len(existing_ids),
                len(new_ids),
            )

        # Log IDs shared across DataFrames and how their values will be resolved 
        if not existing_ids.empty and logger:
            merge_behavior = (
                "nonblank incoming values replaced existing values"
                if overwrite_existing
                else (
                    "existing values were preserved and incoming values "
                    "filled blanks only"
                )
            )

            logger.warning(
                (
                    "Duplicate record identifiers were found while merging "
                    "%s: %s. Merge behavior: %s."
                ),
                source_label,
                ', '.join(map(str, existing_ids)),
                merge_behavior,
            )

        if overwrite_existing:
            # Add columns found only in the incoming DataFrame before updating
            # shared records
            combined = combined.reindex(
                columns=combined.columns.union(
                    incoming.columns,
                    sort=False,
                )
            )

            if not existing_ids.empty:
                # Preserve the original values temporarily so the number of
                # changed cells can be calculated for logging
                before_update = combined.loc[existing_ids].copy()

                # Replace existing values only where the incoming DataFrame 
                # contains a non-null value
                combined.update(
                    incoming.loc[existing_ids],
                )

                if logger:
                    changed_cells = (
                        before_update.fillna('')
                        .ne(
                            combined.loc[existing_ids].fillna('')
                        )
                        .sum()
                        .sum()
                    )

                    logger.info(
                        (
                            "Updated %d value(s) across %d duplicate "
                            "record(s) from %s."
                        ),
                        changed_cells,
                        len(existing_ids),
                        source_label,
                    )

            # Append IDs that were not already present
            if not new_ids.empty:
                combined = pd.concat(
                    [
                        combined,
                        incoming.loc[new_ids],
                    ],
                    axis=0,
                )

        else:
            if not existing_ids.empty and logger:
                # Calculate how many blank cells will receive values from the
                # incoming rows before the merge
                existing_values = combined.loc[
                    existing_ids
                ].copy()

                incoming_values = incoming.loc[
                    existing_ids
                ].reindex(
                    columns=combined.columns.union(
                        incoming.columns,
                        sort=False,
                    )
                )

                filled_cells = (
                    existing_values
                    .reindex(columns=incoming_values.columns)
                    .isna()
                    & incoming_values.notna()
                ).sum().sum()

                logger.info(
                    (
                        "Filled %d blank value(s) across %d duplicate "
                        "record(s) from %s; existing nonblank values "
                        "were retained."
                    ),
                    filled_cells,
                    len(existing_ids),
                    source_label,
                )

            # Fill blanks with incoming values and add any new rows or columns
            combined = combined.combine_first(incoming)

        if logger and not new_ids.empty:
            logger.info(
                "Added %d new record(s) from %s: %s.",
                len(new_ids),
                source_label,
                ', '.join(map(str, new_ids)),
            )

    # Restore the relative column order of input spreadsheets
    ordered_columns = get_merged_column_order(dataframes)
    combined = combined.reindex(columns=ordered_columns)

    # Restore the identifier column and normalize missing values as empty strings
    combined = (
        combined
        .reset_index(drop=True)
        .fillna('')
    )

    if logger:
        logger.info(
            "DataFrame merge complete with %d total record(s).",
            len(combined),
        )

    return combined


def load_google_sheets(
    sheet_ids: list[str],
    sheet_name: str | None,
    credentials_file: str | Path,
    id_col: str,
    logger: logging.Logger,
) -> list[pd.DataFrame]:
    """Load and validate Google Sheets as DataFrames.

    Args:
        sheet_ids: Google Sheet IDs to read.
        sheet_name: Optional tab name.
        credentials_file: Path to Google service account credentials JSON.
        id_col: Identifier column used for merging.
        logger: Logger for process details.

    Returns:
        Validated Google Sheet DataFrames.
    """
    dataframes = []

    for position, sheet_id in enumerate(sheet_ids, start=1):
        logger.info(
            "Reading Google Sheet %s of %s: %s",
            position,
            len(sheet_ids),
            sheet_id,
        )

        df = read_google_sheet(
            sheet_id=sheet_id,
            sheet_name=sheet_name,
            credentials_file=credentials_file,
            logger=logger,
        )

        source_label = f"Google Sheet {sheet_id}"
        df = validate_id_column(df, id_col, source_label)
        dataframes.append(df)

    return dataframes


# --- Main Workflow ---

def merge_batches(
    batch_path: str | Path,
    sheet_ids: list[str] | None = None,
    sheet_ids_file: str | Path | None = None,
    sheet_name: str | None = None,
    id_col: str = DEFAULT_ID_COLUMN,
    credentials_file: str | Path = GOOGLE_CREDENTIALS_FILE,
    overwrite_existing: bool = False,
) -> Path:
    """Merge Google Sheets and save the merged CSV to a batch directory.

    Args:
        batch_path: Workbench batch directory where output is saved.
        sheet_ids: Google Sheet IDs to merge.
        sheet_ids_file: Text file containing Google Sheet IDs.
        sheet_name: Optional tab name to read from each Google Sheet.
        id_col: Identifier column used for merging.
        credentials_file: Path to Google service account credentials JSON.
        overwrite_existing: Whether later sheets should replace existing
            non-empty values.

    Returns:
        Path to the merged CSV.
    """
    logger = None
    log_path = None

    try:
        batch_path = Path(batch_path)
        batch_dir = batch_path.name
        timestamp = datetime.now().strftime('%Y-%m-%d-%H%M%S')

        output_dir = create_directory(batch_path / 'metadata')
        log_dir = create_directory(batch_path / 'logs')

        log_path = log_dir / f'{batch_dir}_{timestamp}_merge_batches.log'
        logger = setup_logger(LOGGER_NAME, log_path)

        resolved_ids = resolve_sheet_ids(sheet_ids, sheet_ids_file)

        logger.info("Merging %d Google Sheets.", len(resolved_ids))
        logger.info("Identifier column: %s", id_col)
        logger.info("Overwrite existing values: %s", overwrite_existing)

        dataframes = load_google_sheets(
            resolved_ids,
            sheet_name,
            credentials_file,
            id_col,
            logger,
        )

        merged = merge_dataframes_by_id(
            dataframes,
            id_col=id_col,
            overwrite_existing=overwrite_existing,
        )

        output_path = output_dir / f'{batch_dir}_merged_batches_{timestamp}.csv'
        df_to_csv(merged, output_path)

        logger.info("Merged output saved to %s", output_path)
        print(
            f"\n{SUCCESS_SYMBOL} Merged {len(resolved_ids)} sheets and "
            f"wrote {len(merged)} rows to: {output_path}"
        )
        print(f"Log saved to: {log_path}")

        return output_path

    except Exception:
        msg = "A critical system error occurred during batch merge."

        if logger:
            logger.exception(msg)

        print(f"\n{ERROR_SYMBOL} {msg}")

        if log_path:
            print(f"See logs: {log_path}")
        else:
            traceback.print_exc()

        raise


def main() -> None:
    """Run the batch merge workflow from the command line."""
    args = parse_arguments()

    merge_batches(
        batch_path=args.batch_path,
        sheet_ids=args.sheet_ids,
        sheet_ids_file=args.sheet_ids_file,
        sheet_name=args.sheet_name,
        id_col=args.id_col,
        credentials_file=args.credentials_file,
        overwrite_existing=args.overwrite_existing,
    )


if __name__ == '__main__':
    main()
