#!/usr/bin/env python3

"""Generate Islandora metadata sheets from manifests and source metadata.

This script combines manifest data with descriptive metadata from Google Sheets
or local spreadsheet files to produce a standardized Islandora metadata sheet.
It supports identifier-based merging, metadata template expansion, duplicate
resolution, and validation of merge results while generating detailed logs for
unmatched and deduplicated records.

Usage:
    # Merge using Google Sheets
    python3 make_metadata_sheet.py \
        --batch_path /workbench/batches/example \
        --manifest_id <manifest_sheet_id> \
        --metadata_id <metadata_sheet_id>

    # Merge using local spreadsheets
    python3 make_metadata_sheet.py \
        --batch_path /workbench/batches/example \
        --manifest_sheet manifest.xlsx \
        --metadata_sheet metadata.xlsx

    # Merge using one or more metadata templates
    python3 make_metadata_sheet.py \
        --batch_path /workbench/batches/example \
        --manifest_id <manifest_sheet_id> \
        --content_type photograph interview
"""

# ---------------------------------------------------------------------------
# Modules
# ---------------------------------------------------------------------------

# Standard library imports
import argparse
import logging
import traceback
from datetime import datetime
from pathlib import Path

# Third-party imports
import pandas as pd

# Local imports
from definitions import CONTENT_TYPES
from utilities import (
    ERROR_SYMBOL,
    LogRegistry,
    create_df,
    create_directory,
    df_to_csv,
    normalize_for_join,
    prompt_for_input,
    read_google_sheet,
    setup_logger,
)


# ---------------------------------------------------------------------------
# Globals
# ---------------------------------------------------------------------------

LOGGER_NAME = LogRegistry.MAKE_METADATA_SHEET

METADATA_TEMPLATE_MAPPING = {
    'av': '1bmTuRiuZT1W_lHtgZv1_CMDHoC6j9JDCtjtwUdcIKc4',
    'interview': '1aLOM_8tUmzbYzjCoj00ESzt1N_o6HslJ-oSudrdMzG8',
    'notated_music': '16pKwfkQGDkj1Xl_3WlmigVbRCWK88bi4_4iMlhrxq04',
    'serial': '1Hh6Wkzwead5yyQW7ZJQSBH2zmm6u-IK2at4yzUcs-Ao',
    'map': '1dVu3Aeo-ee4omRgfmT62pNb5N-lXzGJa0L47I9alvlk',
    'photograph': '1v1gb1ca7FF-n-4J627unxrfRZN94msPAwEn4irLZm-E',
    'manuscript': '1mvrZOTUyYaZRl53lcNSETu5FWZEl8KXsV7dKMyolBDI',
    'image': '17yrJNN6XdDyoehdg768dVh9gyMjNjgdq09yDSAIzaG0',
    'book': '1objzzKww9dzAz7rurrlCFCS7gF16litL67y6h_KZgow',
    'musical_recording': '1wW-HwSquQPr8PLWT4aYuXXxMBdgzi0cB9Mh6pl3UIRE',
    'japanese_prints': '19uKsSiNhh5QvnW6Od3iJD5J7Dihx7l-ZkXqFhIFfQbQ',
    'marc': '1R-t5_p97aUVAU6d20mz00uB-52_Ud4S4-V2t-lANfnQ',
}


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------

# --- Input Resolution and Validation ---

def resolve_metadata_ids(
    metadata_id: str | None = None,
    content_type: list[str] | None = None
) -> list[str]:
    """Resolve explicit metadata IDs and content-type template IDs.

    Args:
        metadata_id: Google Sheet ID for a specific metadata file.
        content_type: Content type values used to identify metadata template
            Google Sheet IDs.

    Returns:
        Metadata Google Sheet IDs to fetch.
    """
    metadata_ids = []

    if metadata_id:
        metadata_ids.append(metadata_id)

    if content_type:
        for ct in content_type:
            template_id = METADATA_TEMPLATE_MAPPING.get(ct)

            if template_id and template_id not in metadata_ids:
                metadata_ids.append(template_id)

    return metadata_ids


def validate_inputs(
    manifest_id: str | None = None,
    manifest_sheet: str | Path | None = None,
    metadata_id: str | None = None,
    metadata_sheet: str | Path | None = None,
    content_type: list[str] | None = None
) -> None:
    """Validate mutually exclusive input options.

    Args:
        manifest_id: Google Sheet ID for the manifest file.
        manifest_sheet: Path to manifest on local device.
        metadata_id: Google Sheet ID for a specific metadata file.
        metadata_sheet: Path to metadata sheet on local device.
        content_type: Content type values used to identify metadata template
            Google Sheet IDs.

    Raises:
        ValueError: If mutually exclusive arguments are used together or if an
            invalid content type is provided.
    """
    if manifest_id and manifest_sheet:
        raise ValueError(
            "Provide either --manifest_id or --manifest_sheet, not both."
        )

    if metadata_id and metadata_sheet:
        raise ValueError(
            "Provide either --metadata_id or --metadata_sheet, not both."
        )

    if content_type:
        invalid = [
            ct for ct in content_type
            if ct not in CONTENT_TYPES
        ]

        if invalid:
            raise ValueError(
                f"Invalid content type(s): {', '.join(invalid)}"
            )


# --- Argument Parsing ---

def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments and prompt for missing values.

    Returns:
        Parsed arguments with required fields set by CLI or prompt.
    """
    parser = argparse.ArgumentParser(
        description="Merge manifest and metadata Google Sheets."
    )
    parser.add_argument(
        '-b',
        '--batch_path',
        type=str,
        help="Path to a batch directory for Workbench ingests."
    )
    parser.add_argument(
        '-m',
        '--manifest_id',
        type=str,
        help="Google Sheet ID for the manifest file."
    )
    parser.add_argument(
        '--manifest_sheet',
        type=str,
        help="Path to manifest on local device."
    )
    parser.add_argument(
        '-d',
        '--metadata_id',
        type=str,
        help="Google Sheet ID for a specific metadata file."
    )
    parser.add_argument(
        '--metadata_sheet',
        type=str,
        help="Path to metadata sheet on local device."
    )
    parser.add_argument(
        '-t',
        '--content_type',
        nargs='+',
        help=f"Allowed: {', '.join(sorted(CONTENT_TYPES))}"
    )
    parser.add_argument(
        '-c',
        '--credentials_file',
        type=str,
        default='/workbench/etc/google_ulswfown_service_account.json',
        help="Path to the Google service account credentials JSON."
    )
    args = parser.parse_args()

    if not args.batch_path:
        args.batch_path = prompt_for_input(
            "Enter the path to the Workbench batch directory: "
        )

    if not args.manifest_id and not args.manifest_sheet:
        args.manifest_id = prompt_for_input(
            "Enter the Google Sheet ID for the manifest: "
        )

    if not args.metadata_ids and not args.metadata_sheet:
        metadata_id = prompt_for_input(
            "Enter the Google Sheet ID for the metadata: "
        )
        args.metadata_ids.append(metadata_id)

    validate_inputs(
        manifest_id=args.manifest_id,
        manifest_sheet=args.manifest_sheet,
        metadata_id=args.metadata_id,
        metadata_sheet=args.metadata_sheet,
        content_type=args.content_type
    )

    args.metadata_ids = resolve_metadata_ids(
        metadata_id=args.metadata_id,
        content_type=args.content_type
    )

    return args


# --- Column Ordering ---

def get_merged_column_order(dfs: list[pd.DataFrame]) -> list[str]:
    """Build a master column list preserving relative column order.

    Args:
        dfs: DataFrames whose headers need to be merged.

    Returns:
        Unique column names ordered according to their relative positions across
        the input DataFrames.
    """
    master_order = []

    for df in dfs:
        for col in df.columns:
            if col in master_order:
                continue

            col_list = list(df.columns)
            idx = col_list.index(col)

            if idx == 0:
                master_order.insert(0, col)
                continue

            prev_col = col_list[idx - 1]

            if prev_col in master_order:
                prev_idx = master_order.index(prev_col)
                master_order.insert(prev_idx + 1, col)
            else:
                master_order.append(col)

    return master_order


# --- Sheet Merging ---

def merge_sheets(
    manifest_df: pd.DataFrame,
    metadata_df: pd.DataFrame,
    logger: logging.Logger
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Merge manifest and metadata DataFrames.

    This function supports two workflows:

    1. ID-Based Merge:
        If metadata identifiers are present, perform a left join between the
        manifest 'id' field and the metadata identifier field, either
        'identifier' or fallback 'id'.

    2. Direct Append:
        If the metadata identifier column exists but contains no data, append
        metadata columns to manifest rows by position.

    Args:
        manifest_df: Manifest DataFrame. Must include an 'id' column.
        metadata_df: Metadata DataFrame. Must include 'identifier' or fallback
            'id' as a join field.
        logger: Logger for recording process steps, warnings, and errors.

    Returns:
        Tuple containing:
            - merged DataFrame after merging or appending.
            - unmatched metadata rows with identifiers not found in manifest.

    Raises:
        KeyError: If the manifest DataFrame is missing 'id'.
        ValueError: If metadata is missing both 'identifier' and 'id', or if
            duplicate normalized identifiers are detected.
    """
    try:
        # Confirm manifest contains required ID column
        if 'id' not in manifest_df.columns:
            msg = "Manifest is missing the required column: 'id'"
            logger.error(msg)
            raise KeyError(msg)

        # Keep ID columns in manifest
        manifest_cols = ['id']

        if 'node_id' in manifest_df.columns:
            manifest_cols.append('node_id')
            logger.info("Found 'node_id' in manifest; including in output.")
        else:
            logger.info(
                "'node_id' not found in manifest; proceeding with 'id' only."
            )

        # Prepare DataFrames
        manifest_df = manifest_df[manifest_cols].copy()
        metadata_df = metadata_df.copy()

        # Identify the ID field in metadata sheet
        id_field = 'identifier'

        if id_field not in metadata_df.columns:
            if 'id' in metadata_df.columns:
                id_field = 'id'
                logger.warning(
                    "Preferred metadata join field 'identifier' was not found; "
                    "using fallback field 'id'."
                )
            else:
                msg = (
                    "Metadata is missing the required join column "
                    "'identifier' (or fallback 'id')."
                )
                logger.error(msg)
                raise ValueError(msg)

        logger.info("Using '%s' as metadata join field.", id_field)

        # Create normalized join keys
        manifest_df['__metadata_id_join__'] = normalize_for_join(
            manifest_df['id']
        )
        metadata_df['__manifest_id_join__'] = normalize_for_join(
            metadata_df[id_field]
        )

        # Handle case if all identifiers are empty: append columns
        if not metadata_df['__manifest_id_join__'].notna().any():
            logger.info(
                "Metadata identifiers are empty; appending columns by position."
            )

            # Drop metadata identifier columns before append so that the
            # manifest identifier becomes the output identifier.
            append_metadata_df = metadata_df.drop(
                columns=['identifier', 'id', '__manifest_id_join__'],
                errors='ignore'
            ).reset_index(drop=True)

            merged = pd.concat(
                [
                    manifest_df[manifest_cols].reset_index(drop=True),
                    append_metadata_df,
                ],
                axis=1
            )

            # Standardize output identifier column name
            merged.rename(columns={'id': 'identifier'}, inplace=True)

            merged.drop(
                columns=['__metadata_id_join__'],
                errors='ignore',
                inplace=True
            )

            return merged, pd.DataFrame()

        # Check for duplicate normalized IDs in manifest
        manifest_dupes = (
            manifest_df['__metadata_id_join__']
            .dropna()
            .value_counts()
        )
        manifest_dupes = manifest_dupes[manifest_dupes > 1]

        if not manifest_dupes.empty:
            sample_dupes = ', '.join(manifest_dupes.index[:10])
            msg = (
                "Manifest contains duplicate normalized IDs. "
                f"Examples: {sample_dupes}"
            )
            logger.error(msg)
            raise ValueError(msg)

        # Check for duplicate normalized identifiers in metadata sheet
        metadata_dupes = (
            metadata_df['__manifest_id_join__']
            .dropna()
            .value_counts()
        )
        metadata_dupes = metadata_dupes[metadata_dupes > 1]

        if not metadata_dupes.empty:
            sample_dupes = ', '.join(metadata_dupes.index[:10])
            msg = (
                "Metadata contains duplicate normalized identifiers. "
                f"Examples: {sample_dupes}"
            )
            logger.error(msg)
            raise ValueError(msg)

        # Standardize metadata identifier column name for output
        if id_field == 'id':
            metadata_df.rename(columns={'id': 'identifier'}, inplace=True)

        # Merge manifest and metadata sheet
        merged = pd.merge(
            manifest_df,
            metadata_df,
            how='left',
            left_on='__metadata_id_join__',
            right_on='__manifest_id_join__',
            suffixes=('', '_metadata'),
            validate='one_to_one'
        )
        logger.info("Merge completed successfully.")

        # Drop 'id' and keep 'identifier' for final output 
        merged.drop(columns=['id'], errors='ignore', inplace=True)

        # Identify and report any records in metadata sheet but not manifest
        in_manifest = metadata_df['__manifest_id_join__'].isin(
            manifest_df['__metadata_id_join__']
        )
        nonempty = metadata_df['__manifest_id_join__'].notna()
        unmatched = metadata_df[nonempty & ~in_manifest].copy()

        if not unmatched.empty:
            logger.warning("%d unmatched rows found.", len(unmatched))

        # Standardize unmatched identifier column name
        if 'id' in unmatched.columns and 'identifier' not in unmatched.columns:
            unmatched.rename(columns={'id': 'identifier'}, inplace=True)

        # Clean up helper columns
        merged.drop(
            columns=['__metadata_id_join__', '__manifest_id_join__'],
            errors='ignore',
            inplace=True
        )

        unmatched.drop(
            columns=['__manifest_id_join__'],
            errors='ignore',
            inplace=True
        )

        return merged, unmatched

    except Exception:
        logger.exception(
            "An unexpected error occurred while merging sheets."
        )
        raise


# --- Metadata Cleanup ---

def deduplicate_metadata_rows(
    df: pd.DataFrame,
    logger: logging.Logger
) -> pd.DataFrame:
    """Deduplicate metadata rows by identifier or id.

    If duplicate metadata identifiers are found, keep the row with the most
    populated fields and log the rows that were dropped.

    Args:
        df: Metadata DataFrame to deduplicate.
        logger: Logger for recording process details.

    Returns:
        Deduplicated metadata DataFrame.
    """
    df = df.copy()
    id_col = None

    if 'identifier' in df.columns:
        id_col = 'identifier'
    elif 'id' in df.columns:
        id_col = 'id'

    if id_col is None:
        return df

    df[id_col] = df[id_col].astype(str).str.strip()

    df['__nonempty_count__'] = (
        df.astype(str)
        .apply(lambda col: col.str.strip().ne(''))
        .sum(axis=1)
    )

    duplicate_ids = df[id_col].value_counts()
    duplicate_ids = duplicate_ids[duplicate_ids > 1]

    if not duplicate_ids.empty:
        logger.warning(
            "Duplicate metadata %s values found: %s",
            id_col,
            ', '.join(str(value) for value in duplicate_ids.index[:20])
        )

        for duplicate_id in duplicate_ids.index:
            duplicate_rows = df[df[id_col] == duplicate_id].copy()
            duplicate_rows = duplicate_rows.sort_values(
                '__nonempty_count__',
                ascending=False
            )

            kept_index = duplicate_rows.index[0]
            dropped_indexes = duplicate_rows.index[1:]

            logger.warning(
                "Keeping metadata row %s for duplicate %s '%s'.",
                kept_index,
                id_col,
                duplicate_id
            )

            for dropped_index in dropped_indexes:
                logger.warning(
                    "Dropping duplicate metadata row %s for %s '%s'.",
                    dropped_index,
                    id_col,
                    duplicate_id
                )

        df = (
            df.sort_values('__nonempty_count__', ascending=False)
            .drop_duplicates(subset=[id_col], keep='first')
        )

    df.drop(columns=['__nonempty_count__'], errors='ignore', inplace=True)
    df = df.sort_index().reset_index(drop=True)

    return df


# --- Template Columns ---

def add_missing_template_columns(
    metadata_df: pd.DataFrame,
    template_dfs: list[pd.DataFrame],
    logger: logging.Logger
) -> pd.DataFrame:
    """Add missing template columns to a metadata DataFrame.

    Args:
        metadata_df: Metadata DataFrame to update.
        template_dfs: Metadata template DataFrames whose columns should be
            represented in the output.
        logger: Logger for recording process details.

    Returns:
        Metadata DataFrame with missing template columns added.
    """
    metadata_df = metadata_df.copy()

    if not template_dfs:
        return metadata_df

    # Determine template column order across all template sheets
    template_cols = get_merged_column_order(template_dfs)

    # Identify missing columns
    missing_cols = [
        col for col in template_cols
        if col not in metadata_df.columns
    ]

    # Add missing columns as blank values
    for col in missing_cols:
        metadata_df[col] = ''

    if missing_cols:
        logger.info(
            "Added %d missing template column(s) to metadata sheet: %s",
            len(missing_cols),
            ', '.join(missing_cols)
        )
    else:
        logger.info(
            "No missing template columns needed to be added to metadata sheet."
        )

    # Preserve original metadata column order, followed by newly added columns
    ordered_cols = list(metadata_df.columns)

    return metadata_df[ordered_cols]


# --- Main Workflow ---

def make_metadata_sheet(
    batch_path: str | Path,
    manifest_id: str | None = None,
    manifest_sheet: str | Path | None = None,
    metadata_id: str | None = None,
    metadata_sheet: str | Path | None = None,
    content_type: list[str] | None = None,
    credentials_file: str = '/workbench/etc/google_ulswfown_service_account.json'
) -> dict[str, Path | None]:
    """Execute the metadata and manifest sheet merging workflow.

    Args:
        batch_path: Path to a batch directory for Workbench ingests.
        manifest_id: Google Sheet ID for the manifest file.
        manifest_sheet: Path to manifest on local device.
        metadata_id: Google Sheet ID for a specific metadata file.
        metadata_sheet: Path to metadata sheet on local device.
        content_type: Content type values used to identify metadata template
            Google Sheet IDs.
        credentials_file: Path to Google service account credentials JSON.

    Returns:
        Output paths for the metadata CSV, log file, and unmatched CSV if
        created.

    Raises:
        ValueError: If required input combinations are missing.
        Exception: If a critical runtime error occurs.
    """
    logger = None
    log_path = None
    unmatched_path = None

    try:
        validate_inputs(
            manifest_id=manifest_id,
            manifest_sheet=manifest_sheet,
            metadata_id=metadata_id,
            metadata_sheet=metadata_sheet,
            content_type=content_type
        )

        metadata_ids = resolve_metadata_ids(
            metadata_id=metadata_id,
            content_type=content_type
        )

        if not manifest_id and not manifest_sheet:
            raise ValueError(
                "Provide either manifest_id or manifest_sheet."
            )

        if not metadata_ids and not metadata_sheet:
            raise ValueError(
                "Provide either metadata_id, metadata_sheet, or content_type."
            )

        # Set up output files
        batch_path = Path(batch_path)
        batch_dir = batch_path.name
        timestamp = datetime.now().strftime('%Y-%m-%d-%H%M%S')
        file_prefix = f'{batch_dir}_{timestamp}'

        # Read Manifest
        log_dir = create_directory(batch_path / 'logs')
        log_path = log_dir / f'{file_prefix}_metadata_sheet_processing.log'
        logger = setup_logger(LOGGER_NAME, log_path)

        if manifest_id:
            logger.info("Reading manifest Google Sheet: %s", manifest_id)

            try:
                manifest_df = read_google_sheet(
                    manifest_id,
                    credentials_file=credentials_file,
                    logger=logger
                )
            except Exception:
                logger.exception(
                    "Failed while reading manifest sheet: %s",
                    manifest_id
                )
                raise
        else:
            logger.info("Reading manifest CSV: %s", manifest_sheet)
            manifest_df = create_df(manifest_sheet)

        # Read Metadata
        if metadata_sheet:
            logger.info("Reading metadata CSV: %s", metadata_sheet)
            metadata_df = create_df(metadata_sheet)
            metadata_df = deduplicate_metadata_rows(metadata_df, logger)

            if metadata_ids:
                template_dfs = []

                for sheet_id in metadata_ids:
                    df = read_google_sheet(
                        sheet_id,
                        credentials_file=credentials_file,
                        logger=logger
                    )

                    if not df.columns.empty:
                        template_dfs.append(df)

                metadata_df = add_missing_template_columns(
                    metadata_df,
                    template_dfs,
                    logger
                )

        # Get columns from relevant metadata sheet(s)
        elif metadata_ids:
            all_metadata_dfs = []

            for sheet_id in metadata_ids:
                logger.info("Fetching metadata from: %s", sheet_id)

                df = read_google_sheet(
                    sheet_id,
                    credentials_file=credentials_file,
                    logger=logger
                )

                if not df.columns.empty:
                    all_metadata_dfs.append(df)
                else:
                    logger.warning(
                        "Sheet %s has no columns; skipping.",
                        sheet_id
                    )

            if not all_metadata_dfs:
                raise ValueError(
                    "No metadata structure found in any provided sheets."
                )

            # Combine data and reorder columns to the master column order
            master_cols = get_merged_column_order(all_metadata_dfs)
            full_metadata_df = pd.concat(
                all_metadata_dfs,
                ignore_index=True
            )
            metadata_df = full_metadata_df[master_cols]

        # Merge manifest and metadata sheets
        logger.info("Merging sheets...")
        merged, unmatched = merge_sheets(manifest_df, metadata_df, logger)

        # Export merged results
        output_dir = create_directory(batch_path / 'metadata')
        output_path = output_dir / f'{file_prefix}_metadata.csv'

        logger.info("Saving merged sheet to %s", output_path)
        df_to_csv(merged, output_path)
        print(f"Metadata sheet saved to {output_path}")

        # Log unmatched rows from metadata sheet
        if not unmatched.empty:
            unmatched_path = log_dir / f'{file_prefix}_unmatched.csv'

            logger.warning(
                "Unmatched rows found, writing to %s",
                unmatched_path
            )
            df_to_csv(unmatched, unmatched_path)
            print(f"Unmatched rows found. Log saved to {unmatched_path}")

        logger.info("Process complete.")

        return {
            'output_path': output_path,
            'log_path': log_path,
            'unmatched_path': unmatched_path,
        }

    except Exception:
        msg = "A critical system error occurred during execution."

        if logger:
            logger.exception(msg)

        print(f"\n{ERROR_SYMBOL} {msg}")

        if log_path:
            print(f"See logs: {log_path}")
        else:
            traceback.print_exc()

        raise


def main() -> None:
    """Run the script from the command line."""
    args = parse_arguments()

    make_metadata_sheet(
        batch_path=args.batch_path,
        manifest_id=args.manifest_id,
        manifest_sheet=args.manifest_sheet,
        metadata_id=args.metadata_id,
        metadata_sheet=args.metadata_sheet,
        content_type=args.content_type,
        credentials_file=args.credentials_file
    )


if __name__ == '__main__':
    main()
