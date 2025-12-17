#!/usr/bin/python3
"""
make_metadata_sheet.py

Script to merge a manifest Google Sheet with a metadata Google Sheet.
The merge rules are:
- If the metadata sheet has non-empty 'identifier' values, perform a left
  merge on manifest.id and metadata.identifier.
- If there are identifiers in metadata that do not match manifest IDs,
  they are written to a separate "_unmatched.csv".
- If the metadata sheet has no identifier values, its columns are appended
  directly to the manifest.

Output: merged CSV file and optional unmatched log.
"""

""" Modules """
# Import standard modules
import argparse
from datetime import datetime
import logging
import os
import pandas as pd

# Import local module
from utilities import *


""" Functions """

def parse_arguments() -> argparse.Namespace:
    """
    Parse and return command-line arguments, prompting for missing values.

    Required arguments:
        --manifest_id (str): Google Sheet ID for the manifest file.
        --metadata_id (str): Google Sheet ID for the metadata file.
        --credentials_file (str): Path to the Google service account credentials 
            JSON file.
        --output_file (str): Path to save the merged CSV output.
        --log_file (str): Path to save the process log file.

    Optional arguments:
        --manifest-sheet (str): Tab name in the manifest sheet.
        --metadata-sheet (str): Tab name in the metadata sheet.

    Returns:
        argparse.Namespace: Parsed arguments with all required fields
        guaranteed to be set (via CLI or interactive prompts).
    """

    parser = argparse.ArgumentParser(
        description="Merge manifest and metadata Google Sheets."
    )

    parser.add_argument(
        "--manifest_id",
        type=str,
        help="Google Sheet ID for the manifest file."
    )
    parser.add_argument(
        "--manifest-sheet",
        type=str,
        help="Tab name in the manifest sheet (optional)."
    )
    parser.add_argument(
        "--metadata_id",
        type=str,
        help="Google Sheet ID for the metadata file."
    )
    parser.add_argument(
        "--metadata-sheet",
        type=str,
        help="Tab name in the metadata sheet (optional)."
    )
    parser.add_argument(
        "--credentials_file",
        type=str,
        default="/workbench/etc/google_ulswfown_service_account.json",
        help="Path to the Google service account credentials JSON."
    )
    parser.add_argument(
        "--output_file",
        type=str,
        help="Path to save the merged CSV output."
    )
    # TODO: Confirm that this should be a parameter or if should be auto-determined
    parser.add_argument(
        "--log_file",
        help="Path to save the process log file."
    )

    args = parser.parse_args()

    # Prompt for required arguments if missing
    if not args.batch_path:
        args.batch_path = prompt_for_input(
            "Enter the path to the Workbench batch directory: "
        )
    if not args.manifest_id:
        args.manifest_id = prompt_for_input(
            "Enter the Google Sheet ID for the manifest: "
        )
    if not args.metadata_id:
        args.metadata_id = prompt_for_input(
            "Enter the Google Sheet ID for the metadata: "
        )
    if not args.credentials_file:
        args.credentials_file = prompt_for_input(
            "Enter the path to the Google credentials JSON file: "
        )

    return args


def _normalize_for_join(series: pd.Series) -> pd.Series:
    """
    Normalize an ID series for joining.

    Args:
        series (pd.Series): Input series (e.g., manifest IDs or metadata identifiers).

    Returns:
        pd.Series: Normalized series where:
            - Values are stripped of whitespace.
            - Empty strings and placeholders ('nan', 'none', 'null', 'n/a', 'na')
              are converted to <NA>.
            - All other values are preserved as strings.
    """
    s = series.astype(str).str.strip()
    lower = s.str.lower()
    empties = {'', 'nan', 'none', 'null', 'n/a', 'na'}
    s = s.mask(lower.isin(empties))
    return s


def merge_sheets(
    manifest_df: pd.DataFrame,
    metadata_df: pd.DataFrame,
    logger: logging.Logger
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Merge manifest and metadata DataFrames per workflow rules.

    Args:
        manifest_df (pd.DataFrame): Manifest DataFrame with at least 'id' and
            'node_id' columns.
        metadata_df (pd.DataFrame): Metadata DataFrame, optionally including 
            data in the 'identifier' column.
        logger (logging.Logger): Logger for process updates.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]:
            - Merged DataFrame that contains only 'id' and 'node_id' from
              the manifest, plus all metadata columns.
            - DataFrame of unmatched metadata rows (those with non-empty
              identifiers not present in the manifest). Empty if none.

    Raises:
        KeyError: If required columns ('id', 'node_id') are missing from the
                  manifest DataFrame.
        Exception: For unexpected errors during merging.
    """
    try:
        # Ensure required manifest columns exist
        if not {"id", "node_id"}.issubset(manifest_df.columns):
            msg = "Manifest is missing one or more of the required columns: " \
            "'id', 'node_id'"
            logger.error(msg)
            raise KeyError(msg)

        # Keep only id and node_id from the manifest
        manifest_df = manifest_df[["id", "node_id"]].copy()
        metadata_df = metadata_df.copy()

        if "identifier" not in metadata_df.columns:
            logger.warning(
                "Metadata sheet is missing the 'identifier' column; " \
                "adding empty column."
            )
            # Insert an empty identifier column at the beginning
            metadata_df.insert(0, "identifier", pd.NA)

        # Build normalized join keys (do NOT overwrite original columns)
        manifest_df["__id_join__"] = _normalize_for_join(manifest_df["id"])
        metadata_df["__identifier_join__"] = _normalize_for_join(
            metadata_df["identifier"]
        )

        # If all identifiers are empty after normalization, append columns
        if not metadata_df["__identifier_join__"].notna().any():
            logger.info("Metadata identifiers are empty; appending columns.")
            merged = pd.concat(
                [manifest_df[["id", "node_id"]].reset_index(drop=True),
                 metadata_df.reset_index(drop=True)],
                axis=1
            )
            return merged, pd.DataFrame()

        # Left merge on the normalized keys
        merged = pd.merge(
            manifest_df,
            metadata_df,
            how="left",
            left_on="__id_join__",
            right_on="__identifier_join__",
            suffixes=("", "_metadata")
        )
        logger.info("Merge completed successfully.")

        # Unmatched = metadata rows with a real identifier that isn’t in manifest
        in_manifest = metadata_df["__identifier_join__"].isin(
            manifest_df["__id_join__"]
        )
        nonempty = metadata_df["__identifier_join__"].notna()
        unmatched = metadata_df[nonempty & ~in_manifest].copy()
        if not unmatched.empty:
            logger.warning("%d unmatched metadata rows found.", len(unmatched))

        # Drop helper join columns from merged output
        merged.drop(
            columns=["__id_join__", "__identifier_join__"],
            errors="ignore",
            inplace=True
        )

        return merged, unmatched

    except KeyError:
        # Already logged above, just re-raise
        raise
    except Exception:
        logger.exception("Unexpected error during merge_sheets.")
        raise


def main():
    """
    Main entry point for merging metadata and manifest sheets.
    """
    args = parse_arguments()
    # Get batch directory and timestamp for output files
    batch_dir = os.path.basename(args.batch_path.rstrip(os.sep))
    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    file_prefix = f"{batch_dir}_{timestamp}"

    # Set up logger
    log_dir = os.path.join(args.batch_path, "logs")
    log_path = os.path.join(log_dir, f"{file_prefix}.log")
    logger = setup_logger('make_metadata_sheet', log_path)

    # Read in Google Sheets
    logger.info("Reading manifest Google Sheet")
    manifest_df = read_google_sheet(
        args.manifest_id,
        sheet_name=args.manifest_sheet,
        credentials_file=args.credentials_file
    )

    logger.info("Reading metadata Google Sheet")
    metadata_df = read_google_sheet(
        args.metadata_id,
        sheet_name=args.metadata_sheet,
        credentials_file=args.credentials_file
    )

    # Merge Google Sheets
    logger.info("Merging sheets")
    merged, unmatched = merge_sheets(manifest_df, metadata_df, logger)

    # Save merged output
    output_dir = os.path.join(args.batch_path, "metadata")
    output_path = os.path.join(output_dir, f"{file_prefix}_metadata.csv")
    logger.info("Saving merged sheet to %s", output_path)
    merged.to_csv(args.output_file, index=False, encoding='utf-8')

    # Log unmatched rows from metadata sheet
    if not unmatched.empty:
        log_csv = os.path.join(log_dir, f"{file_prefix}_unmatched.csv")
        logger.warning("Unmatched rows found, writing to %s", log_csv)
        unmatched.to_csv(log_csv, index=False, encoding='utf-8')

    logger.info("Process complete.")


if __name__ == "__main__":
    main()
