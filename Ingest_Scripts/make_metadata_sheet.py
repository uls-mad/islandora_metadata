#!/usr/bin/python3

"""Merge manifest and metadata Google Sheets using flexible identifier rules.

This script automates the merging of a manifest Google Sheet with a metadata 
Google Sheet. It supports two primary workflows:
1. ID-Based Merge: If metadata identifiers exist, it performs a left join 
   between manifest 'id' and metadata 'identifier'.
2. Direct Append: If no identifiers are present in the metadata, columns 
   are appended directly to the manifest rows.

Unmatched records (metadata identifiers not found in the manifest) are 
logged to a separate CSV for review.
"""

# --- Modules ---

# Import standard modules
import argparse
from datetime import datetime
import logging
import os
import pandas as pd

# Import local module
from definitions import ALLOWED_CONTENT_TYPES
from utilities import *


# --- CONSTANT ---

METADATA_TEMPLATE_MAPPING = {
    "av":                   "1QZJTlxuexgZcEEH2ykvyXMjPhGVPvFgNSGL4ouVS8Do",
    "interview":            "1SAlG6PX5CTG0iqmBm8-T1BYrbOq8de6HQxUcJYMhbOQ",
    "notated_music":        "1Wzsc7GfuXBJfcQ9P_JkuPwcCOeDspG5T7hWvCyAf-Io",
    "serial":               "1-Dsf42gQ6e6r_ll0cITt6lvi_qozzCEvqaHZDC4VfB0",
    "map":                  "18AXRba8mlCSwWBzuuV4CL4XU3dqPrs2iPunBYQXnVo4",
    "photograph":           "1QRVYJ4441rM0yRSH35UpZWYRLkFpMKNc_212G1dwzUU",
    "manuscript":           "1BJZYwe0t2Yz7tOhSD8ns2S8R7MjQqDlOe5-v7jtXqsc",
    "image":                "1DamM4LiGOG0fjMUx_RrODgORX6EiNHbG_SIoKabkn9o",
    "book":                 "1zdgJkH5QCoIWFKHrdC-v2lnTKlQVi_vNg3x59tm5FsM",
    "musical_recording":    "1I8qkiNQN_bcQfS2R_31QbjAIGb9xEuIbrSBBeUIw5vo",
    "photograph":           "1QRVYJ4441rM0yRSH35UpZWYRLkFpMKNc_212G1dwzUU",
    "japanese_prints":      "17NPFIBTDrtZ7Fk_rkE1EYIk9xBoShoejOfxoTU16c-Y",
}


# --- Functions ---

def parse_arguments() -> argparse.Namespace:
    """Parse and return command-line arguments, prompting for missing values.

    Required arguments:
        --manifest_id (str): Google Sheet ID for the manifest file.
        --metadata_id (str): Google Sheet ID for the metadata file.
        --credentials_file (str): Path to the Google service account credentials 
            JSON file.

    Optional arguments:
        --manifest_sheet (str): Tab name in the manifest sheet.
        --metadata_sheet (str): Tab name in the metadata sheet.

    Returns:
        argparse.Namespace: Parsed arguments with all required fields
        guaranteed to be set (via CLI or interactive prompts).
    """

    parser = argparse.ArgumentParser(
        description="Merge manifest and metadata Google Sheets."
    )
    parser.add_argument(
        "-b", "--batch_path", 
        type=str, 
        help="Path to a batch directory for Workbench ingests.")
    parser.add_argument(
        "--manifest_id",
        type=str,
        help="Google Sheet ID for the manifest file."
    )
    parser.add_argument(
        "--manifest_sheet",
        type=str,
        help="Tab name in the manifest sheet (optional)."
    )
    parser.add_argument(
        "--metadata_id",
        type=str,
        help="Google Sheet ID for the metadata file."
    )
    parser.add_argument(
        "--metadata_sheet",
        type=str,
        help="Tab name in the metadata sheet (optional)."
    )
    parser.add_argument(
        "-t", "--content_type",
        type=str,
        help=(
            f"Allowed: {', '.join(sorted(ALLOWED_CONTENT_TYPES))}"
        ),
    )
    parser.add_argument(
        "-c", "--credentials_file",
        type=str,
        default="/workbench/etc/google_ulswfown_service_account.json",
        help="Path to the Google service account credentials JSON."
    )

    args = parser.parse_args()

    # Get ID for metadata template, if content_type provided
    if args.content_type:
        args.metadata_id = METADATA_TEMPLATE_MAPPING.get(args.content_type)

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

    return args


def _normalize_for_join(series: pd.Series) -> pd.Series:
    """Normalize an ID series for joining.

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
    """Merge manifest and metadata DataFrames per workflow rules.

    Args:
        manifest_df (pd.DataFrame): Manifest DataFrame with at least an 'id' column.
            'node_id' is included if present.
        metadata_df (pd.DataFrame): Metadata DataFrame, optionally including 
            data in the 'identifier' column.
        logger (logging.Logger): Logger for process updates.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]:
            - Merged DataFrame containing 'id' (and 'node_id' if available) 
              from the manifest, plus all metadata columns.
            - DataFrame of unmatched metadata rows (those with non-empty
              identifiers not present in the manifest).

    Raises:
        KeyError: If the 'id' column is missing from the manifest DataFrame.
        Exception: For unexpected errors during merging.
    """
    try:
        # Identify available columns
        # 'id' is mandatory; 'node_id' is optional
        if "id" not in manifest_df.columns:
            msg = "Manifest is missing the required column: 'id'"
            logger.error(msg)
            raise KeyError(msg)

        manifest_cols = ["id"]
        if "node_id" in manifest_df.columns:
            manifest_cols.append("node_id")
            logger.info("Found 'node_id' in manifest; including in output.")
        else:
            logger.info(
                "'node_id' not found in manifest; proceeding with 'id' only."
            )

        # Prepare DataFrames
        manifest_df = manifest_df[manifest_cols].copy()
        metadata_df = metadata_df.copy()

        if "identifier" not in metadata_df.columns:
            logger.warning(
                "Metadata sheet is missing the 'identifier' column; " \
                "adding empty column."
            )
            metadata_df.insert(0, "identifier", pd.NA)

        # Normalize keys for joining
        manifest_df["__id_join__"] = _normalize_for_join(
            manifest_df["id"]
        )
        metadata_df["__identifier_join__"] = _normalize_for_join(
            metadata_df["identifier"]
        )

        # If all identifiers are empty after normalization, append columns
        if not metadata_df["__identifier_join__"].notna().any():
            logger.info(
                "Metadata identifiers are empty; appending columns by position."
            )
            merged = pd.concat(
                [manifest_df[manifest_cols].reset_index(drop=True),
                 metadata_df.reset_index(drop=True)],
                axis=1
            )
            # Remove helper columns if they were included in the metadata_df copy
            merged.drop(
                columns=["__id_join__", "__identifier_join__"], 
                errors="ignore", 
                inplace=True
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

        # Identify unmatched records (rows with an identifier not in manifest)
        in_manifest = metadata_df["__identifier_join__"].isin(manifest_df["__id_join__"])
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
    """Execute the metadata and manifest sheet merging workflow.

    Automate the process of aligning physical file manifests with descriptive 
    metadata. Perform the following sequence:

    1.  **Environment Setup:** Generates unique file prefixes using timestamps 
        and configures a file-based logger.
    2.  **Data Acquisition:** Retrieves two distinct Google Sheets (Manifest 
        and Metadata) using provided IDs and sheet names.
    3.  **Data Integration:** Merges the two datasets based on a shared key 
        (handled by `merge_sheets`).
    4.  **Reporting & Output:** Saves the successfully merged records to a 
        final CSV and exports any "orphan" metadata rows to an unmatched log 
        for manual review.

    Side Effects:
        - Creates a log file in the `logs` subdirectory.
        - Writes a merged metadata CSV to the `metadata` subdirectory.
        - Writes an `unmatched.csv` log if metadata rows do not match files.
    """
    # --- Initialization & Logging Setup ---
    args = parse_arguments()

    # Get batch directory and timestamp for output files
    batch_dir = os.path.basename(args.batch_path.rstrip(os.sep))
    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    file_prefix = f"{batch_dir}_{timestamp}"

    # Set up logger
    log_dir = os.path.join(args.batch_path, "logs")
    log_path = os.path.join(log_dir, f"{file_prefix}.log")
    logger = setup_logger('make_metadata_sheet', log_path)

    # Import Google sheeets
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

    # Merge Google sheets
    logger.info("Merging sheets")
    merged, unmatched = merge_sheets(manifest_df, metadata_df, logger)

    # Export merged results
    output_dir = os.path.join(args.batch_path, "metadata")
    output_path = os.path.join(output_dir, f"{file_prefix}_metadata.csv")
    
    logger.info("Saving merged sheet to %s", output_path)
    merged.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Metadata sheet saved to {output_path}")

    # Log unmatched rows from metadata sheet
    if not unmatched.empty:
        log_csv = os.path.join(log_dir, f"{file_prefix}_unmatched.csv")
        logger.warning("Unmatched rows found, writing to %s", log_csv)
        unmatched.to_csv(log_csv, index=False, encoding='utf-8')
        print(f"Unmatched rows found. Log saved to {log_csv}")

    logger.info("Process complete.")


if __name__ == "__main__":
    main()
