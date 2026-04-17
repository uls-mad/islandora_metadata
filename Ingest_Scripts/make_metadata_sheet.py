#!/usr/bin/python3

"""Merge manifest and metadata Google Sheets using flexible identifier rules.

This script automates the merging of a manifest Google Sheet with one or more 
metadata Google Sheets. It supports two primary workflows:
1. ID-Based Merge: If metadata identifiers exist, it performs a left join 
   between manifest 'id' and metadata 'identifier'. Unmatched records are logged 
   to a separate CSV for review.
2. Direct Append: If no identifiers are present in the metadata (e.g., in the 
    case of a metadata template), columns are appended directly to the manifest 
    rows. If a content types is provided, the script will fetch the metadata 
    template for that content type. If more than one content type is provided, 
    the script will aggregate metadata from all corresponding templates before 
    performing the join. 
"""

# --- Modules ---

# Import standard modules
import argparse
import logging
import traceback
from pathlib import Path
from datetime import datetime

# Import third-party module
import pandas as pd

# Import local modules
from definitions import ALLOWED_CONTENT_TYPES
from utilities import (
    prompt_for_input,
    create_directory,
    setup_logger,
    create_df,
    read_google_sheet,
    error_symbol
)


# --- CONSTANTS ---

LOGGER_NAME = "make_metadata_sheet"

METADATA_TEMPLATE_MAPPING = {
    "av":                   "1QZJTlxuexgZcEEH2ykvyXMjPhGVPvFgNSGL4ouVS8Do",
    "interview":            "1SAlG6PX5CTG0iqmBm8-T1BYrbOq8de6HQxUcJYMhbOQ",
    "notated_music":        "1Wzsc7GfuXBJfcQ9P_JkuPwcCOeDspG5T7hWvCyAf-Io",
    "serial":               "1Hh6Wkzwead5yyQW7ZJQSBH2zmm6u-IK2at4yzUcs-Ao",
    "map":                  "18AXRba8mlCSwWBzuuV4CL4XU3dqPrs2iPunBYQXnVo4",
    "photograph":           "1QRVYJ4441rM0yRSH35UpZWYRLkFpMKNc_212G1dwzUU",
    "manuscript":           "1BJZYwe0t2Yz7tOhSD8ns2S8R7MjQqDlOe5-v7jtXqsc",
    "image":                "1DamM4LiGOG0fjMUx_RrODgORX6EiNHbG_SIoKabkn9o",
    "book":                 "1zdgJkH5QCoIWFKHrdC-v2lnTKlQVi_vNg3x59tm5FsM",
    "musical_recording":    "1I8qkiNQN_bcQfS2R_31QbjAIGb9xEuIbrSBBeUIw5vo",
    "japanese_prints":      "17NPFIBTDrtZ7Fk_rkE1EYIk9xBoShoejOfxoTU16c-Y",
}


# --- Functions ---

def parse_arguments() -> argparse.Namespace:
    """Parse and return command-line arguments, prompting for missing values.

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
        "-m", "--manifest_id",
        type=str,
        help="Google Sheet ID for the manifest file."
    )
    parser.add_argument(
        "--manifest_sheet", 
        type=str, 
        help="Path to manifest on local device."
    )
    parser.add_argument(
        "-d", "--metadata_id",
        type=str,
        help="Google Sheet ID for a specific metadata file."
    )
    parser.add_argument(
        "--metadata_sheet", 
        type=str, 
        help="Path to metadata sheet on local device."
    )
    parser.add_argument(
        "-t", "--content_type",
        nargs="+",  # Allows multiple space-separated content types
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

    # Determine which metadata IDs to fetch
    args.metadata_ids = []
    
    # Explicit metadata_id
    if args.metadata_id:
        args.metadata_ids.append(args.metadata_id)
    
    # Content type templates
    if args.content_type:
        for ct in args.content_type:
            tid = METADATA_TEMPLATE_MAPPING.get(ct)
            if tid and tid not in args.metadata_ids:
                args.metadata_ids.append(tid)

    # Prompt for required arguments if missing
    if not args.batch_path:
        args.batch_path = prompt_for_input(
            "Enter the path to the Workbench batch directory: "
        )
    if not args.manifest_id and not args.manifest_sheet:
        args.manifest_id = prompt_for_input(
            "Enter the Google Sheet ID for the manifest: "
        )
    if not args.metadata_ids and not args.metadata_sheet:
        val = prompt_for_input(
            "Enter the Google Sheet ID for the metadata: "
        )
        args.metadata_ids.append(val)

    # Check for invalid input
    if args.manifest_id and args.manifest_sheet:
        raise ValueError(
            "Provide either --manifest_id or --manifest_sheet, not both."
        )
    if args.metadata_id and args.metadata_sheet:
        raise ValueError(
            "Provide either --metadata_id or --metadata_sheet, not both."
        )
    if args.content_type and (args.metadata_id or args.metadata_sheet):
        raise ValueError(
            "Provide either --content_type or --metadata_id or --metadata_sheet."
        )
    if args.content_type:
        invalid = [
            ct for ct in args.content_type if ct not in ALLOWED_CONTENT_TYPES
        ]
        if invalid:
            raise ValueError(f"Invalid content type(s): {', '.join(invalid)}")

    return args


def get_merged_column_order(dfs: list[pd.DataFrame]) -> list[str]:
    """Build a master column list that preserves the relative order of columns.

    Iterate through a sequence of DataFrames and construct a deduplicated list
    of all column names. For new columns, the function attempts to maintain 
    their original relative positioning by inserting them immediately after 
    their predecessor from the source DataFrame.

    Args:
        dfs (list[pd.DataFrame]): A list of pandas DataFrames whose headers 
            need to be merged.

    Returns:
        list[str]: A master list of unique column names ordered according 
            to their relative positions across all input DataFrames.
    """
    master_order = []
    for df in dfs:
        for col in df.columns:
            if col not in master_order:
                # Basic relative insertion: find the predecessor in current df
                col_list = list(df.columns)
                idx = col_list.index(col)
                if idx == 0:
                    master_order.insert(0, col)
                else:
                    prev_col = col_list[idx-1]
                    if prev_col in master_order:
                        prev_idx = master_order.index(prev_col)
                        master_order.insert(prev_idx + 1, col)
                    else:
                        master_order.append(col)
    return master_order


def _normalize_for_join(series: pd.Series) -> pd.Series:
    """Normalize an ID series for joining.

    Args:
        series (pd.Series): Input series (e.g., manifest IDs or metadata identifiers).

    Returns:
        pd.Series: Normalized series where:
            - Values are stripped of whitespace.
            - Empty strings and placeholders ('nan', 'none', 'null', 'n/a', 'na')
              are converted to <NA>.
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
    """Merge manifest and metadata DataFrames according to workflow rules.

    This function merges a manifest DataFrame with a metadata DataFrame using
    normalized identifier fields. It supports two workflows:

    1. ID-Based Merge:
        If metadata identifiers are present, perform a left join between the
        manifest 'id' field and the metadata identifier field ('identifier'
        or fallback 'id').

    2. Direct Append:
        If the metadata identifier column exists but contains no data, append
        metadata columns to manifest rows by position.

    The metadata DataFrame must contain an 'identifier' column. If 'identifier'
    is not present but 'id' is available, the function uses 'id' as a fallback
    and logs a warning. If neither column exists, the function raises an error.

    The output DataFrame always uses 'identifier' as the identifier column
    name, regardless of whether the source identifier came from metadata
    'identifier', metadata 'id', or manifest 'id'.

    Duplicate normalized identifiers in either dataset will raise an error to
    prevent unintended row duplication during merge.

    Args:
        manifest_df (pd.DataFrame): DataFrame containing manifest data. Must
            include an 'id' column and may include 'node_id'.
        metadata_df (pd.DataFrame): DataFrame containing metadata to merge.
            Must include 'identifier' or fallback 'id' as a join field.
        logger (logging.Logger): Logger for recording process steps, warnings,
            and errors.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]:
            - merged: The resulting DataFrame after merging or appending.
            - unmatched: A DataFrame of metadata rows with identifiers not
              found in the manifest (empty if not applicable).

    Raises:
        KeyError: If the manifest DataFrame is missing the required 'id' column.
        ValueError: If the metadata DataFrame is missing both 'identifier' and
            'id', or if duplicate normalized identifiers are detected in either
            the manifest or metadata DataFrame.
    """
    try:
        # Confirm manifest contains required ID column
        if "id" not in manifest_df.columns:
            msg = "Manifest is missing the required column: 'id'"
            logger.error(msg)
            raise KeyError(msg)

        # Determine which columns to keep from manifest
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

        # Identify the ID field in metadata sheet
        id_field = "identifier"
        if id_field not in metadata_df.columns:
            if "id" in metadata_df.columns:
                id_field = "id"
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
        manifest_df["__metadata_id_join__"] = _normalize_for_join(
            manifest_df["id"]
        )
        metadata_df["__manifest_id_join__"] = _normalize_for_join(
            metadata_df[id_field]
        )

        # Handle case if all identifiers are empty: append columns
        if not metadata_df["__manifest_id_join__"].notna().any():
            logger.info(
                "Metadata identifiers are empty; appending columns by position."
            )

            # Drop metadata identifier columns before append so that the
            # manifest identifier becomes the output identifier.
            append_metadata_df = metadata_df.drop(
                columns=["identifier", "id", "__manifest_id_join__"],
                errors="ignore"
            ).reset_index(drop=True)

            merged = pd.concat(
                [
                    manifest_df[manifest_cols].reset_index(drop=True),
                    append_metadata_df
                ],
                axis=1
            )

            # Standardize output identifier column name
            merged.rename(columns={"id": "identifier"}, inplace=True)

            # Remove helper columns from manifest, if present
            merged.drop(
                columns=["__metadata_id_join__"],
                errors="ignore",
                inplace=True
            )

            return merged, pd.DataFrame()

        # Check for duplicate normalized IDs in manifest
        manifest_dupes = (
            manifest_df["__metadata_id_join__"]
            .dropna()
            .value_counts()
        )
        manifest_dupes = manifest_dupes[manifest_dupes > 1]

        if not manifest_dupes.empty:
            sample_dupes = ", ".join(manifest_dupes.index[:10])
            msg = (
                "Manifest contains duplicate normalized IDs. "
                f"Examples: {sample_dupes}"
            )
            logger.error(msg)
            raise ValueError(msg)

        # Check for duplicate normalized identifiers in metadata
        metadata_dupes = (
            metadata_df["__manifest_id_join__"]
            .dropna()
            .value_counts()
        )
        metadata_dupes = metadata_dupes[metadata_dupes > 1]

        if not metadata_dupes.empty:
            sample_dupes = ", ".join(metadata_dupes.index[:10])
            msg = (
                "Metadata contains duplicate normalized identifiers. "
                f"Examples: {sample_dupes}"
            )
            logger.error(msg)
            raise ValueError(msg)

        # Standardize metadata identifier column name for output
        if id_field == "id":
            metadata_df.rename(columns={"id": "identifier"}, inplace=True)

        # Left merge on the normalized keys while preserving the metadata
        # identifier column for output.
        merged = pd.merge(
            manifest_df,
            metadata_df,
            how="left",
            left_on="__metadata_id_join__",
            right_on="__manifest_id_join__",
            suffixes=("", "_metadata"),
            validate="one_to_one"
        )
        logger.info("Merge completed successfully.")

        # In the ID-based merge workflow, keep output 'identifier' and
        # drop manifest 'id' from the final output.
        merged.drop(columns=["id"], errors="ignore", inplace=True)

        # Identify unmatched records using the original metadata_df
        in_manifest = metadata_df["__manifest_id_join__"].isin(
            manifest_df["__metadata_id_join__"]
        )
        nonempty = metadata_df["__manifest_id_join__"].notna()
        unmatched = metadata_df[nonempty & ~in_manifest].copy()

        if not unmatched.empty:
            logger.warning("%d unmatched rows found.", len(unmatched))

        # Standardize unmatched identifier column name
        if "id" in unmatched.columns and "identifier" not in unmatched.columns:
            unmatched.rename(columns={"id": "identifier"}, inplace=True)

        # Clean up helper columns
        merged.drop(
            columns=["__metadata_id_join__", "__manifest_id_join__"],
            errors="ignore",
            inplace=True
        )

        unmatched.drop(
            columns=["__manifest_id_join__"],
            errors="ignore",
            inplace=True
        )

        return merged, unmatched

    except Exception:
        logger.exception(
            "An unexpected error occurred while merging sheets."
        )
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
    logger = None
    log_path = None
    try:
        # --- Initialization & Logging Setup ---
        args = parse_arguments()
        # Get batch directory and timestamp for output files
        batch_path = Path(args.batch_path)
        batch_dir = batch_path.name
        timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
        file_prefix = f"{batch_dir}_{timestamp}"

        # Set up logger
        log_dir = create_directory(batch_path / "logs")
        log_path = log_dir / f"{file_prefix}.log"
        logger = setup_logger(LOGGER_NAME, log_path)

        # --- Sheet Importing Process ---
        # Read Manifest
        if args.manifest_id:   
            logger.info("Reading manifest Google Sheet: %s", args.manifest_id)
            try:
                manifest_df = read_google_sheet(
                    args.manifest_id,
                    credentials_file=args.credentials_file,
                    logger=logger
                )
            except Exception:
                logger.exception(
                    f"Failed while reading manifest sheet: {args.manifest_id}"
                )
                raise
        else:
            logger.info("Reading manifest CSV: %s", args.manifest_sheet)
            manifest_df = create_df(args.manifest_sheet)

        # Read Metadata
        if args.metadata_ids:
            # Get relevant metadata sheet(s)
            all_metadata_dfs = []
            for sheet_id in args.metadata_ids:
                logger.info("Fetching metadata from: %s", sheet_id)
                df = read_google_sheet(
                    sheet_id,  
                    credentials_file=args.credentials_file,
                    logger=logger
                )

                # Check for presence of columns
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
            
            # Determine master column order across all templates
            master_cols = get_merged_column_order(all_metadata_dfs)
            
            # Combine data and reorder columns to the master sequence
            full_metadata_df = pd.concat(all_metadata_dfs, ignore_index=True)
            metadata_df = full_metadata_df[master_cols]
        else:
            logger.info("Reading metadata CSV: %s", args.metadata_sheet)
            metadata_df = create_df(args.metadata_sheet)

        # --- Sheet Merging Process ---
        # Merge manifest and metadata sheets
        logger.info("Merging sheets...")
        merged, unmatched = merge_sheets(manifest_df, metadata_df, logger)

        # Export merged results
        output_dir = create_directory(batch_path / "metadata")
        output_path = output_dir / f"{file_prefix}_metadata.csv"
        
        logger.info("Saving merged sheet to %s", output_path)
        merged.to_csv(output_path, index=False, encoding='utf-8')
        print(f"Metadata sheet saved to {output_path}")

        # Log unmatched rows from metadata sheet
        if not unmatched.empty:
            log_csv = log_dir / f"{file_prefix}_unmatched.csv"
            logger.warning("Unmatched rows found, writing to %s", log_csv)
            unmatched.to_csv(log_csv, index=False, encoding='utf-8')
            print(f"Unmatched rows found. Log saved to {log_csv}")

        logger.info("Process complete.")

    except Exception:
        msg = "A critical system error occurred during execution."
        if logger:
            # Log full technical detail to file
            logger.exception(
                msg
            )

        # Show the user error message
        print(f"\n{error_symbol} {msg}")

        if log_path:
            print(f"See logs: {log_path}")
        else:
            traceback.print_exc()


if __name__ == "__main__":
    main()
