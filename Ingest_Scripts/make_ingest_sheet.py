#!/bin/python3 

""" Modules """

# Import standard modules
import os
import sys
import re
import threading
import time
import traceback
import argparse
from queue import Queue
from datetime import datetime
from typing import Dict, List, Set, Tuple, Union, Optional

# Import third-party modules
import pandas as pd
from edtf import parse_edtf

# Import local modules
from utilities import *
from definitions import *
from batch_manager import *
from progress_tracker import *


""" Global Variables """

global transformations
transformations = []

global exceptions
exceptions = []

global current_file
current_file = None

global current_batch
current_batch = None

DEFAULT_BATCH_SIZE = 10000


""" Class """

class AppConfig:
    """
    A container for all application configuration parameters derived from 
    command-line arguments and user prompts.
    """
    def __init__(
        self,
        user_id: str,
        batch_path: str,
        batch_size: int,
        manifest_id: str,
        metadata_id: str,
        credentials_file: str,
        ingest_task: str,
        metadata_level: str,
        publish: bool,
        manifest_sheet: Optional[str] = None,
        metadata_sheet: Optional[str] = None,
    ):
        # Mandatory parameters
        self.user_id = user_id
        self.batch_path = batch_path
        self.batch_size = batch_size
        self.manifest_id = manifest_id
        self.metadata_id = metadata_id
        self.credentials_file = credentials_file
        self.ingest_task = ingest_task
        self.metadata_level = metadata_level
        self.publish = publish
        
        # Optional parameters
        self.manifest_sheet = manifest_sheet
        self.metadata_sheet = metadata_sheet  


""" Helper Functions """

def parse_arguments() -> AppConfig:
    """
    Parse command-line arguments and interactively prompt for missing required values.

    Returns:
        AppConfig: An object containing all parsed and prompted configuration settings.
    """
    parser = argparse.ArgumentParser(description="Process CSV files in batches.")
    
    # --- Argument Definitions ---
    parser.add_argument(
        "-u", "--user_id", 
        type=str, 
        help="The user ID to associate with the operation."
    )
    parser.add_argument(
        "-b", "--batch_path", 
        type=str, 
        help="Path to a batch directory for Workbench ingests.")
    parser.add_argument(
        "-z", "--batch_size", 
        type=int, 
        default=DEFAULT_BATCH_SIZE, 
        help=f"Number of records per batch (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument(
        "-m", "--manifest_id", 
        type=str, 
        help="Google Sheet ID for the manifest file.")
    parser.add_argument(
        "--manifest_sheet", 
        type=str, 
        help="Tab name in the manifest sheet (optional).")
    parser.add_argument(
        "-d", "--metadata_id", 
        type=str, 
        help="Google Sheet ID for the metadata file.")
    parser.add_argument(
        "--metadata_sheet", 
        type=str, 
        help="Tab name in the metadata sheet (optional).")
    parser.add_argument(
        "-c", "--credentials_file", 
        type=str, 
        default="/workbench/etc/google_ulswfown_service_account.json",
        help="Path to the Google service account credentials JSON.")
    parser.add_argument(
        "-t", "--ingest_task", 
        type=str, 
        choices=["create", "update"], 
        help="Workbench task: 'create' or 'update'.")
    parser.add_argument(
        "-l", "--metadata_level", 
        type=str, 
        choices=["minimal", "complete"], 
        help="Metadata detail level: 'minimal' or 'complete'.")
    parser.add_argument(
        "-p", "--publish", 
        type=str, 
        choices=["y", "n"], 
        help="Specify whether or not the ingest batch should be published ('y' or 'n').")

    # Parse initial arguments
    args = parser.parse_args()
    
    # Prompt user for missing required arguments
    if not args.user_id:
        args.user_id = prompt_for_input("Enter your Pitt user ID: ")
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
        
    # Handle choices-based interactive prompts
    if not args.ingest_task:
        choice_input = prompt_for_input(
            "Enter the Workbench ingest task (create/update): ", 
            valid_choices=['create', 'update']
        )
        args.ingest_task = choice_input
        
    if not args.metadata_level:
        choice_input = prompt_for_input(
            "Enter the metadata level (minimal/complete): ", 
            valid_choices=['minimal', 'complete']
        )
        args.metadata_level = choice_input
        
    if not args.publish:
        choice_input = prompt_for_input(
            "Should the ingest batch be published (y/n)?: ", 
            valid_choices=['y', 'n']
        )
        args.publish = choice_input
    
    # Convert 'y'/'n' string to boolean True/False for the AppConfig class
    args.publish = (args.publish == 'y')

    # Return the Config Object
        # Convert the argparse.Namespace object's contents into a dictionary, 
        # then unpack that dictionary to initialize the AppConfig instance.
    return AppConfig(**vars(args))


def load_input_sheets(config: AppConfig) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load manifest and metadata inputs either from Google Sheets or from
    local CSV files based on the configuration object.

    Priority order for each input:
        1. Use Google Sheet if <id> is provided.
        2. Otherwise, load a local CSV if <sheet> is provided.

    Args:
        config (AppConfig): The application configuration object containing:
            - manifest_id (str)
            - manifest_sheet (str | None)
            - metadata_id (str)
            - metadata_sheet (str | None)
            - credentials_file (str)

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]:
            (manifest_df, metadata_df)
    """
    # ---------- Load manifest ----------
    # Access attributes directly from the config object
    if config.manifest_id:
        manifest_df = read_google_sheet(
            config.manifest_id,
            sheet_name=config.manifest_sheet,
            credentials_file=config.credentials_file
        )
    elif config.manifest_sheet:
        manifest_df = create_df(config.manifest_sheet)

    # ---------- Load metadata ----------
    if config.metadata_id:
        metadata_df = read_google_sheet(
            config.metadata_id,
            sheet_name=config.metadata_sheet,
            credentials_file=config.credentials_file
        )
    elif config.metadata_sheet:
        metadata_df = create_df(config.metadata_sheet)

    return manifest_df, metadata_df


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


import pandas as pd
import logging
import os

def merge_sheets(
    manifest_df: pd.DataFrame,
    metadata_df: pd.DataFrame,
    ingest_task: str,
    logger: logging.Logger
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Merge manifest and metadata DataFrames per workflow rules, 
    deduplicating overlapping columns and logging mismatches.

    Args:
        manifest_df (pd.DataFrame): Manifest DataFrame with at least 'id' and
            'node_id' columns.
        metadata_df (pd.DataFrame): Metadata DataFrame, optionally including 
            data in the 'identifier' column.
        ingest_task (str): Task that Workbench will perform. 
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
    # Define the required and applicable optional columns
    required_columns = [
        "id",
        "file",
        "field_model",
        "field_resource_type",
        "field_domain_access",
        "field_depositor",
        "field_member_of",
        "published"
    ]

    optional_columns = [
        "parent_id",
        "weight",
        "transcript",
        "thumbnail",
    ]

    if ingest_task == "update":
        required_columns.insert(1, "node_id")
    
    try:
        # Check for missing required columns and add them
        for col in required_columns:
            if col not in manifest_df.columns:
                manifest_df[col] = pd.NA

        # Ensure the final list of columns to keep is correctly ordered
        final_column_order = [
            col for col in required_columns if col in manifest_df.columns
        ]
        
        for col in optional_columns:
            if col in manifest_df.columns:
                final_column_order.append(col)
                
        # Use reindex to keep only desired columns in the correct order
        manifest_df = manifest_df.reindex(columns=final_column_order)

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

        # Identify overlapping columns (excluding join keys and ID columns)
        common_cols = set(manifest_df.columns).intersection(
            set(metadata_df.columns)
        )
        common_cols = {
            c for c in common_cols 
            if not c.startswith("__") and c not in ("id", "identifier")
        }

        if common_cols:
            logger.info(
                f"Overlapping columns found: {list(common_cols)}. " \
                "Priority: Metadata Sheet."
            )

        # Left merge on the normalized keys with temporary suffix
        merged = pd.merge(
            manifest_df,
            metadata_df,
            how="left",
            left_on="__id_join__",
            right_on="__identifier_join__",
            suffixes=("", "_OVERLAP_META")
        )
        logger.info("Merge completed successfully.")

        # Deduplicate and Log Mismatches
        for col in common_cols:
            meta_col = f"{col}_OVERLAP_META"
            
            if meta_col in merged.columns:
                # Compare values only for rows where a match was found
                matched_rows = merged[merged["__identifier_join__"].notna()]
                
                # fillna('') ensures NaN == NaN is treated as a match
                mismatches = matched_rows[
                    matched_rows[col].fillna('') != \
                    matched_rows[meta_col].fillna('')
                ]
                
                mismatch_count = len(mismatches)
                if mismatch_count > 0:
                    logger.warning(
                        f"Data discrepancy in column '{col}': {mismatch_count} " \
                        "rows differ between manifest and metadata. " \
                        "Keeping metadata values."
                    )

                # Overwrite manifest version with metadata version
                merged[col] = merged[meta_col]
                
                # Drop the temporary overlap column
                merged.drop(columns=[meta_col], inplace=True)
                logger.info(f"Deduplicated column '{col}'.")

        # Unmatched = metadata rows with a real identifier that isn't in manifest
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

    except Exception:
        logger.exception("Unexpected error during merge_sheets.")
        raise


def should_flush_batch(buffer: list, batch_size: int) -> bool:
    """
    Determine whether the current batch should be flushed to disk.

    Args:
        buffer (list): List of processed records.
        batch_size (int): Maximum number of records in a batch.
        pending_children (list): List of child PIDs that must stay with the parent.

    Returns:
        bool: True if batch should be flushed, False otherwise.
    """
    return len(buffer) >= batch_size


def flush_batch(
    buffer: list,
    batch_count: int,
    config: AppConfig,
) -> pd.DataFrame:
    """
    Write the current buffer to a CSV, save PIDs for media, and prepare a config file.

    Args:
        buffer (list): List of processed records.
        batch_count (int): Current batch number.
        config (AppConfig): The application configuration object containing all 
                            necessary paths, user IDs, and metadata settings.

    Returns:
        pd.DataFrame: DataFrame written from records buffer.
    """
    # --- Step 1: Write Batch CSV ---
    sub_batch_prefix = f"{config.file_prefix}_{batch_count}_ingest_{config.metadata_level}"
    sub_batch_file = f"{sub_batch_prefix}.csv"
    sub_batch_path = os.path.join(config.output_path, sub_batch_file)
    records_df = records_to_csv(buffer, sub_batch_path)

    # --- Step 2: Check for Additional Media Files  ---
    media_files = []
    if "transcript" in records_df.columns: # Can extend this to add more media files
        media_files.append("transcript")

    # --- Step 3: Prepare Config File ---
    prepare_config(
        sub_batch_prefix,
        config.batch_path,
        config.batch_dir,
        config.user_id,
        media_files,
    )

    return records_df


def initialize_record() -> dict:
    """
    Initialize a record with fields as empty dictionaries or lists.

    Returns:
        dict: A record with fields initialized appropriately.
    """
    record = {}
    for field in FIELDS.Field:
        record[field] = []
    return record


def get_mapped_field(
    pid: str, 
    csv_field: str, data: str
) -> Tuple[str | None, str | None]:
    """
    Retrieve the mapped Islandora 2 machine field name and associated taxonomy
    for a given CSV field.

    Args:
        pid (str): The PID of the current record (used for logging).
        csv_field (str): The CSV field name to be mapped.
        data (str): The data value (used for logging if mapping is not found).

    Returns:
        tuple(machine_name or None, taxonomy or None)
    """
    match = TEMPLATE_FIELD_MAPPING.loc[
        TEMPLATE_FIELD_MAPPING['field'] == csv_field, ['machine_name', 'taxonomy']
    ]

    if match.empty:
        match = MANIFEST_FIELD_MAPPING.loc[
            MANIFEST_FIELD_MAPPING['field'] == csv_field, ['machine_name', 'taxonomy']
        ]
        if match.empty:
            add_exception(
                pid,
                csv_field,
                data,
                "could not find matching I2 field"
            )
            return None, None

    machine_name = match['machine_name'].iloc[0]
    taxonomy = match['taxonomy'].iloc[0]

    # Normalize NaN to None
    if pd.isna(machine_name):
        machine_name = None
    if pd.isna(taxonomy):
        taxonomy = None

    return machine_name, taxonomy


def add_exception(
    pid: str, 
    field: str, 
    value: Union[str, List[str]], 
    exception: str
) -> None:
    """
    Add an exception record to the exceptions list.

    Args:
        pid (str): The PID of the record.
        field (str): The field where the exception occurred.
        value (Union[str, list]): The value associated with the exception.
        exception (str): A description of the exception.
    """
    exceptions.append({
        # "file": current_file,
        "batch": current_batch,
        "pid": pid,
        "field": field,
        "value": value,
        "exception": exception
    })


def add_transformation(
    pid: str,
    field: str,
    old_value: str,
    new_value: str,
    transformation: str
) -> None:
    """
    Add a transformation record to the transformations list.

    Args:
        pid (str): The PID of the record.
        field (str): The field where the transformation occurred.
        old_value (str): The original value.
        new_value (str): The transformed value.
        transformation (str): A description of the transformation.
    """
    transformations.append({
        "File": current_file,
        "batch": current_batch,
        "PID": pid,
        "Field": field,
        "Old_Value": old_value,
        "New_Value": new_value,
        "Transformation": transformation
    })


def split_and_clean(text: str) -> List[str]:
    """
    Split a string on non-escaped commas, remove escape characters, and filter out empty strings.

    Args:
        text (str): The input string to process.

    Returns:
        list: A list of cleaned string parts.
    """
    # Remove white spaces
    cleaned_text = remove_whitespaces(text)
    # Create a list of values
    parts = cleaned_text.split('; ')
    # Remove escape characters and filter out empty strings
    return [part.strip() for part in parts if part.strip()]


def remove_whitespaces(text: str) -> str:
    """
    Remove newline characters, trailing whitespaces, and multiple spaces from a string.

    Args:
        text (str): The input string to clean.

    Returns:
        str: A cleaned string.
    """
    if isinstance(text, str):
        cleaned_text = text.replace("\n    ", " ").replace("\n", "").strip()
        cleaned_text = re.sub(r"\s+", " ", cleaned_text)
        return cleaned_text.strip()
    return ""


def get_parent_domain(df: pd.DataFrame, pid: str, parent_id: str) -> list[str]:
    """
    Retrieves the parent domain(s) for a given PID by matching its parent_id in 
    the DataFrame and mapping the domain URIs using the DOMAIN_MAPPING dictionary.

    Args:
        df (pd.DataFrame): The DataFrame containing metadata records.
        pid (str): The PID of the current record, used for error reporting.
        parent_id (str): The PID of the parent record to look up.

    Returns:
        list[str]: A list of mapped domain access values, or an empty list
                   if no values are found or an error occurs.
    """
    parent_domains = []
    try:
        match = df.loc[df["PID"] == parent_id, "RELS_EXT_isMemberOfSite_uri_ms"]
        if not match.empty:
            # Clean and split values, then map using DOMAIN_MAPPING
            raw_values = split_and_clean(match.values[0])
            parent_domains = [
                DOMAIN_MAPPING.get(val, "") \
                for val in raw_values if DOMAIN_MAPPING.get(val, "")
            ]
    except Exception as e:
        message = f"error retrieving parent domain: {e}"
        add_exception(pid, "field_domain_access", None, message)

    return parent_domains
   

def add_value(
    record: dict, 
    csv_field: str, 
    field: str, 
    value: str, 
    prefix: str = None
) -> str:
    """
    Add a value to a field in the record, optionally prepending a prefix.

    Args:
        record (dict): The record to update.
        csv_field (str): The CSV field name.
        field (str): The record field name.
        value (str): The value to add.
        prefix (str, optional): The prefix to prepend to the value.

    Returns:
        value (str): The value added to the record.
    """
    if not field:
        add_exception(
            record['id'][0],
            None,
            value, 
            f"missing I2 field for value from CSV field {csv_field}"
        )
        return record

    value = remove_whitespaces(value)
    values = record.get(field, [])

    if csv_field and (not prefix or prefix.startswith("rlt")):
        field_row = TEMPLATE_FIELD_MAPPING[TEMPLATE_FIELD_MAPPING['field'] == csv_field]
        if not field_row.empty:
            prefix = prefix.replace("rlt", field_row.iloc[0]['prefix']) \
                if prefix else field_row.iloc[0]['prefix']

    if prefix:
        value = f"{prefix}{value}"

    if value and value not in values:
        values.append(value)

    #print(f'Values: {values} | csv_field: {csv_field} | Field: {field} Value: {value}')
    record[field] = values

    return value


def add_title(
    record: dict, 
    value: str
) -> dict:
    """
    Add a title value to the record from CSV field data.

    Args:
        record (dict): The record to update.
        field (str): The record field name.
        value (str): The value to add.

    Returns:
        dict: The updated record.
    """
    add_value(record, None, "title", value)
    add_value(record, None, "field_full_title", value)
    return record


def validate_collection_id(
    pid: str, 
    value: str
) -> str | None:
    """
    Processes a collection ID by mapping it to its corresponding node ID.

    If the collection ID is found in `COLLECTION_NODE_MAPPING`, the corresponding `node_id` is returned.
    Otherwise, an exception is logged, and `None` is returned.

    Args:
        record (dict): The record being processed, containing metadata fields.
        value (str): The collection ID to be mapped.

    Returns:
        str | None: The corresponding node ID if found, otherwise None.
    """
    node_id = value
    matching_rows = COLLECTION_NODE_MAPPING.loc[
        COLLECTION_NODE_MAPPING["node_id"] == value
    ]
    if matching_rows.empty:
        matching_rows = COLLECTION_NODE_MAPPING.loc[
            COLLECTION_NODE_MAPPING["id"] == value, "node_id"
        ]
        if not matching_rows.empty:
            node_id = matching_rows.iloc[0]
        else:
            add_exception(
                pid,
                "field_member_of",
                value,
                f"node ID not found for collection PID"
            )

    return node_id


def process_model(
    record: dict,
    field: str,
    value: str,
) -> bool:
    """
    Validate and process an object model.

    The function looks up ``value`` in the global ``MODEL_MAPPING`` dictionary.
    If a matching model configuration is found, it adds the corresponding
    ``resource_type`` and ``display_hint`` values to the record and returns 
    ``True``. If no match is found, it logs a transformation exception and 
    returns ``False``.

    Args:
        record: The record being processed. Must contain an ``"id"`` key whose
            first element is used for logging.
        field: The CSV field name associated with the value.
        value: The object model identifier to validate and map.

    Returns:
        bool: ``True`` if model is found in taxonomy; ``False`` otherwise.
    """
    model = MODEL_MAPPING.get(value)
    skip = model is None

    if skip:
        add_transformation(
            record["id"][0], 
            field, 
            value,
            None,
            "could not find term in model taxonomy"
        )
        return False

    # Add resource type based on the mapped model
    resource_type = model.get("resource_type")
    add_value(
        record, 
        field, 
        "field_resource_type", 
        resource_type
    )

    # Add display hint based on model
    display_hint = model.get("display_hint")
    add_value(
        record, 
        field, 
        "field_display_hints", 
        display_hint
    )

    return True


def validate_domain(pid: str, value: str) -> bool:
    """
    Checks if the given value exists in the DOMAIN_MAPPING dictionary as a value.
    If it exists, returns True. If not, adds an exception and returns False.

    Args:
        pid (str): The unique identifier for the record being processed.
        value (str): The value to check in the DOMAIN_MAPPING dictionary.

    Returns:
        bool: True if the value is found as a value in DOMAIN_MAPPING; False otherwise.

    Side Effects:
        If the value is not found, an exception is added using the `add_exception` function.
    """
    # Check if the value exists in DOMAIN_MAPPING as a value
    if value in DOMAIN_MAPPING.values():
        return True
    else:
        # Add an exception if the value is not found
        add_exception(
            pid, 
            "field_domain_access", 
            value, 
            "invalid domain"
        )
        return False


def validate_edtf_date(pid: str, value: str) -> bool:
    """
    Validates if the given date string is in a valid EDTF (Extended Date/Time Format).
    Args:
        pid (str): The PID of the current record, used for error reporting.
        value (str): The value to be validated according to EDTF.
    """
    # Check if the date can be parsed as EDTF
    edtf_date = parse_edtf(value)
    if not edtf_date:
        add_exception(pid, "field_edtf_date", value, "invalid EDTF date")
        return False
    return True


def validate_term(
    pid: str,
    field: str,
    value: str,
    taxonomy: str,
) -> bool:
    """
    Validate that a term exists in the specified taxonomy.

    This function checks whether ``value`` appears in the global ``TAXONOMIES``
    DataFrame with the given ``taxonomy`` (stored in the ``'Vocabulary'`` column).
    If the term cannot be found, an exception is logged via ``add_exception`` and
    the function returns ``False``.

    Args:
        pid: PID of the record being processed (used for logging).
        field: Name of the CSV column the value came from (used for logging).
        value: The term to validate.
        taxonomy: Name of the taxonomy/vocabulary in which the term must appear.

    Returns:
        bool: ``True`` if the term is considered valid (found in the taxonomy), 
        ``False`` if it is missing from the taxonomy and an exception was logged.
    """
    mask = ((TAXONOMIES["Name"] == value) &
            (TAXONOMIES["Vocabulary"] == taxonomy))

    matching_rows = TAXONOMIES.loc[mask]

    if matching_rows.empty:
        add_exception(
            pid,
            field,
            value,
            f"could not find term in {taxonomy} taxonomy",
        )
        return False

    return True


def validate_coordinates(pid: str, value: str) -> bool:
    """
    Validate that a string contains a valid latitude/longitude pair in
    decimal or sexagesimal (DMS) format.

    The value must contain two coordinates separated by a comma or semicolon.
    Examples:
        "40.446, -79.982"
        "40°26'46\"N, 79°58'56\"W"

    If invalid, the function logs an exception with:
        add_exception(pid, "field_coordinates", value, "<reason>")

    Args:
        pid (str): Record identifier for logging context.
        value (str): Coordinate string to validate.

    Returns:
        bool: True if valid, False otherwise.
    """
    def fail(message: str) -> bool:
        """Log invalid coordinate and return False."""
        try:
            add_exception(pid, "field_coordinates", value, message)
        except NameError:
            pass
        return False

    if not isinstance(value, str):
        return fail("coordinate value must be a string")

    cleaned = remove_whitespaces(value)
    if not cleaned:
        return fail("coordinate value is empty")

    # Expect exactly two parts: latitude, longitude
    parts = re.split(r"\s*[;,]\s*", cleaned)
    if len(parts) != 2:
        return fail("expected two coordinates (lat, lon) separated by ',' or ';'")

    lat_str, lon_str = parts[0], parts[1]

    # -----------------------------
    # Decimal Degrees Parser
    # -----------------------------
    def parse_decimal(token: str) -> float | None:
        """Parse a decimal degree token like -79.982 or +40.446."""
        if not re.fullmatch(r"[+-]?\d+(?:\.\d+)?", token):
            return None
        try:
            return float(token)
        except ValueError:
            return None

    # -----------------------------
    # DMS Parser (Degrees, Minutes, Seconds)
    # Example: 40°26'46"N
    # -----------------------------
    dms_pattern = re.compile(
        r"""
        ^\s*
        (?P<deg>[+-]?\d+(?:\.\d+)?)      # degrees
        \s*[°ºd]?\s*
        (?:
            (?P<min>\d+(?:\.\d+)?)\s*['m]?\s*
        )?
        (?:
            (?P<sec>\d+(?:\.\d+)?)\s*["s]?\s*
        )?
        (?P<hem>[NnSsEeWw])?
        \s*$
        """,
        re.VERBOSE,
    )

    def parse_dms(token: str) -> float | None:
        """Parse DMS coordinate to decimal degrees."""
        m = dms_pattern.match(token)
        if not m:
            return None
        try:
            deg = float(m.group("deg"))
            minutes = float(m.group("min")) if m.group("min") else 0.0
            seconds = float(m.group("sec")) if m.group("sec") else 0.0
            hemisphere = m.group("hem")
        except Exception:
            return None

        sign = -1.0 if deg < 0 else 1.0
        deg = abs(deg)
        dd = deg + minutes / 60 + seconds / 3600
        dd *= sign

        if hemisphere:
            h = hemisphere.upper()
            if h in ("S", "W"):
                dd = -abs(dd)
            else:
                dd = abs(dd)

        return dd

    # -----------------------------
    # Unified coordinate parser
    # -----------------------------
    def parse_coord_pair(lat_token: str, lon_token: str) -> tuple[float, float] | None:
        """
        Try parsing both latitude and longitude in decimal or DMS form.
        Returns normalized decimal degrees pair or None.
        """
        def parse_one(token: str) -> float | None:
            val = parse_decimal(token)
            if val is not None:
                return val
            return parse_dms(token)

        lat = parse_one(lat_token)
        lon = parse_one(lon_token)
        if lat is None or lon is None:
            return None

        # Range validation
        if not (-90 <= lat <= 90):
            return None
        if not (-180 <= lon <= 180):
            return None

        return lat, lon

    # -----------------------------
    # Validate
    # -----------------------------
    result = parse_coord_pair(lat_str, lon_str)

    if result is None:
        return fail("invalid coordinates (must be decimal or sexagesimal)")

    return True


def process_title(record, title_parts):
    title = title_parts.get('title')
    if title:
        title += f", vol. {title_parts.get('volume')}" if title_parts.get('volume') else ""
        title += f", no. {title_parts.get('number')}" if title_parts.get('number') else ""
        add_title(record, title)
    return title


def validate_record(record: dict, df: pd.DataFrame) -> dict:
    """
    Validate the fields and values of a metadata record.

    Args:
        record (dict): A record to validate.

    Returns:
        record (dict): The validated record.
    """
    # Use the .get() method to safely retrieve the list under 'id'.
    # If 'id' is missing, it defaults to an empty list ([]), preventing a KeyError.
    pids = record.get('id', [])

    # Check if the list is not empty before attempting to access the [0] index.
    # If the list is empty, assign an empty string "" to pid.
    if pids:
        pid = pids[0]
    else:
        pid = ""

    for field, values in record.items():
        match = FIELDS.loc[FIELDS['Field'] == field]
        if match.empty:
            print(f"⚠️  Warning: Field '{field}' not found in FIELDS lookup.")
            return 
        field_manager = match.iloc[0]
        
        if field_manager.Field_Type == "Text (plain)":
            for value in values:
                if len(value) > 255:
                    add_exception(
                        pid,
                        field,
                        value,
                        "value exceeds character limit",
                    )
        
        if field_manager.Field_Type == "Number (integer)":
            for value in values:
                try:
                    int_value = int(value)
                except (ValueError, TypeError):
                    add_exception(
                        pid,
                        field,
                        value,
                        f"Expected an integer, but got " +
                        f"{type(value).__name__}: {value}",
                    )

        if field_manager.Repeatable == "FALSE" and len(values) > 1:
            add_exception(
                pid,
                field,
                values,
                "multiple values in nonrepeatable field",
            )

    for field in REQUIRED_FIELDS:
        parent_id = record.get('parent_id')
        if parent_id:
            parent_id = parent_id[0]
            if field == 'title' and not record[field]:
                add_value(record, None, 'title', pid)
            elif field == 'field_domain_access' and not record[field]:
                parent_domains = get_parent_domain(df, pid, parent_id)
                for domain in parent_domains:
                    add_value(
                        record, 
                        None, 
                        'field_domain_access', 
                        domain
                    )
            elif field == 'field_member_of':
                continue
        if len(record[field]) < 1:
            add_exception(
                pid,
                field,
                None,
                f"record missing required {field}",
            )

    return record


def remove_vetted_fields(record: Dict) -> Dict:
    """
    Removes keys from a record dictionary that are present in the 
    CONTROLLED_FIELDS list.

    Args:
        record (Dict): The input dictionary (a single record) to be processed.

    Returns:
        Dict: A new dictionary containing only the fields that are NOT in
              CONTROLLED_FIELDS.
    """
    filtered_record = {
        key: value 
        for key, value in record.items() 
        if key not in VETTED_FIELDS
    }
    return filtered_record


def format_record(record: dict) -> dict:
    """
    Format the values of a metadata record by converting non-empty lists to 
    pipe-separated strings and removing keys with empty lists.

    This function is typically used as a final step before exporting records 
    to CSV or other flat formats, ensuring that list values are properly 
    serialized and empty fields are excluded.

    Args:
        record (dict): The metadata record with values that may be lists.

    Returns:
        dict: A cleaned and serialized version of the record with no empty lists 
              and list values converted to strings.
    """
    for field, values in list(record.items()):
        if isinstance(values, list):
            if values:
                record[field] = "|".join(values)
            else:
                del record[field]
    
    return record


def records_to_csv(records: list, destination: str):
    """
    Converts a list of dictionaries to a CSV file, dropping empty columns.
    If records contain parent-child relationships, ensures children inherit 
    the parent's domain access value where applicable.

    Args:
        records (list): List of dictionaries to convert.
        destination (str): Filepath for the output CSV file.

    Returns:
        pd.DataFrame: The resulting DataFrame written to CSV, or None if no records.
    """
    # Confirm there are records to save to CSV
    if not records:
        print(f"No records to save for {current_file}." + 
              "The output file will not be created.")
        return

    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame.from_dict(records)

    # Sort records so that parent objects are first
    if "parent_id" in df.columns:
        # Ensure that parent_id is empty for top-level object models
        parent_models = ['Paged Content', 'Compound Object', 'Newspaper']
        df.loc[
            df['field_model'].isin(parent_models), 'parent_id'
        ] = pd.NA

        df.sort_values(
            by="parent_id", 
            ascending=True, 
            na_position="first", 
            inplace=True
        )

    # Ensure the destination directory exists
    os.makedirs(os.path.dirname(destination), exist_ok=True)

    # Write the resulting DataFrame to a CSV file
    df.to_csv(destination, index=False, header=True, encoding='utf-8')

    # Report creation of processed CSV path
    formatted_path = destination.replace("\\", "/")
    print(f"\nCSV file saved to: {formatted_path}")

    return df


""" Key Functions """

def process_record(row: dict) -> dict:
    """
    Process a CSV row dictionary into a transformed metadata record.

    Args:
        row (dict): Dictionary representing a CSV row.

    Returns:
        dict: Transformed metadata record.
    """
    # Initialize record
    record = initialize_record()

    # Add ID to record manually to ensure presence for logging
    pid = row['identifier']
    add_value(record, "identifier", "id", pid)

    # Initialize title dict to store title components
    title_parts = {}

    try:
        # Process values in each field
        for csv_field, data in row.items():
            # Confirm that input field is mapped and data exists in field
            i2_field, taxonomy = get_mapped_field(pid, csv_field, data)
            if not i2_field or pd.isna(data):
                # print(f"skipped field {csv_field}")
                continue

            # Preproccess values
            if i2_field in DELIMITED_FIELDS:
                values = split_and_clean(data)
            else:
                values = [remove_whitespaces(data)]

            for value in values:
                # Transform values that require remediation
                if i2_field == "field_full_title":
                    title_parts[csv_field] = value
                    continue
                elif i2_field == "field_model":
                    process_model(record, csv_field, value)
                elif i2_field == "field_member_of":
                    value = validate_collection_id(pid, value)
                elif i2_field == "field_domain_access":
                    validate_domain(pid, value)
                elif i2_field == "field_coordinates":
                    validate_coordinates(pid, value)
                elif i2_field in CONTROLLED_FIELDS:
                    validate_term(pid, csv_field, value, taxonomy)
                elif i2_field in DATE_FIELDS:
                    validate_edtf_date(pid, value)
                # Add CSV data to I2 field
                if value:
                    add_value(record, csv_field, i2_field, value)

        # Process title
        process_title(record, title_parts)

    except Exception as e:
                print(f"Error processing row {pid}: {e}")
                print(traceback.format_exc())
                add_exception(pid, "row_error", "", str(e))

    return record


def process_files(
    progress_queue: Queue,
    logger: logging.Logger,
    tracker: ProgressTracker, 
    manifest: pd.DataFrame, 
    metadata: pd.DataFrame, 
    config: AppConfig
) -> None:
    """
    Process all records from the merged manifest and metadata sheets in batches.

    Args:
        progress_queue (Queue): Thread-safe queue for reporting progress.
        logger (logging.Logger): The logger instance.
        tracker (ProgressTracker): Instance to track progress.
        manifest (pd.DataFrame): The loaded manifest data.
        metadata (pd.DataFrame): The loaded metadata data.
        config (AppConfig): The application configuration object containing 
                            all paths, sizes, and settings.
    """
    global current_batch
    
    try:
        # --- Stage 1: Initial Data Merge and Preparation ---
        
        # Merge sheets
        ingest_sheet, unmatched_records = merge_sheets(
            manifest, metadata, config.ingest_task, logger
        )

        # Handle publication status for batch
        publish_value = 1 if config.publish else 0
        ingest_sheet['published'] = publish_value

        # Log unmatched rows from metadata sheet
        if not unmatched_records.empty:
            unmatched_log_csv = os.path.join(
                config.log_dir,
                f"{config.file_prefix}_unmatched.csv"
            )
            logger.warning(
                f"Unmatched rows found, writing to {unmatched_log_csv}"
            )
            unmatched_records.to_csv(
                unmatched_log_csv, index=False, encoding='utf-8'
            )

        # Set total number of files for progress tracking
        progress_queue.put((tracker.set_total_files, (1,)))
        progress_queue.put(
            (tracker.set_current_file, ("Ingest Sheet", len(ingest_sheet)))
        )
        
        # --- Stage 2: Batch Processing Loop ---

        buffer = []
        current_batch = 1
        record_count = 0 

        for idx, row in ingest_sheet.iterrows():
            if tracker.cancel_requested.is_set():
                logger.info("Processing cancelled by user.")
                return
            
            record_count += 1
            
            # Process record
            record = process_record(row)
            if record:
                record = validate_record(record, ingest_sheet)
                if config.metadata_level == "minimal": # Use config
                    record = remove_vetted_fields(record) 
                record = format_record(record)
                buffer.append(record)
            else:
                continue

            # Update progress for processed record
            is_last = (idx == ingest_sheet.index[-1])
            progress_queue.put(
                (tracker.update_processed_records, (is_last,))
            )

            # Complete batch if max size reached
            if should_flush_batch(buffer, config.batch_size):
                records_df = flush_batch(
                    buffer,
                    current_batch,
                    config,
                )

                # Set up next batch
                current_batch += 1
                buffer.clear()

        # Update progress after loop finishes
        progress_queue.put((tracker.update_processed_files, ()))

        # --- Stage 3: Flush Remaining Buffer ---
        if buffer:
            records_df = flush_batch(
                buffer,
                current_batch,
                config,
            )

        # Flush last record progress update before printing file saved message
        while not progress_queue.empty():
            func, args = progress_queue.get()
            func(*args)

        # --- Stage 4: Write Reports ---
        write_reports(
            config.log_dir,
            config.timestamp,
            "metadata",
            transformations,
            exceptions
        )

    except Exception as e:
        logger.error("Error during file processing.", exc_info=True)
        print(f"Error during processing: {e}")
        print(traceback.format_exc())
        # Note: If this function is run in a separate thread, sys.exit(1) here 
        # won't stop the main thread/program immediately, but will stop the current thread.
        # Pass an error message back through the queue?
        # sys.exit(1)


""" Driver Code """

if __name__ == "__main__":
    # Parse arguments and get the AppConfig object
    config = parse_arguments()
    
    # Get batch directory name
    config.batch_dir = os.path.basename(config.batch_path.rstrip(os.sep))
    
    # Get a unique timestamp
    config.timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    
    # Create the base file prefix for logs and outputs
    config.file_prefix = f"{config.batch_dir}_{config.timestamp}"

    # Set up directory paths based on batch path
    config.output_path = os.path.join(config.batch_path, "metadata")
    config.log_dir = os.path.join(config.batch_path, "logs")
    config.log_path = os.path.join(config.log_dir, f"{config.file_prefix}.log")

    try:
        # Set up batch directory
        setup_batch_directory(config.batch_path)

        # Set up logger
        logger = setup_logger('make_metadata_sheet', config.log_path)

        # Initialize progress queue and tracker
        update_queue = Queue()
        tracker = ProgressTracker()

        # Convert manifest and metadata files to DataFrames
        # TODO: Update load_input_sheets to accept the config object
        manifest_df, metadata_df = load_input_sheets(config)

        # Run file/record processing in a separate thread
        processing_thread = threading.Thread(
            target=process_files,
            args=(
                update_queue, 
                logger,
                tracker, 
                manifest_df,
                metadata_df,
                config,
            )
        )
        processing_thread.start()

        # Process the update queue in a loop (Monitor thread)
        while processing_thread.is_alive():
            while not update_queue.empty():
                func, args = update_queue.get()
                func(*args)
            time.sleep(0.05)

        # After processing thread ends, flush any remaining updates
        while not update_queue.empty():
            func, args = update_queue.get()
            func(*args)

    except Exception as e:
        print("An error occurred during execution:")
        # Use the logger if it was successfully set up
        if 'logger' in locals():
            logger.error("An error occurred during execution:", exc_info=True)
        print(traceback.format_exc())
