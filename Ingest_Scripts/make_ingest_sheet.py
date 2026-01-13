#!/bin/python3 

"""Batch process metadata and manifest records for Workbench ingest tasks.

Merge archival manifest data with descriptive metadata, validate fields against 
controlled vocabularies and schema, and partition the transformed records into 
manageable batches for system ingest, if necessary. Support both 'create' and 
'update' Workbench ingest workflows.

Main Features:
- Multi-threaded execution: Processes records in a background thread while 
  monitoring progress in the main thread.
- Data Validation: Enforces EDTF date standards, coordinate formatting, and 
  controlled vocabulary matching.
- Batch Management: Automatically flushes records into CSV chunks based on 
  a configurable batch size.
- Interactive CLI: Prompts for required identifiers and paths if they are not 
  provided as command-line arguments.

Usage:
    python3 make_ingest_sheet.py
    python3 make_ingest_sheet.py -u jdoe10 -t update -b /workbench/batches/[BATCH_DIR] -l complete -p y
"""

# ---------------------------------------------------------------------------
# Modules
# ---------------------------------------------------------------------------

# Import standard modules
import os
import re
import time
import argparse
import traceback
import threading
from queue import Queue
from datetime import datetime
from functools import lru_cache
from typing import Dict, List, Tuple, Union, Optional
from pathlib import Path

# Import third-party modules
import pandas as pd
from edtf import parse_edtf
import requests

# Import local modules
from utilities import *
from definitions import *
from batch_manager import *
from progress_tracker import *


# ---------------------------------------------------------------------------
# Global Variables
# ---------------------------------------------------------------------------

global transformations
transformations = []

global exceptions
exceptions = []

global current_file
current_file = None

global current_batch
current_batch = None

DEFAULT_BATCH_SIZE = 10000


# ---------------------------------------------------------------------------
# Class
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def parse_arguments() -> AppConfig:
    """Parse command-line arguments and prompt for missing required values.

    Returns:
        AppConfig: An object containing all parsed and configuration settings.
    """
    parser = argparse.ArgumentParser(description="Process CSV files in batches.")
    
    # --- Argument Definitions ---
    parser.add_argument(
        "-u", "--user_id", 
        type=str, 
        help="The user ID to associate with the operation."
    )
    parser.add_argument(
        "-t", "--ingest_task", 
        type=str, 
        choices=["create", "update"], 
        help="Workbench task: 'create' or 'update'."
    )
    parser.add_argument(
        "-b", "--batch_path", 
        type=str, 
        help="Path to a batch directory for Workbench ingests."
    )
    parser.add_argument(
        "-z", "--batch_size", 
        type=int, 
        default=DEFAULT_BATCH_SIZE, 
        help=f"Number of records per batch (default: {DEFAULT_BATCH_SIZE})"
    )
    parser.add_argument(
        "-m", "--manifest_id", 
        type=str, 
        help="Google Sheet ID for the manifest file."
    )
    parser.add_argument(
        "--manifest_sheet", 
        type=str, 
        help="Path to manifest on local device (optional)."
    )
    parser.add_argument(
        "-d", "--metadata_id", 
        type=str, 
        help="Google Sheet ID for the metadata file."
    )
    parser.add_argument(
        "--metadata_sheet", 
        type=str, 
        help="Path to metadata sheet on local device (optional)."
    )
    parser.add_argument(
        "-c", "--credentials_file", 
        type=str, 
        default="/workbench/etc/google_ulswfown_service_account.json",
        help="Path to the Google service account credentials JSON."
    )
    parser.add_argument(
        "-l", "--metadata_level", 
        type=str, 
        choices=["minimal", "complete", "publish"], 
        help="Metadata detail level: 'minimal', 'complete', or 'publish'."
    )
    parser.add_argument(
        "-p", "--publish", 
        type=str, 
        choices=["y", "n"], 
        help="Specify whether or not the ingest batch should be published ('y' or 'n')."
    )

    # Parse initial arguments
    args = parser.parse_args()
    
    # Prompt user for missing required arguments
    if not args.user_id:
        args.user_id = prompt_for_input(
            "Enter your Pitt user ID: "
        )
    if not args.ingest_task:
        args.ingest_task = prompt_for_input(
            "Enter the Workbench ingest task (create/update): ", 
            valid_choices=['create', 'update']
        )
    if not args.batch_path:
        args.batch_path = prompt_for_input(
            "Enter the path to the Workbench batch directory: "
        )
    if not args.manifest_id \
        and args.ingest_task == "create" \
        and not args.metadata_sheet:
        args.manifest_id = prompt_for_input(
            "Enter the Google Sheet ID for the manifest sheet: "
        )
    if not args.metadata_id and not args.metadata_sheet:
        args.metadata_id = prompt_for_input(
            "Enter the Google Sheet ID for the metadata sheet: "
        )
    if not args.credentials_file and (args.manifest_id or args.metadata_id):
        args.credentials_file = prompt_for_input(
            "Enter the path to the Google credentials JSON file: "
        )
    if not args.metadata_level:
        args.metadata_level = prompt_for_input(
            "Enter the metadata level (minimal, complete, or publish): ", 
            valid_choices=['minimal', 'complete', 'publish']
        )
    if not args.publish:
        args.publish = prompt_for_input(
            "Should the ingest batch be published (y/n)?: ", 
            valid_choices=['y', 'n']
        )

    # Convert 'y'/'n' string to boolean True/False for the AppConfig class
    args.publish = (args.publish == 'y')

    # Return the Config Object
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
    else:
        manifest_df = pd.DataFrame()

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


def merge_sheets(manifest_df, metadata_df, logger):
    """Merge manifest and metadata DataFrames and validate data integrity.

    This function aligns file-level manifest data with descriptive metadata. It
    standardizes the manifest schema, merges datasets on unique identifiers,
    and identifies any metadata records that lack a corresponding file match.

    Args:
        manifest_df (pd.DataFrame): The primary manifest sheet containing file info.
        metadata_df (pd.DataFrame): The supplemental descriptive metadata sheet.
        logger (logging.Logger): Logger instance for status and mismatch reporting.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing:
            - ingest_sheet: The merged and resolved master DataFrame.
            - unmatched: Rows from the merge that failed to find metadata.
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
    
    # Drop manifest columns not in required or optional lists
    allowed_cols = required_columns + optional_columns
    
    manifest_df = manifest_df[
        [c for c in manifest_df.columns if c in allowed_cols]
    ].copy()

    # Ensure all required columns exist in manifest
    for col in required_columns:
        if col not in manifest_df.columns:
            manifest_df.loc[:, col] = None
            logger.info(f"Added missing required column: {col}")

    # Validate or rename metadata merge key
    if "identifier" not in metadata_df.columns:
        if "id" in metadata_df.columns:
            metadata_df = metadata_df.rename(columns={"id": "identifier"})
        else:
            logger.error("Merge failed: metadata missing 'id' or 'identifier'")
            raise ValueError("Missing required merge column in metadata_df")

    # Identify overlapping columns for post-merge validation
    common_cols = set(manifest_df.columns).intersection(set(metadata_df.columns))
    common_cols.discard("id")
    common_cols.discard("identifier")

    # Perform left merge on id and identifier
    ingest_sheet = pd.merge(
        manifest_df,
        metadata_df,
        left_on="id",
        right_on="identifier",
        how="left",
        suffixes=("_manifest", "_metadata")
    )

    # Filter rows that have no matching metadata
    unmatched = ingest_sheet[ingest_sheet["identifier"].isna()].copy()

    # Compare values in duplicate columns and resolve
    for col in common_cols:
        manifest_col = f"{col}_manifest"
        metadata_col = f"{col}_metadata"
        
        # Check for discrepancies between the two sheets
        mismatch_count = (
            ingest_sheet[manifest_col] != ingest_sheet[metadata_col]
        ).sum()
        if mismatch_count > 0:
            logger.warning(
                f"Column '{col}' has {mismatch_count} mismatching values"
            )

        # Keep metadata values and remove manifest duplicates
        ingest_sheet[col] = ingest_sheet[metadata_col]
        ingest_sheet = ingest_sheet.drop(columns=[manifest_col, metadata_col])

    return ingest_sheet, unmatched


def should_flush_batch(buffer: list, batch_size: int) -> bool:
    """Determine whether the current batch should be flushed to disk.

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
    """Write the current buffer to a CSV and prepare a config file.

    Args:
        buffer (list): List of processed records.
        batch_count (int): Current batch number.
        config (AppConfig): The application configuration object containing all 
                            necessary paths, user IDs, and metadata settings.

    Returns:
        pd.DataFrame: DataFrame written from records buffer.
    """
    # Write batch CSV
    sub_batch_prefix = f"{config.file_prefix}_{batch_count}_ingest_" + \
        config.metadata_level
    sub_batch_file = f"{sub_batch_prefix}.csv"
    sub_batch_path = os.path.join(config.output_path, sub_batch_file)
    records_df = records_to_csv(buffer, sub_batch_path)

    # Check for additional media files
    media_files = []
    if "transcript" in records_df.columns: # Can extend to add more media files
        media_files.append("transcript")

    # Prepare config file
    prepare_config(
        sub_batch_prefix,
        sub_batch_file,
        config.batch_path,
        config.batch_dir,
        config.user_id,
        config.ingest_task,
        media_files,
    )

    return records_df


def initialize_record() -> dict:
    """Initialize a record with fields as empty dictionaries or lists.

    Returns:
        dict: A record with fields initialized appropriately.
    """
    record = {}
    for field in FIELDS.Field:
        record[field] = []
    return record


def get_mapped_field(
    pid: str, 
    csv_field: str, 
    data: str
) -> Tuple[str | None, str | None]:
    """Map a metadata template field to its Islandora 2 machine name and taxonomy.

    This function converts legacy CSV headers to Islandora 2 (I2) machine field 
    names. If the field is recognized in the mapping, it returns the specific 
    machine name and its associated taxonomy; otherwise, it logs a warning using 
    the provided record context.

    Args:
        pid: The unique identifier for the current record, used for 
            contextual logging of mapping errors.
        csv_field: The raw header name from the source CSV to be translated.
        data: The actual cell value, included in logs to help troubleshoot 
            why a specific value failed to map.

    Returns:
        tuple[str | None, str | None]: A tuple containing the mapped I2 
            machine name and the associated taxonomy name. Both values 
            will be None if no mapping exists for the provided field.
    """
    match = TEMPLATE_FIELD_MAPPING.loc[
        TEMPLATE_FIELD_MAPPING['field'] == csv_field, 
        ['machine_name', 'taxonomy']
    ]

    if match.empty:
        match = MANIFEST_FIELD_MAPPING.loc[
            MANIFEST_FIELD_MAPPING['field'] == csv_field, 
            ['machine_name', 'taxonomy']
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
    """Add an exception record to the exceptions list.

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
    """Add a transformation record to the transformations list.

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
    """Tokenize a string by semicolon delimiters.

    This function splits the input text on every semicolon (';'). It 
    automatically handles variations like 'value1;value2', 'value1; value2', 
    or 'value1 ; value2'. It ensures that the resulting list contains only 
    non-empty, trimmed strings.

    Args:
        text: The raw input string containing multi-valued metadata 
            delimited by semicolons.

    Returns:
        list[str]: A list of cleaned, non-empty string tokens.
    """
    if not text:
        return []

    # Split on semicolon plus any surrounding whitespace
    parts = re.split(r'\s*;\s*', text)

    # Clean up individual parts and filter out empty strings
    return [p.strip() for p in parts if p.strip()]


def remove_whitespaces(text: str, allow_newlines: bool = False) -> str:
    """Sanitize string by collapsing whitespace, optionally preserving paragraph breaks.

    Standardize text by replacing tabs and multiple consecutive spaces with a 
    single space. If allow_newlines is True, preserve up to two consecutive 
    newlines; otherwise, collapse all whitespace into a single line.

    Args:
        text: The raw input string to be cleaned.
        allow_newlines: Whether to preserve up to two consecutive newlines. 
            Defaults to False (single-line output).

    Returns:
        str: The sanitized string. Returns an empty string if the input 
            is not a string type.
    """
    if not isinstance(text, str):
        return ""

    if allow_newlines:
        # Collapse horizontal whitespace (tabs/spaces) but keep newlines
        cleaned = re.sub(r"[ \t]+", " ", text)
        # Limit consecutive newlines to a maximum of two
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    else:
        # Standard behavior: collapse ALL whitespace (including \n) into one space
        cleaned = re.sub(r"\s+", " ", text)

    return cleaned.strip()


def get_parent_domain(df: pd.DataFrame, pid: str, parent_id: str) -> list[str]:
    """Inherit domain access values from a parent record.

    This function performs a recursive-style lookup by locating a parent record 
    via its PID, extracting its site membership URIs, and translating those 
    URIs into human-readable domain strings using the `DOMAIN_MAPPING` 
    dictionary.

    Args:
        df: The master DataFrame containing all metadata records.
        pid: The PID of the current child record (used for logging context).
        parent_id: The PID of the parent record to be queried.

    Returns:
        list[str]: A list of mapped domain strings (e.g., ['Public Site', 'Archives']).
            Returns an empty list if the parent is not found, has no domains, 
            or if an error occurs during lookup.

    Note:
        Logs failures to the global exception tracker via `add_exception` if 
        the lookup or mapping process encounters an error.
    """
    parent_domains = []
    try:
        # Locate parent row and extract the membership column
        match = df.loc[df["id"] == parent_id, "field_domain_access"]
        
        if not match.empty and pd.notna(match.values[0]):
            # Tokenize the URIs (e.g., "http://site.org; http://other.org")
            parent_domains = split_and_clean(str(match.values[0]))
            
            # # Map URIs to internal domain names; ignore values not in DOMAIN_MAPPING
            # parent_domains = [
            #     mapped for val in raw_values 
            #     if (mapped := DOMAIN_MAPPING.get(val))
            # ]
            
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
) -> str | None:
    """Add a processed value to a record field with dynamic prefixing.

    This function sanitizes the input value, resolves optional prefixes (including 
    special 'rlt' relationship placeholders), and appends the value to the 
    specified field list within the record dictionary.

    Args:
        record: The dictionary representing the metadata record being built.
        csv_field: The source column header from the CSV (used for prefix lookup).
        field: The target machine name for the field in the record dictionary.
        value: The raw data string to be added.
        prefix: An optional string to prepend to the value. If it starts with 
            'rlt', it is dynamically replaced using `TEMPLATE_FIELD_MAPPING`.

    Returns:
        str | None: The processed and prefixed value that was added to the 
            record. Returns None if the target field is missing or invalid.

    Side Effects:
        - Modifies the `record` dictionary in-place by updating the list of 
          values for the given `field`.
        - Logs an error via `add_exception` if the target `field` is not provided.
    """
    if not field:
        add_exception(
            record['id'][0],
            None,
            value, 
            f"missing I2 field for value from CSV field {csv_field}"
        )
        return None

    value = remove_whitespaces(value)
    values = record.get(field, [])

    if csv_field and (not prefix or prefix.startswith("rlt")):
        field_row = TEMPLATE_FIELD_MAPPING[
            TEMPLATE_FIELD_MAPPING['field'] == csv_field
        ]
        if not field_row.empty:
            prefix = prefix.replace("rlt", field_row.iloc[0]['prefix']) \
                if prefix else field_row.iloc[0]['prefix']

    if prefix:
        value = f"{prefix}{value}"

    if value and value not in values:
        values.append(value)
        
    record[field] = values

    return value


def add_title(
    record: dict, 
    value: str
) -> dict:
    """Add a title value to the record from CSV field data.

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


def process_model(
    record: dict,
    field: str,
    value: str,
) -> bool:
    """Validate and process an object model.

    This function looks up ``value`` in the global ``MODEL_MAPPING`` dictionary.
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


def process_title(record, title_parts):
    title = title_parts.get('title')
    if title:
        title += f", vol. {title_parts.get('volume')}" if title_parts.get('volume') else ""
        title += f", no. {title_parts.get('number')}" if title_parts.get('number') else ""
        add_title(record, title)
    return title


@lru_cache(maxsize=500)
def _check_network_status(value: str) -> tuple:
    """Internal helper to perform the cached HTTP request.
    
    Args:
        value (str): The Node ID to check.

    Returns:
        tuple: (bool, str) representing (is_valid, error_description).
            The description is empty if is_valid is True.
    """
    url = f"https://i2.digital.library.pitt.edu/node/{value}"
    try:
        response = requests.head(url, allow_redirects=True, timeout=10)
        
        if response.status_code == 404: # Page Not Found - node doesn't exist
            return False, f"URL returned 404: {url}"
            
        if response.status_code in [
            200, # OK - published
            301, # Moved Permanently - redirect to published collection page
            403, # Forbidden - unpublised collection page
        ]:
            return True, ""
        
        return False, f"Unexpected Status Code {response.status_code}: {url}"

    except requests.exceptions.RequestException as e:
        return False, f"HTTP Request Failed: {str(e)}"


def validate_collection_id(
    pid: str, 
    value: str
) -> bool:
    """Verify if a collection node ID is valid.

    This function builds a URL for a collection in I2 using the given value, 
    checks if the URL resolves, returns a Boolean for the validation check, and 
    logs an exception for the specific record PID if the validation fails. It 
    uses an internal LRU cache to avoid redundant network requests for the same 
    collection ID.

    Args:
        pid (str): The unique identifier for the current record (used for logging).
        value (str): The collection node ID to be validated.

    Returns:
        bool: True if the status code is 200, 301, or 403; False if 404 
            or if the request fails.
    """
    # Call the cached network function
    is_valid, error_message = _check_network_status(value)
    
    if not is_valid:
        add_exception(
            pid, 
            "field_member_of", 
            value, 
            error_message
        )
    
    return is_valid


def validate_domain(pid: str, value: str) -> bool:
    """Validate that a domain string exists within the allowed mapping.

    This function performs a membership check against the values of the 
    `DOMAIN_MAPPING` dictionary. It ensures that the metadata provided 
    matches one of the recognized site domains in the target system. 
    If the value is unrecognized, it records a validation error for the 
    specific record.

    Args:
        pid: The unique identifier for the record (used for logging errors).
        value: The domain access string to be validated (e.g., "Public Site").

    Returns:
        bool: True if the value is a valid mapped domain, False otherwise.

    Side Effects:
        Appends a record to the global exception tracker via `add_exception` 
        if the domain is invalid.
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
    """Validates if the given date string is in a valid EDTF (Extended Date/Time Format).

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
    """Validate that a term exists in the specified taxonomy.

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
    """Validate geographical coordinates in Decimal or Sexagesimal (DMS) format.

    This function parses a coordinate pair string, ensuring it contains two 
    distinct parts (latitude and longitude) separated by a comma or semicolon. 
    It supports:
    
    1.  **Decimal Degrees (DD):** e.g., "40.446, -79.982"
    2.  **Degrees Minutes Seconds (DMS):** e.g., "40°26'46\\"N, 79°58'56\\"W"
    
    This function performs internal normalization to decimal degrees to verify 
    that the values fall within physical global ranges:
    - Latitude: $[-90, 90]$
    - Longitude: $[-180, 180]$

    Args:
        pid: The unique identifier for the record, used for logging errors.
        value: The raw coordinate string to be validated.

    Returns:
        bool: True if the coordinates are well-formed and within valid ranges; 
            False otherwise.

    Side Effects:
        If validation fails, records the specific failure reason (e.g., range 
        error or formatting error) via the global `add_exception` tracker.
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

    # --- Unified coordinate parser ---
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

    # --- Validate ---
    result = parse_coord_pair(lat_str, lon_str)

    if result is None:
        return fail("invalid coordinates (must be decimal or sexagesimal)")

    return True


def validate_record(record: dict, ingest_sheet: pd.DataFrame) -> dict:
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
                parent_domains = get_parent_domain(ingest_sheet, pid, parent_id)
                for domain in parent_domains:
                    add_value(
                        record, 
                        None, 
                        'field_domain_access', 
                        domain
                    )
            elif field == 'field_member_of':
                continue
        
        missing_value = len(record[field]) < 1
        if missing_value:
            if field == "id" and "node_id" in record.keys():
                continue
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


def keep_publish_fields(record: dict) -> dict:
    # Define target keys in a set or tuple for faster lookups
    keep = ("node_id", "published")

    # Create a new dictionary with only the specified keys if they exist
    filtered_record = {key: record[key] for key in keep if key in record}

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
    formatted_path = Path(destination).as_posix()
    print(f"\nIngest file saved to: {formatted_path}")

    return df


# ---------------------------------------------------------------------------
# Key Functions
# ---------------------------------------------------------------------------

def process_record(row: dict) -> dict:
    """Transform a raw CSV row into a structured metadata record.

    This function handles the transformation pipeline for a single record. 
    It performs the following sequence:
    1.  **Initialization:** Creates a fresh record and anchors it with a PID.
    2.  **Field Mapping:** Identifies target system fields for each CSV column.
    3.  **Sanitization:** Handles whitespace removal and multi-value splitting.
    4.  **Validation & Remediation:** Routes data through specialized handlers 
        for coordinates, dates, controlled vocabularies, and system models.
    5.  **Assembly:** Merges title components and finalizes the record structure.

    Args:
        row: A dictionary representing a single row from the source CSV, 
            where keys are headers and values are raw cell data.

    Returns:
        dict: The transformed record dictionary containing mapped, cleaned, 
            and validated data.

    Side Effects:
        - Logs validation failures and processing errors to the global 
          exception tracker via `add_exception`.
        - Prints traceback and error details to the console if a row-level 
          exception occurs.
    """
    # Initialize record
    record = initialize_record()

    # Add ID to record manually to ensure presence for logging
    pid = row.get('identifier', row.get('field_pid'))
    #add_value(record, "identifier", "id", pid)

    # Initialize title dict to store title components
    title_parts = {}

    try:
        # Validate that a PID exists before proceeding
        if not pid:
            pid = "UNKNOWN"
            raise ValueError("Row missing required 'identifier' or 'field_pid'")
        
        # Process values in each field
        for csv_field, data in row.items():
            # Confirm that input field is mapped and data exists in field
            i2_field, taxonomy = get_mapped_field(pid, csv_field, data)
            if not i2_field or pd.isna(data):
                continue

            # Preprocess values
            if i2_field in DELIMITED_FIELDS:
                values = split_and_clean(data)
            elif i2_field in FORMATTED_FIELDS:
                values = [remove_whitespaces(data, allow_newlines=True)]
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
    manifest_df: pd.DataFrame, 
    metadata_df: pd.DataFrame, 
    config: AppConfig
) -> None:
    """Handle the end-to-end processing and batching of metadata records.

    This function executes the ingest workflow in four stages:
    1.  **Preparation:** Merges manifest and metadata (if applicable), logs 
        unmatched records, and initializes thread-safe progress tracking.
    2.  **Transformation:** Iterates through records, applying cleaning, 
        validation, and formatting. Supports real-time user cancellation.
    3.  **Batch Management:** Buffers processed records and "flushes" them to 
        disk based on the configured batch size to manage memory efficiency.
    4.  **Reporting:** Finalizes the run by flushing remaining buffers and 
        generating transformation and exception reports.

    Args:
        progress_queue: Thread-safe queue for UI/console progress updates.
        logger: Logger instance for recording process milestones and errors.
        tracker: ProgressTracker instance for monitoring the current file state.
        manifest_df: DataFrame containing the manifest data.
        metadata_df: DataFrame containing the metadata records.
        config: Application configuration object containing batch sizes, 
            directory paths, and ingest settings.

    Side Effects:
        - Writes unmatched records to a CSV file in the log directory.
        - Updates the global `current_batch` variable.
        - Generates multiple CSV batch files and final diagnostic reports.
        - Communicates with the main thread via `progress_queue`.
    """
    global current_batch
    
    try:
        # --- Stage 1: Initial Data Merge and Preparation ---
        
        # Merge sheets
        if not manifest_df.empty and not metadata_df.empty:
            ingest_sheet, unmatched_records = merge_sheets(
                manifest_df, metadata_df, logger
            )
        else:
            ingest_sheet = metadata_df
            unmatched_records = pd.DataFrame()

        # Handle publication status for batch
        publish_value = "1" if config.publish else "0"
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
            if config.metadata_level == "publish":
                record = keep_publish_fields(row)
            else:
                record = process_record(row)
            
            if record:
                if config.metadata_level != "publish":
                    record = validate_record(record, ingest_sheet)
                if config.metadata_level == "minimal":
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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    """Entry point for the metadata ingest generation tool.

    This function manages the high-level application lifecycle, 
    performing the following steps:
    
    1.  **Configuration & Environment:** Parses CLI arguments and generates 
        unique, timestamped directory paths for outputs and logs.
    2.  **Initialization:** Prepares the local filesystem, initializes 
        thread-safe progress tracking, and sets up centralized logging.
    3.  **Data Loading:** Reads source manifest and metadata files into 
        memory as DataFrames.
    4.  **Concurrent Execution:** Spawns a background worker thread via 
        `process_files` to perform heavy data transformations without 
        blocking the main execution context.
    5.  **Monitoring:** Implements a polling loop in the main thread to 
        consume the `progress_queue`, ensuring real-time status updates 
        are dispatched until processing concludes.

    Args:
        None (Processes inputs via CLI arguments and `parse_arguments`).

    Side Effects:
        - Creates a `metadata/` and `logs/` directory at the batch path.
        - Spawns a new `threading.Thread` for data processing.
        - Writes detailed execution logs to the filesystem.
        - Modifies the state of the provided `ProgressTracker` and 
          `AppConfig` objects.
    """
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
        logger = setup_logger('make_ingest_sheet', config.log_path)

        # Initialize progress queue and tracker
        update_queue = Queue()
        tracker = ProgressTracker()

        # Convert manifest and metadata files to DataFrames
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
    
    finally:
        # If the thread is still running for some reason, ensure it stops
        if 'tracker' in locals():
            tracker.cancel_requested.set()


if __name__ == "__main__":
    main()