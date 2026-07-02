#!/usr/bin/env python3

"""Generate Workbench ingest sheets from manifest and metadata sources.

This script combines manifest and descriptive metadata from Google Sheets or
local spreadsheet files, validates and transforms the metadata into
Islandora-compatible fields, and generates one or more Workbench ingest
spreadsheets. It supports create, update, and publish workflows while
performing schema validation, controlled vocabulary checking, EDTF date
validation, and detailed logging of metadata issues and transformations.

Usage:
    # Create ingest sheets from Google Sheets
    python3 make_ingest_sheet.py \
        --user_id abc123 \
        --ingest_task create \
        --batch_path /workbench/batches/example \
        --manifest_id <manifest_sheet_id> \
        --metadata_id <metadata_sheet_id>

    # Update existing objects using local spreadsheets
    python3 make_ingest_sheet.py \
        --ingest_task update \
        --batch_path /workbench/batches/example \
        --manifest_sheet manifest.xlsx \
        --metadata_sheet metadata.xlsx

    # Publish existing objects
    python3 make_ingest_sheet.py \
        --ingest_task update \
        --metadata_level publish \
        --publish y \
        --batch_path /workbench/batches/example \
        --metadata_id <metadata_sheet_id>

    # Run interactively
    python3 make_ingest_sheet.py
"""

# ---------------------------------------------------------------------------
# Modules
# ---------------------------------------------------------------------------

# Standard library imports
import argparse
import logging
import re
import sys
import threading
import traceback
from dataclasses import dataclass, field, fields
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from queue import Queue

# Third-party imports
import pandas as pd
import requests
from edtf import parse_edtf

# Local imports
from batch_manager import (
    prepare_config,
    setup_batch_directory,
)
from definitions import (
    CONTROLLED_FIELDS,
    DATE_FIELDS,
    DISALLOWED_FIELDS_BY_INGEST_TASK,
    DOMAINS,
    FIELDS,
    FORMATTED_FIELDS,
    LINKED_AGENT_TYPES,
    MANDATORY_FIELDS,
    MANIFEST_FIELD_MAPPING,
    MARC_FIELD_MAPPING,
    MINIMAL_METADATA_FIELDS,
    MODEL_MAPPING,
    METADATA_REQUIRED_FIELDS,
    PUBLISH_FIELDS,
    RELATOR_TERMS,
    TAXONOMIES,
    TEMPLATE_FIELD_MAPPING,
)
from process_mods import check_if_agent_field
from progress_tracker import ProgressTracker
from utilities import (
    DRUPAL_EXTENDED_EDTF_PATTERN,
    ERROR_SYMBOL,
    LogRegistry,
    SUCCESS_SYMBOL,
    WARNING_SYMBOL,
    cap_first,
    create_df,
    create_directory,
    df_to_csv,
    normalize_for_join,
    prompt_for_input,
    read_google_sheet,
    setup_logger,
    write_reports,
)


# ---------------------------------------------------------------------------
# Globals
# ---------------------------------------------------------------------------

DEFAULT_BATCH_SIZE = 10000
LOGGER_NAME = LogRegistry.MAKE_INGEST_SHEET


# ---------------------------------------------------------------------------
# Classes
# ---------------------------------------------------------------------------

@dataclass
class AppConfig:
    """Application configuration values."""

    user_id: str
    batch_path: str | Path
    batch_size: int
    manifest_id: str | None
    metadata_id: str | None
    credentials_file: str
    ingest_task: str
    metadata_level: str
    publish: bool
    manifest_sheet: str | None = None
    metadata_sheet: str | None = None
    batch_dir: str | None = None
    timestamp: str | None = None
    file_prefix: str | None = None
    output_dir: Path | None = None
    log_dir: Path | None = None
    log_path: Path | None = None

    def log_configuration(
        self,
        logger: logging.Logger,
    ) -> None:
        """Log the current application configuration.

        Only values that are not None are included.
        """
        logger.info("Configuration Input:")

        for field in fields(self):
            value = getattr(self, field.name)

            if value in (None, ""):
                continue

            logger.info(
                "  %-18s %s",
                field.name,
                value,
            )


@dataclass
class ProcessingResult:
    """Runtime processing state and report data."""
    
    issues: list[dict] = field(default_factory=list)
    transformations: list[dict] = field(default_factory=list)
    current_batch: int = 1
    unexpected_failure_count: int = 0

    def log_issue(
        self,
        pid: str,
        field: str | None,
        value: str | list[str] | None,
        issue: str,
        log_msg: str | None = None,
    ) -> None:
        """Record a metadata issue and write it to the logger."""
        self.issues.append({
            'batch': self.current_batch,
            'pid': pid,
            'field': field,
            'value': value,
            'exception': issue,
        })

        if not log_msg:
            log_msg = (
                f"{cap_first(issue)} in field '{field}' with value '{value}'."
            )

        logging.getLogger(LOGGER_NAME).error(
            "Record %s: %s",
            pid,
            log_msg
        )

    def log_transformation(
        self,
        pid: str,
        field: str,
        old_value: str,
        new_value: str,
        transformation: str,
        log_msg: str | None = None,
    ) -> None:
        """Record a metadata transformation and write it to the logger."""
        self.transformations.append({
            'batch': self.current_batch,
            'pid': pid,
            'field': field,
            'old_value': old_value,
            'new_value': new_value,
            'transformation': transformation,
        })

        if not log_msg:
            log_msg = (
                f"{cap_first(transformation)} in field '{field}': "
                f"'{old_value}' -> '{new_value}'."
            )

        logging.getLogger(LOGGER_NAME).info(
            "Record %s: %s",
            pid,
            log_msg
        )

    def increment_failure_count(self) -> None:
        """Increment the count of records that failed unexpectedly."""
        self.unexpected_failure_count += 1

    def advance_batch(self) -> None:
        """Advance the current batch counter."""
        self.current_batch += 1


@dataclass(frozen=True)
class FieldMapping:
    """Islandora field mapping information for a source CSV field."""

    field: str | None = None
    taxonomy: str | None = None
    prefix: str | None = None
    repeatable: bool = False

    @property
    def is_mapped(self) -> bool:
        """Return whether a mapping was found."""
        return self.field is not None


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------

# --- Argument Parsing and Input Loading ---

def parse_arguments() -> AppConfig:
    """Parse command-line arguments and prompt for missing required values.

    Returns:
        Application configuration object.
    """
    parser = argparse.ArgumentParser(description="Process CSV files in batches.")

    parser.add_argument(
        '-u',
        '--user_id',
        type=str,
        help="The user ID to associate with the operation.",
    )
    parser.add_argument(
        '-t',
        '--ingest_task',
        type=str,
        choices=['create', 'update'],
        help="Workbench task: 'create' or 'update'.",
    )
    parser.add_argument(
        '-b',
        '--batch_path',
        type=str,
        help="Path to a batch directory for Workbench ingests.",
    )
    parser.add_argument(
        '-z',
        '--batch_size',
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Number of records per batch. Default: {DEFAULT_BATCH_SIZE}.",
    )
    parser.add_argument(
        '-m',
        '--manifest_id',
        type=str,
        help="Google Sheet ID for the manifest file.",
    )
    parser.add_argument(
        '--manifest_sheet',
        type=str,
        help="Path to manifest on local device.",
    )
    parser.add_argument(
        '-d',
        '--metadata_id',
        type=str,
        help="Google Sheet ID for the metadata file.",
    )
    parser.add_argument(
        '--metadata_sheet',
        type=str,
        help="Path to metadata sheet on local device.",
    )
    parser.add_argument(
        '-c',
        '--credentials_file',
        type=str,
        default='/workbench/etc/google_ulswfown_service_account.json',
        help="Path to the Google service account credentials JSON.",
    )
    parser.add_argument(
        '-l',
        '--metadata_level',
        type=str,
        choices=['minimal', 'complete', 'publish'],
        help="Metadata detail level: 'minimal', 'complete', or 'publish'.",
    )
    parser.add_argument(
        '-p',
        '--publish',
        type=str,
        choices=['y', 'n'],
        help="Specify whether the ingest batch should be published.",
    )
    args = parser.parse_args()

    if not args.user_id:
        args.user_id = prompt_for_input("Enter your Pitt user ID: ")

    if not args.ingest_task:
        args.ingest_task = prompt_for_input(
            "Enter the Workbench ingest task (create/update): ",
            valid_choices=['create', 'update'],
        )

    if not args.batch_path:
        args.batch_path = prompt_for_input(
            "Enter the path to the Workbench batch directory: "
        )

    if (
        args.ingest_task == 'create'
        and not (args.manifest_id or args.manifest_sheet)
    ):
        args.manifest_id = prompt_for_input(
            "Enter the Google Sheet ID for the manifest sheet: "
        )

    if not args.metadata_id and not args.metadata_sheet:
        args.metadata_id = prompt_for_input(
            "Enter the Google Sheet ID for the metadata sheet: "
        )

    if not args.metadata_level:
        args.metadata_level = prompt_for_input(
            "Enter the metadata level (minimal, complete, or publish): ",
            valid_choices=['minimal', 'complete', 'publish'],
        )

    if not args.publish:
        args.publish = prompt_for_input(
            "Should the ingest batch be published (y/n)?: ",
            valid_choices=['y', 'n'],
        )

    if args.batch_size < 1:
        raise ValueError("--batch_size must be a positive integer.")

    if args.manifest_id and args.manifest_sheet:
        raise ValueError(
            "Provide either --manifest_id or --manifest_sheet, not both."
        )

    if args.metadata_id and args.metadata_sheet:
        raise ValueError(
            "Provide either --metadata_id or --metadata_sheet, not both."
        )

    args.publish = args.publish == 'y'

    return AppConfig(**vars(args))


def load_input_sheets(config: AppConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load manifest and metadata inputs.

    Args:
        config: Application configuration object.

    Returns:
        Tuple containing manifest and metadata DataFrames.
    """
    logger = logging.getLogger(LOGGER_NAME)

    if config.manifest_id:
        logger.info(
            "Using manifest from Google ID: %s",
            config.manifest_id,
        )
        manifest_df = read_google_sheet(
            config.manifest_id,
            sheet_name=config.manifest_sheet,
            credentials_file=config.credentials_file,
        )
    elif config.manifest_sheet:
        logger.info(
            "Using manifest sheet from local file: %s",
            config.manifest_sheet,
        )
        manifest_df = create_df(config.manifest_sheet)
    else:
        logger.info("No manifest provided.")
        manifest_df = pd.DataFrame()

    if config.metadata_id:
        logger.info(
            "Using metadata sheet from Google ID: %s",
            config.metadata_id,
        )
        metadata_df = read_google_sheet(
            config.metadata_id,
            sheet_name=config.metadata_sheet,
            credentials_file=config.credentials_file,
        )
    elif config.metadata_sheet:
        logger.info(
            "Using metadata sheet from local file: %s",
            config.metadata_sheet,
        )
        metadata_df = create_df(config.metadata_sheet)
    else:
        metadata_df = pd.DataFrame()

    return manifest_df, metadata_df


# --- Sheet Merging ---

def merge_sheets(
    manifest_df: pd.DataFrame,
    metadata_df: pd.DataFrame,
    ingest_task: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Merge manifest and metadata DataFrames.

    Args:
        manifest_df: Manifest DataFrame containing file-level data.
        metadata_df: Metadata DataFrame containing descriptive data.
        ingest_task: Workbench ingest task, either 'create' or 'update'.

    Returns:
        Tuple containing merged ingest sheet and unmatched metadata rows.

    Raises:
        KeyError: If the manifest DataFrame is missing 'id'.
        ValueError: If metadata is missing both 'identifier' and 'id', or if
            duplicate normalized identifiers are detected.
    """
    logger = logging.getLogger(LOGGER_NAME)

    required_columns = [
        'id',
        'field_model',
        'field_resource_type',
        'field_domain_access',
        'field_depositor',
        'field_member_of',
        'published',
    ]

    if ingest_task == 'create':
        required_columns.append('file')
    else:
        required_columns.append('node_id')

    optional_columns = [
        'parent_id',
        'field_weight',
        'transcript',
        'thumbnail',
    ]

    # Drop manifest columns not in required or optional lists
    allowed_cols = required_columns + optional_columns
    manifest_df = manifest_df[
        [col for col in manifest_df.columns if col in allowed_cols]
    ].copy()
    metadata_df = metadata_df.copy()

    # Ensure manifest contains required merge key
    if 'id' not in manifest_df.columns:
        msg = "Manifest is missing the required column: 'id'"
        logger.error(msg)
        raise KeyError(msg)

    # Ensure all required columns exist in manifest
    for col in required_columns:
        if col not in manifest_df.columns:
            manifest_df.loc[:, col] = None
            logger.info("Adding missing required column: %s", col)

    # Identify the ID field in metadata sheet
    id_field = 'identifier'

    if id_field not in metadata_df.columns:
        if 'id' in metadata_df.columns:
            id_field = 'id'
            logger.warning(
                "Preferred metadata sheet join field 'identifier' was not "
                "found; using fallback field 'id'."
            )
        else:
            msg = (
                "Metadata sheet is missing the required join column "
                "'identifier' (or fallback 'id')."
            )
            logger.error(msg)
            raise ValueError(msg)

    logger.info("Using '%s' as metadata join field.", id_field)

    # Standardize metadata identifier column name for output
    if id_field == 'id':
        metadata_df.rename(columns={'id': 'identifier'}, inplace=True)

    # Create normalized join keys
    manifest_df['__manifest_id_join__'] = normalize_for_join(
        manifest_df['id']
    )
    metadata_df['__metadata_id_join__'] = normalize_for_join(
        metadata_df['identifier']
    )

    # Check for duplicate normalized IDs in manifest
    manifest_dupes = (
        manifest_df['__manifest_id_join__']
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

    # Check for duplicate normalized identifiers in metadata
    metadata_dupes = (
        metadata_df['__metadata_id_join__']
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

    # Identify overlapping columns for post-merge validation
    common_cols = set(manifest_df.columns).intersection(set(metadata_df.columns))
    common_cols.discard('id')
    common_cols.discard('identifier')
    common_cols.discard('__manifest_id_join__')
    common_cols.discard('__metadata_id_join__')

    # Merge manifest and metadata sheet
    ingest_sheet = pd.merge(
        manifest_df,
        metadata_df,
        how='left',
        left_on='__manifest_id_join__',
        right_on='__metadata_id_join__',
        suffixes=('_manifest', '_metadata'),
        validate='one_to_one',
    )
    logger.info("Merge completed successfully.")

    # Identify and report any records in metadata sheet but not manifest
    in_manifest = metadata_df['__metadata_id_join__'].isin(
        manifest_df['__manifest_id_join__']
    )
    nonempty = metadata_df['__metadata_id_join__'].notna()
    unmatched = metadata_df[nonempty & ~in_manifest].copy()

    if not unmatched.empty:
        logger.warning("%d unmatched metadata rows found.", len(unmatched))

    # Compare values in duplicate columns and resolve
    for col in common_cols:
        manifest_col = f'{col}_manifest'
        metadata_col = f'{col}_metadata'

        if (
            manifest_col not in ingest_sheet.columns
            or metadata_col not in ingest_sheet.columns
        ):
            continue

        mismatch_count = (
            ingest_sheet[manifest_col] != ingest_sheet[metadata_col]
        ).sum()

        if mismatch_count > 0:
            logger.warning(
                "Column '%s' has %d mismatching values.",
                col,
                mismatch_count,
            )

        # Keep metadata values and remove manifest duplicates
        ingest_sheet[col] = ingest_sheet[metadata_col]
        ingest_sheet.drop(
            columns=[manifest_col, metadata_col],
            inplace=True,
        )

    # Clean up helper columns and manifest 'id' column
    ingest_sheet.drop(
        columns=['id', '__manifest_id_join__', '__metadata_id_join__'],
        errors='ignore',
        inplace=True,
    )

    unmatched.drop(
        columns=['__metadata_id_join__'],
        errors='ignore',
        inplace=True,
    )

    return ingest_sheet, unmatched


# --- Batch Helpers ---

def should_flush_batch(buffer: list, batch_size: int) -> bool:
    """Determine whether the current batch should be flushed to disk.

    Args:
        buffer: Processed record buffer.
        batch_size: Maximum number of records in a batch.

    Returns:
        True if batch should be flushed; otherwise False.
    """
    return len(buffer) >= batch_size


def flush_batch(
    buffer: list,
    batch_count: int,
    config: AppConfig,
) -> pd.DataFrame:
    """Write the current buffer to CSV and prepare a config file.

    Args:
        buffer: Processed records.
        batch_count: Current batch number.
        config: Application configuration object.

    Returns:
        DataFrame written from the record buffer.
    """
    sub_batch_prefix = (
        f'{config.batch_dir}_{batch_count}_ingest_{config.metadata_level}'
    )
    sub_batch_file = f'{sub_batch_prefix}.csv'
    sub_batch_path = config.output_dir / sub_batch_file

    records_df = save_records(buffer, sub_batch_path, config)

    if records_df is None:
        return pd.DataFrame()

    # Check for additional media files
    media_files = []

    if 'transcript' in records_df.columns:
        media_files.append('transcript')

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


def save_records(
    records: list,
    destination: str | Path,
    config: AppConfig,
) -> pd.DataFrame | None:
    """Convert records to a DataFrame, filter fields, and write to CSV.

    Args:
        records: Record dictionaries to write.
        destination: Output CSV path.
        config: Application configuration object.

    Returns:
        DataFrame written to CSV, or None if no records are provided.
    """
    if not records:
        print("No records to save. The output file will not be created.")
        return None

    # Convert list of dictionaries to DataFrame
    records_df = pd.DataFrame.from_dict(records)

    # Filter fields to those allowable by ingest task
    records_df = filter_fields(records_df, config.ingest_task)

    # Sort records so that parent objects are first
    if (
        'parent_id' in records_df.columns 
        and 'field_model' in records_df.columns
        ):
        # Ensure that parent_id is empty for top-level object models
        parent_models = ['Paged Content', 'Compound Object', 'Newspaper']

        records_df.loc[
            records_df['field_model'].isin(parent_models),
            'parent_id',
        ] = pd.NA

        records_df.sort_values(
            by='parent_id',
            ascending=True,
            na_position='first',
            inplace=True,
        )

    # Write the resulting DataFrame to a CSV file
    df_to_csv(records_df, destination)

    # Report creation of processed CSV path
    formatted_path = Path(destination).as_posix()
    print(f"\nIngest file saved: {formatted_path}")

    return records_df


# --- Record Initialization and Formatting ---

def initialize_record() -> dict:
    """Initialize a record with all output fields as empty lists.

    Returns:
        Record dictionary initialized with expected fields.
    """
    return {
        field: []
        for field in FIELDS.Field
    }


def split_and_clean(text: str) -> list[str]:
    """Tokenize a string by pipe delimiters.

    Args:
        text: Raw input string.

    Returns:
        Cleaned list of non-empty values.
    """
    if not text:
        return []

    # Split on pipe or semicolon plus any surrounding whitespace
    parts = re.split(r'\s*[|;]\s*', text)

    # Clean up individual parts and filter out empty strings
    return [
        part.strip()
        for part in parts
        if part.strip()
    ]


def remove_whitespaces(text: str, allow_newlines: bool = False) -> str:
    """Collapse whitespace in a string.

    Args:
        text: Raw input string.
        allow_newlines: Whether to preserve paragraph breaks.

    Returns:
        Cleaned text, or an empty string for non-string input.
    """
    if not isinstance(text, str):
        return ''

    if allow_newlines:
        cleaned = re.sub(r'[ \t]+', ' ', text)
        cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
    else:
        cleaned = re.sub(r'\s+', ' ', text)

    return cleaned.strip()


def format_record(record: dict) -> dict:
    """Serialize record list values and remove empty fields.

    Args:
        record: Metadata record.

    Returns:
        Cleaned record with list values converted to pipe-delimited strings.
    """
    if not record:
        return record

    for field, values in list(record.items()):
        if not isinstance(values, list):
            continue

        if values:
            record[field] = '|'.join(values)
        else:
            del record[field]

    return record


def filter_fields(
    records: pd.DataFrame,
    ingest_task: str,
) -> pd.DataFrame:
    """Drop fields that are not allowed for the specified ingest task.

    Args:
        records: DataFrame containing ingest records.
        ingest_task: Ingest task type, either 'create' or 'update'.

    Returns:
        DataFrame with disallowed fields removed.

    Raises:
        ValueError: If ingest_task is invalid.
    """
    if ingest_task not in DISALLOWED_FIELDS_BY_INGEST_TASK:
        raise ValueError(
            f"Invalid ingest_task: {ingest_task!r}. "
            "Expected 'create' or 'update'."
        )

    disallowed_fields = DISALLOWED_FIELDS_BY_INGEST_TASK[ingest_task]

    fields_to_drop = [
        field for field in disallowed_fields
        if field in records.columns
    ]

    return records.drop(columns=fields_to_drop).copy()


def filter_minimal_metadata_fields(record: dict) -> dict:
    """Keep only minimal metadata fields.

    Args:
        record: Record to filter.

    Returns:
        Record containing only minimal metadata fields.
    """
    return {
        key: value
        for key, value in record.items()
        if key in MINIMAL_METADATA_FIELDS
    }


def filter_publish_fields(record: dict) -> dict:
    """Keep only fields needed for publish updates.

    Args:
        record: Record or row to filter.

    Returns:
        Record containing only publish fields.
    """
    return {
        key: record[key]
        for key in PUBLISH_FIELDS
        if key in record
    }


# --- Mapping Helpers ---

def is_repeatable_field(field: str | None) -> bool:
    """Return whether an Islandora field is repeatable."""
    if not field:
        return False

    match = FIELDS.loc[
        FIELDS['Field'] == field,
        'Repeatable',
    ]

    if match.empty:
        return False

    return match.iat[0] == 'TRUE'


def get_agent_data(
    result: ProcessingResult,
    pid: str,
    field: str,
) -> tuple[str, str] | bool:
    """Resolve linked-agent taxonomy and prefix from a mapped field.

    Args:
        result: Runtime processing result.
        pid: Record PID.
        field: CSV field name.

    Returns:
        Tuple of taxonomy and prefix, or False if field is not agent-like.
    """
    if not field or '_' not in field:
        return False

    role_term, agent_type = field.rsplit('_', 1)
    role_term = role_term.replace('_', ' ')

    relator = RELATOR_TERMS.get(role_term)

    if not relator:
        result.log_issue(
            pid,
            field,
            role_term,
            "could not find relator term",
        )
        return False

    relator_code = relator['code']
    note = relator['note']

    if note:
        result.log_transformation(
            pid,
            field,
            role_term,
            relator_code,
            note,
        )
        logging.getLogger(LOGGER_NAME).warning(
            "Record %s: Relator code %s - %s",
            pid,
            relator_code,
            note,
        )

    agent_type = LINKED_AGENT_TYPES.get(agent_type)

    if not agent_type:
        result.log_issue(
            pid,
            field,
            role_term,
            "could not determine linked agent type",
        )
        return False

    taxonomy = agent_type.replace('_', ' ').title()
    prefix = f'relators:{relator_code}:{agent_type}:'

    return taxonomy, prefix


def get_mapped_field(
    result: ProcessingResult,
    pid: str,
    csv_field: str,
    data: str,
) -> FieldMapping:
    """Return the Islandora field mapping for a source CSV field.

    Searches the template, manifest, and MARC field mapping tables for the
    supplied CSV field. If no direct mapping is found, linked-agent fields are
    handled separately. Any unmapped field is logged as an issue.

    Args:
        result: Runtime processing result.
        pid: Current record PID.
        csv_field: Source CSV field.
        data: Source cell value.

    Returns:
        FieldMapping object containing the mapped Islandora field, taxonomy,
        prefix, and repeatability. If no mapping is found, all optional
        attributes will be None and ``repeatable`` will be False.
    """
    mapping_columns = [
        'machine_name',
        'taxonomy',
        'prefix',
    ]

    for mapping_df in (
        TEMPLATE_FIELD_MAPPING,
        MANIFEST_FIELD_MAPPING,
        MARC_FIELD_MAPPING,
    ):
        match = mapping_df.loc[
            mapping_df['field'] == csv_field,
            mapping_columns,
        ]

        if match.empty:
            continue

        row = match.iloc[0]

        field = (
            None
            if pd.isna(row['machine_name'])
            else row['machine_name']
        )
        taxonomy = (
            None
            if pd.isna(row['taxonomy'])
            else row['taxonomy']
        )
        prefix = (
            None
            if pd.isna(row['prefix'])
            else row['prefix']
        )

        return FieldMapping(
            field=field,
            taxonomy=taxonomy,
            prefix=prefix,
            repeatable=is_repeatable_field(field),
        )

    if check_if_agent_field(csv_field):
        agent_data = get_agent_data(result, pid, csv_field)

        if agent_data:
            taxonomy, prefix = agent_data
            field = 'field_linked_agent'

            return FieldMapping(
                field=field,
                taxonomy=taxonomy,
                prefix=prefix,
                repeatable=is_repeatable_field(field),
            )

    result.log_issue(
        pid,
        csv_field,
        data,
        "could not find matching I2 field",
        (
            f"Could not find matching I2 field for CSV field "
            f"'{csv_field}' with data '{data}'."
        ),
    )

    return FieldMapping()


# --- Record Building Helpers ---

def add_value(
    result: ProcessingResult,
    record: dict,
    csv_field: str | None,
    field: str | None,
    value: str,
    prefix: str | None = None,
) -> str | None:
    """Add a processed value to a record field.

    Args:
        result: Runtime processing result.
        record: Record dictionary being updated.
        csv_field: Source CSV field.
        field: Target machine field.
        value: Raw value to add.
        prefix: Optional value prefix.

    Returns:
        Processed value added to the record, or None.
    """
    if not field:
        log_msg = (
            f"Missing I2 field for value '{value}' from CSV field "
            f"'{csv_field}'."
        )
        result.log_issue(
            record['id'][0],
            None,
            value,
            f"missing I2 field for value from CSV field {csv_field}",
            log_msg,
        )
        return None

    value = remove_whitespaces(value)
    values = record.get(field, [])

    if csv_field and (not prefix or prefix.startswith('rlt')):
        field_row = TEMPLATE_FIELD_MAPPING[
            TEMPLATE_FIELD_MAPPING['field'] == csv_field
        ]

        if not field_row.empty:
            mapped_prefix = field_row.iloc[0]['prefix']

            if prefix:
                prefix = prefix.replace('rlt', mapped_prefix)
            else:
                prefix = mapped_prefix

    if prefix:
        value = f'{prefix}{value}'

    if value and value not in values:
        values.append(value)

    record[field] = values

    return value


def add_title(
    result: ProcessingResult,
    record: dict,
    value: str,
) -> dict:
    """Add a title value to the record.

    Args:
        result: Runtime processing result.
        record: Record to update.
        value: Title value.

    Returns:
        Updated record.
    """
    add_value(result, record, None, 'title', value)
    add_value(result, record, None, 'field_full_title', value)

    return record


def process_title(
    result: ProcessingResult,
    record: dict,
    title_parts: dict,
) -> str | None:
    """Build and add a formatted title from title parts.

    Args:
        result: Runtime processing result.
        record: Record being updated.
        title_parts: Title components.

    Returns:
        Formatted title, if created.
    """
    title = title_parts.get('title')

    if title:
        if title_parts.get('volume'):
            title += f", vol. {title_parts.get('volume')}"

        if title_parts.get('number'):
            title += f", no. {title_parts.get('number')}"

        add_title(result, record, title)

    return title


# --- Validation Helpers ---

@lru_cache(maxsize=500)
def _check_network_status(value: str) -> tuple[bool, str]:
    """Perform cached HTTP validation for a collection node ID.

    Args:
        value: Node ID to check.

    Returns:
        Tuple containing validation status and error description.
    """
    url = f'https://digital.library.pitt.edu/node/{value}'

    try:
        response = requests.head(url, allow_redirects=True, timeout=10)

        if response.status_code == 404: # Page Not Found - node doesn't exist
            return False, f"URL returned 404: {url}"

        if response.status_code in {
            200, # OK - published
            301, # Moved Permanently - redirect to published collection page
            403, # Forbidden - unpublised collection page
        }:
            return True, ''

        return False, f"Unexpected status code {response.status_code}: {url}"

    except requests.exceptions.RequestException as error:
        msg = "HTTP request failed"
        logging.getLogger(LOGGER_NAME).exception(msg)

        return False, f"{msg}: {str(error)}"


def validate_collection_id(
    result: ProcessingResult,
    pid: str,
    value: str,
) -> bool:
    """Validate a collection node ID.

    Args:
        result: Runtime processing result.
        pid: Current record PID.
        value: Collection node ID.

    Returns:
        True if the collection node ID resolves; otherwise False.
    """
    is_valid, error_msg = _check_network_status(value)

    # Call the cached network function
    if not is_valid:
        result.log_issue(
            pid,
            'field_member_of',
            value,
            f"invalid collection node ID: {error_msg}",
        )

    return is_valid


def validate_domain(
    result: ProcessingResult,
    pid: str,
    value: str,
) -> bool:
    """Validate a domain access value.

    Args:
        result: Runtime processing result.
        pid: Current record PID.
        value: Domain access value.

    Returns:
        True if valid; otherwise False.
    """
    if value in DOMAINS:
        return True

    result.log_issue(
        pid,
        'field_domain_access',
        value,
        "invalid domain",
    )

    return False


def validate_edtf_date(
    result: ProcessingResult,
    pid: str,
    field: str,
    value: str,
) -> bool:
    """Validate an EDTF date.

    Args:
        result: Runtime processing result.
        pid: Current record PID.
        field: Source field name.
        value: Date value.

    Returns:
        True if valid; otherwise False.
    """
    try:
        edtf_date = parse_edtf(value)

        if not edtf_date:
            result.log_issue(
                pid,
                field,
                value,
                "invalid EDTF date",
            )
            return False

        return True

    except Exception:
        valid = bool(DRUPAL_EXTENDED_EDTF_PATTERN.search(value))

        if not valid:
            result.log_issue(
                pid,
                field,
                value,
                "could not parse EDTF date",
                f"Could not parse EDTF date '{value}'.",
            )
            return False

        return True


def validate_term(
    result: ProcessingResult,
    pid: str,
    field: str,
    value: str,
    taxonomy: str,
) -> bool:
    """Validate that a term exists in a taxonomy.

    Args:
        result: Runtime processing result.
        pid: Current record PID.
        field: Source field name.
        value: Term value.
        taxonomy: Taxonomy name.

    Returns:
        True if term is valid; otherwise False.
    """
    mask = (
        (TAXONOMIES['Name'] == value)
        & (TAXONOMIES['Vocabulary'] == taxonomy)
    )
    matching_rows = TAXONOMIES.loc[mask]

    if matching_rows.empty:
        mask = (
            (TAXONOMIES['Term ID'] == value)
            & (TAXONOMIES['Vocabulary'] == taxonomy)
        )
        matching_rows = TAXONOMIES.loc[mask]

    if matching_rows.empty:
        result.log_issue(
            pid,
            field,
            value,
            f"could not find term in {taxonomy} taxonomy",
            f"Could not find term '{value}' in {taxonomy} taxonomy.",
        )
        return False

    return True


def validate_coordinates(
    result: ProcessingResult,
    pid: str,
    value: str,
) -> bool:
    """Validate geographical coordinates.

    Args:
        result: Runtime processing result.
        pid: Current record PID.
        value: Coordinate string.

    Returns:
        True if coordinates are valid; otherwise False.
    """
    def fail(message: str) -> bool:
        result.log_issue(
            pid,
            'field_coordinates',
            value,
            f"invalid coordinates: {message}",
        )
        return False

    if not isinstance(value, str):
        return fail("coordinate value must be a string")

    cleaned = remove_whitespaces(value)

    if not cleaned:
        return fail("coordinate value is empty")

    # Expect exactly two parts: latitude, longitude
    parts = re.split(r'\s*[;,]\s*', cleaned)

    if len(parts) != 2:
        return fail("expected two coordinates separated by ',' or ';'")

    lat_str, lon_str = parts

    def parse_decimal(token: str) -> float | None:
        """Parse a decimal degree token."""
        if not re.fullmatch(r'[+-]?\d+(?:\.\d+)?', token):
            return None

        try:
            return float(token)
        except ValueError:
            return None

    # DMS (Degrees, Minutes, Seconds) pattern
    dms_pattern = re.compile(
        r'''
        ^\s*
        (?P<deg>[+-]?\d+(?:\.\d+)?)
        \s*[°ºd]?\s*
        (?:
            (?P<min>\d+(?:\.\d+)?)\s*['m]?\s*
        )?
        (?:
            (?P<sec>\d+(?:\.\d+)?)\s*["s]?\s*
        )?
        (?P<hem>[NnSsEeWw])?
        \s*$
        ''',
        re.VERBOSE,
    )

    def parse_dms(token: str) -> float | None:
        """Parse DMS coordinate to decimal degrees."""
        match = dms_pattern.match(token)

        if not match:
            return None

        try:
            degrees = float(match.group('deg'))
            minutes = (
                float(match.group('min'))
                if match.group('min')
                else 0.0
            )
            seconds = (
                float(match.group('sec'))
                if match.group('sec')
                else 0.0
            )
            hemisphere = match.group('hem')
        except Exception:
            return None

        sign = -1.0 if degrees < 0 else 1.0
        degrees = abs(degrees)
        decimal_degrees = degrees + minutes / 60 + seconds / 3600
        decimal_degrees *= sign

        if hemisphere:
            hemisphere = hemisphere.upper()

            if hemisphere in ('S', 'W'):
                decimal_degrees = -abs(decimal_degrees)
            else:
                decimal_degrees = abs(decimal_degrees)

        return decimal_degrees

    def parse_one(token: str) -> float | None:
        """Parse a decimal or DMS coordinate token."""
        decimal_value = parse_decimal(token)

        if decimal_value is not None:
            return decimal_value

        return parse_dms(token)

    lat = parse_one(lat_str)
    lon = parse_one(lon_str)

    if lat is None or lon is None:
        return fail("invalid coordinates; must be decimal or sexagesimal")

    if not (-90 <= lat <= 90):
        return fail("latitude is out of range")

    if not (-180 <= lon <= 180):
        return fail("longitude is out of range")

    return True


def get_parent_domain(
    ingest_sheet: pd.DataFrame,
    pid: str,
    parent_id: str,
) -> list[str]:
    """Inherit domain access values from a parent record.

    Args:
        ingest_sheet: Master ingest DataFrame.
        pid: PID of the current child record.
        parent_id: PID of the parent record.

    Returns:
        Parent domain values, if found.
    """
    parent_domains = []

    try:
        # Locate parent row and extract the membership column
        match = ingest_sheet.loc[
            ingest_sheet['identifier'] == parent_id,
            'field_domain_access',
        ]

        # Tokenize the URIs
        if not match.empty and pd.notna(match.values[0]):
            parent_domains = split_and_clean(str(match.values[0]))

    except Exception:
        logging.getLogger(LOGGER_NAME).exception(
            "Record %s: Failed to retrieve parent domain.",
            pid,
        )

    return parent_domains


def validate_record(
    result: ProcessingResult,
    record: dict,
    ingest_sheet: pd.DataFrame,
    ingest_task: str,
) -> dict:
    """Validate fields and values in a metadata record.

    Args:
        result: Runtime processing result.
        record: Record to validate.
        ingest_sheet: Full ingest sheet.
        ingest_task: Ingest task.

    Returns:
        Validated record.
    """
    pids = record.get('id', [])
    pid = pids[0] if pids else ''
    logger = logging.getLogger(LOGGER_NAME)

    for field, values in record.items():
        match = FIELDS.loc[FIELDS['Field'] == field]

        if match.empty:
            logger.warning(
                "Record %s: Field '%s' not found in FIELDS lookup.",
                pid,
                field,
            )
            continue

        field_manager = match.iloc[0]

        if field_manager.Field_Type == 'Text (plain)':
            for value in values:
                if len(value) > 255:
                    result.log_issue(
                        pid,
                        field,
                        value,
                        "value exceeds character limit",
                    )

        if field_manager.Field_Type == 'Number (integer)':
            for value in values:
                try:
                    int(value)
                except (ValueError, TypeError):
                    result.log_issue(
                        pid,
                        field,
                        value,
                        (
                            "expected an integer, but got "
                            f"{type(value).__name__}: {value}"
                        ),
                    )

        if field_manager.Repeatable == 'FALSE' and len(values) > 1:
            result.log_issue(
                pid,
                field,
                values,
                "multiple values in nonrepeatable field",
            )

    if ingest_task != 'create':
        return record

    for field in MANDATORY_FIELDS:
        parent_id = record.get('parent_id')

        if parent_id:
            parent_id = parent_id[0]

            if field == 'title' and not record[field]:
                add_value(result, record, None, 'title', pid)

            elif field == 'field_domain_access' and not record[field]:
                parent_domains = get_parent_domain(
                    ingest_sheet,
                    pid,
                    parent_id,
                )

                for domain in parent_domains:
                    add_value(
                        result,
                        record,
                        None,
                        'field_domain_access',
                        domain,
                    )

            # Skip since children do not get collection associations
            elif field in METADATA_REQUIRED_FIELDS:
                continue

        missing_value = len(record[field]) < 1

        if missing_value:
            if field == 'id' and 'node_id' in record:
                continue

            result.log_issue(
                pid,
                field,
                None,
                "record missing required field",
                f"Missing required field {field}.",
            )

    return record


# --- Field-Specific Processing ---

def process_model(
    result: ProcessingResult,
    record: dict,
    field: str,
    value: str,
) -> bool:
    """Validate and process an object model.

    Args:
        result: Runtime processing result.
        record: Record being processed.
        field: Source field name.
        value: Object model value.

    Returns:
        True if model is valid; otherwise False.
    """
    pid = record['id'][0]
    model = MODEL_MAPPING.get(value)

    if not model:
        mask = (
            (TAXONOMIES['Term ID'] == value)
            & (TAXONOMIES['Vocabulary'] == 'Islandora Models')
        )
        matching_rows = TAXONOMIES.loc[mask]

        if not matching_rows.empty:
            model = matching_rows.iloc[0]

    if model is None:
        result.log_issue(
            pid,
            'field_member_of',
            value,
            "could not find term in model taxonomy",
        )
        return False

    resource_type = model.get('resource_type')
    add_value(
        result,
        record,
        field,
        'field_resource_type',
        resource_type,
    )

    display_hint = model.get('display_hint')
    add_value(
        result,
        record,
        field,
        'field_display_hints',
        display_hint,
    )

    return True


# --- Record Processing ---

def process_record(
    result: ProcessingResult,
    row: dict,
    index: int,
) -> dict | None:
    """Transform a raw CSV row into a structured metadata record.

    Args:
        result: Runtime processing result.
        row: Source row.
        index: Row index.

    Returns:
        Processed record, or None if processing fails.
    """
    # Setup record
    record = initialize_record()

    pid = row.get('identifier') or row.get('field_pid')
    pid = remove_whitespaces(str(pid)) if pd.notna(pid) else None

    if not pid:
        msg = f"row {index} missing required identifier"
        result.log_issue(
            'UNKNOWN',
            'identifier',
            None,
            msg,
            f"{msg.capitalize()}.",
        )
        return None

    try:
        add_value(result, record, None, 'id', str(pid))

        # Process values in each field
        title_parts = {}

        for csv_field, data in row.items():
            # Confirm that input field is mapped and data exists
            mapping = get_mapped_field(
                result,
                pid,
                csv_field,
                data,
            )

            if not mapping.is_mapped or pd.isna(data):
                continue

            if mapping.field == 'id':
                continue

            if mapping.repeatable:
                values = split_and_clean(data)
            elif mapping.field in FORMATTED_FIELDS:
                cleaned = remove_whitespaces(data, allow_newlines=True)
                values = [cleaned] if cleaned else []
            else:
                cleaned = remove_whitespaces(data)
                values = [cleaned] if cleaned else []

            for value in values:
                if mapping.field == 'field_full_title':
                    title_parts[csv_field] = value
                    continue

                if mapping.field == 'field_model':
                    process_model(result, record, csv_field, value)
                elif mapping.field == 'field_member_of':
                    validate_collection_id(result, pid, value)
                elif mapping.field == 'field_domain_access':
                    validate_domain(result, pid, value)
                elif mapping.field == 'field_coordinates':
                    validate_coordinates(result, pid, value)
                elif mapping.field in CONTROLLED_FIELDS:
                    validate_term(
                        result,
                        pid,
                        csv_field,
                        value,
                        mapping.taxonomy,
                    )
                elif mapping.field in DATE_FIELDS:
                    validate_edtf_date(result, pid, csv_field, value)

                if value:
                    add_value(
                        result,
                        record,
                        csv_field,
                        mapping.field,
                        value,
                        mapping.prefix,
                    )

        process_title(result, record, title_parts)

    except Exception:
        logging.getLogger(LOGGER_NAME).exception(
            "An error occurred while processing record %s.",
            pid,
        )
        return None

    return record


def process_files(
    progress_queue: Queue,
    tracker: ProgressTracker,
    manifest_df: pd.DataFrame,
    metadata_df: pd.DataFrame,
    config: AppConfig,
    result: ProcessingResult,
) -> None:
    """Process records, write ingest batches, and generate reports.

    Args:
        progress_queue: Queue for progress updates.
        tracker: Progress tracker.
        manifest_df: Manifest DataFrame.
        metadata_df: Metadata DataFrame.
        config: Application configuration object.
        result: Runtime processing result.
    """
    logger = logging.getLogger(LOGGER_NAME)

    try:
        # Merge manifest and metadata sheet
        if not manifest_df.empty and not metadata_df.empty:
            ingest_sheet, unmatched_records = merge_sheets(
                manifest_df,
                metadata_df,
                config.ingest_task,
            )
        else:
            ingest_sheet = metadata_df
            unmatched_records = pd.DataFrame()

        # Add publication field
        publish_value = '1' if config.publish else '0'
        ingest_sheet['published'] = publish_value

        if not unmatched_records.empty:
            unmatched_log_csv = (
                config.log_dir / f'{config.file_prefix}_unmatched.csv'
            )
            logger.warning(
                "Unmatched rows found, writing to %s.",
                unmatched_log_csv,
            )
            df_to_csv(unmatched_records, unmatched_log_csv)

        # Set total number of files for progress tracking
        progress_queue.put((tracker.set_total_files, (1,)))
        progress_queue.put(
            (tracker.set_current_file, ("Ingest Sheet", len(ingest_sheet)))
        )

        # Process Batch
        buffer = []
        result.current_batch = 1

        for idx, row in ingest_sheet.iterrows():
            if tracker.cancel_requested.is_set():
                logger.info("Processing cancelled by user.")
                return

            if config.metadata_level == 'publish':
                record = filter_publish_fields(row)
            else:
                record = process_record(result, row, idx)

            if record:
                if config.metadata_level != 'publish':
                    record = validate_record(
                        result,
                        record,
                        ingest_sheet,
                        config.ingest_task,
                    )

                # TODO: Move this to filter fields
                if config.metadata_level == 'minimal':
                    record = filter_minimal_metadata_fields(record)

                record = format_record(record)
                buffer.append(record)
            else:
                result.increment_failure_count()
                continue

            # Update progress for processed record
            is_last = idx == ingest_sheet.index[-1]
            progress_queue.put(
                (tracker.update_processed_records, (is_last,))
            )

            # Complete batch if max size reached
            if should_flush_batch(buffer, config.batch_size):
                flush_batch(
                    buffer,
                    result.current_batch,
                    config,
                )

                result.advance_batch()
                buffer.clear()

        # Send final progress update
        progress_queue.put((tracker.update_processed_files, ()))
        progress_queue.put(
            ('FAILURE_COUNT', result.unexpected_failure_count)
        )

        # Flush remaining buffer
        if buffer:
            flush_batch(
                buffer,
                result.current_batch,
                config,
            )

        write_reports(
            config.log_dir,
            config.timestamp,
            'metadata',
            result.transformations,
            result.issues
        )

        progress_queue.put(('PROCESSING_COMPLETE', True))

    except Exception as error:
        logger.exception("An error occurred during processing.")
        progress_queue.put((
            print,
            (
                f"\n{ERROR_SYMBOL} Data processor stopped unexpectedly: "
                f"{error}.",
                f"See logs: {config.log_path}",
            ),
        ))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Run the metadata ingest generation workflow."""
    logger = None
    config = None

    try:
        config = parse_arguments()

        # Set up output files
        config.batch_path = Path(config.batch_path)
        config.batch_dir = config.batch_path.name
        config.timestamp = datetime.now().strftime('%Y-%m-%d-%H%M%S')
        config.file_prefix = f'{config.batch_dir}_{config.timestamp}'

        config.output_dir = create_directory(config.batch_path / 'import')
        config.log_dir = create_directory(config.batch_path / 'logs')
        config.log_path = (
            config.log_dir / f'{config.file_prefix}_ingest_sheet_processing.log'
        )

        setup_batch_directory(config.batch_path)

        # Set up logging
        logger = setup_logger(LOGGER_NAME, config.log_path)

        critical_failures = 0
        success = False

        # Log configuration input
        config.log_configuration(logger)

        # Set up progress tracking
        update_queue = Queue()
        tracker = ProgressTracker()
        result = ProcessingResult()

        # Convert manifest and metadata sheet to DataFrames
        manifest_df, metadata_df = load_input_sheets(config)

        # Run file/record processing in a separate thread
        processing_thread = threading.Thread(
            target=process_files,
            args=(
                update_queue,
                tracker,
                manifest_df,
                metadata_df,
                config,
                result,
            ),
        )
        processing_thread.start()

        # Monitor thread
        while processing_thread.is_alive() or not update_queue.empty():
            # After processing thread ends, flush any remaining updates
            while not update_queue.empty():
                update = update_queue.get()
                # Check if this is a status message
                if (
                    isinstance(update, tuple)
                    and len(update) > 0
                    and isinstance(update[0], str)
                ):
                    if update[0] == 'FAILURE_COUNT':
                        critical_failures = update[1]
                        continue

                    if update[0] == 'PROCESSING_COMPLETE':
                        success = update[1]
                        continue
                # Otherwise, treat it as a UI function call
                elif (
                    isinstance(update, tuple)
                    and len(update) == 2
                    and callable(update[0])
                ):
                    try:
                        func, args = update
                        # Guard against non-iterable args
                        if isinstance(args, (list, tuple)):
                            func(*args)
                        else:
                            func(args)

                    except Exception:
                        logger.exception("UI update failed: %s", update)

                else:
                    logger.warning("Unknown queue item type: %s", update)

    except Exception:
        msg = "A critical system error occurred during execution."

        if logger:
            logger.exception(msg)

        log_path = getattr(config, 'log_path', None)

        print(f"\n{ERROR_SYMBOL} {msg}")

        if log_path:
            print(f"See logs: {log_path}")
        else:
            traceback.print_exc()

        sys.exit(1)

    finally:
        # If the thread is still running for some reason, ensure it stops
        if 'tracker' in locals():
            tracker.cancel_requested.set()

    if critical_failures > 0:
        unit = "record" if critical_failures == 1 else "records"
        print(
            f"\n{WARNING_SYMBOL} {critical_failures} {unit} failed to process."
            + (f" See logs: {config.log_path}" if config.log_path else "")
        )
    elif success:
        print(f"\n{SUCCESS_SYMBOL} All records were processed successfully.")


if __name__ == '__main__':
    main()
