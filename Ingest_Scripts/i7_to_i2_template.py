#!/usr/bin/env python3

"""Convert Islandora 7 metadata sheets to the Islandora 2 template format.

This script transforms legacy Islandora 7 metadata into the corresponding
Islandora 2 metadata template by applying configurable field mappings based on
one or more content types. It also performs metadata normalization, required
field generation, controlled vocabulary mapping, date qualification, and
produces an audit log describing all schema changes and transformations.

Usage:
    # Convert using a Google Sheet
    python3 i7_to_i2_template.py \
        --batch_path /workbench/batches/example \
        --metadata_id <metadata_sheet_id> \
        --content_type photograph

    # Convert a local spreadsheet
    python3 i7_to_i2_template.py \
        --batch_path /workbench/batches/example \
        --metadata_sheet metadata.xlsx \
        --content_type image

    # Apply multiple content type mappings
    python3 i7_to_i2_template.py \
        --content_type photograph book manuscript
"""

# ---------------------------------------------------------------------------
# Modules
# ---------------------------------------------------------------------------

# Standard library imports
import argparse
import logging
import re
import sys
import traceback
from datetime import datetime
from pathlib import Path
from collections.abc import Callable

# Third-party imports
import pandas as pd

# Local imports
from definitions import (
    CONTENT_TYPES,
    COPYRIGHT_MAPPING,
    GOOGLE_CREDENTIALS_FILE,
    I7_to_I2_MAPPING,
    LANGUAGES,
    TYPE_MAPPING,
)
from utilities import (
    ERROR_SYMBOL,
    create_df,
    create_directory,
    df_to_csv,
    get_google_sheet_filename,
    prompt_for_input,
    read_google_sheet,
    setup_logger,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOGGER_NAME = 'i7_to_i2_template'

REQUIRED_MAPPING_COLUMNS = {
    'content_type',
    'i7_field',
    'i2_field',
    'obligation',
}

OBLIGATION_LEVELS = {
    '',
    'optional',
    'recommended',
    'required, if applicable',
    'required',
}

CONTENT_TYPE_ALIASES = {
    'books': 'book',
    'images': 'image',
    'interviews': 'interview',
    'manuscripts': 'manuscript',
    'maps': 'map',
    'photographs': 'photograph',
    'serials': 'serial',
}


# ---------------------------------------------------------------------------
# Class
# ---------------------------------------------------------------------------

class AppConfig:
    """A container for application configuration parameters.

    Sanitizes raw strings from command-line arguments and user prompts into
    strongly typed objects, converting path strings into native Path utilities.
    """

    def __init__(
        self,
        batch_path: str,
        credentials_file: str,
        content_type: list[str],
        metadata_id: str | None,
        metadata_sheet: str | None = None,
    ) -> None:
        """Initializes AppConfig with sanitized and typed parameter values."""
        # Mandatory parameters
        self.batch_path = Path(batch_path)
        self.credentials_file = Path(credentials_file)
        self.content_type = content_type

        # Optional parameters
        self.metadata_id = metadata_id
        self.metadata_sheet = Path(metadata_sheet) if metadata_sheet else None
        self.timestamp: str | None = None
        self.file_prefix: str | None = None
        self.log_dir: Path | None = None
        self.log_path: Path | None = None


class ConversionResult:
    """Output paths created during template conversion."""

    def __init__(
        self,
        metadata_path: Path,
        audit_log_path: Path,
        processing_log_path: Path,
    ) -> None:
        """Initialize conversion result paths."""
        self.metadata_path = metadata_path
        self.audit_log_path = audit_log_path
        self.processing_log_path = processing_log_path


# ---------------------------------------------------------------------------
# CLI / I/O
# ---------------------------------------------------------------------------

def normalize_content_types(content_type: str | list[str]) -> list[str]:
    """Normalize one or more content type values.

    Args:
        content_type: Content type input as a string or list of strings. Values
            may be separated by spaces or commas.

    Returns:
        Ordered list of normalized content type values.

    Raises:
        ValueError: If a content type is not recognized.
    """
    input_data = (
        [content_type]
        if isinstance(content_type, str)
        else content_type
    )

    raw: list[str] = []

    for chunk in input_data:
        normalized_chunk = chunk.replace(',', ' ')
        raw.extend(normalized_chunk.split())

    content_types = [ct.lower() for ct in raw]
    invalid = [ct for ct in content_types if ct not in CONTENT_TYPES]

    if invalid:
        invalid_str = ', '.join(sorted(set(invalid)))
        allowed_str = ', '.join(sorted(CONTENT_TYPES))
        raise ValueError(
            f"Invalid content type value(s): {invalid_str}. "
            f"Allowed: {allowed_str}"
        )

    seen: set[str] = set()
    ordered: list[str] = []

    for ct in content_types:
        if ct not in seen:
            ordered.append(ct)
            seen.add(ct)

    return ordered


def parse_arguments() -> AppConfig:
    """Parse CLI arguments and interactively prompt for missing configuration.

    Handles initial script setup, parses command-line flags, triggers GUI or
    CLI prompts for missing paths/IDs, and performs strict validation and
    normalization of content types.

    Returns:
        AppConfig: An object containing all parsed and configuration settings.

    Raises:
        SystemExit: If an invalid content type is provided or if required
            file selection is cancelled.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Remap columns by one or more content types using a mapping CSV."
        )
    )
    parser.add_argument(
        '-b', '--batch_path',
        type=str,
        help="Path to a batch directory for Workbench ingests."
    )
    parser.add_argument(
        '-m', '--metadata_id',
        type=str,
        help="Google Sheet ID for the metadata file."
    )
    parser.add_argument(
        '--metadata_sheet',
        type=str,
        help="Path to metadata sheet on local device (optional)."
    )
    parser.add_argument(
        '-c', '--credentials_file',
        type=str,
        default='/workbench/etc/google_ulswfown_service_account.json',
        help="Path to the Google service account credentials JSON."
    )
    parser.add_argument(
        '-t', '--content_type',
        nargs='+',
        help=(
            "One or more content types (space- or comma-separated). "
            f"Allowed: {', '.join(sorted(CONTENT_TYPES))}"
        ),
    )
    args = parser.parse_args()

    if not args.batch_path:
        while not args.batch_path:
            args.batch_path = prompt_for_input(
                "Enter the full path to the workbench$ batch directory: "
            )

    if not args.metadata_id and not args.metadata_sheet:
        while not args.metadata_id and not args.metadata_sheet:
            args.metadata_id = prompt_for_input(
                "Enter the Google Sheet ID for the metadata: "
            )

    if not args.content_type:
        args.content_type = prompt_for_input(
            "Enter the content type(s) for the batch "
            "(space- or comma-separated): "
        )

    try:
        args.content_type = normalize_content_types(args.content_type)
    except ValueError as error:
        raise SystemExit(str(error)) from error

    # Return the Config Object
    return AppConfig(**vars(args))


def load_mapping(mapping_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize headers and validate a crosswalk mapping.

    This function performs a "fuzzy" header match (case and space insensitive)
    to ensure compatibility with different versions of the mapping. It also
    verifies that all required columns are present and validates that
    'obligation' values match the project's controlled vocabulary.

    Args:
        mapping_df: pandas DataFrame containing field mappings.

    Returns:
        pd.DataFrame: A cleaned DataFrame with standardized headers:
            'content_type', 'i7_field', 'i2_field', and 'obligation'.

    Raises:
        ValueError: If required columns are missing or if 'obligation' contains
            unrecognized values not found in OBLIGATION_LEVELS.
    """
    # Try to match and normalize column names (case-insensitive)
    rename_cols: dict[str, str] = {}
    for e in REQUIRED_MAPPING_COLUMNS:
        matches = [c for c in mapping_df.columns if c.strip().lower() == e]
        if matches:
            rename_cols[matches[0]] = e

    # Apply renaming if any matches were found
    if rename_cols:
        mapping_df = mapping_df.rename(columns=rename_cols)

    # Verify that all required columns are present
    missing = (
        REQUIRED_MAPPING_COLUMNS 
        - set(c.strip().lower() for c in mapping_df.columns)
        )
    if missing:
        raise ValueError(
            "Mapping CSV is missing required columns: {}\nFound columns: {}"
            .format(
                ', '.join(sorted(missing)),
                ', '.join(mapping_df.columns),
            )
        )

    # Strip whitespace from columns to ensure consistent matching
    for col in ['content_type', 'i7_field', 'i2_field', 'obligation']:
        mapping_df[col] = mapping_df[col].astype(str).str.strip()

    # Check for and report unknown obligation values, if any
    unknown = sorted(
        set(
            mapping_df.loc[
                ~mapping_df['obligation'].isin(OBLIGATION_LEVELS),
                'obligation'
            ]
        )
    )
    if unknown:
        raise ValueError(
            "Mapping CSV contains unknown 'obligation' values: {}. "
            "Expected one of: {}".format(
                ', '.join(unknown),
                ', '.join(OBLIGATION_LEVELS),
            )
        )
    return mapping_df


def prepare_mapping(
    mapping_df: pd.DataFrame,
    content_types: list[str]
) -> pd.DataFrame:
    """Load and filter the crosswalk mapping by requested content types.

    Args:
        mapping_df: pandas DataFrame containing field mappings.
        content_types: List of normalized content types to filter by.

    Returns:
        A cleaned and filtered DataFrame of mapping rules.
    """
    try:
        df = load_mapping(mapping_df)
        # Ensure 'content_types' exists in the mapping file
        if 'content_type' not in df.columns:
            raise KeyError("Mapping file is missing 'content_types' column.")

        mask = df['content_type'].apply(
            lambda cell: bool(
                tokenize_ct_value(cell).intersection(content_types)
            )
        )
        mapping_ct = df[mask].copy()

        if mapping_ct.empty:
            raise SystemExit(
                f"No mapping rows found for content_type(s)='{content_types}'"
            )

        mapping_ct['i7_field_clean'] = mapping_ct['i7_field'].\
            fillna('').astype(str).str.strip()
        mapping_ct['i2_field_clean'] = mapping_ct['i2_field'].\
            fillna('').astype(str).str.strip()

        return mapping_ct[mapping_ct['i2_field_clean'] != '']
    except Exception:
        logging.getLogger(LOGGER_NAME).exception(
            "Failed to prepare mapping."
        )
        raise


def load_metadata(config: AppConfig) -> pd.DataFrame:
    """Load metadata from either a Google Sheet or a local file system.

    Args:
        config: Must include:
        - metadata_id: The Google Sheet ID (if applicable).
        - metadata_sheet: Path to a local CSV/Excel file (if applicable).
        - credentials_file: Path to the Google Service Account JSON.

    Returns:
        pd.DataFrame: The ingested metadata ready for processing.
    """
    try:
        if config.metadata_id:
            df = read_google_sheet(
                config.metadata_id,
                sheet_name=None,
                credentials_file=config.credentials_file
            )
        else:
            if not config.metadata_sheet.exists():
                raise FileNotFoundError(
                    f"Metadata file not found: {config.metadata_sheet}"
                )
            df = create_df(config.metadata_sheet)
        if df.empty:
            raise ValueError("The provided metadata file is empty.")

        return df
    except Exception:
        logging.getLogger(LOGGER_NAME).exception("Failed to load metadata.")
        raise


def save_outputs(
    df_final: pd.DataFrame,
    audit_data: dict[str, list],
    mapping_ct: pd.DataFrame,
    config: AppConfig
) -> tuple[Path, Path]:
    """Generate the final metadata CSV and the accompanying audit log.

    Args:
        df_final: The transformed and cleaned metadata DataFrame.
        audit_data: A dictionary containing date logs, added columns, and
            dropped columns.
        mapping_ct: The mapping rules used for this batch.
        config: Parsed command-line arguments containing paths, identifiers,
            and content types.

    Returns:
        Tuple containing the metadata CSV path and audit log CSV path.
    """
    # Determine base filename
    if config.metadata_id:
        filename = get_google_sheet_filename(
            config.metadata_id, config.credentials_file
        )
    else:
        filename = config.metadata_sheet.stem

    ct_label = '_'.join(config.content_type)

    # --- Save Metadata CSV ---
    output_dir = config.batch_path / 'metadata'
    create_directory(output_dir)
    output_path = output_dir / f'{filename}_{ct_label}_metadata.csv'
    df_to_csv(df_final, output_path)

    # --- Build and Save Log CSV ---
    log_rows = audit_data['date_logs']

    for col in audit_data['added_cols']:
        log_rows.append(
            {
                'action': "added column",
                'field': col,
                'reason': "Mapped I2 field; created empty"
            }
        )

    for col in audit_data['dropped_cols']:
        log_rows.append(
            {
                'action': "dropped column",
                'field': col,
                'reason': "Not an I2 field"
            }
        )

    # Validate required fields
    req_mask = mapping_ct['obligation'] == 'required'
    required_targets = sorted(set(mapping_ct.loc[req_mask, 'i2_field_clean']))
    for col in required_targets:
        if col in df_final.columns:
            blanks = df_final[col].isna() | (
                df_final[col].astype(str).str.strip() == ''
            )
            if blanks.any():
                log_rows.append({
                    'action': "flagged missing required field",
                    'field': col,
                    'reason': (
                        f"{int(blanks.sum())} blank value(s) in required field"
                    )
                })

    log_df = pd.DataFrame(log_rows).fillna('')
    log_path = config.log_dir / f'{filename}_{ct_label}_field_audit_log.csv'
    df_to_csv(log_df, log_path)

    # Notify User
    summary = f"Output saved:\n{output_path}\n\nLog saved:\n{log_path}"
    print(f"\nDone!\n{summary}")

    return output_path, log_path


# ---------------------------------------------------------------------------
# Helpers / Processors
# ---------------------------------------------------------------------------

def tokenize_ct_value(value: str) -> set[str]:
    """Parse a multi-valued content type string into normalized tokens.

    Splits the input string by common delimiters (pipe, comma, or semicolon), 
    normalizes each token, and maps known aliases to the
    project's canonical content type values.

    Args:
        value: Raw content type string from a mapping cell (for example,
            ``"images|photograph"``).

    Returns:
        Set of unique, normalized content type values.
    """
    tokens = re.split(r'[|,;/]+', str(value))
    normalized_tokens: set[str] = set()

    for token in tokens:
        normalized = token.strip().lower()

        if not normalized:
            continue

        normalized_tokens.add(
            CONTENT_TYPE_ALIASES.get(normalized, normalized)
        )

    return normalized_tokens


def process_copyright_status(series: pd.Series) -> pd.Series:
    """Map copyright status terms to I2 taxonomy terms.

    Args:
        series: A pandas Series containing raw copyright status strings.

    Returns:
        A Series of mapped codes; returns an empty string for terms not
        found in COPYRIGHT_STATUS_MAPPING.
    """
    return series.astype(str).str.strip().map(COPYRIGHT_MAPPING).fillna('')


def process_language(series: pd.Series) -> pd.Series:
    """Convert MARC language codes to terms.

    Args:
        series: A pandas Series containing raw language strings.

    Returns:
        A Series of mapped codes; returns an empty string for codes not
        found in LANGUAGE_MAPPING.
    """
    return series.astype(str).str.strip().map(LANGUAGES).fillna('')


def process_type_of_resource(series: pd.Series) -> pd.Series:
    """Convert legacy resource type terms to I2 taxonomy terms.

    Args:
        series: A pandas Series containing raw resource type strings.

    Returns:
        A Series of mapped codes; returns an empty string for terms not
        found in TYPE_MAPPING.
    """
    return series.astype(str).str.strip().map(TYPE_MAPPING).fillna('')


# Register per-field processors by I2 column name
PROCESSORS: dict[str, Callable[[pd.Series], pd.Series]] = {
    'copyright_status': process_copyright_status,
    'language': process_language,
    'type_of_resource': process_type_of_resource,
}


def apply_processors(
    df: pd.DataFrame,
    processors: dict[str, Callable[[pd.Series], pd.Series]]
) -> pd.DataFrame:
    """Apply transformation functions to specific DataFrame columns.

    Iterate through a dictionary of processor functions and apply them to
    matching columns in the DataFrame for cleanup or normalization.

    Args:
        df: The DataFrame containing metadata to be processed.
        processors: A dictionary mapping target column names to callable
            functions that accept and return a pandas Series.

    Returns:
        pd.DataFrame: The DataFrame with transformed columns. Original data
            is preserved for columns without a defined processor.
    """
    for col, func in processors.items():
        if col in df.columns:
            try:
                df[col] = func(df[col])
            except Exception:
                logging.getLogger(LOGGER_NAME).exception(
                    "Processor for '%s' failed.", col
                )
    return df


def apply_date_qualification(
    df_work: pd.DataFrame
) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    """Qualify dates and convert dashed date ranges to EDTF intervals."""
    date_change_logs: list[dict[str, str]] = []

    if (
        'normalized_date_qualifier' in df_work.columns
        and 'date' in df_work.columns
    ):
        date_str = df_work['date'].astype(str)

        is_blank = df_work['date'].isna() | date_str.str.strip().isin(
            ['', 'nan', 'none']
        )

        range_mask = (
            ~is_blank
            & date_str.str.strip().str.match(r'^\d{4}\s*-\s*\d{4}$')
        )

        old_dates = df_work.loc[range_mask, 'date'].astype(str).copy()
        df_work.loc[range_mask, 'date'] = (
            df_work.loc[range_mask, 'date']
            .astype(str)
            .str.replace(r'\s*-\s*', '/', regex=True)
        )
        new_dates = df_work.loc[range_mask, 'date'].astype(str)

        for i in df_work.index[range_mask]:
            date_change_logs.append({
                'action': "converted date range",
                'row': int(i) + 2,
                'field': 'date',
                'old': old_dates.loc[i],
                'new': new_dates.loc[i],
                'reason': "converted dashed date range to EDTF interval",
            })

        # Normalize the qualifier for comparison
        q_yes = (
            df_work['normalized_date_qualifier']
            .astype(str)
            .str.strip()
            .str.lower()
            .eq('yes')
        )

        date_str = df_work['date'].astype(str)
        already_suffixed = date_str.str.endswith('~')
        add_mask = q_yes & ~is_blank & ~already_suffixed

        # Capture originals for logging, apply change, then log
        old_dates = df_work.loc[add_mask, 'date'].astype(str).copy()
        df_work.loc[add_mask, 'date'] = (
            df_work.loc[add_mask, 'date'].astype(str) + '~'
        )
        new_dates = df_work.loc[add_mask, 'date'].astype(str)

        # Log updates to date value using the index for Excel row calculation
        for i in df_work.index[add_mask]:
            date_change_logs.append({
                'action': "qualified date",
                'row': int(i) + 2, # offset to account for header
                'field': 'date',
                'old': old_dates.loc[i],
                'new': new_dates.loc[i],
                'reason': 'normalized_date_qualifier == "yes" ',
            })

    return df_work, date_change_logs


def transform_metadata(
    df_in: pd.DataFrame,
    mapping_ct: pd.DataFrame
) -> tuple[pd.DataFrame, dict[str, list]]:
    """Execute schema remapping, date qualification, and final metadata cleanup.

    Args:
        df_in: The raw input DataFrame containing the original metadata.
        mapping_ct: A DataFrame containing mapping rules with 'i7_field_clean'
            as source and 'i2_field_clean' as target field names.

    Returns:
        A tuple (df_final, audit_data) where:
            - df_final (pd.DataFrame): The transformed metadata with finalized
              schema and applied processors.
            - audit_data (dict): A dictionary containing 'date_logs' (list),
              'added_cols' (list), and 'dropped_cols' (list).

    Raises:
        KeyError: If required mapping columns are missing.
        ValueError: If date qualification or processor application fails.
        RuntimeError: If schema remapping or final column selection fails.
    """
    # Check for missing required columns
    required_mapping_cols = {'i7_field_clean', 'i2_field_clean'}
    missing_mapping_cols = required_mapping_cols - set(mapping_ct.columns)
    if missing_mapping_cols:
        raise KeyError(
            "mapping_ct is missing required columns: {}".format(
                ', '.join(sorted(missing_mapping_cols))
            )
        )

    df_work = df_in.copy()
    audit_data = {'date_logs': [], 'added_cols': [], 'dropped_cols': []}

    # Map source fields to target schema and initialize missing target columns
    try:
        for _, row in mapping_ct.iterrows():
            src = row['i7_field_clean']
            tgt = row['i2_field_clean']

            if src and src in df_work.columns:
                df_work[tgt] = df_work[src]
            elif tgt != 'file' and tgt not in df_work.columns:
                df_work[tgt] = ''
    except Exception as error:
        raise RuntimeError(
            f"Failed while mapping I7 fields to I2 fields: {error}"
        ) from error

    # Apply EDTF date qualifiers and record changes in the audit log
    try:
        df_work, date_logs = apply_date_qualification(df_work)
        audit_data['date_logs'] = date_logs
    except Exception as error:
        raise ValueError(
            f"Failed while applying date qualification: {error}"
        ) from error

    # Order columns based on the mapping template
    i2_ordered = []
    seen = set()
    for field in mapping_ct['i2_field_clean']:
        if field and field not in seen:
            if field == 'file' and field not in df_work.columns:
                continue
            i2_ordered.append(field)
            seen.add(field)

    # Ensure all required columns exist and track newly initialized fields
    for col in i2_ordered:
        if col not in df_work.columns:
            df_work[col] = ''
            audit_data['added_cols'].append(col)

    # Identify excluded columns for audit purposes
    audit_data['dropped_cols'] = [c for c in df_work.columns if c not in seen]

    # Prune schema and run final processors
    try:
        df_final = df_work[i2_ordered].copy()
    except KeyError as error:
        raise RuntimeError(
            f"Failed while selecting final output columns: {error}"
        ) from error

    try:
        df_final = apply_processors(df_final, PROCESSORS)
    except Exception as error:
        raise ValueError(
            f"Failed while applying field processors: {error}"
        ) from error

    return df_final, audit_data


# --- Main Workflow ---

def run_i7_to_i2_template(
    batch_path: str | Path,
    content_type: str | list[str],
    metadata_id: str | None = None,
    metadata_sheet: str | Path | None = None,
    credentials_file: str | Path = GOOGLE_CREDENTIALS_FILE,
) -> ConversionResult:
    """Convert I7 metadata to the I2 template format from another script.

    This is the convenient callable entry point for other modules. It accepts
    normal function arguments, builds the internal AppConfig object, and runs
    the conversion workflow.

    Args:
        batch_path: Batch directory where metadata and logs should be written.
        content_type: One or more content type values. Values may be provided
            as a list or as a space-/comma-separated string.
        metadata_id: Google Sheet ID for the metadata source.
        metadata_sheet: Path to a local metadata spreadsheet.
        credentials_file: Path to the Google service account credentials JSON.

    Returns:
        ConversionResult containing output metadata, audit log, and processing
        log paths.

    Raises:
        ValueError: If required arguments are missing, mutually exclusive, or
            invalid.
    """
    if not metadata_id and not metadata_sheet:
        raise ValueError(
            "Provide either metadata_id or metadata_sheet."
        )

    if metadata_id and metadata_sheet:
        raise ValueError(
            "Provide either metadata_id or metadata_sheet, not both."
        )

    config = AppConfig(
        batch_path=str(batch_path),
        credentials_file=str(credentials_file),
        content_type=normalize_content_types(content_type),
        metadata_id=metadata_id,
        metadata_sheet=str(metadata_sheet) if metadata_sheet else None,
    )

    return convert_i7_to_i2_template(config)


def convert_i7_to_i2_template(config: AppConfig) -> ConversionResult:
    """Run the Islandora 7 to Islandora 2 template conversion workflow.

    This function sets up logging, prepares the mapping, loads source metadata, 
    transforms the data, and writes the output metadata CSV and audit log.

    Args:
        config: Application configuration object.

    Returns:
        ConversionResult containing the metadata output path, audit log path,
        and processing log path.

    Raises:
        Exception: Re-raises any processing error after logging it.
    """
    logger = None

    try:
        # Get a unique timestamp
        config.timestamp = datetime.now().strftime('%Y-%m-%d-%H%M%S')

        # Set up logger
        batch_dir = config.batch_path.name
        config.file_prefix = f'{batch_dir}_{config.timestamp}'
        config.log_dir = create_directory(config.batch_path / 'logs')
        config.log_path = (
            config.log_dir / f'{config.file_prefix}_template_conversion.log'
        )
        logger = setup_logger(LOGGER_NAME, config.log_path)

        # Load and prepare the crosswalk mapping
        mapping_ct = prepare_mapping(I7_to_I2_MAPPING, config.content_type)

        # Ingest source data
        df_in = load_metadata(config)

        # Transform data
        df_final, audit_data = transform_metadata(df_in, mapping_ct)

        # Generate outputs
        metadata_path, audit_log_path = save_outputs(
            df_final,
            audit_data,
            mapping_ct,
            config,
        )

        return ConversionResult(
            metadata_path=metadata_path,
            audit_log_path=audit_log_path,
            processing_log_path=config.log_path,
        )

    except Exception:
        msg = "A critical system error occurred during execution."

        if logger:
            logger.exception(msg)

        raise


def main() -> None:
    """Coordinate the end-to-end metadata conversion workflow.

    Parses command-line arguments, runs the conversion workflow, and handles
    user-facing fatal error messaging for CLI usage.
    """
    config = None

    try:
        config = parse_arguments()
        convert_i7_to_i2_template(config)

    except Exception:
        msg = "A critical system error occurred during execution."

        # Show the user error message
        print(f"\n{ERROR_SYMBOL} {msg}")

        log_path = getattr(config, 'log_path', None)

        if log_path:
            print(f"See logs: {log_path}")
        else:
            traceback.print_exc()

        sys.exit(1)


if __name__ == '__main__':
    # Make pandas not warn about dtype conversions when adding new columns
    pd.options.mode.copy_on_write = False
    main()
