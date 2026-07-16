#!/usr/bin/env python3

"""Convert a Islandora (Workbench) export sheet to a metadata sheet.

The script reverses the field-oriented transformations used when generating an
Islandora Workbench ingest sheet. It:

- maps Islandora machine-field columns back to metadata sheet field names;
- removes prefixes from values except for fields mapped from ``note``;
- changes pipe-delimited values to semicolon-and-space delimiters;
- uses prefixes to route values that share one Islandora field;
- drops export columns that are not represented in the mapping tables; and
- writes the resulting metadata sheet to CSV.

Usage:
    # Run interactively
    python3 make_metadata_sheet.py

    # Provide the export and output paths
    python3 make_metadata_sheet.py \
        --export_sheet /path/to/export.csv \
        --batch_path /path/to/batch/metadata
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
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

# Third-party imports
import pandas as pd
from tqdm import tqdm

# Local imports
from definitions import (
    MANIFEST_FIELD_MAPPING,
    MARC_FIELD_MAPPING,
    METADATA_AGENT_TYPES,
    RELATOR_CODES,
    TEMPLATE_FIELD_MAPPING,
)
from utilities import (
    ERROR_SYMBOL,
    SUCCESS_SYMBOL,
    WARNING_SYMBOL,
    LogRegistry,
    create_df,
    create_directory,
    df_to_csv,
    prompt_for_input,
    setup_logger,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOGGER_NAME = LogRegistry.MAKE_METADATA_SHEET
DEFAULT_SEPARATOR = '; '

LINKED_AGENT_PREFIX_PATTERN = re.compile(
    r'^relators:'
    r'(?P<relator_code>[^:]+):'
    r'(?P<agent_type>person|family|corporate_body|conference):'
    r'(?P<value>.*)$'
)


# ---------------------------------------------------------------------------
# Classes
# ---------------------------------------------------------------------------

@dataclass
class AppConfig:
    """Application configuration values.

    Attributes:
        export_sheet: Workbench export CSV or Excel file.
        batch_path: Path to a directory for Workbench batches.
        timestamp: Timestamp used in the processing log filename.
        output_path: Final metadata sheet CSV path.
        log_dir: Directory containing the processing log.
        log_path: Processing log path.
    """

    export_sheet: str | Path
    batch_path: str | Path
    timestamp: str | None = None
    output_path: Path | None = None
    log_dir: Path | None = None
    log_path: Path | None = None

    def __post_init__(self) -> None:
        """Normalize path-like values."""
        self.export_sheet = Path(self.export_sheet)
        self.batch_path = Path(self.batch_path)


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------

# --- Argument Parsing ---

def parse_arguments() -> AppConfig:
    """Parse command-line arguments and prompt for missing paths.

    Returns:
        Application configuration object.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Convert a Workbench export sheet to a metadata sheet."
        )
    )
    parser.add_argument(
        '-b',
        '--batch_path',
        type=str,
        help="Path to a batch directory for Workbench ingests.",
    )
    parser.add_argument(
        '-e',
        '--export_sheet',
        type=str,
        help="Path to the Workbench export CSV or Excel file.",
    )

    args = parser.parse_args()

    if not args.export_sheet:
        args.export_sheet = prompt_for_input(
            "Enter the path to the Workbench export sheet: "
        )

    if not args.batch_path:
        args.batch_path = prompt_for_input(
            "Enter the directory for the metadata sheet: "
        )

    return AppConfig(**vars(args))


# --- Mapping Helpers ---

def normalize_mapping_table(mapping_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize given field-mapping table for reverse conversion.

    Args:
        mapping_df: Mapping table containing source and machine field names.

    Returns:
        Mapping rows with standardized fields required by this script.
    """
    required_columns = {
        'field',
        'machine_name',
    }

    missing_columns = required_columns - set(mapping_df.columns)

    if missing_columns:
        raise ValueError(
            "Field mapping is missing required column(s): "
            f"{', '.join(sorted(missing_columns))}"
        )

    normalized = mapping_df.copy()

    if 'prefix' not in normalized.columns:
        normalized['prefix'] = ''

    normalized = normalized[
        ['field', 'machine_name', 'prefix']
    ].copy()

    for column in normalized.columns:
        normalized[column] = (
            normalized[column]
            .fillna('')
            .astype(str)
            .str.strip()
        )

    normalized = normalized[
        normalized['field'].ne('')
        & normalized['machine_name'].ne('')
    ]

    return normalized


def load_reverse_mappings() -> pd.DataFrame:
    """Combine and deduplicate all available field mappings.

    Mapping-table order establishes precedence. Exact duplicate mappings are
    removed, but mappings with different prefixes are retained because the
    prefixes may be required to route exported values.

    Returns:
        Combined reverse mapping table.
    """
    mapping_tables = [
        normalize_mapping_table(TEMPLATE_FIELD_MAPPING),
        normalize_mapping_table(MANIFEST_FIELD_MAPPING),
        normalize_mapping_table(MARC_FIELD_MAPPING),
    ]

    mappings = pd.concat(
        mapping_tables,
        ignore_index=True,
    )

    return mappings.drop_duplicates(
        subset=[
            'field',
            'machine_name',
            'prefix',
        ],
        keep='first',
    ).reset_index(drop=True)


def normalize_prefix(prefix: str) -> str:
    """Normalize a prefix for reliable comparison.

    Args:
        prefix: Prefix from a mapping row or exported value.

    Returns:
        Stripped prefix ending in a colon, or an empty string.
    """
    prefix = str(prefix or '').strip()

    if not prefix:
        return ''

    return prefix if prefix.endswith(':') else f'{prefix}:'


def parse_linked_agent_value(
    value: str,
) -> tuple[str, str, str, str] | None:
    """Parse an exported linked-agent value.

    Args:
        value: Exported linked-agent value.

    Returns:
        Tuple containing the complete prefix, relator code, agent type, and
        unprefixed agent value. Returns None when the value does not match the
        expected linked-agent structure.
    """
    match = LINKED_AGENT_PREFIX_PATTERN.fullmatch(value.strip())

    if not match:
        return None

    relator_code = match.group('relator_code').strip()
    agent_type = match.group('agent_type').strip()
    agent_value = match.group('value').strip()
    prefix = normalize_prefix(
        f'relators:{relator_code}:{agent_type}'
    )

    return prefix, relator_code, agent_type, agent_value


def find_linked_agent_field(
    prefix: str,
    mappings: pd.DataFrame,
) -> str | None:
    """Find the metadata sheet field for an exact linked-agent prefix.

    Args:
        prefix: Concrete linked-agent prefix parsed from the export value.
        mappings: Combined field mapping table.

    Returns:
        Matching metadata sheet field, if configured.
    """
    linked_agent_mappings = mappings.loc[
        mappings['machine_name'].eq('field_linked_agent')
    ].copy()

    if linked_agent_mappings.empty:
        return None

    linked_agent_mappings['normalized_prefix'] = (
        linked_agent_mappings['prefix']
        .map(normalize_prefix)
    )

    match = linked_agent_mappings.loc[
        linked_agent_mappings['normalized_prefix'].eq(prefix),
        'field',
    ]

    if match.empty:
        return None

    return str(match.iloc[0])


def generate_linked_agent_field(
    relator_code: str,
    agent_type: str,
    logger: logging.Logger,
) -> str:
    """Generates a metadata field name by combining a relator term and agent type.

    Args:
        relator_code: The raw MARC relator code (e.g., 'cre', 'art') to lookup.
        agent_type: The raw agent type (e.g., 'person', 'corporate_body').
        logger: A logger instance used to emit a warning if the relator code does
            not map to a known term.

    Returns:
        The generated field name combining either the resolved relator term or
        the fallback code with the resolved agent type (e.g., 'artist_person'
        or 'art_person').

    Raises:
        AttributeError: If `relator_code` is not present in the global
            `RELATOR_CODES` dictionary, as calling `.get("term")` on `None`
            will fail.
    """
    normalized_code = relator_code.strip().casefold()
    relator_entry = RELATOR_CODES.get(normalized_code)
    relator_term = relator_entry.get("term") if relator_entry else None

    metadata_agent_type = METADATA_AGENT_TYPES.get(
        agent_type,
        agent_type,
    )

    if relator_term:
        normalized_term = re.sub(
            r'\s+',
            '_',
            relator_term.strip().casefold(),
        )
        return f'{normalized_term}_{metadata_agent_type}'

    fallback_field = (
        f'{normalized_code}_{metadata_agent_type}'
    )

    logger.warning(
        "No relator term was found for code '%s'. Using generated field '%s'.",
        relator_code,
        fallback_field,
    )

    return fallback_field


def reverse_map_linked_agents(
    raw_values: list[str],
    mappings: pd.DataFrame,
    output: dict[str, list[str]],
    logger: logging.Logger,
) -> None:
    """Route linked-agent values to metadata sheet fields."""
    for raw_value in raw_values:
        parsed = parse_linked_agent_value(raw_value)

        if parsed is None:
            logger.warning(
                "Linked-agent value could not be routed: %s",
                raw_value,
            )
            continue

        prefix, relator_code, agent_type, agent_value = parsed
        target_field = find_linked_agent_field(prefix, mappings)

        if target_field is None:
            target_field = generate_linked_agent_field(
                relator_code,
                agent_type,
                logger,
            )
            logger.info(
                "Generated metadata field '%s' for prefix '%s'.",
                target_field,
                prefix,
            )

        append_output_values(
            output,
            target_field,
            [agent_value],
        )


# --- Value Helpers ---

def split_values(value: Any) -> list[str]:
    """Split a repeatable export value on a pipe delimiter.

    Args:
        value: Raw spreadsheet cell value.

    Returns:
        Ordered nonblank values.
    """
    if value is None or pd.isna(value):
        return []

    text = str(value).strip()

    if not text:
        return []

    return [
        part.strip()
        for part in re.split(r'\s*[|]\s*', text)
        if part.strip()
    ]


def remove_prefix(
    value: str,
    prefix: str,
) -> str | None:
    """Remove a required prefix from the beginning of a value.

    Args:
        value: Exported field value.
        prefix: Prefix associated with the metadata sheet field.

    Returns:
        Value without the prefix. Returns None when a prefix is configured but
        the value does not use it.
    """
    if not prefix:
        return value

    if not value.startswith(prefix):
        return None

    return value.removeprefix(prefix).strip()


def deduplicate_values(values: list[str]) -> list[str]:
    """Deduplicate values while preserving first-seen order.

    Args:
        values: A list of raw string values to deduplicate.

    Returns:
        A list of unique, non-empty string values in their original relative order.
    """
    return list(dict.fromkeys(value for value in values if value))


def serialize_values(values: list[str]) -> str:
    """Serialize values using the metadata sheet separator.

    Args:
        values: A list of raw string values to deduplicate and serialize.

    Returns:
        A single string containing the ordered, unique, non-empty values
        concatenated with the default separator.
    """
    return DEFAULT_SEPARATOR.join(deduplicate_values(values))


def append_output_values(
    output: dict[str, list[str]],
    field: str,
    values: list[str],
) -> None:
    """Appends values to a specific field in a metadata sheet dictionary.

    Args:
        output: The target dictionary tracking field-to-value mappings, where
            each field maps to a list of its associated strings.
        field: The name of the metadata field to modify.
        values: The list of string values to append to the field.

    Returns:
        None. Modifies the `output` dictionary in-place.
    """
    if not values:
        return

    output.setdefault(field, [])
    output[field].extend(values)


# --- Conversion ---

def reverse_map_record(
    row: pd.Series,
    mappings: pd.DataFrame,
    logger: logging.Logger,
) -> dict[str, str]:
    """Convert one Workbench export row to metadata sheet fields.

    Values are routed using the mapping from ``machine_name`` to ``field``.
    When several metadata sheet fields share one machine field, prefixes are
    used to distinguish values when possible.

    Note prefixes are retained. Linked-agent prefixes determine the output
    metadata field. Prefixes on other mapped values are removed.

    When unprefixed values have multiple possible mappings, values are assigned
    to ``title`` if it is one of the candidates. Otherwise, they are preserved
    under the original export field name.

    Args:
        row: Workbench export record.
        mappings: Combined reverse field mapping table.
        logger: Process logger.

    Returns:
        metadata sheet fields and their serialized values.
    """
    output: dict[str, list[str]] = {}

    # Process each I2 field
    for machine_field, field_mappings in mappings.groupby(
        'machine_name',
        sort=False,
    ):
        if machine_field not in row.index:
            continue

        raw_values = split_values(row[machine_field])

        if not raw_values:
            continue

        # Handle special case of linked agent field prefixes
        if machine_field == 'field_linked_agent':
            reverse_map_linked_agents(
                raw_values,
                mappings,
                output,
                logger,
            )
            continue

        mapped_fields = list(
            dict.fromkeys(field_mappings['field'])
        )

        # Handle cases where I2 field maps to only one metadata sheet field
        if len(mapped_fields) == 1:
            target_field = mapped_fields[0]

            prefixes = [
                normalize_prefix(prefix)
                for prefix in field_mappings['prefix']
                if normalize_prefix(prefix)
            ]

            cleaned_values: list[str] = []

            for raw_value in raw_values:
                cleaned_value = raw_value.strip()

                # Maintain note prefixes as public display labels
                if machine_field != 'field_note':
                    matching_prefix = next(
                        (
                            prefix
                            for prefix in prefixes
                            if raw_value.startswith(prefix)
                        ),
                        None,
                    )

                    if matching_prefix:
                        cleaned_value = raw_value.removeprefix(
                            matching_prefix
                        ).strip()

                if cleaned_value:
                    cleaned_values.append(cleaned_value)

            append_output_values(
                output,
                target_field,
                cleaned_values,
            )
            continue

        # Handle cases where I2 field maps to several metadata fields
        # Track values routed
        # by a prefix so they are not assigned more than once.
        claimed_indexes: set[int] = set()
        unprefixed_fields: list[str] = []

        for mapping in field_mappings.itertuples(index=False):
            source_field = mapping.field
            prefix = normalize_prefix(mapping.prefix)

            if not prefix:
                unprefixed_fields.append(source_field)
                continue

            matched_values: list[str] = []

            for index, raw_value in enumerate(raw_values):
                if index in claimed_indexes:
                    continue

                if not raw_value.startswith(prefix):
                    continue

                claimed_indexes.add(index)

                cleaned_value = (
                    raw_value.strip()
                    if machine_field == 'field_note'
                    else raw_value.removeprefix(prefix).strip()
                )

                if cleaned_value:
                    matched_values.append(cleaned_value)

            append_output_values(
                output,
                source_field,
                matched_values,
            )

        # Values that were not identified by a prefix still need a destination.
        remaining_values = [
            value
            for index, value in enumerate(raw_values)
            if index not in claimed_indexes
        ]

        if not remaining_values:
            continue

        if len(unprefixed_fields) == 1:
            target_field = unprefixed_fields[0]

        elif 'title' in mapped_fields:
            target_field = 'title'

        else:
            # Preserve ambiguous values under the original export field rather
            # than assigning them to an incorrect metadata sheet field.
            target_field = machine_field

            logger.warning(
                (
                    "Machine field '%s' maps to multiple metadata fields "
                    "(%s), but the following value(s) could not be routed by "
                    "prefix. They were preserved under '%s': %s"
                ),
                machine_field,
                ', '.join(mapped_fields),
                machine_field,
                ' | '.join(remaining_values),
            )

        append_output_values(
            output,
            target_field,
            remaining_values,
        )

    # Convert accumulated value lists to metadata sheet cell values.
    return {
        field: serialize_values(values)
        for field, values in output.items()
    }


def convert_export_to_metadata(
    export_df: pd.DataFrame,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """Convert a Workbench export DataFrame to a metadata sheet DataFrame.

    Args:
        export_df: Workbench export records.
        logger: Optional process logger.

    Returns:
        Human-readable metadata DataFrame containing only mapped fields.
    """
    logger = logger or logging.getLogger(LOGGER_NAME)
    mappings = load_reverse_mappings()

    mapped_machine_fields = set(mappings['machine_name'])
    retained_machine_fields = [
        column
        for column in export_df.columns
        if column in mapped_machine_fields
    ]
    dropped_columns = [
        column
        for column in export_df.columns
        if column not in mapped_machine_fields
    ]

    logger.info(
        "Found %d mapped export column(s).",
        len(retained_machine_fields),
    )

    if dropped_columns:
        logger.info(
            "Dropping %d unmapped export column(s): %s",
            len(dropped_columns),
            ', '.join(dropped_columns),
        )

    if not retained_machine_fields:
        raise ValueError(
            "The export sheet does not contain any columns represented in "
            "the field mappings."
        )

    records = []

    row_iterator = tqdm(
        export_df.iterrows(),
        total=len(export_df),
        desc="Converting records",
        unit="record",
    )

    for row_number, (_, row) in enumerate(
        row_iterator,
        start=2,
    ):
        try:
            records.append(
                reverse_map_record(
                    row=row,
                    mappings=mappings,
                    logger=logger,
                )
            )
        except Exception:
            logger.exception(
                "Failed to reverse-map export row %d.",
                row_number,
            )
            raise

    metadata_df = pd.DataFrame.from_records(records)

    # Preserve the field order defined by the combined mapping tables.
    mapped_field_order = list(dict.fromkeys(mappings['field']))
    output_columns = [
        field
        for field in mapped_field_order
        if field in metadata_df.columns
    ]

    metadata_df = metadata_df.reindex(
        columns=output_columns,
        fill_value='',
    )

    return metadata_df.fillna('')


# --- File Processing ---

def make_metadata_sheet(
    export_sheet: str | Path,
    batch_path: str | Path,
    logger: logging.Logger | None = None,
) -> Path:
    """Create a metadata sheet from a Workbench export file.

    This is the callable entry point for other modules.

    Args:
        export_sheet: Workbench export CSV or Excel file.
        batch_path: Directory where the metadata sheet should be written.
        logger: Optional process logger.

    Returns:
        Path to the generated metadata sheet CSV.
    """
    export_path = Path(export_sheet)
    batch_path = create_directory(batch_path)

    if not export_path.exists():
        raise FileNotFoundError(
            f"Workbench export sheet not found: {export_path}"
        )

    logger = logger or logging.getLogger(LOGGER_NAME)
    logger.info("Reading Workbench export sheet: %s", export_path)

    export_df = create_df(export_path)

    logger.info(
        "Loaded %d record(s) and %d column(s) from the export sheet.",
        len(export_df),
        len(export_df.columns),
    )

    metadata_df = convert_export_to_metadata(
        export_df,
        logger=logger,
    )

    output_path = batch_path / f'{export_path.stem}_metadata.csv'
    df_to_csv(metadata_df, output_path)

    logger.info(
        "Metadata sheet saved with %d record(s) and %d column(s): %s",
        len(metadata_df),
        len(metadata_df.columns),
        output_path,
    )

    return output_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Run the Workbench-export-to-metadata conversion workflow."""
    logger = None
    log_path = None

    try:
        config = parse_arguments()
        config.timestamp = datetime.now().strftime('%Y-%m-%d-%H%M%S')
        config.batch_path = create_directory(config.batch_path)
        config.output_path = create_directory(config.batch_path / 'metadata')
        config.log_dir = create_directory(config.batch_path / 'logs')
        config.log_path = (
            config.log_dir
            / f'{config.timestamp}_make_metadata_sheet.log'
        )
        log_path = config.log_path

        logger = setup_logger(
            LOGGER_NAME,
            config.log_path,
        )

        config.output_path = make_metadata_sheet(
            export_sheet=config.export_sheet,
            batch_path=config.batch_path,
            logger=logger,
        )

        print(
            f"\n{SUCCESS_SYMBOL} Metadata sheet saved: "
            f"{config.output_path.as_posix()}"
        )
        print(f"Log saved to: {config.log_path.as_posix()}")

    except Exception as error:
        message = f"Metadata sheet conversion failed: {error}"

        if logger:
            logger.exception(message)

        print(f"\n{ERROR_SYMBOL} {message}")

        if log_path:
            print(f"See logs: {Path(log_path).as_posix()}")
        else:
            traceback.print_exc()

        sys.exit(1)


if __name__ == '__main__':
    main()
