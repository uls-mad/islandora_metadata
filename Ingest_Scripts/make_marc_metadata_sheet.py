#!/usr/bin/env python3

"""Generate Islandora metadata sheets from MARC bibliographic records.

This script extracts bibliographic metadata from MARC records, transforms the
records to MODS using an XSLT stylesheet, and converts the resulting metadata
into an Islandora metadata sheet. It supports batch processing, optional
manifest merging, XML export, detailed processing logs, and reporting of 
metadata issues and transformations.

Usage:
    # Process an input directory
    python3 make_marc_metadata_sheet.py \
        --batch_path /workbench/batches/example

    # Merge extracted metadata with a manifest
    python3 make_marc_metadata_sheet.py \
        --batch_path /workbench/batches/example \
        --manifest_id <manifest_sheet_id>

    # Save intermediate MODS XML
    python3 make_marc_metadata_sheet.py \
        --batch_path /workbench/batches/example \
        --save_xml
"""

# ---------------------------------------------------------------------------
# Modules
# ---------------------------------------------------------------------------

# Standard library imports
import argparse
import logging
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path

# Third-party imports
import pandas as pd
from lxml import etree as ET
from pymarc import (
    MARCReader,
    Record,
    XMLWriter,
    parse_xml_to_array
)
from tqdm import tqdm

# Local imports
from definitions import (
    GOOGLE_CREDENTIALS_FILE,
    MARC_FIELD_MAPPING,
    UTILITY_FILES_DIR,
)
from process_mods import process_mods
from taxonomy_manager import load_taxonomies
from utilities import (
    LogRegistry,
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
# Global
# ---------------------------------------------------------------------------

LOGGER_NAME = LogRegistry.MAKE_MARC_METADATA_SHEET


# ---------------------------------------------------------------------------
# Classes
# ---------------------------------------------------------------------------

@dataclass
class AppConfig:
    """Application configuration parameters.

    Attributes:
        batch_path: Batch directory containing MARC files.
        manifest_id: Google Sheet ID for the manifest.
        credentials_file: Path to the Google service account JSON.
        save_xml: Whether to save intermediate MODS XML files.
        split: Whether to save separate metadata files for each input file.
        refresh_taxonomies: Whether to refresh the taxonomy cache before
            processing.
        timestamp: Timestamp used for output and log filenames.
        manifest_sheet: Path to a local manifest spreadsheet.
        output_dir: Directory for generated metadata files.
        log_dir: Directory for processing reports and logs.
        log_path: Runtime log file path.
    """

    batch_path: str | Path
    manifest_id: str | None
    credentials_file: str | Path
    save_xml: bool
    split: bool
    refresh_taxonomies: bool = False
    timestamp: str | None = None
    manifest_sheet: str | Path | None = None
    output_dir: str | Path | None = None
    log_dir: str | Path | None = None
    log_path: str | Path | None = None

    def __post_init__(self) -> None:
        """Normalize path-like configuration values to Path objects."""
        self.batch_path = Path(self.batch_path)
        self.credentials_file = Path(self.credentials_file)

        if self.manifest_sheet:
            self.manifest_sheet = Path(self.manifest_sheet)

        if self.output_dir:
            self.output_dir = Path(self.output_dir)

        if self.log_dir:
            self.log_dir = Path(self.log_dir)

        if self.log_path:
            self.log_path = Path(self.log_path)


class MARCTransformer:
    """Transform MARC records into MODS XML using an XSLT stylesheet."""

    def __init__(self, xslt_path: str | Path) -> None:
        """
        Initialize the transformer with an XSLT stylesheet.

        Args:
            xslt_path: Path to the `.xsl` stylesheet file.

        Raises:
            FileNotFoundError: If the XSLT file does not exist.
            etree.XMLSyntaxError: If the XSLT cannot be parsed.
        """
        self.transform = self._load_xslt(xslt_path)

    def _load_xslt(self, xslt_path: str | Path) -> ET.XSLT:
        """
        Load and compile an XSLT stylesheet.

        Args:
            xslt_path: Path to the XSLT file.

        Returns:
            A compiled `lxml.etree.XSLT` transformer.
        
        Raises:
            FileNotFoundError: If the file does not exist.

        """
        xslt_path = Path(xslt_path)

        if not xslt_path.exists():
            raise FileNotFoundError(f"XSLT file not found: {xslt_path}")

        parser = ET.XMLParser(load_dtd=True, resolve_entities=True)

        with xslt_path.open('rb') as f:
            xsl_tree = ET.parse(f, parser=parser, base_url=str(xslt_path))

        return ET.XSLT(xsl_tree)

    def convert_record_to_marcxml(self, record: Record) -> ET._Element:
        """
        Convert a pymarc Record into an lxml MARCXML element.

        Writes the record to an in-memory MARCXML stream, parses it into an
        XML tree, and extracts the `<record>` element from the `<collection>`.

        Args:
            record: The pymarc `Record` to convert.

        Returns:
            An `lxml.etree._Element` representing the MARC `<record>` element.

        
        Raises:
            TypeError: If `record` is not a pymarc `Record`.
            ValueError: If XML parsing fails or the `<record>` element is missing.

        """
        if not isinstance(record, Record):
            raise TypeError(f"Unexpected record type: {type(record)}")

        record_stream = BytesIO()
        writer = XMLWriter(record_stream)
        writer.write(record)
        writer.close(close_fh=False)
        record_stream.seek(0)

        try:
            collection_tree = ET.parse(record_stream)
        except ET.XMLSyntaxError as e:
            raise ValueError(f"Could not parse MARCXML record: {e}") from e

        record_element = collection_tree.find(
            './/{http://www.loc.gov/MARC21/slim}record'
        )

        if record_element is None:
            raise ValueError("No <record> element found inside <collection>.")

        return record_element

    def transform_to_mods(self, record_element: ET._Element) -> ET._Element:
        """
        Transform a MARCXML element into a MODS XML element.

        Applies the configured XSLT transformation to a MARC `<record>` element
        and returns the resulting MODS root element.

        Args:
            record_element: The MARCXML `<record>` element.

        Returns:
            The resulting MODS XML root element.
            
        Raises:
            ValueError: If the transformation fails or produces an invalid result.

        """
        mods_result = self.transform(record_element)

        if isinstance(mods_result, ET._XSLTResultTree):
            mods_root = mods_result.getroot()
        elif isinstance(mods_result, ET._Element):
            mods_root = mods_result
        else:
            raise ValueError(
                "XSLT transformation did not return a valid result tree."
            )

        if mods_root is None:
            raise ValueError("XSLT transformation returned no root element.")

        return mods_root


# ---------------------------------------------------------------------------
#  Functions
# ---------------------------------------------------------------------------

# --- Setup Helpers ---

def parse_arguments() -> AppConfig:
    """
    Parse command-line arguments or prompt user for directory inputs.

    Returns:
        An AppConfig object containing the validated runtime parameters.
    """
    parser = argparse.ArgumentParser(
        description="Process MARC records into a metadata spreadsheet."
    )
    parser.add_argument(
        '-i', '--batch_path',
        type=str,
        help="Path to input directory containing MARC files."
    )
    parser.add_argument(
        '-m', '--manifest_id',
        type=str,
        help="Google Sheet ID for the manifest file."
    )
    parser.add_argument(
        '--manifest_sheet',
        type=str,
        help="Path to manifest on local device (optional)."
    )
    parser.add_argument(
        '-c', '--credentials_file',
        type=str,
        default=GOOGLE_CREDENTIALS_FILE,
        help="Path to the Google service account credentials JSON."
    )
    parser.add_argument(
        '-x', '--save_xml',
        action='store_true',
        help="Save intermediate MODS XML files for each record."
    )
    parser.add_argument(
        '-s', '--split',
        action='store_true',
        help="Save a separate metadata CSV for each input file."
    )
    parser.add_argument(
        '--refresh_taxonomies',
        action='store_true',
        help="Refresh the taxonomy cache before processing records.",
    )
    args = parser.parse_args()

    if not args.batch_path:
        while not args.batch_path:
            args.batch_path = prompt_for_input(
                "Enter the full path to the batch directory: "
            )

    if not args.manifest_id and not args.manifest_sheet:
        args.manifest_id = prompt_for_input(
            "Enter the Google Sheet ID for the manifest: "
        )

    return AppConfig(**vars(args))


def load_marc_records(file_path: str | Path) -> list[Record]:
    """
    Load MARC records from a binary MARC or MARCXML file.

    Args:
        file_path: Path to a `.mrc` or `.xml` file.

    Returns:
        A list of pymarc `Record` objects.

    Raises:
        ValueError: If the file extension is not `.mrc` or `.xml`.
    """
    file_path = Path(file_path)

    if file_path.suffix.lower() == '.mrc':
        with file_path.open('rb') as f:
            return list(MARCReader(f, to_unicode=True, force_utf8=True))

    if file_path.suffix.lower() == '.xml':
        with file_path.open('rb') as f:
            return parse_xml_to_array(f)

    raise ValueError(f"Unsupported MARC file type: {file_path.suffix}")


# --- DataFrame Helpers ---

def sort_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sort DataFrame columns based on mapping order.

    Deduplicates the mapping field list while preserving order, inserts
    non-subject agent fields after `frequency`, and appends unmapped columns
    to the end in their original order.

    Args:
        df: DataFrame whose columns should be sorted.

    Returns:
        A DataFrame with reordered columns.
    """
    agent_suffixes = (
        '_person',
        '_corporate',
        '_conference',
        '_family',
        '_untyped',
    )

    ordered_fields = list(
        dict.fromkeys(MARC_FIELD_MAPPING['field'])
    )

    agent_cols = [
        col for col in df.columns
        if (
            col.endswith(agent_suffixes)
            and 'subject' not in col
            and col not in ordered_fields
        )
    ]

    ordered_without_agents = [
        col for col in ordered_fields
        if col not in agent_cols
    ]

    try:
        insert_index = ordered_without_agents.index('frequency') + 1
    except ValueError:
        try:
            insert_index = ordered_without_agents.index('subject_topic')
        except ValueError:
            insert_index = len(ordered_without_agents)

    ordered_fields_with_agents = (
        ordered_without_agents[:insert_index]
        + agent_cols
        + ordered_without_agents[insert_index:]
    )

    mapped_cols = [
        col for col in ordered_fields_with_agents
        if col in df.columns
    ]

    remaining_cols = [
        col for col in df.columns
        if col not in mapped_cols
    ]

    return df[mapped_cols + remaining_cols]


def summarize_values(
    values: list,
    max_items: int = 20,
    separator: str = ', '
) -> str:
    """Summarize a list of values for logging.

    Converts values to strings, joins the first ``max_items`` values, and
    appends a count of additional values when applicable.

    Args:
        values: Values to summarize.
        max_items: Maximum number of values to display.
        separator: Separator used between displayed values.

    Returns:
        A summarized string representation of the values.
    """
    values = [str(value) for value in values if value is not None]

    display_values = values[:max_items]
    extra_count = len(values) - len(display_values)

    message = separator.join(display_values)

    if extra_count > 0:
        message += f" ... (+{extra_count} more)"

    return message


def load_manifest(
    config: AppConfig,
) -> pd.DataFrame:
    """
    Load a manifest DataFrame from Google Sheets or a local file.

    If a Google Sheet ID is provided, the manifest is loaded from Google
    Sheets. Otherwise, if a local manifest sheet path is provided, the
    manifest is loaded from the local file. If neither is provided, an
    empty DataFrame is returned.

    Args:
        config: Application configuration object.
    Returns:
        A manifest DataFrame, or an empty DataFrame if no source is provided.
    """
    logger = logging.getLogger(LOGGER_NAME)
    if config.manifest_id:
        logger.info(
            "Using manifest from Google Sheet ID: %s",
            config.manifest_id
        )

        return read_google_sheet(
            config.manifest_id,
            sheet_name=config.manifest_sheet,
            credentials_file=config.credentials_file
        )

    if config.manifest_sheet:
        logger.info(
            "Using manifest sheet from local file: %s",
            config.manifest_sheet
        )

        return create_df(config.manifest_sheet)

    logger.info("No manifest provided.")

    return pd.DataFrame()


def merge_manifest_metadata(
    manifest_df: pd.DataFrame,
    metadata_df: pd.DataFrame,
    logger: logging.Logger
) -> pd.DataFrame:
    """Merge manifest and metadata DataFrames using MMS ID fields.

    Merges metadata rows to manifest rows using either the 
    'field_record_source_id' or 'mmsid' column in the manifest, matched against 
    'record_identifier' in the metadata sheet. 

    Args:
        manifest_df: DataFrame containing manifest data.
        metadata_df: DataFrame containing extracted MARC metadata.
        logger: Logger for reporting join results.

    Returns:
        A merged DataFrame with manifest data preserved.
    """
    try:
        manifest_df = manifest_df.copy()
        metadata_df = metadata_df.copy()

        if 'id' in manifest_df.columns:
            manifest_df.rename(columns={'id': 'identifier'}, inplace=True)

        if 'record_identifier' not in metadata_df.columns:
            msg = "Metadata is missing required join column: 'record_identifier'"
            logger.error(msg)
            print(msg)
            raise KeyError(msg)

        # Determine Join Columns
        if 'field_record_source_id' in manifest_df.columns:
            manifest_join_col = 'field_record_source_id'
            manifest_cols = ['field_record_source_id']
            if 'node_id' in manifest_df.columns:
                manifest_cols.append('node_id')
        elif 'mmsid' in manifest_df.columns:
            manifest_join_col = 'mmsid'
            if 'identifier' in manifest_df.columns:
                manifest_cols = ['identifier']
            else:
                msg = "Manifest contains 'mmsid' but is missing 'identifier'."
                logger.error(msg)
                raise KeyError(msg)
        else:
            msg = (
                "Manifest is missing required join column. "
                "Expected either 'field_record_source_id' or 'mmsid'."
            )
            logger.error(msg)
            raise KeyError(msg)

        if 'title' in manifest_df.columns:
            manifest_cols.append('title')

        logger.info(
            "Joining manifest '%s' to 'record_identifier'.",
            manifest_join_col
        )

        # Normalize Keys and Slice DataFrames
        manifest_join_series = normalize_for_join(
            manifest_df[manifest_join_col]
        ).astype(str)
        manifest_df = manifest_df.loc[:, manifest_cols].copy()
        manifest_df['__manifest_join_key__'] = manifest_join_series
        
        metadata_df['__metadata_join_key__'] = normalize_for_join(
            metadata_df['record_identifier']
        ).astype(str)

        # Validate Metadata Uniqueness
        metadata_dupes = metadata_df[
            metadata_df['__metadata_join_key__'].notna() &
            metadata_df['__metadata_join_key__'].duplicated(keep=False)
        ]
        if not metadata_dupes.empty:
            sample = metadata_dupes['__metadata_join_key__'].dropna()
            sample_dupes = sample.astype(str).unique()[:10]
            msg = f"Duplicate identifiers in metadata: {', '.join(sample_dupes)}"
            logger.error(msg)
            print(msg)
            raise ValueError(msg)

        manifest_log_col = manifest_cols[0]

        # REPORTING CONDITION A: Metadata records that did not match the manifest
        unmatched_mask = ~metadata_df['__metadata_join_key__'].isin(
            manifest_df['__manifest_join_key__']
        )
        unmatched_metadata_df = metadata_df[unmatched_mask]

        if not unmatched_metadata_df.empty:
            logger.warning(
                "%d metadata rows not found in manifest.",
                len(unmatched_metadata_df)
            )
            
            unmatched_ids = (
                unmatched_metadata_df['record_identifier']
                .dropna()
                .astype(str)
                .tolist()
            )

            logger.warning(
                "Unmatched metadata record_identifiers: %s",
                summarize_values(unmatched_ids)
            )

        # Perform the Left Merge
        merged = pd.merge(
            manifest_df,
            metadata_df,
            how='left',
            left_on='__manifest_join_key__',
            right_on='__metadata_join_key__',
            suffixes=('_manifest', ''),
            validate='many_to_one'
        )

        # Consolidate title columns if a name collision occurred
        if 'title' in merged.columns and 'title_manifest' in merged.columns:
            merged['title'] = merged['title'].fillna(merged['title_manifest'])
            merged.drop(columns=['title_manifest'], inplace=True)

        # REPORTING CONDITION B: Manifest rows missing metadata
        unmatched_manifest = merged[
            merged['__metadata_join_key__'].isna() &
            merged['__manifest_join_key__'].notna()
        ].copy()

        if not unmatched_manifest.empty:
            logger.warning(
                "%d manifest rows failed to match metadata.",
                len(unmatched_manifest)
            )
            logger.warning(
                "Unmatched manifest identifiers (expected MARC data): %s",
                summarize_values(
                    unmatched_manifest[manifest_log_col]
                    .dropna()
                    .astype(str)
                    .tolist()
                )
            )

        # Clean up internal keys and position primary ID
        merged.drop(
            columns=['__metadata_join_key__', '__manifest_join_key__'],
            errors='ignore',
            inplace=True
        )

        if 'identifier' in merged.columns:
            remaining_cols = [
                col for col in merged.columns if col != 'identifier'
            ]
            merged = merged[['identifier'] + remaining_cols]

        return merged

    except Exception:
        msg = (
            "An unexpected error occurred while merging the manifest and "
            "metadata sheet."
        )
        logger.exception(msg)
        print(msg)
        raise


# --- Main Workflow ---

def process_files(
    transformer: MARCTransformer,
    manifest_df: pd.DataFrame,
    config: AppConfig,
) -> int:
    """
    Process MARC files in a directory and save CSV output.

    Reads all `.mrc` and `.xml` files from the input directory, converts each
    MARC record to MODS XML, processes the MODS XML, and saves the output to a
    CSV file. Processing errors are captured in a shared log CSV.

    Args:
        transformer: Initialized MARC transformer used to convert records.
        manifest_df: Loaded manifest DataFrame.
        config: Application configuration containing input and output paths.

    Returns:
        Total number of exceptions encountered during processing, including 
        file-level, record-level, and unexpected runtime exceptions.
    """
    all_records = []
    transformations = []
    issues = []
    exceptions = 0
    logger = logging.getLogger(LOGGER_NAME)
    
    def save_batch(records_list: list, filename: str) -> None:
        """Helper to handle sorting, merging, and CSV writing."""
        if not records_list:
            return
        df = sort_fields(pd.DataFrame(records_list))
        if not manifest_df.empty:
            df = merge_manifest_metadata(manifest_df, df, logger)
        df_to_csv(df, config.output_dir / filename)
    
    try:
        files = [
            file_path for file_path in config.batch_path.iterdir()
            if (
                file_path.is_file()
                and file_path.suffix.lower() in {'.mrc', '.xml'}
            )
        ]

        file_bar = tqdm(files, desc="Processing Files", unit='file')

        for file_path in file_bar:
            try:
                file_records = []
                marc_records = load_marc_records(file_path)
                record_bar = tqdm(
                    marc_records,
                    desc=f"Records in {file_path.name}",
                    leave=False,
                    unit='record'
                )

                for i, record in enumerate(record_bar):
                    try:
                        marc_xml = transformer.convert_record_to_marcxml(record)
                        mods_xml = transformer.transform_to_mods(marc_xml)
                        result = process_mods(
                            mods_xml
                        )

                        if config.save_xml:
                            result.save_mods_xml(config.output_dir)

                        if result.record:
                            file_records.append(result.record)
                            all_records.append(result.record)
                        
                        if result.transformations:
                            transformations.extend(result.transformations)
                        
                        if result.issues:
                            issues.extend(result.issues)

                    except Exception:
                        exceptions += 1
                        logger.exception(
                            "Failed to process record %s in %s.",
                            i,
                            file_path.name,
                        )

                if config.split and file_records:
                    save_batch(file_records, f"{file_path.stem}_metadata.csv")
            except Exception:
                exceptions += 1
                logger.exception(
                    "An error occurred while processing %s.",
                    file_path.name,
                )
        
        if not config.split and all_records:
            save_batch(all_records, f"{config.batch_path.name}_metadata.csv")

        if issues or transformations:
            write_reports(
                config.log_dir,
                config.timestamp,
                'metadata',
                transformations,
                issues
            )

        msg = "MARC file processing complete!"

        logger.info(msg)
        print(msg)
    except Exception as e:
        exceptions += 1
        msg = "An unexpected error occurred during processing."
        logger.exception(msg)
        print(f"{msg}: {e}")
    
    return exceptions


def main() -> None:
    """
    Run the MARC-to-MODS processing workflow.
    
    Parses command-line arguments, validates required input, initializes 
    logging and output directories, and processes all MARC files in the input 
    directory. Displays completion or error messages depending on execution 
    outcome.
    """
    logger = None
    log_path = None
    config = parse_arguments()
    xslt_path = UTILITY_FILES_DIR / 'marc2mods.xsl'

    try:
        # Set up directories
        config.timestamp = datetime.now().strftime('%Y-%m-%d-%H%M%S')
        config.batch_path = create_directory(config.batch_path)
        config.output_dir = create_directory(config.batch_path / 'metadata')
        config.log_dir = create_directory(config.batch_path / 'logs')

        # Set up logger
        config.log_path = (
            config.log_dir
            / f"{config.timestamp}_metadata_sheet_processing.log"
        )
        log_path = config.log_path
        logger = setup_logger(LOGGER_NAME, config.log_path)

        # Load taxonomies
        load_taxonomies(
            refresh=config.refresh_taxonomies,
            logger=logger,
        )

        # Process MARC files
        transformer = MARCTransformer(xslt_path)
        manifest_df = load_manifest(config)

        exceptions = process_files(
            transformer,
            manifest_df,
            config,
        )
    
        if exceptions:
            print(
                f"{exceptions} exceptions occurred during processing. "
                f"See logs: {log_path}"
            )

    except Exception as e:
        if logger:
            logger.exception("A critical error occurred.")

        msg = (
            f"A critical error occurred: {e}"
        )
        if log_path:
            msg = f"{msg}\nSee logs: {log_path}"

        print(msg)


if __name__ == '__main__':
    main()
