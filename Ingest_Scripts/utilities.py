#!/usr/bin/env python3

"""Shared utility functions for metadata processing scripts."""

# ---------------------------------------------------------------------------
# Modules
# ---------------------------------------------------------------------------

# Standard library imports
import csv
import logging
import re
import traceback
from pathlib import Path
from typing import Any

# Third-party imports
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Set the log file format
LOG_FORMATTER = logging.Formatter(
    fmt='%(asctime)s.%(msecs)03d %(levelname)s %(message)s',
    datefmt='%Y%m%d %H:%M:%S'
)

# Status symbols
GREEN = '\033[92m'
YELLOW = '\033[33m'
RED = '\033[91m'
CYAN = '\033[96m'
RESET = '\033[0m'

SUCCESS_SYMBOL = f"{GREEN}[✓]{RESET}"
WARNING_SYMBOL = f"{YELLOW}[!]{RESET}"
ERROR_SYMBOL = f"{RED}[X]{RESET}"
TRANSFORM_SYMBOL = f"{CYAN}[*]{RESET}"

# Status symbols
DRUPAL_EXTENDED_EDTF_PATTERN = re.compile(
    r'^'
    r'\d{1,3}X{1,3}'
    r'/'
    r'(?:'
    r'\d{4}[?~%]?'
    r'|'
    r'\d{1,3}X{1,3}[?~%]?'
    r'|'
    r'\.\.'
    r')?'
    r'$'
)


# ---------------------------------------------------------------------------
# Classes
# ---------------------------------------------------------------------------

class LogRegistry:
    """Registry of primary module logger names."""

    I7_TO_I2_TEMPLATE = 'i7_to_i2_template'
    MAKE_INGEST_SHEET = 'make_ingest_sheet'
    MAKE_MARC_METADATA_SHEET = 'make_marc_metadata_sheet'
    MAKE_METADATA_SHEET = 'make_metadata_sheet'
    MERGE_BATCHES = 'merge_batches'
    SETUP_TAXONOMY_INGEST = 'setup_taxonomy_ingest'
    SETUP_TAXONOMY_PROJECT = 'setup_taxonomy_project'


# ---------------------------------------------------------------------------
# Funtions
# ---------------------------------------------------------------------------

# --- Prompt and GUI Helpers ---

def prompt_for_input(
    prompt_text: str,
    valid_choices: list[str] | None = None
) -> str:
    """Prompt for non-empty user input.

    Args:
        prompt_text: Text to display when requesting input.
        valid_choices: Optional list of allowed responses.

    Returns:
        User-provided input string.

    Raises:
        KeyboardInterrupt: If the user interrupts input.
    """
    while True:
        user_input = input(prompt_text).strip()

        if not user_input:
            print("Input cannot be empty. Please try again.")
            continue

        if valid_choices and user_input not in valid_choices:
            print(
                "Invalid input. Must be one of: "
                f"{', '.join(valid_choices)}."
            )
            continue

        return user_input


# --- Logging Helpers ---

def setup_logger(
    name: str,
    log_file: str | Path,
    level: int = logging.DEBUG
) -> logging.Logger:
    """Configure a logger that writes to a file.

    Args:
        name: Logger name.
        log_file: Path to the log file.
        level: Logging level.

    Returns:
        Configured logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        logger.handlers.clear()

    handler = logging.FileHandler(log_file, encoding='utf-8')
    handler.setFormatter(LOG_FORMATTER)
    logger.addHandler(handler)

    return logger


# --- Filesystem Helpers ---

def create_directory(directory_path: str | Path) -> Path:
    """Create a directory and any missing parent directories.

    Args:
        directory_path: Directory path to create.

    Returns:
        Created directory path.

    Raises:
        PermissionError: If permissions are insufficient.
        OSError: If another filesystem error occurs.
    """
    path = Path(directory_path)

    try:
        path.mkdir(parents=True, exist_ok=True)
        return path
    except PermissionError as error:
        message = f"Permission denied: Cannot create directory at {path}."
        logger = logging.getLogger(__name__)

        if logger.hasHandlers() or logging.getLogger().hasHandlers():
            logger.exception(message)
        else:
            print(f"ERROR: {message}")
            traceback.print_exc()

        raise PermissionError(
            f"Insufficient permissions to create: {path}"
        ) from error
    except OSError:
        logging.exception(
            "OS error occurred while creating directory %s.",
            path
        )
        raise


# --- Text Helpers ---

def remove_whitespaces(text: str, allow_newlines: bool = False) -> str:
    """Normalize string spacing by collapsing and stripping arbitrary whitespaces.
    
    Acts as a backwards-compatible drop-in replacement for legacy text cleaning functions.

    Args:
        text: Raw input string.
        allow_newlines: Whether to preserve and clean up paragraph breaks.
            If False, explicit line breaks (\n, \r) are flattened into standard spaces.

    Returns:
        Cleaned text, or an empty string for non-string input.
    """
    # Bypass non-string inputs
    if not isinstance(text, str):
        return ''

    if allow_newlines:
        # Collapse internal spaces and tabs, but leave newlines
        cleaned = re.sub(r'[ \t]+', ' ', text)
        # Collapse multiple line breaks into two max
        cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
    else:
        # Collapse carriage returns into one space
        cleaned = text.replace('\r', ' ')
        # Collapse all consecutive whitespace into one space
        cleaned = re.sub(r'\s+', ' ', cleaned)

    return cleaned.strip()


def normalize_for_join(series: pd.Series) -> pd.Series:
    """Normalize identifier values for DataFrame joins.

    Args:
        series: Series containing identifier values.

    Returns:
        Normalized Series with null-like values masked.
    """
    normalized = series.astype(str).str.strip()
    empty_values = {'', 'nan', 'none', 'null', 'n/a', 'na'}

    return normalized.mask(normalized.str.lower().isin(empty_values))


def cap_first(text: str) -> str:
    """Capitalize the first character without changing the rest.

    Args:
        text: Text to capitalize.

    Returns:
        Text with the first character capitalized, or an empty string for
        invalid input.
    """
    if not text or not isinstance(text, str):
        return ''

    return text[0].upper() + text[1:]


# --- CSV and DataFrame Helpers ---

def create_df(filepath: str | Path) -> pd.DataFrame:
    """Read a CSV or Excel file into a DataFrame as strings.

    Args:
        filepath: Path to a CSV or Excel file.

    Returns:
        DataFrame with values read as strings and empty values preserved.

    Raises:
        ValueError: If the file format is unsupported.
    """
    filepath = Path(filepath)
    ext = filepath.suffix.lower()

        # Load CSV data

    if ext == '.csv':
        return pd.read_csv(
            filepath,
            dtype=str,
            encoding='utf-8',
            keep_default_na=False,
            na_filter=False
        )

    if ext in {'.xlsx', '.xls'}:
        return pd.read_excel(
            filepath,
            dtype=str,
            keep_default_na=False,
            na_filter=False
        )

    raise ValueError(f"Unsupported file format: {ext}")


def df_to_csv(df: pd.DataFrame, filepath: Path | str) -> None:
    """Write a DataFrame to CSV using standard project settings.

    Args:
        df: DataFrame to write.
        filepath: Output CSV filepath.
    """
    df.to_csv(
        filepath,
        index=False,
        encoding='utf-8'
    )


def csv_to_dict(
    filepath: str | Path,
    key_col: str | int = 0,
    has_header: bool = True,
    delimiter: str = ','
) -> dict:
    """Load a CSV into a simple or nested dictionary.

    For two-column CSVs, returns a simple dictionary mapping one column to the
    other. For CSVs with more than two columns and headers, returns a nested
    dictionary where the selected key column maps to the remaining column data.

    Args:
        filepath: Path to the CSV file.
        key_col: Column name or index to use as dictionary key.
        has_header: Whether the CSV has a header row.
        delimiter: CSV delimiter.

    Returns:
        Dictionary representation of the CSV.
    """
    filepath = Path(filepath)

    with filepath.open(newline='', encoding='utf-8-sig') as csv_file:
        if not has_header:
            reader = csv.reader(csv_file, delimiter=delimiter)

            return {
                row[0].strip(): row[1].strip()
                for row in reader
                if len(row) >= 2 and row[0].strip()
            }

        reader = csv.DictReader(csv_file, delimiter=delimiter)
        fieldnames = reader.fieldnames or []

        if not fieldnames:
            return {}

        if isinstance(key_col, int):
            key_col = fieldnames[key_col]

        other_cols = [col for col in fieldnames if col != key_col]

        if len(fieldnames) == 2:
            value_col = other_cols[0]

            return {
                row[key_col].strip(): row[value_col].strip()
                for row in reader
                if row.get(key_col)
            }

        return {
            row[key_col].strip(): {
                col: row[col].strip() if row.get(col) else ''
                for col in other_cols
            }
            for row in reader
            if row.get(key_col)
        }


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



# --- Reporting Helper ---

def write_reports(
    output_dir: Path,
    timestamp: str,
    label: str | None,
    transformations: list,
    exceptions: list
) -> None:
    """Write transformation and exception reports to CSV files.

    Args:
        output_dir: Directory where report files will be saved.
        timestamp: Timestamp to include in report filenames.
        label: Optional label to append to report filenames.
        transformations: Transformation records.
        exceptions: Exception records.
    """
    def grammarize(rows: list, unit: str) -> str:
        """Return a count-aware phrase such as "1 transformation was"."""
        count = len(rows)
        verb = "was" if count == 1 else "were"
        unit = unit if count == 1 else f"{unit}s"

        return f"{count} {unit} {verb}"

    label_part = f'_{label}' if label else ''
    file_prefix = f'{timestamp}{label_part}'

    if transformations:
        transformations_filepath = (
            output_dir / f'{file_prefix}_transformations.csv'
        )

        df_to_csv(
            pd.DataFrame.from_dict(transformations),
            transformations_filepath
        )

        print(
            f"\n{TRANSFORM_SYMBOL} "
            f"{grammarize(transformations, 'transformation')} made. "
            f"See logs: {transformations_filepath}"
        )

    if exceptions:
        exceptions_filepath = output_dir / f'{file_prefix}_exceptions.csv'

        df_to_csv(
            pd.DataFrame.from_dict(exceptions),
            exceptions_filepath
        )

        print(
            f"\n{WARNING_SYMBOL} "
            f"{grammarize(exceptions, 'metadata exception')} encountered. "
            f"See logs: {exceptions_filepath}"
        )
    else:
        print(
            f"\n{SUCCESS_SYMBOL} "
            "No metadata exceptions were encountered."
        )


# --- Google API Helpers ---

def connect_to_google_sheet(
    credentials_file: str | Path,
    logger: logging.Logger | None = None,
    readonly: bool = True,
) -> Any:
    """Connect to the Google Sheets API using a service account.

    Args:
        credentials_file: Path to the service account JSON credentials file.
        logger: Optional logger for error messages.
        readonly: Whether to request read-only Google Sheets access.

    Returns:
        Google Sheets API service object.

    Raises:
        FileNotFoundError: If the credentials file does not exist.
        RuntimeError: If the service cannot be created.
    """
    credentials_path = Path(credentials_file)

    if not credentials_path.exists():
        msg = f"Configuration file not found: {credentials_path.resolve()}"
        if logger:
            logger.error(msg)
        raise FileNotFoundError(msg)

    scope = (
        'https://www.googleapis.com/auth/spreadsheets.readonly'
        if readonly
        else 'https://www.googleapis.com/auth/spreadsheets'
    )
    scopes = [scope]

    try:
        creds = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=scopes
        )
        return build('sheets', 'v4', credentials=creds)
    except Exception as error:
        msg = "Failed to create Google Sheets service."
        if logger:
            logger.exception(msg)
        raise RuntimeError(msg) from error


def read_google_sheet(
    sheet_id: str,
    sheet_name: str | None = None,
    credentials_file: str | Path = 'credentials.json',
    logger: logging.Logger | None = None
) -> pd.DataFrame:
    """Read a Google Sheet tab into a DataFrame.

    Args:
        sheet_id: Google Sheet ID.
        sheet_name: Optional tab name. If omitted, the first tab is used.
        credentials_file: Path to service account JSON credentials.
        logger: Optional logger.

    Returns:
        DataFrame containing sheet data.

    Raises:
        ValueError: If the sheet cannot be accessed or tab is missing.
    """
    service = connect_to_google_sheet(credentials_file, logger=logger)

    if not sheet_name or str(sheet_name).strip() == 'None':
        try:
            meta = service.spreadsheets().get(
                spreadsheetId=sheet_id
            ).execute()
            sheet_name = meta['sheets'][0]['properties']['title']
        except HttpError as error:
            msg = (
                "Access denied or invalid ID: Could not reach Google Sheet "
                f"{sheet_id}. Check service account permissions."
            )
            if logger:
                logger.exception(msg)
            raise ValueError(msg) from error

    try:
        # Fetch data from specified tab
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=sheet_name
        ).execute()
    except HttpError as error:
        msg = (
            f"Tab not found: Failed to fetch '{sheet_name}' "
            f"from Google Sheet {sheet_id}."
        )
        if logger:
            logger.exception(msg)
        raise ValueError(msg) from error
    except Exception:
        msg = f"Unexpected error while reading Google Sheet {sheet_id}."
        if logger:
            logger.exception(msg)
        raise

    # Process result into a DataFrame
    values = result.get('values', [])

    if not values:
        return pd.DataFrame()

    # Pad each row to match number of headers
    headers = values[0]
    rows = values[1:] if len(values) > 1 else []
    max_cols = len(headers)

    padded_rows = [
        row + [None] * (max_cols - len(row))
        for row in rows
    ]

    # Drop rows that are completely empty or contain common null-strings
    filtered_rows = [
        row for row in padded_rows
        if any(
            cell not in (None, '', ' ', 'nan', 'NaN')
            for cell in row
        )
    ]

    return pd.DataFrame(filtered_rows, columns=headers)


def read_google_sheets_in_folder(
    folder_id: str,
    credentials_file: str | Path = 'credentials.json',
    logger: logging.Logger | None = None
) -> list[pd.DataFrame]:
    """Read all Google Sheets in a Drive folder.

    Args:
        folder_id: Google Drive folder ID.
        credentials_file: Path to service account JSON credentials.
        logger: Optional logger.

    Returns:
        List of DataFrames, one per Google Sheet.

    Raises:
        HttpError: If Drive access fails.
        Exception: If a sheet cannot be read.
    """
    scopes = [
        'https://www.googleapis.com/auth/drive.readonly',
        'https://www.googleapis.com/auth/spreadsheets.readonly',
    ]

    creds = service_account.Credentials.from_service_account_file(
        credentials_file,
        scopes=scopes
    )
    drive_service = build('drive', 'v3', credentials=creds)

    try:
        # Find all Sheets files in the folder
        query = (
            f"'{folder_id}' in parents "
            "and mimeType='application/vnd.google-apps.spreadsheet' "
            'and trashed=false'
        )
        results = drive_service.files().list(
            q=query,
            fields='files(id, name)'
        ).execute()

        files = results.get('files', [])

        if not files:
            if logger:
                logger.warning("No Google Sheets found in folder %s", folder_id)
            return []

        dataframes = []

        for file in files:
            if logger:
                logger.info(
                    "Reading Google Sheet: %s (%s)",
                    file['name'],
                    file['id']
                )

            try:
                df = read_google_sheet(
                    file['id'],
                    credentials_file=credentials_file,
                    logger=logger
                )
                df.attrs['sheet_name'] = file['name']
                dataframes.append(df)
            except Exception:
                if logger:
                    logger.exception(
                        "Failed to read Google Sheet %s (%s)",
                        file['name'],
                        file['id']
                    )
                raise

        return dataframes

    except HttpError:
        if logger:
            logger.exception(
                "Drive API error while accessing folder %s",
                folder_id
            )
        raise
    except Exception:
        if logger:
            logger.exception(
                "Unexpected error while listing sheets in folder %s",
                folder_id
            )
        raise


def get_google_sheet_filename(
    sheet_id: str,
    credentials_file: str | Path = 'credentials.json',
    logger: logging.Logger | None = None
) -> str:
    """Retrieve the title of a Google Sheet.

    Args:
        sheet_id: Google Sheet ID.
        credentials_file: Path to service account JSON credentials.
        logger: Optional logger.

    Returns:
        Google Sheet title.

    Raises:
        ValueError: If the sheet cannot be accessed.
        AttributeError: If the title is missing from metadata.
        RuntimeError: If an unexpected error occurs.
    """
    service = connect_to_google_sheet(credentials_file, logger=logger)

    try:
        # Get the sheet's metadata
        meta = service.spreadsheets().get(
            spreadsheetId=sheet_id,
            fields='properties.title'
        ).execute()

        # Retrieve the title (filename) from the metadata
        if 'properties' in meta and 'title' in meta['properties']:
            filename = meta['properties']['title']

            if logger:
                logger.info(
                    "Successfully retrieved filename for Sheet ID %s: %s",
                    sheet_id,
                    filename
                )

            return filename

        msg = f"Missing title in metadata for Google Sheet ID: {sheet_id}"
        if logger:
            logger.error(msg)
        raise AttributeError(msg)

    except HttpError as error:
        msg = (
            f"Failed to retrieve metadata for Google Sheet ID {sheet_id}. "
            "Check ID and service account permissions."
        )
        if logger:
            logger.exception(msg)
        raise ValueError(msg) from error
    except Exception as error:
        msg = (
            "Unexpected error while fetching filename for Google Sheet ID "
            f"{sheet_id}."
        )
        if logger:
            logger.exception(msg)
        raise RuntimeError(msg) from error


def get_google_sheet_titles_by_gid(
    sheet_id: str,
    credentials_file: str | Path = 'credentials.json',
    logger: logging.Logger | None = None,
) -> dict[int, str]:
    """Return worksheet titles keyed by numeric GID.

    Args:
        sheet_id: Google spreadsheet ID.
        credentials_file: Path to service account JSON credentials.
        logger: Optional logger.

    Returns:
        Mapping of worksheet GIDs to worksheet titles.
    """
    service = connect_to_google_sheet(
        credentials_file,
        logger=logger,
    )

    try:
        response = service.spreadsheets().get(
            spreadsheetId=sheet_id,
            fields='sheets.properties(sheetId,title)',
        ).execute()
    except HttpError as error:
        msg = (
            f"Failed to retrieve tab metadata for Google Sheet {sheet_id}."
        )
        if logger:
            logger.exception(msg)
        raise ValueError(msg) from error

    return {
        int(sheet['properties']['sheetId']): sheet['properties']['title']
        for sheet in response.get('sheets', [])
    }


def get_first_tab_info(
    sheet_id: str,
    credentials_file: str | Path = 'credentials.json',
    logger: logging.Logger | None = None
) -> dict[str, str | int]:
    """Get the GID and title of the first tab in a Google Sheet.

    Args:
        sheet_id: Google Sheet ID.
        credentials_file: Path to service account JSON credentials.
        logger: Optional logger.

    Returns:
        Dictionary with ``gid`` and ``name`` values.

    Raises:
        ValueError: If the sheet has no tabs or metadata cannot be fetched.
    """
    service = connect_to_google_sheet(credentials_file, logger=logger)

    try:
       # Fetch spreadsheet metadata
        meta = service.spreadsheets().get(
            spreadsheetId=sheet_id,
            fields='sheets.properties'
        ).execute()

        # Get metadata for each sheet/tab
        sheets = meta.get('sheets', [])

        if not sheets:
            msg = f"Google Sheet {sheet_id} contains no tabs."
            if logger:
                logger.error(msg)
            raise ValueError(msg)

        # Get metadata for first sheet/tab
        first_sheet_properties = sheets[0]['properties']
        sheet_gid = first_sheet_properties.get('sheetId')
        sheet_name = first_sheet_properties.get('title')

        if sheet_gid is None or sheet_name is None:
            msg = (
                "Could not retrieve GID or name for the first tab of "
                f"Sheet {sheet_id}."
            )
            if logger:
                logger.error(msg)
            raise ValueError(msg)

        return {
            'gid': sheet_gid,
            'name': sheet_name,
        }

    except HttpError as error:
        msg = f"Failed to fetch metadata for Google Sheet {sheet_id}"
        if logger:
            logger.exception(msg)
        raise ValueError(msg) from error
    except Exception:
        msg = (
            "Unexpected error while getting first tab info for Google Sheet "
            f"{sheet_id}"
        )
        if logger:
            logger.exception(msg)
        raise


def clear_google_sheet_ranges(
    sheet_id: str,
    ranges: list[str],
    credentials_file: str | Path = 'credentials.json',
    logger: logging.Logger | None = None,
) -> None:
    """Clear values from one or more Google Sheet ranges.

    Formatting and data validation are preserved.

    Args:
        sheet_id: Google spreadsheet ID.
        ranges: A1 notation ranges to clear.
        credentials_file: Path to service account JSON credentials.
        logger: Optional logger.
    """
    if not ranges:
        return

    service = connect_to_google_sheet(
        credentials_file,
        logger=logger,
        readonly=False,
    )

    try:
        service.spreadsheets().values().batchClear(
            spreadsheetId=sheet_id,
            body={'ranges': ranges},
        ).execute()
    except HttpError as error:
        msg = f"Failed to clear ranges in Google Sheet {sheet_id}."
        if logger:
            logger.exception(msg)
        raise ValueError(msg) from error


def update_google_sheet_ranges(
    sheet_id: str,
    data: list[dict[str, Any]],
    credentials_file: str | Path = 'credentials.json',
    logger: logging.Logger | None = None,
    value_input_option: str = 'RAW',
) -> None:
    """Write values to one or more Google Sheet ranges.

    Args:
        sheet_id: Google spreadsheet ID.
        data: Batch update entries containing ranges and values.
        credentials_file: Path to service account JSON credentials.
        logger: Optional logger.
        value_input_option: Google Sheets value input mode.
    """
    if not data:
        return

    service = connect_to_google_sheet(
        credentials_file,
        logger=logger,
        readonly=False,
    )

    try:
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=sheet_id,
            body={
                'valueInputOption': value_input_option,
                'data': data,
            },
        ).execute()
    except HttpError as error:
        msg = f"Failed to update ranges in Google Sheet {sheet_id}."
        if logger:
            logger.exception(msg)
        raise ValueError(msg) from error
