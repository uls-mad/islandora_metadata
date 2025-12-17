# Import standard modules
import os
import logging
import pandas as pd
from typing import Optional

# Import third-party modules
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

try:
    from tkinter import filedialog
    TK_AVAILABLE = True
except ImportError:
    TK_AVAILABLE = False

# Setup the log file format
LOG_FORMATTER = logging.Formatter(
    fmt='%(asctime)s.%(msecs)03d %(levelname)s %(message)s',
    datefmt="%Y%m%d %H:%M:%S"
)


""" Functions """

def prompt_for_input(
        prompt_text: str, 
        valid_choices: Optional[list[str]] = None
) -> str:
    """Handles the interactive prompting loop for arguments."""
    while True:
        user_input = input(prompt_text).strip()
        if user_input:
            if valid_choices and user_input not in valid_choices:
                print(f"Invalid input. Must be one of: {', '.join(valid_choices)}.")
            else:
                return user_input
        print("Input cannot be empty. Please try again.")


def setup_logger(name: str, log_file: str,
                 level: int = logging.DEBUG) -> logging.Logger:
    """
    Configure a logger that writes to a log file.

    Args:
        name (str): Logger name.
        log_file (str): Path to log file.
        level (int): Logging level.

    Returns:
        logging.Logger: Configured logger instance.
    """
    handler = logging.FileHandler(log_file)
    handler.setFormatter(LOG_FORMATTER)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger


def get_directory(io_type: str, title: str, tk_available: bool) -> str:
    """
    Prompt the user to select a directory, either via a GUI dialog or manual input.

    Args:
        io_type (str): The type of directory being selected (e.g., "input" or "output").
        title (str): The prompt or title for the file dialog or manual input request.
        tk_available (bool): Indicates whether tkinter is available for GUI selection.

    Returns:
        str: The path to the selected directory.

    Raises:
        SystemExit: If no directory is selected.
    """
    if tk_available:
        dir = filedialog.askdirectory(title=title)
    else:
        dir = input(f"{title}: ")
    if not dir:
        print(f"No {io_type} directory selected.")
        exit(0)
    return dir


def create_df(filepath: str) -> pd.DataFrame:
    """
    Reads a CSV file into a Pandas DataFrame with all values treated as strings.

    Args:
        filepath (str): The path to the CSV file.

    Returns:
        pd.DataFrame: A DataFrame containing the CSV data with all values as strings,
                      with empty values retained as empty strings (no NaN).
    """
    df = pd.read_csv(
        filepath, 
        dtype=str, 
        encoding='utf-8',
        keep_default_na=False, 
        na_filter=False, 
    )
    return df


def write_reports(
    output_dir: str,
    timestamp: str,
    label: str | None,
    transformations: list,
    exceptions: list
):
    """
    Writes two CSV reports: one for transformations and one for exceptions.

    Args:
        output_dir (str): Path to the folder where reports will be saved.
        timestamp (str): Timestamp to include in the filenames of the reports.
        label (str | None): An additional label to describe file.
        transformations (list): List of transformations made during processing.
        exceptions (list): List of exceptions encountered during processing.

    Returns:
        None
    """
    label = f"_{label}" if label else label
    
    # Save exceptions to DataFrame
    if transformations:
        transformations_df = pd.DataFrame.from_dict(transformations)
    
        # Write DataFrame to CSV
        transformations_filepath = os.path.join(
            output_dir,
            f'{timestamp}{label}_transformations.csv'
        )
        transformations_df.to_csv(
            transformations_filepath,
            index=False,
            encoding='utf-8'
        )
        notification_symbol = "↩️ " if TK_AVAILABLE else "[*]"
        print(
            f"\n{notification_symbol} {len(transformations)} transformation"
            f"{' was' if len(transformations) == 1 else 's were'} made. "
            "See logs for details."
        )

    # Save exceptions to DataFrame
    if exceptions:
        exceptions_df = pd.DataFrame.from_dict(exceptions)

        # Write DataFrame to CSV
        exceptions_filepath = os.path.join(
            output_dir,
            f'{timestamp}{label}_exceptions.csv'
        )
        exceptions_df.to_csv(
            exceptions_filepath,
            index=False,
            encoding='utf-8'
        )
        warning_symbol = "⚠️ " if TK_AVAILABLE else "[!]"
        print(
            f"\n{warning_symbol} {len(exceptions)} exceptions were encountered. "
            "See logs for details."
        )
        
    else:
        success_symbol = "✅" if TK_AVAILABLE else "✓"
        print(f"\n{success_symbol} No exceptions were encountered.")


def connect_to_google_sheet(credentials_file: str,
                            logger: logging.Logger | None = None):
    """
    Connect to the Google Sheets API using a service account.

    Args:
        credentials_file (str): Path to the service account JSON credentials file.
        logger (logging.Logger, optional): Logger for writing error messages.
            If None, no logging is performed.

    Returns:
        googleapiclient.discovery.Resource: A Google Sheets API service object.

    Raises:
        FileNotFoundError: If the credentials file does not exist.
        Exception: If the service cannot be created for any other reason.
    """
    if not os.path.exists(credentials_file):
        msg = f"Configuration file not found: {credentials_file}"
        if logger:
            logger.error(msg)
        raise FileNotFoundError(msg)

    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']

    try:
        creds = service_account.Credentials.from_service_account_file(
            credentials_file,
            scopes=scopes
        )
        return build('sheets', 'v4', credentials=creds)
    except Exception as e:
        msg = f"Failed to create Google Sheets service: {str(e)}"
        if logger:
            logger.exception(msg)
        raise Exception(msg) from e
    

def get_google_sheet_filename(sheet_id: str,
                              credentials_file: str = "credentials.json",
                              logger: logging.Logger | None = None) -> str:
    """
    Retrieves the title (filename) of a Google Sheet given its ID.

    Args:
        sheet_id (str): ID of the Google Sheet.
        credentials_file (str): Path to service account JSON credentials.
        logger (logging.Logger, optional): Logger for error/info messages.

    Returns:
        str: The title (filename) of the Google Sheet.

    Raises:
        ValueError: If the sheet ID is invalid or the file cannot be accessed.
        Exception: For unexpected errors during the fetch.
    """
    # 1. Connect to the Google Sheets API service using the existing function.
    service = connect_to_google_sheet(credentials_file, logger=logger)

    try:
        # 2. Call the spreadsheets().get() method to retrieve the sheet's metadata.
        # This is the same method used to auto-detect the sheet_name in read_google_sheet.
        meta = service.spreadsheets().get(
            spreadsheetId=sheet_id,
            # Request only the 'properties' field to keep the API response small
            fields='properties.title' 
        ).execute()

        # 3. Extract and return the title (filename) from the metadata.
        if 'properties' in meta and 'title' in meta['properties']:
            filename = meta['properties']['title']
            if logger:
                logger.info(f"Successfully retrieved filename for Sheet ID {sheet_id}: '{filename}'")
            return filename
        else:
            # Should not happen if the request succeeds, but good for safety.
            msg = f"Missing title in metadata for Google Sheet ID: {sheet_id}"
            if logger:
                logger.error(msg)
            raise Exception(msg)

    except HttpError as e:
        msg = f"Failed to retrieve metadata for Google Sheet ID {sheet_id}. Check ID and service account permissions."
        if logger:
            logger.error(msg)
            logger.exception(e)
        raise ValueError(msg) from e
    except Exception as e:
        msg = f"Unexpected error while fetching filename for Google Sheet ID {sheet_id}"
        if logger:
            logger.exception(msg)
        raise Exception(msg) from e


def read_google_sheet(sheet_id: str,
                      sheet_name: str = None,
                      credentials_file: str = "credentials.json",
                      logger: logging.Logger | None = None
                      ) -> pd.DataFrame:
    """
    Read the first (or given) tab of a Google Sheet into a DataFrame.

    Args:
        sheet_id (str): ID of the Google Sheet.
        sheet_name (str, optional): Tab name. If None, first tab is used.
        credentials_file (str): Path to service account JSON credentials.
        logger (logging.Logger, optional): Logger for error/info messages.

    Returns:
        pd.DataFrame: Data from the sheet with padded rows and empty rows removed.

    Raises:
        ValueError: If the given sheet/tab name does not exist.
        Exception: For unexpected errors during the fetch.
    """
    service = connect_to_google_sheet(credentials_file, logger=logger)

    try:
        # Auto-detect first sheet if none given
        if not sheet_name:
            meta = service.spreadsheets().get(
                spreadsheetId=sheet_id
            ).execute()
            sheet_name = meta['sheets'][0]['properties']['title']

        # Fetch data from the sheet/tab
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=sheet_name
        ).execute()

    except HttpError as e:
        msg = f"Failed to fetch '{sheet_name}' tab from Google Sheet {sheet_id}"
        if logger:
            logger.error(msg)
            logger.exception(e)
        raise ValueError(msg) from e
    except Exception as e:
        msg = f"Unexpected error while reading Google Sheet {sheet_id}"
        if logger:
            logger.exception(msg)
        raise

    values = result.get('values', [])
    if not values:
        return pd.DataFrame()

    headers = values[0]  # first row = headers
    rows = values[1:] if len(values) > 1 else []

    # Pad each row to match number of headers
    max_cols = len(headers)
    padded_rows = [row + [None] * (max_cols - len(row)) for row in rows]

    # Drop rows that are completely empty
    filtered_rows = [
        r for r in padded_rows
        if any(cell not in (None, "", " ", "nan", "NaN") for cell in r)
    ]

    return pd.DataFrame(filtered_rows, columns=headers)


def read_google_sheets_in_folder(
    folder_id: str,
    credentials_file: str = "credentials.json",
    logger: logging.Logger | None = None
) -> list[pd.DataFrame]:
    """
    Read all Google Sheets in a given Google Drive folder and return a list
    of DataFrames (one per file, first tab only).

    Args:
        folder_id (str): ID of the Google Drive folder.
        credentials_file (str): Path to service account JSON credentials.
        logger (logging.Logger, optional): Logger for error/info messages.

    Returns:
        list[pd.DataFrame]: A list of DataFrames, one per Google Sheet file.

    Raises:
        Exception: If the folder cannot be accessed or files cannot be read.
    """
    # Authenticate with Drive
    scopes = [
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/spreadsheets.readonly"
    ]
    creds = service_account.Credentials.from_service_account_file(
        credentials_file, scopes=scopes
    )
    drive_service = build("drive", "v3", credentials=creds)

    try:
        # Find all Sheets files in the folder
        query = (
            f"'{folder_id}' in parents "
            "and mimeType='application/vnd.google-apps.spreadsheet' "
            "and trashed=false"
        )
        results = drive_service.files().list(
            q=query,
            fields="files(id, name)"
        ).execute()

        files = results.get("files", [])
        if not files:
            msg = f"No Google Sheets found in folder {folder_id}"
            if logger:
                logger.warning(msg)
            return []

        dataframes: list[pd.DataFrame] = []
        for file in files:
            if logger:
                logger.info("Reading Google Sheet: %s (%s)", file["name"], file["id"])
            try:
                df = read_google_sheet(
                    file["id"],
                    credentials_file=credentials_file,
                    logger=logger
                )
                df.attrs["sheet_name"] = file["name"]  # preserve filename if helpful
                dataframes.append(df)
            except Exception as e:
                msg = f"Failed to read Google Sheet {file['name']} ({file['id']})"
                if logger:
                    logger.error(msg)
                    logger.exception(e)
                raise

        return dataframes

    except HttpError as e:
        msg = f"Drive API error while accessing folder {folder_id}"
        if logger:
            logger.error(msg)
            logger.exception(e)
        raise
    except Exception as e:
        msg = f"Unexpected error while listing sheets in folder {folder_id}"
        if logger:
            logger.exception(msg)
        raise


def get_first_tab_info(sheet_id: str,
                       credentials_file: str = "credentials.json",
                       logger: logging.Logger | None = None
                       ) -> dict[str, str | int]:
    """
    Get the GID and title of the first tab in a Google Sheet.

    Args:
        sheet_id (str): ID of the Google Sheet.
        credentials_file (str): Path to service account JSON credentials.
        logger (logging.Logger, optional): Logger for error/info messages.

    Returns:
        dict: A dictionary with 'gid' (int) and 'name' (str) of the first tab.

    Raises:
        ValueError: If the sheet has no tabs or if the fetch fails.
        Exception: For unexpected errors.
    """
    service = connect_to_google_sheet(credentials_file, logger=logger)

    try:
        # Fetch spreadsheet metadata, which includes sheet/tab properties
        meta = service.spreadsheets().get(
            spreadsheetId=sheet_id,
            fields='sheets.properties'
        ).execute()
        
        # The 'sheets' list contains objects, one for each tab.
        sheets = meta.get('sheets', [])
        if not sheets:
            msg = f"Google Sheet {sheet_id} contains no tabs."
            if logger: logger.error(msg)
            raise ValueError(msg)

        # The first tab is at index 0
        first_sheet_properties = sheets[0]['properties']
        
        # Get first sheet's GID (sheetId) and title
        sheet_gid = first_sheet_properties.get('sheetId')
        sheet_name = first_sheet_properties.get('title')

        if sheet_gid is None or sheet_name is None:
             msg = f"Could not retrieve GID or name for the first tab of Sheet {sheet_id}."
             if logger: logger.error(msg)
             raise ValueError(msg)

        return {
            "gid": sheet_gid,
            "name": sheet_name
        }

    except HttpError as e:
        msg = f"Failed to fetch metadata for Google Sheet {sheet_id}"
        if logger: logger.error(msg); logger.exception(e)
        raise ValueError(msg) from e
    except Exception as e:
        msg = f"Unexpected error while getting first tab info for Google Sheet {sheet_id}"
        if logger: logger.exception(msg)
        raise
