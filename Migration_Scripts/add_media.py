#!/bin/python3 

""" Modules """

# Import standard modules
import os
import sys
import argparse
import traceback
from datetime import datetime
try:
    import tkinter as tk
    TK_AVAILABLE = True
except ImportError:
    TK_AVAILABLE = False

# Import third-party module
import pandas as pd

# Import local modules
from file_utils import get_directory, create_df, write_reports
from definitions import DATASTREAMS_MAPPING


""" Mapping """

EXPECTED_DSIDS_BY_MODEL = {
    'Image': ['JP2'],
    'Page' : ['JP2'],
    #'Audio': ['MP3_PROXY', 'PROXY_MP3']
    #'Digital Document': ['PDF'],
    #'Video': ['MKV', 'MP4','OGG'], 
}


""" Functions """

def parse_arguments():
    """
    Parse command-line arguments to retrieve the batch directory.

    Returns:
        tuple: batch_path (str | None)
    """
    parser = argparse.ArgumentParser(
        description="Process CSV files to add media filenames to metadata records."
    )
    parser.add_argument(
        "--batch_path",
        type=str,
        default=None,
        help="Path to a batch directory (default: will prompt if not provided)."
    )
    args = parser.parse_args()
    return args.batch_path


def add_exception(
    exceptions: list, 
    pid: str, 
    field: str, 
    exception: str, 
    current_file: str
) -> None:
    """
    Add an exception record to the exceptions list.

    Args:
        exceptions (list): List to store exception records.
        pid (str): The PID of the record.
        field (str): The datastream field where the exception occurred.
        exception (str): A description of the exception.
        current_file (str): Name of the current CSV file being processed.
    """
    exceptions.append({
        "File": current_file,
        "PID": pid,
        "Field": field,
        "Exception": exception
    })


def check_for_missing_media(
    df: pd.DataFrame,
    expected_dsids_by_model: dict,
    exceptions: list,
    current_file: str
) -> list:
    """
    Checks for rows expected to have media files (based on their model type)
    but that are missing the appropriate datastream IDs in the 'dile' column.

    Args:
        df (pd.DataFrame): DataFrame containing metadata, including 'field_model' and 'file'.
        datastreams (dict): Dictionary mapping datastream column names to lists of DSID strings.
        exceptions (list): List to store exception records.
        current_file (str): Name of the file currently being processed.

    Returns:
        list: The updated list of exception records.
    """
    # Check for required columns
    if 'field_model' not in df.columns or 'file' not in df.columns:
        exceptions.append({
            "File": current_file,
            "PID": None,
            "Field": "'field_model' or 'file'",
            "Exception": "Required field(s) missing"
        })
        return exceptions

    for model, expected_dsids in expected_dsids_by_model.items():
        # Filter rows that are of the specified model
        model_rows = df[df['field_model'] == model]

        for _, row in model_rows.iterrows():
            pid = str(row['id'])
            dsid = str(row['file']).strip()

            # Check if any expected DSID is present in the 'File' value
            if not dsid or not any(dsid in dsid for dsid in expected_dsids):
                exception = (
                    f"Missing expected media for {model} model: "
                    f"{', '.join(expected_dsids)}"
                )
                exceptions.append({
                    "File": current_file,
                    "PID": pid,
                    "Field": "file",
                    "Exception": exception
                })

    return exceptions


def add_media_files(
    media_files: list,
    df: pd.DataFrame, 
    datastreams: dict, 
    exceptions: list, 
    current_file: str
) -> tuple[pd.DataFrame, list]:
    """
    Adds filenames from `media_files` to DataFrame columns based on `datastreams` mapping.

    Args:
        media_files (list): List of media filenames.
        df (pd.DataFrame): Input DataFrame containing `id` and datastream columns.
        datastreams (dict): Dictionary mapping datastream columns to expected datastream identifiers.
        exceptions (list): List to store exception records.
        current_file (str): Name of the current CSV file being processed.

    Returns:
        tuple:
            pd.DataFrame: DataFrame with updated columns containing matching filenames.
            list: Updated list of exceptions for any unmatched media files.
    """
    for ds_field, dsid in datastreams.items():
        if ds_field in df.columns:
            # Filter DataFrame for records that should have media files
            df[ds_field] = df[ds_field].astype(str).str.strip().replace(
                {'': pd.NA}
            )
            filtered_df = df[df[ds_field].notna()]

            # Add media filenames to DataFrame
            filenames_map = {}

            for _, row in filtered_df.iterrows():
                pid = str(row['id'])
                matching_files = [
                    filename for filename in media_files
                    if pid.replace(':', '_') in filename and dsid in filename
                ]

                # Log that media files were expected but not found
                if not matching_files:
                    add_exception(
                        exceptions, 
                        pid, 
                        ds_field, 
                        "No file found", 
                        current_file
                    )

                # Convert the list of found media filenames to a string
                filenames_map[row.name] = '|'.join(matching_files) \
                    if matching_files else ''

            # Update DataFrame column with filenames
            df[ds_field] = df.index.map(filenames_map).fillna('')

    return df, exceptions


def process_csv_files(csv_dir: str, media_dir: str) -> list:
    """
    Processes all CSV files in the given directory, updating them with matching media filenames.

    Args:
        csv_dir (str): Directory containing CSV files.
        media_dir (str): Directory containing media files.

    Returns:
        list: A list of exceptions encountered during processing.
    """
    exceptions = []

    if not os.path.isdir(csv_dir):
        print(f"Error: Metadata folder not found at: {csv_dir}")
        return exceptions

    if not os.path.isdir(media_dir):
        print(f"Error: Media folder not found at: {media_dir}")
        return exceptions

    csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]
    media_files = [f for f in os.listdir(media_dir) if not f.endswith('.csv')]

    try:
        for filename in csv_files:
            # Set filepaths
            current_file = filename
            csv_path = os.path.join(csv_dir, filename)
            output_filename = filename.replace('.csv', '_media.csv')
            output_filepath = os.path.join(media_dir, output_filename)

            # Load CSV into DataFrame
            df = create_df(csv_path)

            # Check for objects missing expected media files
            exceptions = check_for_missing_media(
                df, 
                EXPECTED_DSIDS_BY_MODEL, 
                exceptions, 
                current_file
            )

            # Update records with media filenames
            updated_df, exceptions = add_media_files(
                media_files, 
                df, 
                DATASTREAMS_MAPPING, 
                exceptions, 
                current_file
            )

            # Save output CSV
            updated_df.to_csv(
                output_filepath, 
                index=False, 
                header=True, 
                encoding='utf-8'
            )

            # Log output file creation
            print("CSV file created: " + output_filepath.replace('\\', '/'))

    except Exception as e:
        print(f"Error during processing: {e}")
        print(traceback.format_exc())
        sys.exit(1)

    return exceptions


""" Driver Code """

if __name__ == "__main__":
    batch_path = parse_arguments()
    
    try:
        if TK_AVAILABLE:
            # Set up tkinter window for GUI
            root = tk.Tk()
            root.withdraw()
            input_prompt = 'Select Batch Folder with Input CSV Files'
        else:
            input_prompt = 'Enter Batch Folder with Input CSV Files'

        # Get directories and timestamp for file handling
        if batch_path is None:
            batch_path = get_directory('input', input_prompt, TK_AVAILABLE)
        csv_dir = os.path.join(batch_path, "metadata")
        media_dir = os.path.join(batch_path, "import")
        log_dir = os.path.join(batch_path, "logs")
        timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")

        # Process CSV files
        exceptions = process_csv_files(csv_dir, media_dir)

        # Report exceptions, if any
        write_reports(log_dir, timestamp, "media", [], exceptions)

    except Exception as e:
        print(f"Unexpected error: {e}")
        print(traceback.format_exc())
        sys.exit(1)

    finally:
        if TK_AVAILABLE:
            # Close GUI window
            root.destroy()

        sys.exit(0)
