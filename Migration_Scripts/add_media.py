#!/bin/python3 

""" Modules """

# Import standard modules
import os
import sys
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
from file_utils import *
from definitions import DATASTREAMS_MAPPING


""" Functions """

def add_exception(exceptions: list, pid: str, field: str, exception: str, current_file: str) -> None:
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


def add_media_files(
    media_files: list,
    df: pd.DataFrame, 
    datastreams: dict, 
    exceptions: list, 
    current_file: str
) -> pd.DataFrame:
    """
    Adds filenames from `media_files` to DataFrame columns based on `datastreams` mapping.

    Args:
        media_files (list): List of media filenames.
        df (pd.DataFrame): Input DataFrame containing `pid` and datastream columns.
        datastreams (dict): Dictionary mapping datastream columns to possible datastream identifiers.
        exceptions (list): List to store exception records.
        current_file (str): Name of the current CSV file being processed.

    Returns:
        pd.DataFrame: DataFrame with updated columns containing matching filenames.
    """
    for ds_field, dsids in datastreams.items():
        if ds_field in df.columns:
            # Filter DataFrame for records that should have media files
            df[ds_field] = df[ds_field].str.strip().replace({'': pd.NA})
            filtered_df = df[df[ds_field].notna()]

            # Add media filenames to DataFrame
            filenames_map = {}

            for _, row in filtered_df.iterrows():
                pid = str(row['id'])
                matching_files = [
                    filename for filename in media_files \
                        if pid.replace(':', '_') in filename \
                        and any(dsid in filename for dsid in dsids)
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

    return df


def process_csv_files(csv_dir: str, media_dir: str):
    """
    Processes all CSV files in the given directory, updating them with matching media filenames.

    Args:
        csv_dir (str): Directory containing CSV files.
        media_dir (str): Directory containing media files.

    Raises:
        SystemExit: If an error occurs during processing.
    """
    csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]
    media_files = [f for f in os.listdir(media_dir) if not f.endswith('.csv')]
    exceptions = []

    try:
        for filename in csv_files:
            # Set filepaths
            current_file = filename
            csv_path = os.path.join(csv_dir, filename)
            output_filename = filename.replace('.csv', '_media.csv')
            output_filepath = os.path.join(media_dir, output_filename)

            # Load CSV into DataFrame
            df = create_df(csv_path)

            # Update records with media filenames
            updated_df = add_media_files(
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
            print("CSV file processed successfully: " + 
                  output_filepath.replace('\\', '/')
            )

    except Exception as e:
        print(f"Error during processing: {e}")
        print(traceback.format_exc())
        sys.exit(1)

    return exceptions


""" Driver Code """

if __name__ == "__main__":
    # Save initial working directory
    initial_cwd = os.getcwd()

    try:
        # Change working directory to script's directory
        os.chdir(os.path.dirname(os.path.abspath(__file__)))

        if TK_AVAILABLE:
            # Set up tkinter window for GUI
            root = tk.Tk()
            root.withdraw()
            title = 'Select Batch Folder with Input CSV Files'
        else:
            title = 'Enter Batch Folder with Input CSV Files'

        # Get directories and timestamp for file handling
        batch_dir = get_directory(
            'input', title, TK_AVAILABLE
        )

        # Use batch_dir to construct subdirectories
        csv_dir = os.path.join(batch_dir, "metadata")
        media_dir = os.path.join(batch_dir, "import")
        log_dir = os.path.join(batch_dir, "logs")

        # Set timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")

        # Process CSV files
        exceptions = process_csv_files(csv_dir, media_dir)

        # Report exceptions, if any
        write_reports(log_dir, timestamp, [], exceptions)

    except Exception as e:
        print(f"Unexpected error: {e}")
        print(traceback.format_exc())
        sys.exit(1)

    finally:
        # Change working directory back to initial working directory
        os.chdir(initial_cwd)

        if TK_AVAILABLE:
            # Close GUI window
            root.destroy()

        sys.exit(0)
