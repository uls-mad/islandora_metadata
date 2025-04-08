#!/bin/python3 

""" Modules """

# Import standard modules
import os
try:
    from tkinter import filedialog
    TK_AVAILABLE = True
except ImportError:
    TK_AVAILABLE = False

# Import third-party module
import pandas as pd


""" Functions """

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
        filepath, dtype=str, keep_default_na=False, na_filter=False
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
