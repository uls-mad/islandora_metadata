""" Modules """

# Import standard modules
import pandas as pd
from tkinter import filedialog


""" Functions """

def get_directory(io_type: str, title: str) -> str:
    """
    Prompt the user to select a directory of a given I/O type.

    Args:
        io_type (str): The I/O type of the directory (input or output)
        title: The title for the file dialog.

    Returns:
        str: Path to the selected directory.

    Raises:
        SystemExit: If the user does not select a directory.
    """
    dir = filedialog.askdirectory(title=title)
    if not dir:
        print(f"No directory {io_type} selected.")
        exit(0)
    return dir

def create_df(filepath: str):
    df = pd.read_csv(
        filepath, dtype=str, keep_default_na=False, na_filter=False
    )
    return df
