#!/bin/python3 

""" Modules """

# Import standard modules
import os
import shutil
from pathlib import Path
import pandas as pd

""" Constant """

BATCH_SUBDIRS = [
    "configs",
    "export",
    "import",
    "logs",
    "metadata",
    "rollback",
    "tmp",
]


""" Functions """

def setup_batch_directory(batch_dir: str):
    """
    Sets up the batch directory by creating necessary subdirectories and copying the config.yml file.

    Args:
        batch_dir (str): Path to the batch directory.
    """
    batch_path = Path(batch_dir)

    # Ensure batch directory exists
    batch_path.mkdir(parents=True, exist_ok=True)

    # Create subdirectories
    for subdir in BATCH_SUBDIRS:
        (batch_path / subdir).mkdir(exist_ok=True)

    # Copy config.yml into the configs directory
    config_filename = "default_create_config.yml"
    config_src = Path("Utility_Files") / config_filename
    config_dest = batch_path / "configs" / config_filename

    if config_src.exists():
        shutil.copy(config_src, config_dest)
    else:
        print("config.yml not found in script directory.")


def save_pids_for_media(
    batch_dir: str, 
    df: pd.DataFrame, 
    datastreams: dict
) -> None:
    """
    Saves PIDs from the DataFrame to a TXT file for each datastream field in the `import` subdirectory,
    ensuring that duplicate PIDs are not written across multiple runs.

    Args:
        batch_dir (str): Directory containing the batch; PIDs will be saved in `batch_dir/import/`.
        df (pd.DataFrame): DataFrame containing the 'id' column and datastream fields.
        datastreams (dict): Dictionary mapping datastream columns to possible identifiers.

    Behavior:
        - Filters records where the datastream field is not empty.
        - Extracts PIDs from the 'id' column.
        - Saves the PIDs to a TXT file named `{dsid[0]}_pids.txt` in `batch_dir/import/`.
        - Ensures that duplicate PIDs are not added if the script is run multiple times.
        - Creates the `import` directory if it does not exist.
    """
    import_dir = os.path.join(batch_dir, "import")
    os.makedirs(import_dir, exist_ok=True) 

    for ds_field, dsids in datastreams.items():
        if ds_field in df.columns:
            # Filter DataFrame for records that should have media files
            df[ds_field] = df[ds_field].astype(str).str.strip().replace({'': pd.NA})
            filtered_df = df[df[ds_field].notna()]
            
            # Get all PIDs from the 'id' column
            pids = set(filtered_df['id'].dropna().unique())
            
            if pids:
                # Define the output file path
                txt_filename = f"{dsids[0]}_pids.txt"
                txt_filepath = os.path.join(import_dir, txt_filename)
                
                # Read existing PIDs from the file (if it exists)
                existing_pids = set()
                if os.path.exists(txt_filepath):
                    with open(txt_filepath, "r", encoding="utf-8") as f:
                        existing_pids = {line.strip() for line in f}

                # Determine new PIDs to write
                new_pids = pids - existing_pids

                if new_pids:
                    # Append only new PIDs to the file
                    with open(txt_filepath, "a", encoding="utf-8") as f:
                        for pid in sorted(new_pids):  # Sorting for consistency
                            f.write(f"{pid}\n")

                    print(f"Added {len(new_pids)} new PIDs to {txt_filepath}")
                else:
                    print(f"No new PIDs to add for {dsids[0]}. " 
                          + "File already up to date."
                        )
