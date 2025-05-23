#!/bin/python3 

""" Modules """

# Import standard modules
import os

# Import third-party module
import pandas as pd
from dotenv import load_dotenv

# Load environment variable
load_dotenv()
import_password = os.getenv("IMPORT_PASSWORD")


""" Constant """

BATCH_SUBDIRS = [
    "configs",
    "export",
    "import",
    "import/media",
    "logs",
    "metadata",
    "rollback",
    "tmp",
]


""" Functions """

def setup_batch_directory(
    batch_path: str
):
    """
    Sets up the batch directory by creating necessary subdirectories and copying the config.yml file.

    Args:
        batch_path (str): Path to the batch directory.
    """
    # Ensure batch directory exists
    os.makedirs(batch_path, exist_ok=True)

    # Create subdirectories
    for subdir in BATCH_SUBDIRS:
        os.makedirs(os.path.join(batch_path, subdir), exist_ok=True)


def save_pids_for_media(
    batch_dir: str, 
    df: pd.DataFrame, 
    datastreams_map: dict
) -> set:
    """
    Saves PIDs from the DataFrame to a TXT file for each datastream field in the `import` subdirectory,
    ensuring that duplicate PIDs are not written across multiple runs.

    Args:
        batch_dir (str): Directory containing the batch; PIDs will be saved in `batch_dir/import/`.
        df (pd.DataFrame): DataFrame containing the 'id' column and datastream fields.
        datastreams_map (dict): Dictionary mapping datastream columns to possible identifiers.

    Behavior:
        - Filters records where the datastream field is not empty.
        - Extracts PIDs from the 'id' column.
        - Saves the PIDs to a TXT file named `{dsid}_pids.txt` in `batch_dir/import/`.
        - Ensures that duplicate PIDs are not added if the script is run multiple times.
        - Creates the `import` directory if it does not exist.
    
    Returns:
    set: A set of datastream identifiers (dsid) for which new PIDs were added.
    """
    import_dir = os.path.join(batch_dir, "import")
    os.makedirs(import_dir, exist_ok=True) 

    # Write a PIDS file
    pids = set(df['id'].dropna().unique())

    # Define the output file path
    txt_filename = f"PIDs.txt"
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
            for pid in sorted(new_pids):
                f.write(f"{pid}\n")
        
        # Report on added PIDS in output, if any
        print(f"Added {len(new_pids)} new PIDs to {txt_filepath}")
    else:
        print(f"No new PIDs to add. File already up to date."
        )

    # Get datastreams for batch
    datastreams = set()

    for ds_field, dsid in datastreams_map.items():
        if ds_field in df.columns:
            # Filter DataFrame for records that should have media files
            df[ds_field] = df[ds_field].astype(str).str.strip().replace({'': pd.NA})
            filtered_df = df[df[ds_field].notna()]
            
            # Get all PIDs from the 'id' column
            pids = set(filtered_df['id'].dropna().unique())
            
            # Add datastream for output
            if pids:
                datastreams.add(dsid)

    return datastreams


def prepare_config(
    batch_path: str, 
    batch_dir: str, 
    batch_count: int,
    timestamp: str, 
    user_id: str,
    datastreams: str
):
    """
    Read and customize the import config file for a specific batch.

    Args:
        batch_path (str): Full path to the root of the batch folder.
        batch_dir (str): Name of the batch directory (used in config substitutions).
        batch_count (int): The current batch number. 
        timestamp (str): Timestamp string used in filenames.
        user_id (str): User ID to insert into the config.
        datastreams (set): A set of datastream identifiers (dsid) for the given batch.

    Returns:
        str: Path to the customized config file, or None if the default config is missing.
    """
    # Define source and destination config paths
    default_config = "default_create_config.yml"
    config_src = os.path.join("Utility_Files", default_config)
    config_filename = f"import_{batch_dir}_{batch_count}.yml"
    config_dest = os.path.join(batch_path, "configs", config_filename)

    # Ensure configs directory exists
    os.makedirs(os.path.dirname(config_dest), exist_ok=True)

    # Read and update placeholders from default config
    if not os.path.exists(config_src):
        print("default_create_config.yml not found in Utility_Files.")
        return None

    with open(config_src, "r", encoding="utf-8") as f:
        content = f.read()

    # Replace placeholders
    import_batch = f"{batch_dir}_{timestamp}_{batch_count}"
    output_batch = f"{batch_dir}_{batch_count}"
    content = content.replace("[IMPORT_PASSWORD]", import_password)
    content = content.replace("[BATCH_DIRECTORY]", batch_dir)
    content = content.replace("[IMPORT_BATCH]", import_batch)
    content = content.replace("[OUTPUT_BATCH]", output_batch)
    content = content.replace("[USER_ID]", user_id)

    # Uncomment additional files for datastreams in batch
    if any(key in datastreams for key in ('HOCR', 'OCR', 'TRANSCRIPT')):
        content = content.replace("#additional_files", "additional_files")

    for dsid, comment in {
        'HOCR': "# - hocr",
        'OCR': "# - extracted_text",
        'TRANSCRIPT': "# - transcript"
    }.items():
        if dsid in datastreams:
            content = content.replace(comment, comment[2:])

    # Write the customized config to the destination
    with open(config_dest, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"\nConfig prepared: {config_dest}")
    return config_dest


def write_io_scripts(batch_path: str, batch_dir: str, datastreams: set) -> str | None:
    """
    Read a I/O workflow script templates, replace placeholders with batch-specific values,
    and write a new script file that includes only the datastreams relevant to the current batch.

    Args:
        batch_path (str): Full path to the root of the batch folder.
        batch_dir (str): Name of the batch directory (used to replace placeholders).
        datastreams (set): Set of datastream identifiers to include in the output script.

    Returns:
        str | None: Path to the customized I/O workflow script file, or None if the template is missing.
    """
    io_scripts = "io_scripts.txt"
    io_scripts_src = os.path.join("Utility_Files", io_scripts)
    output_batch = f"{batch_dir}_1"

    if not os.path.exists(io_scripts_src):
        print("Template file io_scripts.txt not found in Utility_Files.")
        return None

    # Read in I/O workflow script template
    with open(io_scripts_src, "r", encoding="utf-8") as f:
        content = f.read()

    # Replace placeholders
    content = content.replace("[BATCH_DIRECTORY]", batch_dir)
    content = content.replace("[OUTPUT_BATCH]", output_batch)

    # Split into lines and re-add newline after each
    lines = [line + '\n\n' for line in content.splitlines()]

    # Prepare output directory and path
    import_dir = os.path.join(batch_path, "import")
    os.makedirs(import_dir, exist_ok=True)
    txt_filepath = os.path.join(import_dir, io_scripts)

    # Write relevant drush scripts and RELS-EXT Update script to TXT file
    with open(txt_filepath, "w", encoding="utf-8") as f:
        for line in lines:
            dsid, sep, rest = line.partition(": ")
            if sep and dsid not in datastreams:
                continue
            f.write(line)

    print(f"\nI/O workflow script(s) written to: {txt_filepath}")
    return txt_filepath
