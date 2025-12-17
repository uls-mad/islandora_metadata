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


def prepare_config(
    batch_prefix: str,
    batch_path: str, 
    batch_dir: str, 
    user_id: str,
    media_files: list[str]
):
    """
    Read and customize the import config file for a specific batch.

    Args:
        batch_prefix (str): 
        batch_path (str): Full path to the root of the batch folder.
        batch_dir (str): Name of the batch directory (used in config substitutions).
        batch_count (int): The current batch number. 
        timestamp (str): Timestamp string used in filenames.
        user_id (str): User ID to insert into the config.
        media_files (list): I2 fields for additional media files.

    Returns:
        str: Path to the customized config file, or None if the default config is missing.
    """
    # Define source and destination config paths
    default_config = "default_create_config.yml"
    config_src = os.path.join("Utility_Files", default_config)
    config_filename = f"{batch_prefix}.yml"
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
    content = content.replace("[IMPORT_PASSWORD]", import_password)
    content = content.replace("[BATCH_DIRECTORY]", batch_dir)
    content = content.replace("[BATCH_PREFIX]", batch_prefix)
    content = content.replace("[USER_ID]", user_id)

    # Uncomment additional media file types in batch
    if media_files:
        content = content.replace("#additional_files", "additional_files")

    for field, comment in {
        'transcript': "# - transcript" # Can extend list as needed
    }.items():
        if field in media_files:
            content = content.replace(comment, comment[2:])

    # Write the customized config to the destination
    with open(config_dest, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"\nConfig prepared: {config_dest}")
    return config_dest
