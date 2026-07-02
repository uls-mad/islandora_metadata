#!/bin/python3

"""Batch Directory and Configuration Setup Tool.

This module provides utility functions to initialize directory structures
for batch operations and customize YAML configuration files based on
environment variables and runtime parameters.
"""

# --- Modules ---

# Standard library imports
import os
from pathlib import Path
from typing import Optional

# Third-party imports
from dotenv import load_dotenv

# Local imports
from definitions import UTILITY_FILES_DIR


# --- Functions ---

def setup_batch_directory(batch_path: Path) -> None:
    """Set up the batch directory structure.

    Creates the root batch directory and all required processing
    subdirectories if they do not already exist.

    Args:
        batch_path: Path to the batch directory.
    """
    # Create subdirectories
    batch_subdirs = [
        'configs',
        'export',
        'import',
        'logs',
        'metadata',
        'rollback',
        'tmp',
    ]

    for subdir in batch_subdirs:
        (batch_path / subdir).mkdir(parents=True, exist_ok=True)


def prepare_config(
    batch_prefix: str,
    batch_file: str,
    batch_path: Path,
    batch_dir: str,
    user_id: str,
    ingest_task: str,
    media_files: list[str]
) -> Optional[str]:
    """Read and customize the import config file for a specific batch.

    Args:
        batch_prefix: Pattern used for naming output batch files.
        batch_file: CSV filename for output batch file.
        batch_path: Full path to the root of the batch folder.
        batch_dir: Name of the batch directory (used in config substitutions).
        user_id: User ID to insert into the config.
        ingest_task: Ingest task to insert into the config ('create' or
            'update').
        media_files: I2 fields for additional media files.

    Returns:
        Path to the customized config file, or None if the default config is
        missing.
    """
    # Load environment variable right where it's needed
    load_dotenv()
    import_password = os.getenv('IMPORT_PASSWORD', '')

    # Define source and destination config paths
    default_config = 'default_create_config.yml'
    config_src = UTILITY_FILES_DIR / default_config
    config_filename = f'{batch_prefix}.yml'
    config_dest = batch_path / 'configs' / config_filename

    # Ensure configs directory exists
    config_dest.parent.mkdir(parents=True, exist_ok=True)

    # Read and update placeholders from default config
    if not config_src.exists():
        print('default_create_config.yml not found in Utility_Files.')
        return None

    with config_src.open('r', encoding='utf-8') as f:
        content = f.read()

    # Replace placeholders
    content = content.replace('[IMPORT_PASSWORD]', import_password)
    content = content.replace('[BATCH_DIRECTORY]', batch_dir)
    content = content.replace('[BATCH_PREFIX]', batch_prefix)
    content = content.replace('[BATCH_FILE]', batch_file)
    content = content.replace('[USER_ID]', user_id)
    content = content.replace('[INGEST_TASK]', ingest_task)

    # Uncomment additional media file types in batch
    if media_files:
        content = content.replace('#additional_files', 'additional_files')

    for field, comment in {
        'transcript': '# - transcript'  # Can extend list as needed
    }.items():
        if field in media_files:
            content = content.replace(comment, comment[2:])

    # Write the customized config to the destination
    with config_dest.open('w', encoding='utf-8') as f:
        f.write(content)

    print(f"\nConfig file saved: {config_dest.as_posix()}")
    return config_dest
