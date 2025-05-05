# Islandora 7 to Islandora 2.0 Migration Scripts

## Contents
- [Overview](#overview)
- [Files Found Here](#files-found-here)
    - [`solr_to_i2.py`](#solr_to_i2py)
    - [`batch_manager.py`](#batch_managerpy)
    - [`definitions.py`](#definitionspy)
    - [`inventory_manager.py`](#inventory_managerpy)
    - [`progress_tracker.py`](#progress_trackerpy)
    - [`file_utils.py`](#file_utilspy)
    - [`add_media.py`](#add_mediapy)
    - [`clean_up_batch.py`](#clean_up_batchpy)
- [How to Run the Scripts](#how-to-run-the-scripts)   
    - [Running `solr_to_i2.py`](#running-solr_to_i2py)
    - [Running `add_media.py`](#running-add_mediapy)
    - [Running `clean_up_batch.py`](#running-clean_up_batchpy)
- [Dependencies](#dependencies)
- [License](#license)


## Overview  
This repository contains scripts and mapping files for extracting and transforming metadata between Islandora 7 (via CSV files exported from Solr) and Islandora 2 (I2). The project includes tools for processing, remediating, and validating metadata to ensure accurate metadata migration.  

This project processes CSV files containing metadata records, transforms and validates them, and saves the processed records in a structured format. The processing includes mapping fields, handling special cases, and ensuring data integrity. The application includes a GUI for selecting input and output directories and tracking the progress of the extraction and transformation process.

## Files Found Here

### `solr_to_i2.py`
This script is the main processing module for transforming Solr metadata records for ingest in I2. It reads CSV files containing metadata records from Solr exports, transforms them into the prescribed format, and saves the processed metadata in output CSV files.

#### Key components:
- **`process_files()`**: Iterates over all CSV files in the given input directory and processes them.
- **`process_records()`**: Processes individual CSV files, maps fields, cleans values, and validates records.
- **GUI Integration**: Uses `tkinter`, if available in the environemnt, to allow users to select input and output directories.
- **Multithreading**: Runs the processing in a separate thread to keep the GUI responsive.

- Supports:
  - `--user_id`: Specify your Pitt user ID for tracking config changes
  - `--batch_path`: Path to the batch directory containing Solr export CSVs
  - `--batch_size`: Number of records to include in each output file
  - Interactive dialog via `tkinter` or input via terminal if flags are provided

- Requires: 
    - `batch_manager.py`
    - `definitions.py`
    - `inventory_manager.py`
    - `progress_tracker.py`
    - `file_utils.py`

### `batch_manager.py`
Handles batch processing for digital object workflows by setting up directories and managing PID tracking for media imports.  

#### Key components:
- **`setup_batch_directory()`**: 
    - Creates a batch directory and its required subdirectories (configs, export, import, logs, metadata, rollback, tmp).
    - Copies default_create_config.yml from Utility_Files into the configs directory.
- **`save_pids_for_media()`**: 
    - Extracts PIDs from the id column of a DataFrame based on datastream presence.
    - Saves PIDs to a TXT file in the import directory, ensuring duplicates are not added across multiple runs.
    - Supports multiple datastream fields with configurable mappings.
- **`BATCH_SUBDIRS`**: Defines required subdirectories in batch directory to organize batch processing files.

### `definitions.py`
Contains global constants, inclduding special field lists and data mappings. 

#### Key components:
- **`create_df()`**: Reads a CSV file into a pandas DataFrame with standardized settings. Used for importing CSV files with metadata scehmas and mappings.
- **Field Lists**: Defines categories of metadata fields (e.g., `REQUIRED_FIELDS`, `TITLE_FIELDS`) that require processing.
- **Mappings**: Maps fields and values from the input CSV to standardized field names and values.

- Requires: 
   - `file_utils.py`
   - CSV files located in the [Remediation_Mappings](Remediation_Mappings/) and [Schema_Mappings](Schema_Mappings/) directories, as listed in the "Imported Mappings" section.

### `inventory_manager.py`  
Manages the inventory and batching of processed digital objects, ensuring deduplication of collection data.

#### Key components:
- **`load_inventories()`**: Loads or initializes the object inventory from a CSV file.  
- **`order_files()`**: Reorders a list of filenames, ensuring files in `COLLECTIONS_TO_HOLD` are processed last.  
- **`process_parent_id()`**: Cleans and standardizes multi-valued metadata fields by deduplicating, sorting, and removing Fedora prefixes.  
- **`handle_record()`**: Processes a single metadata record, updating or inserting it into the inventory while handling page-based objects correctly.  
- **`save_inventories()`**: Saves the updated object inventory to a CSV file for persistence.  
- **Field Lists**: Defines inventory field structures (`BATCH_INVENTORY_FIELDS`, `OBJECT_INVENTORY_FIELDS`).  
- **Collection Management**: Specifies special collections that should be held for later processing (`COLLECTIONS_TO_HOLD`).  
- **Object Models**: Identifies Fedora object types that represent pages (`PAGE_MODELS`).  

### `progress_tracker.py`
Manages progress updates for the GUI and logs processing status.

Key component:
- **`ProgressTracker` class**: Tracks total and processed files and records, and allows users to cancel the process.

### `file_utils.py`
Provides utility functions for file handling, directory selection, and report generation in data processing workflows.

#### Key components:
- **`get_directory()`**:
    - Opens a file dialog (if tkinter is available in the environment) or displays an input message in the terminal  to prompt the user to select an input or output directory.
    - Exits the script if no directory is selected.
- **`create_df()`**:
    - Reads a CSV file into a Pandas DataFrame, treating all values as strings.Ensures that empty values remain as empty strings rather than NaN.
- **`write_reports()`**:
    - Writes transformation and exception logs to timestamped CSV reports.
    - Saves reports in the specified output directory.
    - Ensures no report is generated if no exceptions occurred.

### `add_media.py`
Processes metadata CSV files by associating records with corresponding media filenames based on predefined datastream mappings.

#### Key components:
- **`add_exception()`**: Logs missing or mismatched media file exceptions.
- **`add_media_files()`**: Matches media filenames to records in the DataFrame.
- **`process_csv_files()`**: Processes all CSV files in the input directory, updating them with media filenames.

- Supports:
  - `--batch_path`: Path to the batch directory with processed metadata files
  - Interactive directory selection via `tkinter` or input via terminal if no flag is provided


- Requires: 
    - `definitions.py`
    - `file_utils.py`

### `clean_up_batch.py`  
Automates the final cleanup of a batch directory by deleting media files, zipping the remaining contents, optionally moving the archive to a `Done` directory (at the same level as the batch directory), and deleting the original batch directory.

#### Key components:
- **`prompt_to_delete()`**: Prompts user to review and confirm deletion of media files.
- **`zip_batch_directory()`**: Compresses the batch directory into a `.zip` archive.
- **`move_zip_to_done()`**: Moves the archive to a `Done` folder if it exists one level above.
- **`delete_batch_directory()`**: Deletes the entire batch directory and its contents.

- Supports:
  - `--batch_path` flag to run headlessly or via terminal input
  - Interactive directory selection via `tkinter` or input via terminal if no flag is provided

- Requires:  
  - `file_utils.py`


## How to Run the Scripts

To begin the data migration workflow, create a batch directory and place the Solr export CSV files for the collections you want to process in the directory.

### Running `solr_to_i2.py`

You can run the script in **two ways**:

#### Option 1: Use Command-Line Flags

You can provide the following flags when running the script:

- `--user_id` — your Pitt user ID (required if not using prompt)
- `--batch_path` — the path to the batch directory containing input CSVs
- `--batch_size` — the number of records per output metadata file (optional, defaults to 10000)

**Example:**
```bash
python3 solr_to_i2.py --user_id jdoe25 --batch_path "/path/to/batch" --batch_size 20000
```

#### Option 2: Run Without Flags

If you don't provide flags, the script will prompt you to enter your user ID and select or input a batch directory. It will also use the default batch size unless otherwise specified.

#### Output

Monitor progress in the GUI (if available) or in the terminal. Once processing completes, output files will be saved to subdirectories inside the batch folder:

- **`metadata/`** — processed metadata records in timestamped CSV files  
- **`logs/`** — transformation and exception logs in timestamped CSV files  
- **`import/`** — TXT files listing PIDs with media datastreams (e.g., HOCR, JP2, TRANSCRIPT)

### Running `add_media.py`

This script is ran after exporting the relevant media datastreams to the `import/media` subdirectory.   

You can run the script in two ways.

#### Option 1: With `--batch_path` flag
Provide the path to the batch directory as a command-line argument:
```bash
python3 add_media.py --batch_path "/path/to/batch"
```
#### Option 2: Without flag
If no flag is provided, the script will prompt you to select the batch directory using a file dialog or by entering the path in the terminal.

#### Output

Monitor progress in the terminal. Once processing completes, output files will be saved to subdirectories inside the batch folder:

- **`import/`** — processed CSV file(s) with filenames for media files in batch
- **`logs/`** — exception loga in a timestamped CSV file

### Running `clean_up_batch.py`

After the batch has been ingested, this script is ran to clean up and archive the batch directory.   

You can run the script in two ways.

#### Option 1: With `--batch_path` flag
Provide the path to the batch directory as a command-line argument:
```bash
python3 clean_up_batch.py --batch_path "/path/to/batch"
```
#### Option 2: Without flag
If no flag is provided, the script will prompt you to select the batch directory using a file dialog or by entering the path in the terminal.

#### Output

The script will:
   - Display a summary of media files found
   - Prompt for confirmation before deleting media
   - ZIP the remaining contents of the batch directory
   - Move the ZIP file to the `Done/` directory (if present)
   - Delete the original batch directory

## Dependencies
- Python 3.x
- datetime, os, pathlib, re, shutil, sys, queue, threading, tkinter, traceback, typing
- numpy
- pandas
- edtf

## License
This project is available under the MIT License.

