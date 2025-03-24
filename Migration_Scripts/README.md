# Islandora 7 to Islandora 2.0 Migration Scripts

## Contents
- [Overview](#overview)
- [Files Found Here](#files-found-here)
- [How to Run the Application](#how-to-run-the-application)
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

- Requires: 
    - `definitions.py`
    - `file_utils.py`


## How to Run the Application
To begin the data migration workflow, create a batch directory and add in the Solr export CSV files for the collections in the batch. 

1. Run `solr_to_i2.py`.
2. Select the batch directory containing the batch CSV file(s) using the file dialog or enter the directory path in the terminal.
3. Monitor progress in the GUI and terminal.
4. Processed files will be saved to their corresponding subdirectories.
    - Processed metadata records are saved to an output CSV file with a timestamped filename in the `metadata` subdirectory
    - Logs of exceptions and transformations (if any) are saved to CSVs file with a timestamped filename in the `logs` subdirectory
    - PIDs for records with media datastreams (HOCR, JP2, TRANSCRIPT) are saved to TXT files in the `import` subdirectory

After exporting the relevant media datastreams to the `import` subdirectory.

5. Run `add_media.py`
6. Select the batch directory using the file dialog or enter the directory path in the terminal.
7. Monitor progress in the terminal.
8. Processed CSV files with media filenames will be saved to the `import` subdirectory. 

## Dependencies
- Python 3.x
- datetime, os, pathlib, re, shutil, sys, queue, threading, tkinter, traceback, typing
- numpy
- pandas
- edtf

## License
This project is available under the MIT License.

