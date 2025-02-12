# Islandora 7 to Islandora 2.0 Migration Scripts

## Overview  
This repository contains scripts and mapping files for extracting and transforming metadata between Islandora 7 (via CSV files exported from Solr) and Islandora 2 (I2). The project includes tools for processing, remediating, and validating metadata to ensure accurate metadata migration.  

This project processes CSV files containing metadata records, transforms and validates them, and saves the processed records in a structured format. The processing includes mapping fields, handling special cases, and ensuring data integrity. The application includes a GUI for selecting input and output directories and tracking the progress of the extraction and transformation process.

## Files Found Here

### `solr_to_i2.py`
This script is the main processing module. It reads CSV files containing metadata records from Solr exports, transforms them into the prescribed format, and saves the processed data in output CSV files. Key functionalities include:
- **`process_files`**: Iterates over all CSV files in the given input directory and processes them.
- **`process_records`**: Processes individual CSV files, maps fields, cleans values, and validates records.
- **GUI Integration**: Uses `tkinter` to allow users to select input and output directories.
- **Multithreading**: Runs the processing in a separate thread to keep the GUI responsive.

- Requires: `definitions.py`, `progress_tracker.py`, `inventory_manager.py`

### `definitions.py`
Contains global constants, inclduding special field lists and data mappings. Key components include:
- **`create_df(filepath)`**: Reads a CSV file into a pandas DataFrame with standardized settings. Used for importing CSV files with metadata scehmas and mappings.
- **Field Lists**: Defines categories of metadata fields (e.g., `REQUIRED_FIELDS`, `TITLE_FIELDS`) that require processing.
- **Mappings**: Maps fields and values from the input CSV to standardized field names and values.

- Requires: CSV files located in the Remediation_Mappings and Schema_Mappings directories, as listed in the "Imported Mappings" section.

### `progress_tracker.py`
Manages progress updates for the GUI and logs processing status. Key component:
- **`ProgressTracker` class**: Tracks total and processed files and records, and allows users to cancel the process.

### `inventory_manager.py`  
Manages the inventory and batching of processed digital objects, ensuring deduplication of collection data. Key components:
- **`load_inventories()`**: Loads or initializes the object inventory from a CSV file.  
- **`order_files(files)`**: Reorders a list of filenames, ensuring files in `COLLECTIONS_TO_HOLD` are processed last.  
- **`process_parent_id(value)`**: Cleans and standardizes multi-valued metadata fields by deduplicating, sorting, and removing Fedora prefixes.  
- **`handle_record(file, record)`**: Processes a single metadata record, updating or inserting it into the inventory while handling page-based objects correctly.  
- **`save_inventories()`**: Saves the updated object inventory to a CSV file for persistence.  
- **Field Lists**: Defines inventory field structures (`BATCH_INVENTORY_FIELDS`, `OBJECT_INVENTORY_FIELDS`).  
- **Collection Management**: Specifies special collections that should be held for later processing (`COLLECTIONS_TO_HOLD`).  
- **Object Models**: Identifies Fedora object types that represent pages (`PAGE_MODELS`).  

## How to Run the Application
1. Run `solr_to_i2.py`.
2. Select the input directory containing CSV files.
3. Select the output directory where processed files will be saved.
4. Monitor progress in the GUI.
5. Processed files will be saved with a timestamped filename.

## Dependencies
- Python 3.x
- numpy
- pandas
- edtf

## License
This project is available under the MIT License.

