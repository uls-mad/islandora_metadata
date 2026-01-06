# Archival Ingestion & Taxonomy Management Tools

## Contents
- [Overview](#overview)
- [Files Found Here](#files-found-here)
    - [`i7_to_i2_template.py`](#i7_to_i2_templatepy)
    - [`make_metadata_sheet.py`](#make_metadata_sheetpy)
    - [`make_ingest_sheet.py`](#make_ingest_sheetpy)
    - [`setup_taxonomy_project.py`](#setup_taxonomy_projectpy)
    - [`setup_taxonomy_ingest.py`](#setup_taxonomy_ingestpy)
    - [`batch_manager.py`](#batch_managerpy)
    - [`utilities.py`](#utilitiespy)
    - [`definitions.py`](#definitionspy)
- [Dependencies](#dependencies)
- [License](#license)

---

## Overview  
This repository provides a specialized toolkit for the ingestion of digitized archival materials into Islandora 2.0 at the University of Pittsburgh. 

## Files Found Here

### `i7_to_i2_template.py`
This script uses a CSV crosswalk (`Utility_Files/i7_to_i2_metadata_template_mapping.csv`) to convert legacy (Islandora 7) metadata template fields into the Islandora 2.0 schema based on the content type(s) of the batch.

#### Key features:
- **Content Type Filtering**: Supports specific mapping logic for types (e.g., AV, images, books, manuscripts, notated music, serials). Multiple types can be passed at once (e.g., `image photograph`).
- **Controlled Vocabulary Mapping**: Uses processors to convert human-readable labels for `copyright_status`, `language`, and `type_of_resource` into standardized codes defined in `definitions.py`.
- **Hybrid Interface**: Automatically detects if `tkinter` is available to provide a GUI folder picker; otherwise, it falls back to terminal-based prompts.

### `make_metadata_sheet.py`
This script connects to the Google Sheets API to merge manifests (filenames and node IDs) with descriptive metadata.

#### Key components:
- **ID-Based Merge**: Performs a left-join between the manifest `id` and metadata `identifier`.
- **Direct Append Mode**: If no identifiers are found in the metadata, it automatically switches to a direct column-wise append.
- **Logging**: Generates an `unmatched.csv` log for metadata rows that do not have a corresponding file in the manifest.

### `make_ingest_sheet.py`
This script merges the manifest with transformed metadata and performs rigorous validation to ensure the data is ready for Workbench ingest.

#### Key components:
- **Multi-threaded Processing**: Uses a background worker thread to process records, allowing the main thread to provide real-time progress updates and handle user cancellations.
- **Strict Validation**: Enforces controlled vocabularies and EDTF date standards, valid formatting for coordinates, and data type compliance. It also ensures children in Compound Objects or Paged Content inherit the correct `domain_access` from their parents.
- **Create vs. Update**: Supports both new ingests (`create`) and metadata-only revisions (`update`).

### `setup_taxonomy_project.py`
This script extacts unrecognized terms in taxonomy-controlled fields (flagged by `make_ingest_sheet.py`) and initializes a spreadsheet for reviewing, identifying, and reconciling terms that can extend taxonomies. 

#### Key components:
- **Deduplication Logic:** Aggregates identical errors across records.
- **Project Initiation:** Generates a template with `field`, `value`, `exception`, `count`, `term_name` and `uri` columns, designed for tracking and vocabulary reconciliation in OpenRefine.
- **Workflow Isolation:** Stores outputs in a `/remediation` sub-folder to prevent cluttering the primary ingest metadata.

#### `setup_taxonomy_ingest.py`
This script imports a completed taxonomy remediation project (CSV) (once the `term_name` and `uri` columns have been filled) and generates taxonomy CSVs and configuration files for Workbench ingests.

#### Key components:
- **URI Resolution**: Determines authority sources (AAT, LCSH, NAF, VIAF) based on URI patterns.
- **Config Generation**: Automatically populates YAML templates with batch paths and environment credentials.

### `batch_manager.py`
Handles batch processing for digital object workflows by setting up directories and managing PID tracking for media imports.  

#### Key components:
This script automates the creation of the local workspace required for a successful Workbench ingest.

Key Components:
- **Standardized Batch Hierarchy**: Automatically generates a uniform set of subdirectories (including /configs, /import, /logs, and /media). 
- **Dynamic YAML Configuration**: Customizes configuration file templates by injecting batch-specific metadata, such as unique batch prefixes, user IDs, and local file paths.

### `utilities.py`
Provides cross-script helper functions for file handling and data standardization.

#### Key components:
- **Google Workspace Integration**: Functions for authenticated connection to the Google Sheets and Drive APIs, enabling automated metadata harvesting from remote sheets.
- **Lossless Data Ingestion**: The create_df function, which enforces string-only data loading for CSV and Excel files. This prevents common archival data corruption, such as the loss of leading zeros in identifiers or the unintended conversion of date strings.
- **Adaptive User Interface**: Logic to detect the execution environment (via tkinter) and provide either a graphical folder-selection dialog or a standard CLI prompt, ensuring the scripts work both on local workstations and remote servers.
- **Standardized Reporting & Logging**: Tools that manage background activity logs and generate standardized reports (e.g., transformation and exception logs), ensuring metadata modifications and flags are documented and easy to review.
- **Interactive Input Handling**: Robust prompting logic with built-in validation to ensure required script arguments are collected correctly during manual execution.

### `definitions.py`
This script serves as the central configuration hub for the entire toolkit, containing global constants, field lists, and processor mappings.

#### Key Components:
- **Field Lists**: Defines categories of metadata fields (e.g., `REQUIRED_FIELDS`, `TITLE_FIELDS`) that require specialized handling during data processing.
- **Mappings**: Provides lookups that translate legacy terminology into modern standards.
- **External Reference Loading**: Dynamically imports and processes external CSV files. This allows schema and taxonomies to be updated outside of Python code.

## Dependencies
- Python 3.10+
- pandas: For data transformation and analysis.
- pathlib: For OS-agnostic path management.
- python-dotenv: For managing sensitive import credentials.
- edtf: For extended date-time format validation.

## License
This project is available under the MIT License.
