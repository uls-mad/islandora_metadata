# Islandora 2 Ingest Workflow Scripts

This project is a Python-based toolkit for preparing, processing, validating, and ingesting metadata into Islandora 2 using Islandora Workbench. The scripts automates the workflow from source metadata to Workbench ingest spreadsheets. 

Although developed for the University of Pittsburgh Library System, the overall workflow is applicable to many Islandora implementations.

---

## Overview

The scripts support a robust metadata preparation workflow for Islandora 2, including:

- Converting legacy Islandora 7 metadata templates to Islandora 2 metadata spreadsheets ("sheets") &rarr; `i7_to_i2_template.py`
- Transforming MARC bibliographic records into Islandora 2 metadata sheets &rarr; `make_marc_metadata_sheet.py`
- Creating Islandora Workbench ingest sheets and configuration files &rarr; `make_ingest_sheet.py`, `setup_taxonomy_ingest.py`
- Merging metadata sheets for multiple batches to reduce the number of ingests required `merge_batches.py`
- Creating metadata sheets from Islandora Workbench exports for remediation projects &rarr; `make_metadata_sheet.py`
- Generating taxonomy remediation projects &rarr; `setup_taxnomy_project,py`
- Refreshing taxonomy snapshots &rarr; `refresh_taxonomies`

These scripts automate metadata extraction, transformation, validation, and ingest preparation to streamline workflows, improve metadata quality, and provide detailed logging for troubleshooting and auditing.

---

# Workflows

## Standard Metadata Workflow

```text
Metadata Template
      │
      ▼
Metadata Sheet
      │
      ▼
make_ingest_sheet.py
      │
      ▼
Islandora Workbench Ingest CSV
Islandora Workbench Ingest Configuration
Processing Logs
      │
      ▼
Islandora Workbench Ingest
```

## Converting Islandora 7 Metadata Sheet

```text
Legacy Metadata Sheet (Islandora 7)
        │
        ▼
i7_to_i2_template.py
        │
        ▼
Metadata Sheet (Islandora 2)
Processing Logs
```

## Creating Metadata Sheets from MARC

```text
MARC Manifest Template (with Alma MMSIDs)
      │
      ▼
Alma Bibliographic Records Export
      │
      ▼
MARC Records
      │
      ▼
make_marc_metadata_sheet.py
      │
      ▼
Metadata Sheet
Processing Logs
```

## Taxonomy Maintenance Workflow

```text
Metadata Exception Report
(from make_ingest_sheet.py)
      │
      ▼
setup_taxonomy_project.py
      │
      ▼
Manual Review / Remediation / Reconciliation
      │
      ▼
setup_taxonomy_ingest.py
      │
      ▼
Islandora Workbench Ingest CSV
Islandora Workbench Ingest Configuration
      │
      ▼
Islandora Workbench Ingest
      │
      ▼
refresh_taxonomies.py
      │
      ▼
Updated taxonomy snapshots (GitHub Repository, Documentation & Templates in Google Drive)
```

---

# Repository Structure

## Core Workflow

These scripts perform the primary metadata processing workflow.

| Script | Description |
|----------|-------------|
| `make_ingest_sheet.py` | Validate metadata and generate Islandora Workbench ingest batches. |
| `make_marc_metadata_sheet.py` | Transform MARC records into Islandora metadata sheets. |
| `i7_to_i2_template.py` | Convert Islandora 7 metadata sheets into the Islandora 2 schema. |
| `merge_batches.py` | Merge multiple metadata sheets. |

---

## Taxonomy Utilities

These scripts support taxonomy remediation and management.

| Script | Description |
|----------|-------------|
| `setup_taxonomy_project.py` | Generate taxonomy remediation projects from metadata exception logs. |
| `setup_taxonomy_ingest.py` | Generate taxonomy ingest batches from completed remediation projects. |
| `refresh_taxonomies.py` | Refresh the local Islandora taxonomy cache and snapshots in [Utility_Files](Utility_Files) and in documention in Google Drive on demand. |
| `taxonomies_cache.py` | Fetch, normalize, cache, and load Islandora taxonomy data. |
| `taxnomy_manager.py` | Manage loading, refreshing, and caching Islandora taxonomies. |

---

## Metadata Processing Modules

These modules provide the metadata processing logic, primarily for `make_marc_metadata_sheet.py` but may also be used by other scripts.

| Module | Description |
|----------|-------------|
| `process_mods.py` | Extract metadata from MODS XML. |
| `process_related_item.py` | Process MODS relatedItem elements. |
| `process_dates.py` | Validate and convert MARC to EDTF dates. |

---

## Shared Infrastructure

These helper modules are shared across the toolkit.

| Module | Description |
|----------|-------------|
| `definitions.py` | Shared constants, mappings, field definitions, schemas, and controlled vocabularies. |
| `utilities.py` | Common helper functions for Google Sheets, DataFrames, logging, filesystem operations, reports, and validation. |

---

# Features

## Metadata Validation

The toolkit validates metadata against:

- Islandora field schema (data type, obligation, cardinality)
- Taxonomies in Drupal/Islandora
- Collection and domain membership
- Controlled vocabularies (e.g., MARC value lists, Art & Architecture Thesaurus)
- Name authority files (LCNAF, VIAF)
- Extended Date Time Format (EDTF)
- Geographic coordinate formats (decimal and sexagesimal)

## Logging and Reporting

Every workflow produces detailed audit information.

Depending on the script, reports may include:

- Runtime logs
- Metadata exceptions   
      - Unknown fields   
      - Missing required fields   
      - Invalid/unknown taxonomy terms   
      - Invalid EDTF dates   
      - Invalid value formats   
- Metadata transformations
- Unmatched records
- Batch summaries
- Processing statistics

This audit trail facilitates understanding **what changed**, **why it changed**, and **which records require manual review**.

## Google Workspace Integration

The toolkit integrates with Google Workspace to support collaborative metadata workflows.

Depending on the script, it can:

- Use a Google service account for authenticated access
- Read metadata directly from Google Sheets
- Update Google Sheets
- Synchronize taxonomy snapshot

## Taxonomy Snapshots

To support reliable validation without requiring a live connection to Islandora, the toolkit maintains local JSON snapshots of Islandora taxonomies.

These snapshots are used to:

- Validate taxonomy terms during ingest preparation
- Generate taxonomy remediation projects
- Reduce repeated API requests to Drupal

Snapshots are refreshed on demand using `refresh_taxonomies.py` after taxonomy changes are made in Islandora.

---

# Requirements

## Python

- Python 3.11+

## Packages

- pandas
- lxml
- pymarc
- edtf
- requests
- openpyxl
- google-api-python-client
- google-auth
- google-auth-oauthlib
- google-auth-httplib2

---

## Design Philosophy

The toolkit is designed around several guiding principles:

- Preserve source metadata whenever possible.
- Make metadata transformations explicit and traceable.
- Validate early to prevent ingest failures.
- Produce detailed logs for auditing and troubleshooting.
- Separate configuration and mappings from processing logic.
- Automate repetitive tasks while preserving opportunities for human review.

---

# Configuration

Several scripts support multiple metadata input file types:

- Google Sheets
- Local CSV files
- Local Excel files

Google Sheets access requires a Google service account credentials JSON file.

Most scripts prompt for missing inputs if command-line arguments are omitted.

---

# Utility Files

The toolkit relies on several configuration files in [Utility_Files](Utility_Files), including:

- Field mappings
- Controlled vocabulary lists 
- Taxonomy data
- Islandora field definitions
- Workbench configuration templates
- MARC-to-MODS stylesheets

These files are centralized in `definitions.py` and shared across the project.

---

# Coding Conventions

The project follows an internal style:

- PEP 8 formatting
- Google-style docstrings
- Built-in generic type hints (`list`, `dict`, `tuple`, etc.)
- Single quotes for identifiers, dictionary keys, field names, and internal constants
- Double quotes for user-facing text
- Structured logging using a shared logger
- Dataclasses for runtime configuration and processing results
- Shared reference values and data collections in `definitions.py`
- Shared helper functions located in `utilities.py`

---

# Future Improvements

Potential future enhancements include:

- Unit and integration test suite
- Customizable, YAML-based configuration for running scripts instead of command-line argument, hard-coded constants, and mapping files
- Parallel metadata processing
- Additional repository migration utilities

---

# License
This project is available under the MIT License.

