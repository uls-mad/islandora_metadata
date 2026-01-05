#!/bin/python3

"""Generate taxonomy ingest sheets and Workbench configurations from project CSVs.

This sript automates the splitting of a master taxonomy CSV into individual 
vocabulary batches. It maps input fields to Drupal vocabulary IDs, determines 
authority sources (LCSH, AAT, etc.) from URIs, and generates the necessary 
YAML configuration files required for Drupal Workbench operations.

Usage:
    python setup_taxonomy_ingest.py -t /workbench/batches/[BATCH_DIR]/remediation/[TAXONOMY_PROJECT_COMPLETE].csv

Dependencies:
    - pandas: For data manipulation.
    - python-dotenv: For managing import credentials.
    - Utility_Files/create_taxonomy_template.yml: Must exist as a base config.
"""

# --- Modules ---

# Import standard modules
import os
import argparse
import sys
from pathlib import Path

# Import third-party modules
import pandas as pd
from dotenv import load_dotenv

# Import local modules
from utilities import prompt_for_input, create_df


# --- Constants ---

# Map input field names to vocabulary IDs
VOCAB_MAPPING = {
    "format": "physical_form",
    "genre": "genre",
    "genre_japanese_prints": "genre_japanese_prints",
    "source_collection": "source_collection_title",
    "source_collection_id": "source_collection_identifier",
    "source_repository": "source_repository",
    "subject_geographic": "geo_location",
    "subject_name_conference": "person",
    "subject_name_corporate": "corporate",
    "subject_name_family": "family",
    "subject_name_person": "person",
    "subject_temporal": "temporal",
    "subject_title": "subject_title",
    "subject_topic": "subject",
    "arranger": "person",
    "composer": "person",
    "conductor": "person",
    "contributor_corporate": "corporate",
    "contributor_person": "person",
    "creator_conference": "conference",
    "creator_corporate": "corporate",
    "creator_family": "family",
    "creator_person": "person",
    "instrumentalist": "person",
    "interviewee": "person",
    "interviewer": "person",
    "lyricist": "person",
    "photographer_corporate": "corporate",
    "photographer_person": "person",
    "singer": "person",
}


# --- Functions ---

def parse_arguments():
    """Parse command line arguments for the taxonomy project.

    If the taxonomy project path is not provided via the '-t' flag, 
    trigger an interactive prompt to collect the path.

    Returns:
        argparse.Namespace: An object containing the 'taxonomy_project' path string.

    """
    parser = argparse.ArgumentParser(description="Taxonomy Ingest Generator")
    parser.add_argument(
        "-t", "--taxonomy_project",
        help="Path to taxonomy CSV"
    )
    args = parser.parse_args()

    if not args.taxonomy_project:
        args.taxonomy_project = prompt_for_input(
            "Enter path to completed taxonomy project (CSV): "
        )
    return args


def get_authority_source(uri):
    """Determine the authority source code based on URI string content.

    Args:
        uri: The full URI string.

    Returns:

        str: A shortcode identifier (e.g., 'aat', 'lcsh', 'naf', 'viaf') 
            or an empty string if no match is found.
    """
    if "aat" in uri:
        return "aat"
    if "subjects" in uri:
        return "lcsh"
    if "names" in uri:
        return "naf"
    if "viaf" in uri:
        return "viaf"
    return ""


def main():
    """Split taxonomy data into ingest sheets and generate Workbench configs.

    Handle the end-to-end processing of taxonomy projects by validating 
    input columns, grouping data by field type, and generating individual 
    ingest CSVs based on VOCAB_MAPPING. Populate YAML configuration templates 
    with environment-specific credentials and generates a manifest of commands 
    for the workbench runner.

    Side Effects:
        - Loads environment variables from a local .env file.
        - Creates 'logs' and 'tmp' directories within the project folder.
        - Writes multiple CSV ingest sheets and YAML config files to disk.
        - Generates a 'workbench_commands.txt' file in the project directory.
        - Terminates the process with sys.exit(1) if required columns or 
          vocabulary mappings are missing.
    """
    args = parse_arguments()

    # Load environment variable
    load_dotenv()
    import_password = os.getenv("IMPORT_PASSWORD")
    
    # Set directory paths
    project_path = os.path.abspath(args.taxonomy_project)
    project_dir = os.path.dirname(project_path)

    # Create utility directories for workbench operations
    for sub_dir in ["logs", "tmp"]:
        os.makedirs(os.path.join(project_dir, sub_dir), exist_ok=True)

    # Load project data
    df = create_df(project_path)
    
    # Ensure all required columns exist
    required = ["field", "term_name", "uri"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        print(f"Error: Missing columns {missing}")
        sys.exit(1)

    commands = []

    # Process each field group
    for field, group in df.groupby("field"):
        vocab_id = VOCAB_MAPPING.get(field)
        if not vocab_id:
            print(f"Error: No vocabulary mapping for field {field}")
            sys.exit(1)

        # Build ingest sheet
        ingest_data = {"term_name": group["term_name"].tolist()}
        
        # Add authority link fields based on field type
        is_source = "source_" in field
        if field != "format" and not is_source:
            # Determine correct authority column name
            if field in ["creator_conference", "subject_name_conference"]:
                auth_col = "field_authority_sources_conf"
            elif field == "subject_genre":
                auth_col = "field_authority_sources_subj_gen"
            elif field == "subject_title":
                auth_col = "field_authority_sources_subj_ti"
            else:
                auth_col = "field_authority_link"

            # Build authority values from URI logic
            auth_values = []
            for uri in group["uri"]:
                src = get_authority_source(uri)
                clean_uri = uri.replace("%3A", ":").replace("%2F", "/")
                auth_values.append(f"{src}%%{clean_uri}")
            ingest_data[auth_col] = auth_values

        # Add description column, if applicable
        desc_fields = [
            "genre", 
            "genre_japanese_prints", 
            "source_collection"
        ]
        if field in desc_fields and "description" in group.columns:
            ingest_data["description"] = group["description"].tolist()

        # Save ingest CSV
        ingest_df = pd.DataFrame(ingest_data)
        ingest_name = f"taxonomy_{vocab_id}_batch.csv"
        ingest_path = os.path.join(project_dir, ingest_name)
        ingest_df.to_csv(ingest_path, index=False, encoding="utf-8")
        print(f"Ingest sheet for {vocab_id} saved: {ingest_path}")

        # Prepare config file from template
        template_path = os.path.join(
            "Utility_Files", "create_taxonomy_template.yml"
        )
        with open(template_path, "r", encoding="utf-8") as f:
            config_content = f.read()

        # Update config placeholders
        replacements = {
            "[PASSWORD]": import_password,
            "[INPUT_CSV]": Path(ingest_path).as_posix(),
            "[VOCAB_ID]": vocab_id,
            "[INPUT_DIR]": Path(project_dir).as_posix()
        }
        for placeholder, value in replacements.items():
            config_content = config_content.replace(placeholder, str(value))

        # Save config file
        config_name = f"create_taxonomy_{vocab_id}_batch.yml"
        config_path = os.path.join(project_dir, config_name)
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(config_content)
        print(f"Config file for {vocab_id} saved: {config_path}")

        # Store command for the runner file
        commands.append(f"workbench --config {config_path} --check")

    # Generate command manifest
    cmd_file_path = os.path.join(project_dir, "workbench_commands.txt")
    with open(cmd_file_path, "w", encoding="utf-8") as f:
        f.write("\n".join(commands))
    print(f"Commands saved: {cmd_file_path}")


if __name__ == "__main__":
    main()