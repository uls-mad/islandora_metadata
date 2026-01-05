#!/bin/python3 

"""Generate a taxonomy remediation template from a batch exceptions log.

This script identifies taxonomy-related errors within an ingest exceptions log 
and aggregates them into a standardized format. By grouping recurring issues 
and stripping record-specific PIDs, it creates a unique list of terms that 
require mapping to controlled vocabulary (term names and URIs).

Main Features:
- Exception Filtering: Isolates rows containing "taxonomy" errors.
- Frequency Analysis: Calculates a 'count' for each error to help prioritize 
  high-impact remediation.
- Template Generation: Automatically initializes 'term_name' and 'uri' columns 
  for manual or automated data entry.
- Organized Output: Saves results into a dedicated 'remediation' subdirectory 
  within the batch path.

Usage:
    python3 setup_taxonomy_project.py --batch_path /workbench/batches/[BATCH_DIR] --exceptions_log /workbench/batches/[BATCH_DIR]/logs/[METADATA_EXCEPTIONS_LOG].csv
"""

# --- Modules ---

# Import standard modules
import os
import argparse

# Import local modules
from utilities import prompt_for_input, create_df


# --- Functions ---

def parse_arguments():
    """Parse command line arguments or prompt the user for required paths.

    If batch or exception paths are not provided via command line flags, 
    trigger interactive prompts to collect the necessary directory and 
    file locations.

    Returns:
        argparse.Namespace: An object containing 'batch_path' (str) and 
            'exceptions_log' (str).
    """
    parser = argparse.ArgumentParser(description="Taxonomy Remediation Tool")
    parser.add_argument(
        "--batch_path", 
        help="Path to the batch directory"
    )
    parser.add_argument(
        "--exceptions_log", 
        help="Path to local metadata file"
    )
    
    args = parser.parse_args()

    # Ensure batch directory is provided
    if not args.batch_path:
        args.batch_path = prompt_for_input(
            "Enter the path to the Workbench batch directory: "
        )

    # Ensure at least one metadata source is provided
    if not args.exceptions_log:
        args.exceptions_log = prompt_for_input(
            "Enter the path to the exceptions log: "
        )

    return args


def process_taxonomy_metadata(df):
    """Filter, count, and expand metadata for taxonomy remediation.

    This function isolates taxonomy-specific exceptions, calculates the 
    frequency of each unique field/value pair, removes batch-specific 
    identifiers (PID/Batch), and initializes placeholder columns for 
    manual cleanup.

    Args:
        df: A pandas DataFrame containing the raw exception log data.

    Returns:
        pd.DataFrame: A cleaned and grouped DataFrame containing unique 
            taxonomy exceptions with an added 'count' column and 
            placeholder 'term_name' and 'uri' columns.
    """
    # Filter rows for taxonomy exceptions
    df = df[df["exception"].str.contains("taxonomy", na=False)].copy()

    # Calculate counts for duplicate occurrences
    counts = df.groupby(["field", "value", "exception"]).size().reset_index(
        name="count"
    )

    # Remove batch and pid columns
    df = df.drop(columns=["batch", "pid"])

    # Remove duplicate rows and keep the first instance
    df = df.drop_duplicates().copy()

    # Merge counts back into the unique dataframe
    df = df.merge(counts, on=["field", "value", "exception"], how="left")

    # Initialize standard empty columns
    df["term_name"] = ""
    df["uri"] = ""

    return df


def main():
    """Coordinate the taxonomy remediation data processing and file generation.

    Handle the loading of the exceptions log, apply transformation logic 
    to isolate taxonomy issues, and manage the file system by creating 
    output directories and saving the finalized remediation CSV.

    Side Effects:
        - Loads a CSV or Excel file into memory via create_df.
        - Creates a 'remediation' subdirectory within the provided batch path.
        - Writes a new remediation CSV file to the file system.
        - Prints status and location messages to the console.
    """
    args = parse_arguments()

    # Load data from the specified source
    df = create_df(args.exceptions_log)

    # Process dataframe logic
    processed_df = process_taxonomy_metadata(df)

    # Setup the remediation directory
    remediation_path = os.path.join(args.batch_path, "remediation")
    if not os.path.exists(remediation_path):
        os.makedirs(remediation_path)
        print(f"Created directory: {remediation_path}")

    # Define output filename and path
    batch_name = os.path.basename(args.batch_path.rstrip(os.sep))
    filename = f"{batch_name}_taxonomy_remediation.csv"
    output_file = os.path.join(remediation_path, filename)

    # Save processed data to CSV
    processed_df.to_csv(output_file, index=False, encoding="utf-8")
    
    # Report final status
    print(f"Remediation project file saved to: {output_file}")


if __name__ == "__main__":
    main()