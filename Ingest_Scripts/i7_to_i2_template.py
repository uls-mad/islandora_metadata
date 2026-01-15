#!/usr/bin/python3

"""Convert spreadsheet fields from I7 to I2 format based on content type mapping.

This script maps fields in Google Sheets or Excel spreadsheets using a 
configuration CSV file. It filters the mapping logic based on specific content 
types (e.g., 'av', 'images') and handles column renaming, additions, and removals 
automatically.

Main Features:
- Multi-valued content type support (separated by |, ;, or /).
- One-to-many field mapping.
- Automatic generation of required I2 columns, even if empty in source.
- Detailed logging of qualified dates, added columns, and dropped fields.

Usage:
    python3 i7_to_i2_metadata_template_mapping.py --content_type av
    python3 i7_to_i2_metadata_template_mapping.py --content_type images books
"""

# ---------------------------------------------------------------------------
# Modules
# ---------------------------------------------------------------------------

# Import standard modules
from __future__ import annotations
import argparse
import re
import sys
from pathlib import Path
from typing import Callable, Dict, List, Set
try:
    from tkinter import Tk, filedialog, messagebox
    import tkinter as tk
    TK_AVAILABLE = True
except ImportError:
    TK_AVAILABLE = False

# Import third-party module
import pandas as pd

# Import local modules
from definitions import COPYRIGHT_STATUS_MAPPING, TYPE_MAPPING, LANGUAGE_MAPPING, ALLOWED_CONTENT_TYPES, I7_to_I2_MAPPING
from utilities import *


# ---------------------------------------------------------------------------
# Constant
# ---------------------------------------------------------------------------

OBLIGATION_LEVELS: List[str] = [
    "",
    "optional",
    "recommended",
    "required, if applicable",
    "required",
]


# ---------------------------------------------------------------------------
# CLI / I/O
# ---------------------------------------------------------------------------

def parse_arguments() -> argparse.Namespace:
    """Parse CLI arguments and interactively prompt for missing configuration.

    This function handles the initial setup of the script, including parsing 
    command-line flags, triggering GUI or CLI prompts for missing paths/IDs, 
    and performing strict validation and normalization of content types.

    Returns:
        argparse.Namespace: An object containing the following attributes:
            - batch_path (str): Full path to the workbench batch directory.
            - metadata_id (str): Google Sheet ID for metadata (if provided).
            - metadata_sheet (str): Local path to a CSV or Excel file (if 
              provided).
            - credentials_file (str): Path to the Google service account JSON.
            - content_type (list[str]): Normalized, lowercase, and deduplicated 
              list of validated content types.

    Raises:
        SystemExit: If an invalid content type is provided or if a required 
            file selection is cancelled.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Remap columns by one or more content types using a mapping CSV."
        )
    )
    parser.add_argument(
        "-b", "--batch_path", 
        type=str, 
        help="Path to a batch directory for Workbench ingests."
    )
    parser.add_argument(
        "-m", "--metadata_id",
        type=str,
        help="Google Sheet ID for the metadata file."
    )
    parser.add_argument(
        "--metadata_sheet", 
        type=str, 
        help="Path to metadata sheet on local device (optional).")
    parser.add_argument(
        "-c", "--credentials_file",
        type=str,
        default="/workbench/etc/google_ulswfown_service_account.json",
        help="Path to the Google service account credentials JSON."
    )
    parser.add_argument(
        "-t", "--content_type",
        nargs="+",
        help=(
            "One or more content types (space- or comma-separated). "
            f"Allowed: {', '.join(sorted(ALLOWED_CONTENT_TYPES))}"
        ),
    )
    args = parser.parse_args()
    
    if not args.batch_path:
        while not args.batch_path:
            if TK_AVAILABLE:
                print(
                    "Select the workbench$ batch directory", 
                    end="", 
                    flush=True
                )
                args.batch_path = ask_for_path(
                    title="Select the workbench$ batch directory",

                )
                print(f": {args.batch_path}")
            else:
                args.batch_path = prompt_for_input(
                    "Enter the full path to the workbench$ batch directory: "
                )
    if not args.metadata_id and not args.metadata_sheet:
        while not args.metadata_id and not args.metadata_sheet:
            if TK_AVAILABLE:
                print("Select input CSV or Excel file", end="", flush=True)
                args.metadata_sheet = ask_for_path(
                    title="Select input CSV or Excel file",
                    filetypes=[
                        ("Supported files", "*.csv *.xlsx *.xlsm *.xls"),
                        ("CSV files", "*.csv"),
                        ("Excel files", "*.xlsx *.xlsm *.xls"), 
                        ("All files", "*.*")
                    ],
                )
                print(f": {args.metadata_sheet}")
            else:
                args.metadata_id = prompt_for_input(
                    "Enter the Google Sheet ID for the metadata: "
                )
    if args.metadata_id and not args.credentials_file:
        args.credentials_file = prompt_for_input(
            "Enter the path to the Google credentials JSON file: "
        )
    if not args.content_type:
        args.content_type = prompt_for_input(
            "Enter the content type(s) for the batch (space- or comma-separated): "
        )

    # Ensure args.content_type is a list 
    input_data = [
        args.content_type
    ] if isinstance(args.content_type, str) else args.content_type

    # Flatten and split content type input by commas and spaces
    raw: List[str] = []
    for chunk in input_data:
        # Replace commas with spaces, then split by any whitespace
        normalized_chunk = chunk.replace(',', ' ')
        raw.extend(normalized_chunk.split())

    cts = [ct.lower() for ct in raw]
    invalid_cts = [ct for ct in cts if ct not in ALLOWED_CONTENT_TYPES]

    if invalid_cts:
        raise SystemExit(
            f"Invalid --content_type values: {
                ', '.join(sorted(set(invalid_cts)))
            }. "
            f"Allowed: {', '.join(sorted(ALLOWED_CONTENT_TYPES))}"
        )

    # Process content type(s) and remove duplicates
    seen: Set[str] = set()
    ordered: List[str] = []
    for ct in cts:
        if ct not in seen:
            ordered.append(ct)
            seen.add(ct)

    args.content_type = ordered
    
    return args


def ask_for_path(
    title: str, 
    filetypes: Optional[List[tuple[str, str]]] = None
) -> str:
    """Open a GUI dialog to select a file or directory and bring it to the front.

    If filetypes are provided, the dialog will select a file. If filetypes is 
    None, the dialog will select a directory.

    Args:
        title: The text displayed in the dialog window title bar.
        filetypes: A list of allowed file extensions. Defaults to None.

    Returns:
        The absolute path to the selected file or directory.

    Raises:
        SystemExit: If the user cancels the dialog or closes the window.
    """
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)

    # Use askdirectory if no filetypes are specified, else askopenfilename
    if filetypes is None:
        path = filedialog.askdirectory(parent=root, title=title)
    else:
        path = filedialog.askopenfilename(
            parent=root, 
            title=title, 
            filetypes=filetypes
        )

    root.destroy()

    if not path:
        error_msg = "directory" if filetypes is None else "file"
        raise SystemExit(f"\nNo {error_msg} selected. Exiting.")
        
    return path


def load_mapping(mapping_csv: str) -> pd.DataFrame:
    """Read, normalize headers, and validate a crosswalk mapping CSV.

    This function performs a "fuzzy" header match (case and space insensitive) 
    to ensure compatibility with different versions of the mapping file. It also 
    verifies that all required columns are present and validates that 
    'obligation' values match the project's controlled vocabulary.

    Args:
        mapping_csv: Path to the CSV file containing field mappings.

    Returns:
        pd.DataFrame: A cleaned DataFrame with standardized headers: 
            'content_type', 'i7_field', 'i2_field', and 'obligation'.

    Raises:
        ValueError: If required columns are missing or if 'obligation' contains 
            unrecognized values not found in OBLIGATION_LEVELS.
    """
    # Read the mapping CSV as strings (so nothing is auto-converted to NaN)
    df = pd.read_csv(mapping_csv, dtype=str, keep_default_na=False)

    # Define expected columns in the mapping file
    expected = {"content_type", "i7_field", "i2_field", "obligation"}

    # Try to match and normalize column names (case-insensitive)
    rename_cols: Dict[str, str] = {}
    for e in expected:
        matches = [c for c in df.columns if c.strip().lower() == e]
        if matches:
            rename_cols[matches[0]] = e

    # Apply renaming if any matches were found
    if rename_cols:
        df = df.rename(columns=rename_cols)

    # Verify that all required columns are present
    missing = expected - set(c.strip().lower() for c in df.columns)
    if missing:
        raise ValueError(
            "Mapping CSV is missing required columns: {}\nFound columns: {}"
            .format(
                ", ".join(sorted(missing)),
                ", ".join(df.columns),
            )
        )

    # Strip whitespace from columns to ensure consistent matching
    for col in ["content_type", "i7_field", "i2_field", "obligation"]:
        df[col] = df[col].astype(str).str.strip()

    # Check for and report unknown obligation values, if any
    valid_ob = set(OBLIGATION_LEVELS)
    unknown = sorted(
        set(df.loc[~df["obligation"].isin(valid_ob), "obligation"])
    )
    if unknown:
        raise ValueError(
            "Mapping CSV contains unknown 'obligation' values: {}. "
            "Expected one of: {}".format(
                ", ".join(unknown),
                ", ".join(OBLIGATION_LEVELS),
            )
        )
    return df


def prepare_mapping(csv_path: str, content_types: List[str]) -> pd.DataFrame:
    """Load and filter the crosswalk mapping by requested content types.

    Args:
        csv_path: Path to the mapping CSV.
        content_types: List of normalized content types to filter by.

    Returns:
        A cleaned and filtered DataFrame of mapping rules.
    """
    df = load_mapping(csv_path)
    # requested = expand_requested_cts(content_types)
    
    mask = df["content_type"].apply(
        lambda cell: bool(tokenize_ct_value(cell).intersection(content_types))
    )
    mapping_ct = df[mask].copy()

    if mapping_ct.empty:
        raise SystemExit(
            f"No mapping rows found for content_type(s)='{content_types}'"
        )

    mapping_ct["i7_field_clean"] = mapping_ct["i7_field"].\
        fillna("").astype(str).str.strip()
    mapping_ct["i2_field_clean"] = mapping_ct["i2_field"].\
        fillna("").astype(str).str.strip()
    
    return mapping_ct[mapping_ct["i2_field_clean"] != ""]


def load_metadata(args) -> pd.DataFrame:
    """Load metadata from either a Google Sheet or a local file system.

    Args:
        args: A namespace object containing the following attributes:
            metadata_id (str): The Google Sheet ID (if applicable).
            credentials_file (str): Path to the Google Service Account JSON.
            metadata_sheet (str): Path to a local CSV/Excel file.

    Returns:
        pd.DataFrame: The ingested metadata ready for processing.
    """
    if args.metadata_id:
        return read_google_sheet(
            args.metadata_id, 
            sheet_name=None, 
            credentials_file=args.credentials_file
        )
    return create_df(args.metadata_sheet)


def save_outputs(
    df_final: pd.DataFrame, 
    audit_data: dict, 
    mapping_ct: pd.DataFrame, 
    args
) -> None:
    """Generate the final metadata CSV and the accompanying audit log.

    Args:
        df_final: The transformed and cleaned metadata DataFrame.
        audit_data: A dictionary containing date logs, added columns, and dropped columns.
        mapping_ct: The mapping rules used for this batch.
        args: The command-line arguments containing paths and identifiers.
    """
    # Determine base filename
    if args.metadata_id:
        filename = get_google_sheet_filename(
            args.metadata_id, args.credentials_file
        )
    else:
        filename = Path(args.metadata_sheet).stem

    ct_label = "_".join(args.content_type)
    
    # --- Save Metadata CSV ---
    output_dir = os.path.join(args.batch_path, "metadata")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{filename}_{ct_label}_metadata.csv")
    df_final.to_csv(output_path, index=False, encoding="utf-8")

    # --- Build and Save Log CSV ---
    log_rows = audit_data["date_logs"]

    for col in audit_data["added_cols"]:
        log_rows.append(
            {
                "action": "added column", 
                "field": col, 
                "reason": "Mapped I2 field; created empty"
            }
        )
    
    for col in audit_data["dropped_cols"]:
        log_rows.append(
            {
                "action": "dropped column", 
                "field": col, 
                "reason": "Not an I2 field"
            }
        )

    # Validate required fields
    req_mask = mapping_ct["obligation"] == "required"
    required_targets = sorted(set(mapping_ct.loc[req_mask, "i2_field_clean"]))
    for col in required_targets:
        if col in df_final.columns:
            blanks = df_final[col].isna() | (df_final[col].astype(str).str.strip() == "")
            if blanks.any():
                log_rows.append({
                    "action": "flagged missing required field",
                    "field": col,
                    "reason": f"{int(blanks.sum())} blank value(s) in required field"
                })

    log_df = pd.DataFrame(log_rows).fillna("")
    log_dir = os.path.join(args.batch_path, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"{filename}_{ct_label}_log.csv")
    log_df.to_csv(log_path, index=False, encoding="utf-8")

    # Notify User
    summary = f"Output saved:\n{output_path}\n\nLog saved:\n{log_path}"
    if TK_AVAILABLE:
        show_message("info", "Done", summary)
    else:
        print(f"\nDone!\n{summary}")


# ---------------------------------------------------------------------------
# Helpers / Processors
# ---------------------------------------------------------------------------

def show_message(type, title, message):
    """Display a Tkinter message box on top of all other windows.

    Args:
        msg_type: The type of message box to display ('info' or 'error').
        title: The title text of the message box.
        message: The message body text.
    """
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    
    if type == "info":
        messagebox.showinfo(title, message, parent=root)
    elif type == "error":
        messagebox.showerror(title, message, parent=root)
    
    root.destroy()


def tokenize_ct_value(value: str) -> Set[str]:
    """Parse a multi-valued content type string into a set of normalized tokens.

    Splits the input string by common delimiters (pipe, comma, semicolon, or 
    forward slash) and normalizes each token. To ensure broader matching, it 
    automatically generates a singular version of any token ending in 's'.

    Args:
        cell: The raw string from a mapping cell (e.g., "images|photograph").

    Returns:
        A set of unique, lowercase, stripped tokens including singular variants.
    """
    tokens = re.split(r"[|,;/]+", str(value))
    out: Set[str] = set()
    for token in tokens:
        t = (token or "").strip().lower()
        if not t:
            continue
        if t.endswith("s"):
            out.add(t[:-1])
        else:
            out.add(t)
    return out


def process_copyright_status(series: pd.Series) -> pd.Series:
    """Map copyright status terms to I2 taxonomy terms.

    Args:
        series: A pandas Series containing raw copyright status strings.

    Returns:
        A Series of mapped codes; returns an empty string for terms not 
        found in COPYRIGHT_STATUS_MAPPING.
    """
    return series.astype(str).str.strip().map(
        lambda x: COPYRIGHT_STATUS_MAPPING.get(x, "") 
    )


def process_language(series: pd.Series) -> pd.Series:
    """Convert MARC language codes to terms.

    Args:
        series: A pandas Series containing raw language strings.

    Returns:
        A Series of mapped codes; returns an empty string for codes not 
        found in LANGUAGE_MAPPING.
    """
    return series.astype(str).str.strip().map(
        lambda x: LANGUAGE_MAPPING.get(x, "") 
    )


def process_type_of_resource(series: pd.Series) -> pd.Series:
    """Convert legacy resource type terms to I2 taxonomy terms.

    Args:
        series: A pandas Series containing raw resource type strings.

    Returns:
        A Series of mapped codes; returns an empty string for terms not 
        found in TYPE_MAPPING.
    """
    return series.astype(str).str.strip().map(
        lambda x: TYPE_MAPPING.get(x, "") 
    )


# Register per-field processors by I2 column name
PROCESSORS: Dict[str, Callable[[pd.Series], pd.Series]] = {
    "copyright_status": process_copyright_status,
    "language": process_language,
    "type_of_resource": process_type_of_resource,
}


def apply_processors(
    df: pd.DataFrame, 
    processors: Dict[str, Callable[[pd.Series], pd.Series]]
) -> pd.DataFrame:
    """Apply transformation functions to specific DataFrame columns.

    Iterate through a dictionary of processor functions and apply them to 
    matching columns in the DataFrame for cleanup or normalization.

    Args:
        df: The DataFrame containing metadata to be processed.
        processors: A dictionary mapping target column names to callable 
            functions that accept and return a pandas Series.

    Returns:
        pd.DataFrame: The DataFrame with transformed columns. Original data 
            is preserved for columns without a defined processor.
    """
    for col, func in processors.items():
        if col in df.columns:
            try:
                df[col] = func(df[col])
            except Exception as exc:  # non-fatal
                print(
                    f"WARNING: Processor for '{col}' failed: {exc}", 
                    file=sys.stderr
                )
    return df


def apply_date_qualification(
    df_work: pd.DataFrame
) -> tuple[pd.DataFrame, List[Dict[str, str]]]:
    """Append "~" to dates based on the normalized date qualifier.

    Args:
        df_work: The working DataFrame containing metadata.

    Returns:
        A tuple containing the modified DataFrame and a list of change log entries.
    """
    date_change_logs: List[Dict[str, str]] = []
    
    if "normalized_date_qualifier" in df_work.columns and "date" in df_work.columns:
        # Normalize the qualifier for comparison
        q_yes = (
            df_work["normalized_date_qualifier"]
            .astype(str)
            .str.strip()
            .str.lower()
            .eq("yes")
        )

        date_str = df_work["date"].astype(str)
        is_blank = df_work["date"].isna() | date_str.str.strip().isin(
            ["", "nan", "none"]
        )
        already_suffixed = date_str.str.endswith("~")
        contains_slash = date_str.str.contains("/")

        add_mask = q_yes & ~is_blank & ~already_suffixed

        # Capture originals for logging, apply change, then log
        old_dates = df_work.loc[add_mask, "date"].astype(str).copy()
        df_work.loc[add_mask, "date"] = df_work.loc[
            add_mask, "date"
        ].astype(str) + "~"
        new_dates = df_work.loc[add_mask, "date"].astype(str)

        # Log updates to date value using the index for Excel row calculation
        for i in df_work.index[add_mask]:
            excel_row = int(i) + 2  # header=1 → first data row=2
            date_change_logs.append(
                {
                    "action": "qualified date",
                    "row": excel_row,
                    "field": "date",
                    "old": old_dates.loc[i],
                    "new": new_dates.loc[i],
                    "reason": 'normalized_date_qualifier == "yes" ',
                }
            )

    return df_work, date_change_logs


def transform_metadata(
    df_in: pd.DataFrame, 
    mapping_ct: pd.DataFrame
) -> tuple[pd.DataFrame, dict]:
    """Execute schema remapping, date qualification, and final metadata cleanup.

    Args:
        df_in: The raw input DataFrame containing the original metadata.
        mapping_ct: A DataFrame containing mapping rules with 'i7_field_clean' 
            as source and 'i2_field_clean' as target field names.

    Returns:
        A tuple (df_final, audit_data) where:
            - df_final (pd.DataFrame): The transformed metadata with finalized 
              schema and applied processors.
            - audit_data (dict): A dictionary containing 'date_logs' (list), 
              'added_cols' (list), and 'dropped_cols' (list).
    """
    df_work = df_in.copy()
    audit_data = {"date_logs": [], "added_cols": [], "dropped_cols": []}

    # Map source fields to target schema and initialize missing target columns
    for _, row in mapping_ct.iterrows():
        src, tgt = row["i7_field_clean"], row["i2_field_clean"]
        if src and src in df_work.columns:
            df_work[tgt] = df_work[src]
        elif tgt != "file" and tgt not in df_work.columns:
            df_work[tgt] = ""

    # Apply EDTF date qualifiers and record changes in the audit log
    df_work, date_logs = apply_date_qualification(df_work)
    audit_data["date_logs"] = date_logs

    # Order columns based on the mapping template
    i2_ordered = []
    seen = set()
    for field in mapping_ct["i2_field_clean"]:
        if field and field not in seen:
            if field == "file" and field not in df_work.columns:
                continue
            i2_ordered.append(field)
            seen.add(field)

    # Ensure all required columns exist and track newly initialized fields
    for col in i2_ordered:
        if col not in df_work.columns:
            df_work[col] = ""
            audit_data["added_cols"].append(col)

    # Identify excluded columns, prune schema, and run final processors
    audit_data["dropped_cols"] = [c for c in df_work.columns if c not in seen]
    df_final = df_work[i2_ordered].copy()
    df_final = apply_processors(df_final, PROCESSORS)

    return df_final, audit_data


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Coordinate the end-to-end metadata conversion and validation workflow.

    Initialize the execution environment, parse command-line arguments, 
    and manage the sequential flow of mapping preparation, data ingestion, 
    transformation, and output generation. 

    Side Effects:
        - Initializes and destroys a hidden Tkinter root window, if available.
        - Loads external mapping CSVs and source metadata.
        - Writes processed metadata and audit logs to the file system.
        - Terminates the script with sys.exit(1) upon encountering a fatal error.
    """
    args = parse_arguments()
    root = None
    
    if TK_AVAILABLE:
        root = tk.Tk()
        root.withdraw()

    try:
        # Load and prepare the crosswalk mapping
        mapping_ct = prepare_mapping(I7_to_I2_MAPPING, args.content_type)

        # Ingest source data
        df_in = load_metadata(args)

        # Transform data
        df_final, audit_data = transform_metadata(df_in, mapping_ct)

        # Generate outputs
        save_outputs(df_final, audit_data, mapping_ct, args)

    except Exception as exc:
        if TK_AVAILABLE:
            show_message("error", "Error", str(exc))
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        if root:
            root.destroy()


if __name__ == "__main__":
    # Make pandas not warn about dtype conversions when adding new columns
    pd.options.mode.copy_on_write = False
    main()
