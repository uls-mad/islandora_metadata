#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Excel → CSV Field Remapper by Content Type

Description
-----------
This script maps fields in a Google Sheet or Excel spreadsheet based on a  
field mapping CSV. It supports one or more content types and automatically 
renames, adds, and removes columns according to the mapping configuration.

Main Features
--------------
- Supports multi-valued `content_type` cells in the mapping (separated by | , ; /).
- Performs one-to-many mapping (a single I7 field can map to multiple I2 fields).
- Ensures all mapped I2 fields exist in the output, even if empty.
- Logs dropped, added, and missing required fields.

Usage Notes
------------
- Run the script from the command line with one or more content types:
      python3 i7_to_i2_metadata_template_mapping.py --content_type av
      python3 i7_to_i2_metadata_template_mapping.py --content_type av images books
- The script will prompt you to select:
    1. An Excel input file, if no GID for a metadata sheet is passed
    2. A location to save the output CSV
- If a GID for a metadata sheet is entered, the path to the ulswfown Google 
Service Account config file must also be entered
- Along with the output, a separate log CSV will be created, summarizing field 
changes, missing required values, and date updates.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Callable, Dict, List, Set

import pandas as pd
try:
    from tkinter import Tk, filedialog, messagebox
    import tkinter as tk
    TK_AVAILABLE = True
except ImportError:
    TK_AVAILABLE = False

from definitions import COPYRIGHT_STATUS_MAPPING, TYPE_MAPPING, LANGUAGE_MAPPING
from utilities import *


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ALLOWED_CONTENT_TYPES: Set[str] = {
    "av",
    "interview",
    "musical_recording",
    "image",
    "japanese_prints"
    "photograph",
    "map",
    "book",
    "manuscript",
    "notated_music",
    "serial",
}

OBLIGATION_LEVELS: List[str] = [
    "",
    "optional",
    "recommended",
    "required, if applicable",
    "required",
]


# ---------------------------------------------------------------------------
# Helpers / Processors
# ---------------------------------------------------------------------------

def _norm(text: str) -> str:
    """Normalize text for consistent mapping lookups."""
    return (
        (text or "")
        .strip()
        .lower()
        .replace("\u2011", "-")
        .replace("\u2013", "-")
        .replace("\u2014", "-")
    )


def process_copyright_status(series: pd.Series) -> pd.Series:
    """
    Map copyright status terms to a controlled vocabulary field_code.
    Returns an empty string if the term is not found in COPYRIGHT_STATUS_MAPPING.
    """
    return series.astype(str).str.strip().map(
        # The lambda function checks the normalized/stripped value against the map.
        # If the key is not found, .get() returns "", which is the desired fallback.
        lambda x: COPYRIGHT_STATUS_MAPPING.get(x, "") 
    )

def process_language(series: pd.Series) -> pd.Series:
    """
    Map language terms to a controlled vocabulary field_code.
    Returns an empty string if the term is not found in LANGUAGE_MAPPING.
    """
    return series.astype(str).str.strip().map(
        # The lambda function checks the normalized/stripped value against the map.
        # If the key is not found, .get() returns "", which is the desired fallback.
        lambda x: LANGUAGE_MAPPING.get(x, "") 
    )

def process_type_of_resource(series: pd.Series) -> pd.Series:
    """
    Map type of resource terms to a controlled vocabulary field_code.
    Returns an empty string if the term is not found in TYPE_MAPPING.
    """
    return series.astype(str).str.strip().map(
        # The lambda function checks the normalized/stripped value against the map.
        # If the key is not found, .get() returns "", which is the desired fallback.
        lambda x: TYPE_MAPPING.get(x, "") 
    )


# Register per-field processors by I2 column name
PROCESSORS: Dict[str, Callable[[pd.Series], pd.Series]] = {
    "copyright_status": process_copyright_status,
    "language": process_language,
    "type_of_resource": process_type_of_resource,
}


def tokenize_ct_cell(cell: str) -> Set[str]:
    """
    Split a multi-valued mapping content_type cell.

    Supports separators: | , ; /
    Adds a singular variant for tokens ending with 's'.
    """
    tokens = re.split(r"[|,;/]+", str(cell))
    out: Set[str] = set()
    for token in tokens:
        t = (token or "").strip().lower()
        if not t:
            continue
        out.add(t)
        if t.endswith("s"):
            out.add(t[:-1])
    return out


def expand_requested_cts(requested: List[str]) -> Set[str]:
    """Expand requested content types with simple plural/singular variants."""
    expanded: Set[str] = set()
    for ct in requested:
        ct = (ct or "").strip().lower()
        if not ct:
            continue
        expanded.add(ct)
        if ct.endswith("s"):
            expanded.add(ct[:-1])
        else:
            expanded.add(ct + "s")
    return expanded


# ---------------------------------------------------------------------------
# CLI / I/O
# ---------------------------------------------------------------------------


def parse_arguments() -> List[str]:
    """
    Parse and validate one or more content types.

    Returns
    -------
    list[str]
        Validated content types (lowercase), order preserved, deduplicated.

    Raises
    ------
    SystemExit
        If any provided content type is invalid.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Remap columns by one or more content types using a mapping CSV."
        )
    )
    parser.add_argument(
        "-b", "--batch_path", 
        type=str, 
        help="Path to a batch directory for Workbench ingests.")
    parser.add_argument(
        "--metadata_id",
        "-m",
        type=str,
        help="Google Sheet ID for the metadata file."
    )
    parser.add_argument(
        "--credentials_file",
        "-c",
        type=str,
        default="../../etc",
        help="Path to the Google service account credentials JSON."
    )
    parser.add_argument(
        "--content_type",
        "-t",
        nargs="+",
        required=True,
        help=(
            "One or more content types (space- or comma-separated). "
            f"Allowed: {', '.join(sorted(ALLOWED_CONTENT_TYPES))}"
        ),
    )
    args = parser.parse_args()
    
    if not args.batch_path:
        args.batch_path = prompt_for_input(
            "Enter the full path to the workbench$ batch directory: "
        )
    if not args.metadata_id:
        if TK_AVAILABLE:
            args.excel_path = ask_open_file(
                title="Select input Excel file",
                filetypes=[
                    ("Excel files", "*.xlsx;*.xlsm;*.xls"), 
                    ("All files", "*.*")
                ],
            )
        else:
            args.metadata_id = prompt_for_input(
                "Enter the Google Sheet ID for the metadata: "
            )
    if args.metadata_id and not args.credentials_file:
        args.credentials_file = prompt_for_input(
            "Enter the path to the Google credentials JSON file: "
        )


    # Flatten comma-separated chunks, then normalize
    raw: List[str] = []
    for chunk in args.content_type:
        raw.extend([p.strip() for p in chunk.split(",") if p.strip()])

    cts = [ct.lower() for ct in raw]
    bad = [ct for ct in cts if ct not in ALLOWED_CONTENT_TYPES]
    if bad:
        raise SystemExit(
            "Invalid --content_type values: {}. Allowed: {}".format(
                ", ".join(sorted(set(bad))),
                ", ".join(sorted(ALLOWED_CONTENT_TYPES)),
            )
        )

    # Preserve order but dedupe
    seen: Set[str] = set()
    ordered: List[str] = []
    for ct in cts:
        if ct not in seen:
            ordered.append(ct)
            seen.add(ct)

    args.content_type = ordered
    
    return args


def ask_open_file(title: str, filetypes: List[tuple[str, str]]) -> str:
    """Show a file open dialog and return the chosen path."""
    path = filedialog.askopenfilename(title=title, filetypes=filetypes)
    if not path:
        raise SystemExit("No file selected. Exiting.")
    return path


def ask_save_file(default_name: str) -> str:
    """Show a file save dialog and return the chosen path."""
    path = filedialog.asksaveasfilename(
        title="Save output CSV as…",
        initialfile=default_name,
        defaultextension=".csv",
        filetypes=[("CSV files", "*.csv")],
        confirmoverwrite=True,
    )
    if not path:
        raise SystemExit("No output path selected. Exiting.")
    return path


def load_mapping(mapping_csv: str) -> pd.DataFrame:
    """
    Read and normalize the mapping file.

    Expects columns (case/space-insensitive):
        - content_type
        - i7_field
        - i2_field
        - obligation  (values: '', optional, recommended, required, if applicable, required)

    Returns
    -------
    pd.DataFrame
        Cleaned mapping with 'obligation_norm' added/validated.
    """
    # Read the mapping CSV as strings (so nothing is auto-converted to NaN)
    df = pd.read_csv(mapping_csv, dtype=str, keep_default_na=False)

    # Define expected columns in the mapping file
    expected = {"content_type", "i7_field", "i2_field", "obligation"}

    # Try to match and normalize column names (case-insensitive)
    rename_cols: Dict[str, str] = {}
    for need in expected:
        matches = [c for c in df.columns if c.strip().lower() == need]
        if matches:
            rename_cols[matches[0]] = need

    # Apply renaming if any matches were found
    if rename_cols:
        df = df.rename(columns=rename_cols)

    # Verify that all required columns are present
    missing = expected - set(c.strip().lower() for c in df.columns)
    if missing:
        raise ValueError(
            "Mapping CSV is missing required columns: {}\nFound columns: {}".format(
                ", ".join(sorted(missing)),
                ", ".join(df.columns),
            )
        )

    # Strip whitespace from columns to ensure consistent matching
    for col in ["content_type", "i7_field", "i2_field", "obligation"]:
        df[col] = df[col].astype(str).str.strip()

    # Normalize obligation values (strip whitespace and lowercase)
    df["obligation_norm"] = df["obligation"].str.lower()

    # Check for and report unknown obligation values, if any
    valid_ob = set(OBLIGATION_LEVELS)
    unknown = sorted(
        set(df.loc[~df["obligation_norm"].isin(valid_ob), "obligation_norm"])
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


def load_excel_as_str(excel_path: str) -> pd.DataFrame:
    """Read Excel as strings and trim header whitespace."""
    try:
        df = pd.read_excel(excel_path, dtype=str, engine="openpyxl")
    except Exception:
        df = pd.read_excel(excel_path, dtype=str)
    if df.empty:
        df = df.astype("string")
    df.columns = [str(c).strip() for c in df.columns]
    return df


def apply_processors(
    df: pd.DataFrame, processors: Dict[str, Callable[[pd.Series], pd.Series]]
) -> pd.DataFrame:
    """
    Apply column-level processors to the DataFrame if the target columns exist.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to transform (I2 field names as columns).
    processors : dict[str, callable]
        Mapping of i2_field -> function(pd.Series) -> pd.Series

    Returns
    -------
    pd.DataFrame
        Transformed DataFrame.
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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    # Parse command-ling arguments
    args = parse_arguments()

    # Initialize Tk window
    root = Tk()
    root.withdraw()

    try:
        mapping_csv = (
            "Utility_Files/i7_to_i2_metadata_template_mapping.csv"
        )
        mapping_df = load_mapping(mapping_csv)

        # Filter mapping by selected content types (robust to multi-valued cells)
        requested = expand_requested_cts(args.content_type)
        mask = mapping_df["content_type"].apply(
            lambda cell: bool(tokenize_ct_cell(cell).intersection(requested))
        )
        mapping_ct = mapping_df[mask].copy()

        if mapping_ct.empty:
            raise SystemExit(
                "No mapping rows found for content_type(s)='{}' in {}\n"
                "Tip: mapping 'content_type' cells can be multi-valued like "
                "'image|photograph'; this filter considers any overlap.".format(
                    ", ".join(args.content_type),
                    mapping_csv,
                )
            )

        # Normalized mapping fields
        mapping_ct["i7_field_clean"] = (
            mapping_ct["i7_field"].fillna("").astype(str).str.strip()
        )
        mapping_ct["i2_field_clean"] = (
            mapping_ct["i2_field"].fillna("").astype(str).str.strip()
        )

        # Filter for I7 fields that map to I2 fields
        mapping_ct = mapping_ct[mapping_ct["i2_field_clean"] != ""]

        # Select and load Excel input
        if args.metadata_id:
            df_in = read_google_sheet(
                args.metadata_id,
                sheet_name=None,
                credentials_file=args.credentials_file
            )
        else:
            df_in = load_excel_as_str(args.excel_path)

        # Build working DataFrame from input; create/overwrite target columns
        df_work = df_in.copy()
        used_sources_actual: Set[str] = set()

        for _, row in mapping_ct.iterrows():
            src = row["i7_field_clean"]
            tgt = row["i2_field_clean"]
            if src and src in df_work.columns:
                # One-to-many copy is supported by repeating rows per target
                df_work[tgt] = df_work[src]
                used_sources_actual.add(src)
            else:
                # Ensure the target column exists, even if the source is absent
                if tgt not in df_work.columns:
                    df_work[tgt] = ""

        # Append "~" to 'date' if normalized_date_qualifier == "yes" and date 
        # does not contain "/" (date range)
        date_change_logs: List[Dict[str, str]] = []
        if "normalized_date_qualifier" in df_work.columns \
            and "date" in df_work.columns:
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

            # Log updates to date value
            for i in df_work.index[add_mask]:
                excel_row = int(i) + 2  # header=1 → first data row=2
                date_change_logs.append(
                    {
                        "action": "qualified date",
                        "row": excel_row,
                        "field": "date",
                        "old": old_dates.loc[i],
                        "new": new_dates.loc[i],
                        "reason": (
                            'normalized_date_qualifier == "yes" '
                            # 'and date does not contain "/"'
                        ),
                    }
                )

        # Ensure all I2 fields for the selected content types exist
        i2_ordered: List[str] = []
        seen_i2: Set[str] = set()
        for field in mapping_ct["i2_field_clean"]:
            if field and field not in seen_i2:
                i2_ordered.append(field)
                seen_i2.add(field)

        added_all: List[str] = []
        for col in i2_ordered:
            if col not in df_work.columns:
                df_work[col] = ""
                added_all.append(col)

        # Drop all non-I2 columns
        allowed_cols = set(i2_ordered)
        non_i2_cols = [c for c in df_work.columns if c not in allowed_cols]
        df_work = df_work.drop(columns=non_i2_cols, errors="ignore")

        # Reorder: mapped I2 fields exactly in mapping order (no leftovers)
        df_final = df_work.loc[:, i2_ordered]

        # Apply processors (e.g., copyright_status)
        df_final = apply_processors(df_final, PROCESSORS)

        # Choose output CSV path
        ct_label = "_".join(args.content_type)
        output_dir = os.path.join(args.batch_path, "metadata")
        if args.metadata_id:
            google_sheet = get_google_sheet_filename(
                args.metadata_id,
                args.credentials_file,
                logger=None
            )
            output_path = os.path.join(
                output_dir, 
                f"{google_sheet}_{ct_label}_metadata.csv"
            )
        else:
            output_path = os.path.join(
                output_dir, 
                f"{Path(args.excel_path).stem}_{ct_label}_metadata.csv"
            )

        # Save output CSV
        df_final.to_csv(output_path, index=False, encoding="utf-8")

        # -------------------
        # Build the log CSV
        # -------------------
        log_rows: List[Dict[str, str]] = []

        # Include date-qualification logs first (if any)
        log_rows.extend(date_change_logs)

        # Log added I2 columns (created empty)
        for col in added_all:
            log_rows.append(
                {
                    "action": "added column",
                    "field": col,
                    "reason": "Mapped I2 field; created empty column",
                }
            )

        # Log dropped non-I2 columns (source/helper columns)
        for col in non_i2_cols:
            log_rows.append(
                {
                    "action": "dropped column",
                    "field": col,
                    "reason": "Not an I2 field",
                }
            )

        # Report missing values for required fields
        req_mask = mapping_ct["obligation_norm"] == "required"
        required_targets = sorted(
            set(mapping_ct.loc[req_mask, "i2_field_clean"])
        )
        for col in required_targets:
            if col in df_final.columns:
                blanks = df_final[col].isna() | (
                    df_final[col].astype(str).str.strip() == ""
                )
                missing_count = int(blanks.sum())
                if missing_count > 0:
                    log_rows.append(
                        {
                            "action": "flagged missing required field",
                            "field": col,
                            "reason": (
                                f"{missing_count} blank value(s) in required field"
                            ),
                        }
                    )
            else:
                # Shouldn't happen because all I2 fields are added, but address just in case 
                log_rows.append(
                    {
                        "action": "flagged missing required field",
                        "field": col,
                        "reason": "Required field absent from output (unexpected)",
                    }
                )

        # Create a log frame with optional columns, if present
        optional_cols = [
            k for k in ["row", "old", "new"] if any(k in r for r in log_rows)
        ]
        log_cols = ["action", "field", "reason", *optional_cols]
        log_df = pd.DataFrame(log_rows, columns=log_cols).fillna("")

        log_path = str(Path(out_csv).with_name(Path(out_csv).stem + "_log.csv"))
        log_df.to_csv(log_path, index=False, encoding="utf-8")

        # Notify user of process completion
        messagebox.showinfo(
            "Done", 
            f"Output saved:\n{out_csv}\n\nLog saved:\n{log_path}"
        )

    except Exception as exc:
        messagebox.showerror("Error", str(exc))
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        try:
            root.destroy()
        except Exception:
            pass


if __name__ == "__main__":
    # Make pandas not warn about dtype conversions when adding new columns
    pd.options.mode.copy_on_write = False
    main()
