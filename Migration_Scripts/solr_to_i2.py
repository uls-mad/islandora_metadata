#!/bin/python3 

""" Modules """

# Import standard modules
import os
import sys
import re
import threading
import time
import traceback
import argparse
from queue import Queue
from datetime import datetime
from typing import List, Tuple, Union
try:
    from tkinter import simpledialog
    import tkinter as tk
    TK_AVAILABLE = True
except ImportError:
    TK_AVAILABLE = False

# Import third-party modules
import numpy as np
import pandas as pd
from edtf import parse_edtf

# Import local modules
from file_utils import *
from definitions import *
from batch_manager import *
from inventory_manager import *
from progress_tracker import *


""" Global Variables """

global transformations
transformations = []

global exceptions
exceptions = []

global current_file
current_file = None

global current_batch
current_batch = None

DEFAULT_BATCH_SIZE = 10000


""" Helper Functions """

def parse_arguments():
    """
    Parse command-line arguments to retrieve the batch size, user ID, and optional batch directory.

    Returns:
        tuple: (user_id (str), batch_path (str | None), batch_size (int))
    """
    parser = argparse.ArgumentParser(description="Process CSV files in batches.")
    parser.add_argument(
        "--user_id",
        type=str,
        default=None,
        help="The user ID to associate with the processing operation (default: will prompt if not provided)."
    )
    parser.add_argument(
        "--batch_path",
        type=str,
        default=None,
        help="Path to a batch directory (default: will prompt if not provided)."
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Number of records per batch (default: {DEFAULT_BATCH_SIZE})"
    )
    args = parser.parse_args()
    return args.user_id, args.batch_path, args.batch_size


def process_queue(root, update_queue):
    """
    Continuously processes GUI update tasks from the queue.

    Args:
        root (tk.Tk): The root Tkinter window.
        update_queue (Queue): A thread-safe queue with GUI update functions.

    Returns:
        None
    """
    while not update_queue.empty():
        func, args = update_queue.get()
        func(*args)
    root.after(25, process_queue, root, update_queue)


def clean_parent_ids(input_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans parent relationship fields by:
    - Removing references to finding aid PIDs from child memberOf and constituentOf fields
      where the value ends with the PID.
    - Removing references to PIDs that are not present in the DataFrame.

    Args:
        input_df (pd.DataFrame): The DataFrame to clean.

    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """
    if "PID" not in input_df.columns or "RELS_EXT_hasModel_uri_ms" not in input_df.columns:
        return input_df

    # Get all valid PIDs in the DataFrame
    valid_pids = set(input_df["PID"].dropna())

    # Remove references to finding aid parents
    finding_aid_pids = input_df.loc[
        input_df["RELS_EXT_hasModel_uri_ms"] == "info:fedora/islandora:findingAidCModel",
        "PID"
    ].dropna().tolist()

    for pid in finding_aid_pids:
        if "RELS_EXT_isMemberOf_uri_ms" in input_df.columns:
            input_df.loc[
                input_df["RELS_EXT_isMemberOf_uri_ms"].fillna("").str.endswith(pid),
                "RELS_EXT_isMemberOf_uri_ms"
            ] = pd.NA

    # Remove any references to parent PIDs not in the DataFrame
    for col in ["RELS_EXT_isMemberOf_uri_ms", "RELS_EXT_isConstituentOf_uri_ms"]:
        if col in input_df.columns:
            input_df[col] = input_df[col].apply(
                lambda v: v if pd.isna(v) or \
                    any(v.endswith(pid) for pid in valid_pids) else pd.NA
            )

    return input_df


def sort_parents_first(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sorts the DataFrame so that parent objects come first, each followed by their
    children (recursively). Remaining records are added at the end.

    Args:
        df (pd.DataFrame): Input DataFrame containing PIDs, model types, and relationship fields.

    Returns:
        pd.DataFrame: Sorted DataFrame.
    """
    if "PID" not in df.columns or "RELS_EXT_hasModel_uri_ms" not in df.columns:
        return df

    df = df.copy()
    df["PID"] = df["PID"].fillna("")
    df = df.sort_values("PID").reset_index(drop=True)

    # Extract PID suffix if value has namespace
    def extract_pid(val):
        if pd.isna(val):
            return None
        return val.split("/")[-1] if "/" in val else val

    member_field = "RELS_EXT_isMemberOf_uri_ms" \
        if "RELS_EXT_isMemberOf_uri_ms" in df.columns else None
    constituent_field = "RELS_EXT_isConstituentOf_uri_ms" \
        if "RELS_EXT_isConstituentOf_uri_ms" in df.columns else None

    if member_field:
        df[member_field] = df[member_field].apply(extract_pid)
    if constituent_field:
        df[constituent_field] = df[constituent_field].apply(extract_pid)

    # Build parent -> children map
    children_map = {}
    pid_to_row = dict(zip(df["PID"], df.to_dict("records")))

    for _, row in df.iterrows():
        pid = row["PID"]
        model = row["RELS_EXT_hasModel_uri_ms"]
        if model in PARENT_MODELS:
            mask = pd.Series([False] * len(df), index=df.index)
            if member_field:
                mask |= df[member_field] == pid
            if constituent_field:
                mask |= df[constituent_field] == pid
            children = df.loc[mask, "PID"].dropna().tolist()
            children_map[pid] = children

    # Recursively collect ordered PIDs: parent -> children -> grandchildren, etc.
    visited = set()
    ordered_pids = []

    def visit(pid):
        if pid in visited or pid not in pid_to_row:
            return
        visited.add(pid)
        ordered_pids.append(pid)
        for child_pid in children_map.get(pid, []):
            visit(child_pid)

    # Visit all parents first
    for parent_pid in children_map.keys():
        visit(parent_pid)

    # Add remaining records (orphans/standalones)
    for pid in df["PID"]:
        if pid not in visited:
            ordered_pids.append(pid)

    return df.set_index("PID").loc[ordered_pids].reset_index()


def track_child_objects(
    row: pd.Series,
    input_df: pd.DataFrame,
    parent_pid: str | None,
    pending_children: list[str]
) -> tuple[str | None, list[str]]:
    """
    Updates parent-child tracking state based on the current row.

    If the current row represents a parent object, identifies its children from
    the DataFrame and returns the updated `parent_pid` and `pending_children`.
    If the current row is a child, removes it from the pending list.
    If all children have been processed, resets the parent context.

    Args:
        row (pd.Series): The current row being processed.
        input_df (pd.DataFrame): The full DataFrame being processed.
        parent_pid (str | None): The PID of the current parent object, if any.
        pending_children (list): List of child PIDs that still need to be processed.

    Returns:
        tuple: Updated (parent_pid, pending_children)
    """
    pid = row.get("PID")
    model_uri = row.get("RELS_EXT_hasModel_uri_ms")

    if pd.notna(model_uri) and model_uri in PARENT_MODELS:
        parent_pid = pid
        member_of_col = input_df.get("RELS_EXT_isMemberOf_uri_ms")
        constituent_of_col = input_df.get("RELS_EXT_isConstituentOf_uri_ms")

        # Identify children of the current parent
        mask = pd.Series([False] * len(input_df), index=input_df.index)

        if member_of_col is not None:
            mask |= (member_of_col == parent_pid)

        if constituent_of_col is not None:
            mask |= (constituent_of_col == parent_pid)

        pending_children = input_df.loc[mask, "PID"].dropna().tolist()

    elif parent_pid and pid in pending_children:
        # Remove child from pending list
        pending_children.remove(pid)
    elif parent_pid and not pending_children:
        # Reset parent context
        parent_pid = None

    return parent_pid, pending_children


def should_flush_batch(buffer: list, batch_size: int, pending_children: list) -> bool:
    """
    Determine whether the current batch should be flushed to disk.

    Args:
        buffer (list): List of processed records.
        batch_size (int): Maximum number of records in a batch.
        pending_children (list): List of child PIDs that must stay with the parent.

    Returns:
        bool: True if batch should be flushed, False otherwise.
    """
    return len(buffer) >= batch_size and not pending_children


def flush_batch(
    buffer: list,
    batch_count: int,
    output_path: str,
    file_prefix: str,
    batch_path: str,
    batch_dir: str
) -> tuple[pd.DataFrame, set]:
    """
    Write the current buffer to a CSV, save PIDs for media, and prepare a config file.

    Args:
        buffer (list): List of processed records.
        batch_count (int): Current batch number.
        output_path (str): Directory to write the batch CSV.
        file_prefix (str): Filename prefix for the batch.
        batch_path (str): Path to the batch directory.
        batch_dir (str): Name of the batch directory.

    Returns:
        tuple: DataFrame written and set of updated datastreams.
    """
    sub_batch_file = f"{file_prefix}_{batch_count}.csv"
    sub_batch_path = os.path.join(output_path, sub_batch_file)
    records_df = records_to_csv(buffer, sub_batch_path)

    batch_datastreams = save_pids_for_media(
        batch_path,
        records_df,
        DATASTREAMS_MAPPING
    )

    prepare_config(
        batch_path,
        batch_dir,
        batch_count,
        timestamp,
        user_id,
        batch_datastreams
    )

    return records_df, batch_datastreams


def initialize_record() -> dict:
    """
    Initialize a record with fields as empty dictionaries or lists.

    Returns:
        dict: A record with fields initialized appropriately.
    """
    record = {}
    for field in FIELDS.Field:
        if field in TITLE_FIELDS:
            record[field] = {}
        else:
            record[field] = []
    return record


def get_mapped_field(pid: str, solr_field: str, data: str) -> str | None:
    """
    Retrieve the mapped Islandora 2 machine field name for a given Solr field.

    Args:
        pid (str): The PID of the current record (used for logging).
        solr_field (str): The Solr field name to be mapped.
        data (str): The data value (used for logging if mapping is not found).

    Returns:
        str or None: The mapped field name, or None if no mapping exists.
    """
    match = FIELD_MAPPING.loc[
        FIELD_MAPPING["solr_field"] == solr_field, "machine_name"
    ]

    if match.empty:
        if solr_field not in UNMAPPED_FIELDS:
            add_exception(pid, solr_field, data, "could not find matching I2 field")
        return None

    return match.iloc[0]


def add_exception(
    pid: str, 
    field: str, 
    value: Union[str, List[str]], 
    exception: str
) -> None:
    """
    Add an exception record to the exceptions list.

    Args:
        pid (str): The PID of the record.
        field (str): The field where the exception occurred.
        value (Union[str, list]): The value associated with the exception.
        exception (str): A description of the exception.
    """
    exceptions.append({
        "File": current_file,
        "batch": current_batch,
        "PID": pid,
        "Field": field,
        "Value": value,
        "Exception": exception
    })


def add_transformation(
    pid: str,
    field: str,
    old_value: str,
    new_value: str,
    transformation: str
) -> None:
    """
    Add a transformation record to the transformations list.

    Args:
        pid (str): The PID of the record.
        field (str): The field where the transformation occurred.
        old_value (str): The original value.
        new_value (str): The transformed value.
        transformation (str): A description of the transformation.
    """
    transformations.append({
        "File": current_file,
        "batch": current_batch,
        "PID": pid,
        "Field": field,
        "Old_Value": old_value,
        "New_Value": new_value,
        "Transformation": transformation
    })


def split_and_clean(text: str) -> List[str]:
    """
    Split a string on non-escaped commas, remove escape characters, and filter out empty strings.

    Args:
        text (str): The input string to process.

    Returns:
        list: A list of cleaned string parts.
    """
    # Remove white spaces
    cleaned_text = remove_whitespaces(text)
    # Split on non-escaped commas
    parts = re.split(r"(?<!\\),", cleaned_text)
    # Remove escape characters and filter out empty strings
    return [part.replace("\\,", ",") for part in parts if part.strip()]


def concat(values: list) -> str:
    """
    Concatenate a list of values into a semicolon-separated string.

    Args:
        values (list): A list of strings to concatenate.

    Returns:
        str: A single string with non-empty values joined by a pipe ("|").
    """
    return ";".join(str(v).strip() for v in values if v and str(v).strip())


def remove_whitespaces(text: str) -> str:
    """
    Remove newline characters, trailing whitespaces, and multiple spaces from a string.

    Args:
        text (str): The input string to clean.

    Returns:
        str: A cleaned string.
    """
    if isinstance(text, str):
        cleaned_text = text.replace("\n    ", " ").replace("\n", "").strip()
        cleaned_text = re.sub(r"\s+", " ", cleaned_text)
        return cleaned_text.strip()
    return ""


def dedup(value: str) -> str:
    """
    Split a string on unescaped commas, deduplicate the resulting list, 
    and rejoin the unique values with a comma.

    Args:
        value (str): The input string with potential duplicates.

    Returns:
        str: A comma-separated string with unique values.
    """
    # Split on commas not preceded by a backslash
    parts = re.split(r'(?<!\\),', value)
    # Get unique parts of value
    unique_parts = list(dict.fromkeys(parts))
    return ','.join(unique_parts)


def validate_edtf_date(date: str) -> bool:
    """
    Validates if the given date string is in a valid EDTF (Extended Date/Time Format).
    """
    # Check if the date can be parsed as EDTF
    edtf_date = parse_edtf(date)
    if edtf_date:
        return True
    return False


def validate_edtf_dates(edtf_dates: List[str]) -> Tuple[List[str], List[str]]:
    """
    Validate a list of EDTF dates and separate them into valid and invalid lists.

    Args:
        edtf_dates (list): List of EDTF date strings.

    Returns:
        tuple: A tuple containing lists of valid and invalid EDTF dates.
    """
    valid_dates = []
    invalid_dates = []
    for date in edtf_dates:
        if date in {
            '18XX/', '184X/', '186X/1975~', '196X/', '197X/', '19XX/', '19XX/..'
         } or validate_edtf_date(date):
            valid_dates.append(date)
        else:
            invalid_dates.append(date)
    return valid_dates, invalid_dates


def get_parent_domain(df: pd.DataFrame, pid: str, parent_id: str) -> list[str]:
    """
    Retrieves the parent domain(s) for a given PID by matching its parent_id in 
    the DataFrame and mapping the domain URIs using the DOMAIN_MAPPING dictionary.

    Args:
        df (pd.DataFrame): The DataFrame containing metadata records.
        pid (str): The PID of the current record, used for error reporting.
        parent_id (str): The PID of the parent record to look up.

    Returns:
        list[str]: A list of mapped domain access values, or an empty list
                   if no values are found or an error occurs.
    """
    parent_domains = []
    try:
        match = df.loc[df["PID"] == parent_id, "RELS_EXT_isMemberOfSite_uri_ms"]
        if not match.empty:
            # Clean and split values, then map using DOMAIN_MAPPING
            raw_values = split_and_clean(match.values[0])
            parent_domains = [
                DOMAIN_MAPPING.get(val, "") \
                for val in raw_values if DOMAIN_MAPPING.get(val, "")
            ]
    except Exception as e:
        message = f"error retrieving parent domain: {e}"
        add_exception(pid, "field_domain_access", None, message)

    return parent_domains
   

def add_value(
    record: dict, 
    solr_field: str, 
    field: str, 
    value: str, 
    prefix: str = None
) -> str:
    """
    Add a value to a field in the record, optionally prepending a prefix.

    Args:
        record (dict): The record to update.
        solr_field (str): The Solr field name.
        field (str): The record field name.
        value (str): The value to add.
        prefix (str, optional): The prefix to prepend to the value.

    Returns:
        value (str): The value added to the record.
    """
    if not field:
        add_exception(
            record['id'][0],
            None,
            value, 
            f"missing I2 field for value from Solr field {solr_field}"
        )
        return record

    value = remove_whitespaces(value)
    values = record.get(field, [])

    if solr_field and (not prefix or prefix.startswith("rlt")):
        field_row = FIELD_MAPPING[FIELD_MAPPING['solr_field'] == solr_field]
        if not field_row.empty:
            prefix = prefix.replace("rlt", field_row.iloc[0]['prefix']) \
                if prefix else field_row.iloc[0]['prefix']

    if prefix:
        value = f"{prefix}{value}"

    if value and value not in values:
        values.append(value)

    record[field] = values

    return value


def add_title(
    record: dict, 
    solr_field: str, 
    field: str, 
    value: str
) -> dict:
    """
    Add a title value to the record based on the Solr field type.

    Args:
        record (dict): The record to update.
        solr_field (str): The Solr field name.
        field (str): The record field name.
        value (str): The value to add.

    Returns:
        dict: The updated record.
    """
    title = record[field]
    value = remove_whitespaces(value)

    if "nonSort" in solr_field:
        title['nonSort'] = value
    elif "subTitle" in solr_field:
        title['subTitle'] = value
    elif "partNumber" in solr_field:
        title['partNumber'] = value
    elif "partName" in solr_field:
        title['partName'] = value
    else:
        title['title'] = value

    return record


def add_source(
    record: dict, 
    solr_field: str, 
    field: str, 
    value: str
) -> dict:
    """
    Add a source value to the record by matching it in the source collection mapping.

    Args:
        record (dict): The record to update.
        solr_field (str): The Solr field name.
        field (str): The record field name.
        value (str): The value to match and add.

    Returns:
        dict: The updated record.
    """
    matching_rows = SOURCE_COLLECTION_MAPPING[
        SOURCE_COLLECTION_MAPPING[solr_field] == value
    ]
    
    if matching_rows.empty:
        add_exception(
            record['id'][0],
            field,
            value, 
            f"could not find value in column {solr_field}"
        )
        return record

    new_value = matching_rows.iloc[0][field]
    record[field].append(new_value)
    return record


def add_attributed_names(
    record: dict, 
    personal_names: dict
) -> dict:
    """
    Add personal names without a relator if they do not already exist in the record.

    Args:
        record (dict): The record to update.
        personal_names (dict): A dictionary containing sets of personal names:
            - 'no_relator': Personal names without a relator.
            - 'has_relator': Personal names with a relator.

    Returns:
        dict: The updated record with added attributed personal names, if any.
    """
    att_names = personal_names['no_relator'] - personal_names['has_relator']
    for name in att_names:
        add_value(
            record, 
            'mods_name_personal_namePart_ms', 
            'field_linked_agent', 
            f'person:{name}'
        )
    return record


def add_geo_field(record: dict, pid: str) -> dict:
    """
    Add geographic field values to a record based on a single matching row in GEO_FIELDS_MAPPING.

    Args:
        record (dict): The record being processed.
        pid (str): The PID to match against the "PID" column in GEO_FIELDS_MAPPING.

    Returns:
        dict: The updated record with mapped field values added.
    """
    matching_row = GEO_FIELDS_MAPPING.loc[GEO_FIELDS_MAPPING['PID'] == pid]

    if not matching_row.empty:
        row = matching_row.iloc[0]
        for field in GEO_FIELDS + ['field_subjects', 'field_subjects_name']:
            if field == "field_geographic_features_categories":
                field = "field_geographic_features"
            value = row.get(field)
            if value:
                prefix = "corporate_body:" if field == "field_subjects_name" else None
                for val in value.split("|"): 
                    add_value(record, None, field, val, prefix)

    return record


def process_parent_id(value: str) -> str | None:
    """
    Processes a parent ID by removing the "info:fedora/" prefix and mapping 
    collection IDs to their corresponding node IDs.

    If the value corresponds to a root PID, it is replaced with None. 
    If the value represents a collection, its node ID is added to the record 
    under "field_member_of."

    Args:
        record (dict): The record being processed.
        value (str): The original parent ID.

    Returns:
        str | None: The processed parent ID or None if the value corresponds 
                    to a root PID.
    """
    new_value = value.removeprefix("info:fedora/")
    if new_value in {"pitt:root", "islandora:root"}:
        return None
    return new_value


def process_collection_id(
    record: dict, 
    solr_field: str, 
    value: str
) -> str | None:
    """
    Processes a collection ID by mapping it to its corresponding node ID.

    If the collection ID is found in `COLLECTION_NODE_MAPPING`, the corresponding `node_id` is returned.
    Otherwise, an exception is logged, and `None` is returned.

    Args:
        record (dict): The record being processed, containing metadata fields.
        solr_field (str): The name of the Solr field being processed.
        value (str): The collection ID to be mapped.

    Returns:
        str | None: The corresponding node ID if found, otherwise None.
    """
    new_value = process_parent_id(value)
    matching_rows = COLLECTION_NODE_MAPPING.loc[
        COLLECTION_NODE_MAPPING["id"] == new_value, "node_id"
    ]

    node_id = matching_rows.iloc[0] if not matching_rows.empty else None

    if node_id is None:
        add_exception(
            record["id"][0],
            solr_field,
            value,
            f"Node not found for collection {value}"
        )

    return node_id


def process_model(
    record: dict, 
    solr_field: str, 
    value: str
) -> tuple[str | None, bool]:
    """
    Processes an object model by mapping it to a predefined type and determining 
    whether the row should be skipped.

    Args:
        record (dict): The record being processed.
        solr_field (str): The Solr field name associated with the value.
        value (str): The object model identifier.

    Returns:
        tuple[str | None, bool]: A tuple containing:
            - The mapped model type, or None if the value is invalid.
            - A boolean indicating whether the row should be skipped.
    """
    skip_row = value not in OBJECT_MAPPING

    if skip_row:
        add_transformation(
            record["id"][0], 
            solr_field, 
            value,
            None,
            "skipped object due to model type"
        )
        return None, skip_row

    # Get object type and model
    object_type = OBJECT_MAPPING[value]
    model_type = object_type["model"]

    # Add resource type based on the mapped model
    resource_type = object_type.get("resource_type")
    add_value(
        record, 
        solr_field, 
        "field_resource_type", 
        resource_type
    )

    # Add display hints based on model
    display_hint = DISPLAY_HINTS_MAPPING.get(model_type)
    add_value(
        record, 
        solr_field, 
        "field_display_hints", 
        display_hint
    )

    return model_type, skip_row


def process_title(record: dict) -> dict:
    """
    Process title fields in a record and generate a concatenated full title.

    Args:
        record (dict): The record being processed.

    Returns:
        dict: The updated record with processed title data.
    """
    for field in TITLE_FIELDS:
        title = record[field]

        if not title:
            record[field] = []
            continue

        title_str = ""

        if "nonSort" in title and title.get("nonSort"):
            title_str += title['nonSort'] + " "

        if "title" in title and title.get("title"):
            title_str += title['title']

        if "subTitle" in title and title.get("subTitle"):
            title_str += ": " + title['subTitle']

        if "partNumber" in title and title.get("partNumber"):
            title_str += ", " + title['partNumber']

        if "partName" in title and title.get("partName"):
            title_str += ", " + title['partName']

        record[field] = [title_str]

        if field == "field_full_title":
            add_value(record, None, "title", title_str)

    if not record.get("title") and record.get("field_model", [''])[0] != "Page":
        add_exception(record['id'][0], "title", None, "record missing title")

    return record


def process_language(pid: str, value: str) -> str:
    """
    Maps a language field code to its corresponding term name using LANGUAGE_MAPPING.
    If no match is found, logs an exception using `add_exception`.

    Args:
        pid (str): The PID associated with the record.
        value (str): The language field code to look up.

    Returns:
        str: The corresponding language term name if found, otherwise the original value.
    """
    matching_row = LANGUAGE_MAPPING.loc[
        LANGUAGE_MAPPING['field_code'] == value, "term_name"
    ]
    if matching_row.empty:
        add_exception(
            pid=pid,
            field="field_language",
            value=value,
            exception=f"No language term found for code: '{value}'"
        )
        return value
    return matching_row.iloc[0]


def process_country(pid: str, value: str) -> str:
    """
    Maps a country field code to its corresponding term name using COUNTRY_MAPPING.
    If no match is found, logs an exception using `add_exception`.

    Args:
        pid (str): The PID associated with the record.
        value (str): The country field code to look up.

    Returns:
        str: The corresponding country term name if found, otherwise the original value.
    """
    matching_row = COUNTRY_MAPPING.loc[
        COUNTRY_MAPPING['field_code_country'] == value, "term_name"
    ]
    if matching_row.empty:
        add_exception(
            pid=pid,
            field="field_place_published_pitt",
            value=value,
            exception=f"No country term found for code: '{value}'"
        )
        return value
    return matching_row.iloc[0]


def process_issuance(pid: str, solr_field: str, value: str) -> str:
    """
    Process the mode of issuance for a given record by mapping it to a predefined value.

    Args:
        pid (str): The persistent identifier of the record.
        solr_field (str): The name of the Solr field being processed.
        value (str): The original issuance value.

    Returns:
        str: The mapped issuance value if found; otherwise, the original value.

    Notes:
        - If the value is not found in `ISSUANCE_MAPPING`, an exception is logged,
          and the original value is returned.
    """
    new_value = ISSUANCE_MAPPING.get(value)

    if not new_value:
        add_exception(
            pid,
            solr_field,
            value,
            "could not find mode of issuance in mapping"
        )
        return value

    return new_value


def process_name(
    record: dict, 
    personal_names: dict, 
    solr_field: str, 
    field: str, 
    value: str
) -> Tuple[dict, dict]:
    """
    Process a linked agent and add the corresponding value to the record and personal names.

    Args:
        record (dict): The record to update.
        personal_names (dict): A dictionary tracking personal names, categorized into those with and without a relator.
            Keys:
                - 'no_relator': List of personal names without a relator.
                - 'has_relator': List of personal names with a relator.
        solr_field (str): The Solr field name corresponding to the value.
        field (str): The record field name to be updated.
        value (str): The value to process and map to a valid name.

    Returns:
        tuple[dict, dict]: 
            - Updated record with linked agent values added.
            - Updated personal_names dictionary reflecting changes.
    """
    matching_rows = NAME_MAPPING[
        (NAME_MAPPING['Solr_Field'] == solr_field) & 
        (NAME_MAPPING['Original_Name'] == value)
    ]

    if matching_rows.empty:
        add_exception(record['id'][0], solr_field, value, 
                      "could not find name in mapping")
        return record, personal_names

    for _, row in matching_rows.iterrows():
        name_type = LINKED_AGENT_TYPES.get(row['Type'], row['Type'])
        # note = row['Note']

        if row['Action'] == "remove":
            message = f"skipped {name_type} name '{value}'"
            # message = f"skipped {name_type} name '{value}'" \
                # + (f"- Note: {note}" if note else "")
            add_transformation(
                record['id'][0], 
                solr_field, 
                value, 
                None, 
                message
            )
            return record, personal_names

        new_value = row['Valid_Name']

        if name_type in ['title', 'geographic', 'topic']:
            if name_type == "title":
                field = "field_subject_title"
            elif name_type == "geographic":
                field = "field_geographic_subject"
            elif name_type == "topic":
                field = "field_subject"
            new_value = add_value(record, solr_field, field, new_value)
            add_exception(
                record['id'][0],
                field,
                new_value,
                f"confirm whether value should be a linked agent, {name_type} " + 
                "heading or other"
            )
            return record, personal_names
        
        relator = row['Relator'] if row['Relator'].strip() else "rlt"

        if relator != "rlt":
            relator = f"relators:{relator}:"
            personal_names['has_relator'].add(new_value)
        elif solr_field == "mods_name_personal_namePart_ms":
            personal_names['no_relator'].add(new_value)
            return record, personal_names
        elif "personal" in solr_field:
            personal_names['has_relator'].add(new_value)

        prefix = f"{relator}{name_type}:" \
            if name_type in LINKED_AGENT_TYPES.values() else "rlt"
        
        add_value(record, solr_field, field, new_value, prefix)

    return record, personal_names


def process_dates(record: dict) -> dict:
    """
    Process date-related fields in the record.

    Args:
        record (dict): The record to update.

    Returns:
        dict: The updated record.
    """
    pid = record['id'][0]
    matching_row = EDTF_DATES[EDTF_DATES['PID'] == pid]
    
    if matching_row.empty:
        return record

    dates = matching_row.iloc[0]
    edtf_dates = dates.get("field_edtf_date", "").split("|")
    cleaned_dates = dates.get("field_date", "").split("|")
    copyright_dates = dates.get("field_copyright_date", "").split("|")

    if edtf_dates == ['']:
        return record

    valid_dates, invalid_dates = validate_edtf_dates(edtf_dates)

    if invalid_dates:
        add_exception(pid, "field_date", invalid_dates, "found invalid dates")

    for date in valid_dates:
        add_value(record, None, "field_edtf_date", date)

    for date in cleaned_dates:
        add_value(record, None, "field_date_str", date)

    for date in copyright_dates:
        add_value(record, None, "field_copyright_date", date)

    return record


def process_subject(
    record: dict,
    solr_field: str,
    value: str
) -> dict:
    """
    Process the subject data in a record using mappings and rules.

    Args:
        record (dict): The record being processed.
        solr_field (str): The Solr (MODS) field to match in the Solr_Field column.
        value (str): The value to match in the Original_Heading column.

    Returns:
        dict: The updated record.
    """
    # Filter rows where Solr_Field matches mods_field and Original_Heading matches value
    matching_rows = SUBJECT_MAPPING[
        (SUBJECT_MAPPING['Solr_Field'] == solr_field) & 
        (SUBJECT_MAPPING['Original_Heading'] == value)
    ]

    if matching_rows.empty:
        add_exception(
            record['id'][0], 
            solr_field, 
            value,
            "could not find subject in mapping"
        )
        return record

    # Iterate through each filtered row
    for _, row in matching_rows.iterrows():
        # Use the value in the Type column as the key in SUBJECT_FIELD_MAPPING to get the field
        valid_heading = row.get('Valid_Heading')
        subject_type = row.get('Type')
        field = SUBJECT_FIELD_MAPPING.get(subject_type)
        note = row.get('Note')
        is_removal = row['Action'] == "remove"
        
        # Handle transformation notes and removal
        if note or is_removal:
            message = (
                f"skipped subject {subject_type} heading. Note: {note}"
                if is_removal else note
            )
            add_transformation(
                record['id'][0],
                solr_field,
                value,
                valid_heading,
                message
            )
            if is_removal:
                return record

        if field:
            # Use the value in the Valid_Heading column as the value
            new_value = row['Valid_Heading']
            prefix = LINKED_AGENT_TYPES.get(subject_type)
            prefix = f"{prefix}:" if prefix else prefix

            if row['Authority'] == "aat":
                field = "field_genre"
                prefix = "genre:"

            add_value(record, solr_field, field, new_value, prefix)

    return record


def process_genre(record: dict, value: str) -> dict:
    """
    Process genre data in a record using a mapping file.

    Args:
        record (dict): The record being processed.
        value (str): The genre value to match in the mapping.

    Returns:
        dict: The updated record with processed genre data.
    """
    matching_rows = GENRE_MAPPING[
        GENRE_MAPPING['mods_genre_authority_aat_ms'] == value
    ]

    if not matching_rows.empty:
        for _, row in matching_rows.iterrows():
            genre_value = row.get("field_genre")
            if genre_value:  # Ensure the value is not empty or None
                add_value(
                    record, 
                    None, 
                    "field_genre", 
                    genre_value, 
                    "genre:"
                )
    else:
        # Check GENRE_JP_MAPPING
        matching_rows = GENRE_JP_MAPPING[
            GENRE_JP_MAPPING['term_name'] == value
        ]

        if not matching_rows.empty:
            for _, row in matching_rows.iterrows():
                genre_value = row.get("term_name")
                if genre_value:
                    add_value(
                        record, 
                        None, 
                        "field_genre", 
                        genre_value, 
                        "genre_japanese_prints:"
                    )
        else:
            add_exception(
                record['id'][0],
                "mods_genre_authority_aat_ms",
                value,
                "could not find genre in mapping",
            )

    return record


def process_form(record: dict, value: str) -> dict:
    """
    Process form data in a record using a mapping file.

    Args:
        record (dict): The record being processed.
        value (str): The form value to match in the mapping.

    Returns:
        dict: The updated record with processed form data.
    """
    matching_rows = FORM_MAPPING[
        FORM_MAPPING['mods_physicalDescription_form_ms'] == value
    ]

    if not matching_rows.empty:
        for _, row in matching_rows.iterrows():
            form_value = row.get("field_physical_form")
            if form_value:
                for form in form_value.split("|"):
                    add_value(record, None, "field_physical_form", form)

            genre_value = row.get("field_genre")
            if genre_value:
                prefix = "genre:"
                for genre in genre_value.split("|"):
                    add_value(record, None, "field_genre", genre, prefix)

            extent_value = row.get("field_extent")
            if extent_value:
                for extent in extent_value.split("|"):
                    add_value(record, None, "field_extent", extent)
    else:
        add_exception(
            record['id'][0],
            "mods_physicalDescription_form_ms",
            value,
            "could not find form in mapping",
        )

    return record


def process_source(record: dict, source_data: dict) -> dict:
    """
    Process source data in a record using mappings and rules.

    Args:
        record (dict): The record being processed.
        source_data (dict): Source data to match in the mappings.

    Returns:
        dict: The updated record with processed source data.
    """
    if source_data:
        conditions = [
            SOURCE_COLLECTION_MAPPING[key] == value
            for key, value in source_data.items()
        ]
        if conditions:
            matching_rows = SOURCE_COLLECTION_MAPPING[
                np.logical_and.reduce(conditions)
            ]
            if not matching_rows.empty:
                row = matching_rows.iloc[0]
                source_fields = SOURCE_FIELDS + ['field_related_title_part_of']
                for source_field in source_fields:
                    source_value = row[source_field]
                    if source_field in row and source_value:
                        add_value(record, None, source_field, source_value)
            else:
                add_exception(
                    record['id'][0],
                    None,
                    source_data,
                    "could not find matching source collection data",
                )
    else:
        identifier = record.get("identifier")
        if identifier:
            matching_rows = SOURCE_COLLECTION_MISSING[
                SOURCE_COLLECTION_MISSING['PID'] == identifier
            ]
            if not matching_rows.empty:
                row = matching_rows.iloc[0]
                source_fields = SOURCE_FIELDS + ['field_related_title_part_of']
                for source_field in source_fields:
                    source_value = row[source_field]
                    if source_field in row and source_value:
                        add_value(record, None, source_field, source_value)
            else:
                add_exception(
                    record['id'][0],
                    None,
                    None,
                    "could not find source collection data for identifier",
                )

    return record


def process_rights(value: str) -> str:
    """
    Process rights data and map it to a new value.

    Args:
        value (str): The rights value to process.

    Returns:
        str: The mapped rights value.
    """
    new_value = ""
    if "http://rightsstatements.org/vocab/" in value:
        new_value = RIGHTS_MAPPING[value]
    return new_value


def validate_record(record: dict, df: pd.DataFrame) -> dict:
    """
    Validate the fields and values of a metadata record.

    Args:
        record (dict): A record to validate.

    Returns:
        record (dict): The validated record.
    """
    pid = record['id'][0]
    for field, values in record.items():
        match = FIELDS.loc[FIELDS['Field'] == field]
        if match.empty:
            print(f"⚠️  Warning: Field '{field}' not found in FIELDS lookup.")
            return 
        field_manager = match.iloc[0]
        
        if field_manager.Field_Type == "Text (plain)":
            for value in values:
                if len(value) > 255:
                    add_exception(
                        pid,
                        field,
                        value,
                        "value exceeds character limit",
                    )
        
        if field_manager.Field_Type == "Number (integer)":
            for value in values:
                try:
                    int_value = int(value)
                except (ValueError, TypeError):
                    add_exception(
                        pid,
                        field,
                        value,
                        f"Expected an integer, but got " +
                        f"{type(value).__name__}: {value}",
                    )

        if field_manager.Repeatable == "FALSE" and len(values) > 1:
            add_exception(
                pid,
                field,
                values,
                "multiple values in nonrepeatable field",
            )

    for field in REQUIRED_FIELDS:
        parent_id = record.get('parent_id')
        if parent_id:
            parent_id = parent_id[0]
            if field == 'title' and not record[field]:
                add_value(record, None, 'title', pid)
            elif field == 'field_domain_access' and not record[field]:
                parent_domains = get_parent_domain(df, pid, parent_id)
                for domain in parent_domains:
                    add_value(
                        record, 
                        None, 
                        'field_domain_access', 
                        domain
                    )
            elif field == 'field_member_of':
                continue
        if len(record[field]) < 1:
            add_exception(
                pid,
                field,
                None,
                f"record missing required {field}",
            )

    return record


def format_record(record: dict) -> dict:
    """
    Format the values of a metadata record by converting non-empty lists to 
    pipe-separated strings and removing keys with empty lists.

    This function is typically used as a final step before exporting records 
    to CSV or other flat formats, ensuring that list values are properly 
    serialized and empty fields are excluded.

    Args:
        record (dict): The metadata record with values that may be lists.

    Returns:
        dict: A cleaned and serialized version of the record with no empty lists 
              and list values converted to strings.
    """
    for field, values in list(record.items()):
        if isinstance(values, list):
            if values:
                record[field] = "|".join(values)
            else:
                del record[field]
    
    return record


def records_to_csv(records: list, destination: str):
    """
    Converts a list of dictionaries to a CSV file, dropping empty columns.
    If records contain parent-child relationships, ensures children inherit 
    the parent's domain access value where applicable.

    Args:
        records (list): List of dictionaries to convert.
        destination (str): Filepath for the output CSV file.

    Returns:
        pd.DataFrame: The resulting DataFrame written to CSV, or None if no records.
    """
    # Confirm there are records to save to CSV
    if not records:
        print(f"No records to save for {current_file}." + 
              "The output file will not be created.")
        return

    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame.from_dict(records)

    # Sort records so that parent objects are first
    if "parent_id" in df.columns:
        # Ensure that parent_id is empty for top-level object models
        parent_models = ['Paged Content', 'Compound Object', 'Newspaper']
        df.loc[
            df['field_model'].isin(parent_models), 'parent_id'
        ] = pd.NA

        df.sort_values(
            by="parent_id", 
            ascending=True, 
            na_position="first", 
            inplace=True
        )

    # Ensure the destination directory exists
    os.makedirs(os.path.dirname(destination), exist_ok=True)

    # Write the resulting DataFrame to a CSV file
    df.to_csv(destination, index=False, header=True, encoding='utf-8')

    # Report creation of processed CSV path
    formatted_path = destination.replace("\\", "/")
    print(f"\nCSV file saved to: {formatted_path}")

    return df


""" Key Functions """

def process_record(filename: str, row: dict) -> dict:
    """
    Process a CSV row dictionary into a transformed metadata record.

    Args:
        filename (str): Name of the CSV file to process.
        row (dict): Dictionary representing a CSV row.

    Returns:
        dict: Transformed metadata record.
    """
    # Initialize record
    record = initialize_record()
    personal_names = {"no_relator": set(), "has_relator": set()}
    source_data = {}
    geo_field = False

    # Add ID to record manually to ensure presence for logging
    pid = row["PID"]
    add_value(record, None, "id", pid)

    try:
        # Check if record has already been processed
        skip_record, inventory_file = check_record(filename, row)
        if skip_record:
            add_transformation(
                pid,
                None,
                None,
                None,
                f"skipped object included in {inventory_file}"
            )
            return None

        # Process values in each field
        for solr_field, data in row.items():
            # Confirm that Solr field is mapped and data exists in field
            field = get_mapped_field(pid, solr_field, data)
            if not field or pd.isna(data):
                continue

            # Preproccess values
            values = split_and_clean(data)

            for value in values:
                # Transform values that require remediation
                if field == "field_member_of":
                    value = process_collection_id(
                        record, solr_field, value
                    )
                elif field == "parent_id":
                    value = process_parent_id(value)
                elif field in TITLE_FIELDS:
                    record = add_title(record, solr_field, field, value)
                    continue
                elif field == "field_language":
                    value = process_language(pid, value)
                elif field == "field_place_published_pitt":
                    value = process_country(pid, value)
                elif field == "field_linked_agent":
                    record, personal_names = process_name(
                        record, personal_names, solr_field, field, value
                    )
                    continue
                elif field in DATE_FIELDS:
                    continue
                elif field == "field_mode_of_issuance":
                    value = process_issuance(pid, solr_field, value)
                elif field in SUBJECT_FIELDS:
                    record = process_subject(record, solr_field, value)
                    continue
                elif field == "field_genre":
                    record = process_genre(record, value)
                    continue
                elif field == "field_type_of_resource":
                    value = TYPE_MAPPING[value]
                elif field == "field_physical_form":
                    record = process_form(record, value)
                    continue
                elif field in GEO_FIELDS:
                    geo_field = True
                    continue
                elif field in SOURCE_FIELDS:
                    source_data[solr_field] = dedup(data)
                    continue
                elif field == "field_copyright_holder":
                    value = concat(values)
                elif field == "field_rights_statement":
                    value = process_rights(value)
                elif field == "field_model":
                    value, skip_record = process_model(
                        record, solr_field, value
                    )
                elif field == "field_domain_access":
                    value = DOMAIN_MAPPING.get(value, "")
                elif field == "field_preservica_date":
                    value = value[:10]

                # Add Solr data to I2 field
                if value and not skip_record:
                    add_value(record, solr_field, field, value)

            if skip_record:
                return None

        # Process title
        record = process_title(record)

        # Process dates
        record = process_dates(record)

        # Process source fields
        record = process_source(record, source_data)

        # Add attributed personal names
        record = add_attributed_names(record, personal_names)

        # Add map fields
        if geo_field:
            record = add_geo_field(record, pid)

    except Exception as e:
                print(f"Error processing row {pid}: {e}")
                print(traceback.format_exc())
                add_exception(pid, "row_error", "", str(e))

    return record


def process_files(
    progress_queue: Queue,
    tracker: ProgressTrackerCLI | ProgressTrackerGUI, 
    batch_path: str,
    batch_dir: str,
    output_path: str,
    file_prefix: str,
    batch_size: int
):
    """
    Process all CSV files in the input directory in batches.

    Args:
        progress_queue (Queue): Thread-safe queue for reporting progress.
        tracker (ProgressTrackerCLI | ProgressTrackerGUI): Instance of the ProgressTrackerCLI or ProgressTrackerGUI class to track progress.
        batch_path (str): Path to the batch directory containing the input CSV files.
        batch_dir (str): Name of batch directory. 
        output_path (str): Path to the directory where the processed output files will be saved.
        file_prefix (str): Prefix for output batch metadata files.
        batch_size (int): Number of records per batch.
    """
    try:
        # Load object inventory
        load_inventory()

        # Get list of valid CSV files in input folder (must be in inventory)
        valid_files = filter_valid_files(batch_path)

        # If there are no valid files, end processing
        if not valid_files:
            print("\nNo valid files to process. Quitting...")
            if TK_AVAILABLE:
                tracker.root.quit()
            return

        # Set total number of files for progress tracking
        progress_queue.put((tracker.set_total_files, (len(valid_files),)))

        buffer = []
        batch_count = 1
        datastreams = set()

        for filename in valid_files:
            try:
                global current_file
                current_file = filename
                global current_batch
                current_batch = batch_count

                # Read input CSV into a DataFrame
                cur_input_path = os.path.join(batch_path, filename)
                input_df = pd.read_csv(cur_input_path, dtype=str).\
                    replace("", pd.NA).\
                    dropna(axis=1, how="all")

                # Remove findingAid parents from their children's memberOf fields
                input_df = clean_parent_ids(input_df)
                input_df = sort_parents_first(input_df)

                # Update progress tracker with current file being processed
                progress_queue.put(
                    (tracker.set_current_file, (filename, len(input_df)))
                )

                # Initialize variables for parent-child tracking
                parent_pid = None
                pending_children = []

                # Process records
                for idx, row in input_df.iterrows():
                    if tracker.cancel_requested.is_set():
                        return

                    # Track parent and children relationships
                    parent_pid, pending_children = track_child_objects(
                        row, 
                        input_df, 
                        parent_pid, 
                        pending_children
                    )
                    
                    # Process record
                    record = process_record(filename, row)
                    if record:
                        record = validate_record(record, input_df)
                        record = format_record(record)
                        buffer.append(record)
                    else:
                        continue

                    # Update progress for processed record
                    if TK_AVAILABLE:
                        if idx % 10 == 0 or idx == input_df.index[-1]:
                            progress_queue.put(
                                (tracker.update_processed_records, ())
                            )
                    else:
                        is_last = (idx == input_df.index[-1])
                        progress_queue.put(
                            (tracker.update_processed_records, (is_last,))
                        )

                    # Complete batch if max size reached and no pending children
                    if should_flush_batch(buffer, batch_size, pending_children):
                        records_df, batch_datastreams = flush_batch(
                            buffer,
                            batch_count,
                            output_path,
                            file_prefix,
                            batch_path,
                            batch_dir
                        )
                        datastreams = datastreams.union(batch_datastreams)

                        # Set up next batch
                        batch_count += 1
                        buffer.clear()

            except Exception as e:
                print(f"Critical error in processing {filename}: {e}")
                print(traceback.format_exc())

            # Update progress
            progress_queue.put((tracker.update_processed_files, ()))

        if buffer:
            records_df, batch_datastreams = flush_batch(
                buffer,
                batch_count,
                output_path,
                file_prefix,
                batch_path,
                batch_dir
            )
            datastreams = datastreams.union(batch_datastreams)

        # Flush last record progress update before printing file saved message
        if not TK_AVAILABLE:
            while not progress_queue.empty():
                func, args = progress_queue.get()
                func(*args)

        # Create TXT file with drush export and workbench import scripts
        write_io_scripts(batch_path, batch_dir, datastreams)

        # Write reports
        log_dir = os.path.join(batch_path, "logs")
        write_reports(
            log_dir,
            timestamp,
            "metadata",
            transformations,
            exceptions
        )

    except Exception as e:
        print(f"Error during processing: {e}")
        print(traceback.format_exc())
        sys.exit(1)


""" Driver Code """

if __name__ == "__main__":
    # Initialize root variable
    root = None

    # Run file/record processing in a separate thread
    user_id, batch_path, batch_size = parse_arguments()

    try:
        if TK_AVAILABLE:
            # Set up tkinter window for GUI
            root = tk.Tk()
            root.withdraw()

            # Get user ID and batch directory if not provided yet
            if user_id is None:
                user_id = simpledialog.askstring("User ID Required", "Enter your user ID:")
            input_prompt = "Select Batch Folder with Input CSV Files"
        else:
            if user_id is None:
                user_id = input("Enter your Pitt user ID: ")
            input_prompt = "Enter Batch Folder with Input CSV Files"

        if batch_path is None:
            batch_path = get_directory('input', input_prompt, TK_AVAILABLE)
        print(f"\nProcessing batch directory: {batch_path}")

        # Get batch directory and timestamp for output files
        batch_dir = os.path.basename(batch_path.rstrip(os.sep))
        timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
        file_prefix = f"{batch_dir}_{timestamp}"

        # Set up batch directory
        setup_batch_directory(batch_path)

        # Set output directory path
        output_path = os.path.join(batch_path, "metadata")

        # Initialize progress tracker
        update_queue = Queue()
        tracker = ProgressTrackerFactory(root, update_queue)

        # Run file/record processing in a separate thread
        processing_thread = threading.Thread(
            target=process_files,
            args=(
                update_queue, 
                tracker, 
                batch_path, 
                batch_dir,
                output_path, 
                file_prefix, 
                batch_size
            )
        )
        processing_thread.start()

        if TK_AVAILABLE:
            # Schedule periodic processing of the GUI update queue and 
            root.after(25, process_queue, root, update_queue)

            # Start the Tkinter event loop
            root.mainloop()
        else:
            # In CLI mode, process the update queue in a loop
            while processing_thread.is_alive():
                while not update_queue.empty():
                    func, args = update_queue.get()
                    func(*args)
                time.sleep(0.05)

            # After processing thread ends, flush any remaining updates
            while not update_queue.empty():
                func, args = update_queue.get()
                func(*args)

    except Exception as e:
        print("An error occurred during execution:")
        print(traceback.format_exc())

