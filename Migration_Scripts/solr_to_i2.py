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

DEFAULT_BATCH_SIZE = 5000


""" Helper Functions """

def parse_arguments():
    """
    Parse command-line arguments to retrieve the batch size.

    Returns:
        int: The number of records to process per batch.
    """
    parser = argparse.ArgumentParser(description="Process CSV files in batches.")
    parser.add_argument(
        "--batch_size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Number of records per batch (default: {DEFAULT_BATCH_SIZE})"
    )
    args = parser.parse_args()
    return args.batch_size


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


def get_mapped_field(pid: str, solr_field: str, data: str):
    """
    Retrieve the mapped target field for a Solr field.

    Args:
        pid (str): PID for logging purposes.
        solr_field (str): The Solr field to map.
        data (str): Field value for logging purposes.

    Returns:
        str or None: The mapped field name, or None if no mapping exists.
    """

    field = FIELD_MAPPING.loc[
                FIELD_MAPPING['solr_field'] == solr_field, "machine_name"
            ].iloc[0]

    # Confirm whether Solr field exists in migration field mapping
    if not field:
        # Report Solr field that is unaccounted for
        if solr_field not in UNMAPPED_FIELDS:
            add_exception(pid, solr_field, data,
                            "could not find matching I2 field")
            
    return field


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


def add_value(
    record: dict, 
    solr_field: str, 
    field: str, 
    value: str, 
    prefix: str = None
) -> dict:
    """
    Add a value to a field in the record, optionally prepending a prefix.

    Args:
        record (dict): The record to update.
        solr_field (str): The Solr field name.
        field (str): The record field name.
        value (str): The value to add.
        prefix (str, optional): The prefix to prepend to the value.

    Returns:
        dict: The updated record.
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

    return record


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
            record = add_value(record, None, "title", title_str)

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

        if name_type == "title":
            add_value(record, solr_field, "field_subject_title", new_value)
            return record, personal_names
        elif name_type == "geographic":
            add_value(record, solr_field, "field_geographic_subject", new_value)
            return record, personal_names
        elif name_type == "topic":
            add_value(record, solr_field, "field_subject", new_value)
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
        subject_type = row['Type']
        note = row.get('Note')
        field = SUBJECT_FIELD_MAPPING.get(subject_type)
        
        # If Action == "remove", skip processing and return the record
        if row['Action'] == "remove":
            message = f"skipped subject {subject_type} heading" \
                + (f". Note: {note}" if note else "")
            add_transformation(
                record['id'][0], 
                solr_field, 
                value, 
                None, 
                message)
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


def validate_record(record: dict) -> None:
    """
    Validate the fields and values of a record.

    Args:
        record (dict): The record to validate.

    Returns:
        None
    """
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
                        record['id'][0],
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
                        record["id"][0],
                        field,
                        value,
                        f"Expected an integer, but got " +
                        f"{type(value).__name__}: {value}",
                    )

        if field_manager.Repeatable == "FALSE" and len(values) > 1:
            add_exception(
                record['id'][0],
                field,
                values,
                "multiple values in nonrepeatable field",
            )

    for field in REQUIRED_FIELDS:
        constituent_object = record.get('parent_id')
        if field == "field_member_of" and constituent_object:
            continue
        if len(record[field]) < 1:
            add_exception(
                record['id'][0],
                field,
                None,
                f"record missing required {field}",
            )


def complete_record(record: dict) -> dict:
    """
    Finalize the record by converting list values to pipe-separated strings and 
    removing keys with empty lists.

    Args:
        record (dict): The record to finalize.

    Returns:
        dict: The finalized record with empty list values removed.
    """
    for field, values in list(record.items()):
        if isinstance(values, list):
            if values:  # If the list is not empty, convert it
                record[field] = "|".join(values)
            else:  # If the list is empty, remove the key
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

    if "parent_id" in df.columns:
        # Sort records so that parent objects are first
        df.sort_values(
            by="parent_id", 
            ascending=True, 
            na_position="first", 
            inplace=True
        )

        # Ensure that children have the same domain access as their parents
        parent_models = ['Paged Content', 'Publication Issue']

        # Get parent records with eligible models and non-null domain access
        parent_df = df[
            df["field_model"].isin(parent_models) &
            df["field_domain_access"].notna() &
            df["id"].notna()
        ][["id", "field_domain_access"]].rename(columns={"id": "parent_id"})

        # Merge domain access from parents into child records where missing
        df = df.merge(
            parent_df, on="parent_id", how="left", suffixes=('', '_from_parent')
        )

        # Fill in missing child domain access values with parent's value
        df["field_domain_access"] = df["field_domain_access"].combine_first(
            df["field_domain_access_from_parent"]
        )

        # Drop the temporary column
        df.drop(columns=["field_domain_access_from_parent"], inplace=True)

    # Ensure the destination directory exists
    os.makedirs(os.path.dirname(destination), exist_ok=True)

    # Write the resulting DataFrame to a CSV file
    df.to_csv(destination, index=False, header=True, encoding='utf-8')

    # Report creation of processed CSV path
    formatted_path = destination.replace("\\", "/")
    print(f"CSV file has been saved to {formatted_path}")

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
        skip_record = check_record(filename, row)
        if skip_record:
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
                elif field == "field_type_of_resources_legacy":
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

        # Validate record
        validate_record(record)

        # Complete record
        record = complete_record(record)

    except Exception as e:
                print(f"Error processing row {pid}: {e}")
                print(traceback.format_exc())
                add_exception(pid, "row_error", "", str(e))

    return record


def process_files(
    progress_queue: Queue,
    tracker: ProgressTrackerCLI | ProgressTrackerGUI, 
    input_dir: str,
    output_dir: str,
    batch_size: int
):
    """
    Process all CSV files in the input directory in batches.

    Args:
        progress_queue (Queue): Thread-safe queue for reporting progress.
        tracker (ProgressTrackerCLI | ProgressTrackerGUI): Instance of the ProgressTrackerCLI or ProgressTrackerGUI class to track progress.
        input_dir (str): Path to the directory containing the input CSV files.
        output_dir (str): Path to the directory where the processed output files will be saved.
        batch_size (int): Number of records per batch.
    """
    # Get list of CSV files in input folder
    files = [f for f in order_files(os.listdir(input_dir)) if f.endswith('.csv')]

    # Set total number of files for progress tracking
    progress_queue.put((tracker.set_total_files, (len(files),)))

    # Get batch directory name and timestamp for output files
    batch_dir_name = os.path.basename(input_dir.rstrip(os.sep))
    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    output_fn_base = f"{batch_dir_name}_{timestamp}"

    # Load/Initialize inventory for processed records
    load_inventory()

    buffer = []
    batch_count = 1

    try:
        for filename in files:
            try:
                global current_file
                current_file = filename

                # Read input CSV into a DataFrame
                input_path = os.path.join(input_dir, filename)
                input_df = pd.read_csv(input_path, dtype=str).\
                    replace("", pd.NA).\
                    dropna(axis=1, how="all")

                # Update progress tracker with current file being processed
                progress_queue.put(
                    (tracker.set_current_file, (filename, len(input_df)))
                )

                # Process records
                for idx, row in input_df.iterrows():
                    if tracker.cancel_requested.is_set():
                        return

                    record = process_record(filename, row)
                    if record:
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

                    # Complete batch if max size reached
                    if len(buffer) == batch_size:
                        # Save records to a CSV file in the output folder 
                        batch_filename = f"{output_fn_base}_{batch_count}.csv"
                        batch_filepath = os.path.join(output_dir, batch_filename)
                        records_df = records_to_csv(buffer, batch_filepath)

                        # Save PIDs for media export
                        save_pids_for_media(
                            input_dir,
                            records_df, 
                            DATASTREAMS_MAPPING
                        )

                        # Set up next batch
                        batch_count += 1
                        buffer.clear()

            except Exception as e:
                print(f"Critical error in processing {filename}: {e}")
                print(traceback.format_exc())

            # Update progress
            progress_queue.put((tracker.update_processed_files, ()))

        if buffer:
            # Save records to a CSV file in the output folder 
            batch_filename = f"{output_fn_base}_{batch_count}.csv"
            batch_filepath = os.path.join(output_dir, batch_filename)
            records_df = records_to_csv(buffer, batch_filepath)

            # Save PIDs for media export
            save_pids_for_media(input_dir, records_df, DATASTREAMS_MAPPING)
        
        # Flush last record progress update before printing file saved message
        if not TK_AVAILABLE:
            while not progress_queue.empty():
                func, args = progress_queue.get()
                func(*args)

    except Exception as e:
        print(f"Error during processing: {e}")
        print(traceback.format_exc())
        sys.exit(1)
    
    # Write reports
    log_dir = os.path.join(input_dir, "logs")
    write_reports(log_dir, timestamp, "metadata", transformations, exceptions)


""" Driver Code """

if __name__ == "__main__":
    # Initialize root variable
    root = None

    try:
        if TK_AVAILABLE:
            # Set up tkinter window for GUI
            root = tk.Tk()
            root.withdraw()

            # Set prompt for user
            prompt = 'Select Batch Folder with Input CSV Files'
        else:
            prompt = 'Enter Batch Folder with Input CSV Files'

        # Initialize progress tracker
        update_queue = Queue()
        tracker = ProgressTrackerFactory(root, update_queue)

        # Get batch directory
        batch_dir = get_directory('input', prompt, TK_AVAILABLE)
        print(f"\nProcessing batch directory: {batch_dir}")

        # Set up batch directory
        setup_batch_directory(batch_dir)

        # Set output directory
        output_dir = os.path.join(batch_dir, "metadata")

        # Run file/record processing in a separate thread
        batch_size = parse_arguments()

        # Run file/record processing in a separate thread
        processing_thread = threading.Thread(
            target=process_files,
            args=(update_queue, tracker, batch_dir, output_dir, batch_size)
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

