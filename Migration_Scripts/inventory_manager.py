""" Modules """

# Import standard modules
import os
from pathlib import Path
import pandas as pd


""" CONSTANTS/VARIABLES """

OBJECT_INVENTORY_FIELDS = [
    'File',
    'PID',
    'mods_titleInfo_title_ms',
    'RELS_EXT_isMemberOfCollection_uri_ms',
    'RELS_EXT_hasModel_uri_ms',
    'fedora_datastream_info_HOCR_ID_ms',
    'fedora_datastream_info_JP2_ID_ms',
    'fedora_datastream_info_TRANSCRIPT_ID_ms',
    'Num_Pages'
]

COLLECTIONS_TO_HOLD = [
    'pitt_collection_49.csv',
    'pitt_collection_137.csv',
    'pitt_collection_370.csv',
    'pitt_collection_109.csv'
    'pitt_collection_160.csv'
    'pitt_collection_9.csv',
    'pitt_collection_159.csv',
    'pitt_collection_4.csv',
    'pitt_collection_5.csv',
    'pitt_collection_12.csv',
    'pitt_collection_8.csv',
    'pitt_collection_3.csv',
    'pitt_collection_190.csv',
    'pitt_collection_143.csv',
    'pitt_collection_111.csv',
    'pitt_collection_107.csv',
    'pitt_collection_2.csv',
    'pitt_collection_7.csv',
    'pitt_collection_241.csv',
    'pitt_collection_123.csv',
    'pitt_collection_153.csv',
    'pitt_collection_373.csv',
    'null_collection_objects.csv',
]

COLLECTIONS_TO_IGNORE = [
    'pitt_collection_131.csv',
    'pitt_collection_158.csv',
    'pitt_collection_165.csv',
    'pitt_collection_166.csv',
    'pitt_collection_167.csv',
    'pitt_collection_178.csv',
    'pitt_collection_189.csv',
    'pitt_collection_206.csv',
    'pitt_collection_208.csv',
    'pitt_collection_209.csv',
    'pitt_collection_210.csv',
    'pitt_collection_211.csv',
    'pitt_collection_220.csv',
    'pitt_collection_221.csv',
    'pitt_collection_222.csv',
    'pitt_collection_224.csv',
    'pitt_collection_227.csv',
    'pitt_collection_242.csv',
    'pitt_collection_248.csv',
    'pitt_collection_255.csv',
    'pitt_collection_260.csv',
    'pitt_collection_264.csv',
    'pitt_collection_265.csv',
    'pitt_collection_267.csv',
    'pitt_collection_268.csv',
    'pitt_collection_271.csv',
    'pitt_collection_272.csv',
    'pitt_collection_276.csv',
    'pitt_collection_277.csv',
    'pitt_collection_280.csv',
    'pitt_collection_284.csv',
    'pitt_collection_297.csv',
    'pitt_collection_301.csv',
    'pitt_collection_307.csv',
    'pitt_collection_348.csv',
    'pitt_collection_364.csv',
    'pitt_collection_371.csv',
    'pitt_collection_387.csv',
    'pitt_collection_39.csv',
    'pitt_collection_398.csv',
    'pitt_collection_406.csv',
    'pitt_collection_413.csv',
    'pitt_collection_414.csv',
    'pitt_collection_419.csv',
    'pitt_collection_424.csv',
    'pitt_collection_429.csv',
    'pitt_collection_439.csv',
    'pitt_collection_614.csv',
]

PAGE_MODELS = [
    'info:fedora/islandora:pageCModel', 
    'info:fedora/islandora:manuscriptPageCModel',
    'info:fedora/islandora:newspaperPageCModel'
]

# Load or create inventory DataFrame
OBJECT_INVENTORY_FILE = Path("Utility_Files") / "object_inventory.csv"

# Initialize inventory variables
object_inventory = None


""" FUNCTIONS """

def load_inventory():
    global object_inventory
    if os.path.exists(OBJECT_INVENTORY_FILE):
        object_inventory = pd.read_csv(
            OBJECT_INVENTORY_FILE, dtype=str
        ).fillna("")
    else:
        object_inventory = pd.DataFrame(columns=OBJECT_INVENTORY_FIELDS)


def order_files(files: list) -> list:
    """
    Reorders a list of filenames by moving specified files to the end while removing ignored files.

    This function:
    - Moves files listed in `COLLECTIONS_TO_HOLD` to the end while preserving their order.
    - Removes files listed in `COLLECTIONS_TO_IGNORE`.

    Args:
        files (list): A list of filenames.

    Returns:
        list: A reordered list with `COLLECTIONS_TO_HOLD` files at the end and `COLLECTIONS_TO_IGNORE` files removed.
    """
    migrated_files = [f for f in files if f not in COLLECTIONS_TO_IGNORE]
    hold_files = [f for f in COLLECTIONS_TO_HOLD if f in migrated_files]
    other_files = [f for f in migrated_files if f not in COLLECTIONS_TO_HOLD]
    
    return other_files + hold_files


def handle_parent_id(value: str) -> str:
    """
    Process a comma-separated string of values by removing Fedora prefixes, 
    deduplicating, sorting, and joining the values with a pipe ('|') separator.

    Args:
        value (str): A comma-separated string of values.

    Returns:
        str: A processed string with unique, sorted values joined by '|', 
             with any prefixes removed from values containing ':'.
    """
    # Deduplicate, sort, and split by comma
    values = sorted(set(value.split(",")))

    # Remove Fedora prefixes (everything before ':', if present)
    processed_values = [v.split(':')[-1] for v in values]

    # Rejoin values with a pipe separator
    return "|".join(processed_values)


def check_file(file: str) -> bool:
    """
    Checks whether a given file exists in the 'File' column of the `object_inventory` DataFrame.

    Args:
        file (str): The filename to check.

    Returns:
        bool: True if the file exists in `object_inventory`, False otherwise.
    """
    return file in object_inventory['File'].values


def check_record(file: str, record: pd.Series) -> bool:
    """
    Checks if a record should be skipped based on its PID and file association.

    This function determines whether a given record should be skipped by checking 
    if its PID (or parent PID for pages) exists in the global `object_inventory` 
    DataFrame. If the PID is found but is associated with a different file, 
    the record is marked to be skipped.

    Args:
        file (str): The filename currently being processed.
        record (pd.Series): A Pandas Series representing the record.

    Returns:
        bool: True if the record should be skipped, False otherwise.
    """
    global object_inventory
    pid = record['PID']
    object_model = handle_parent_id(record['RELS_EXT_hasModel_uri_ms'])
    skip = False

    if object_model in PAGE_MODELS:
        pid = record['RELS_EXT_isMemberOf_uri_ms'].replace('info:fedora/', '')

    matching_rows = object_inventory.loc[object_inventory['PID'] == pid]

    if not matching_rows.empty:
        inventory_file = matching_rows.iloc[0]['File']
        skip = (inventory_file != file)

    return skip


def handle_record(file: str, record: pd.Series):
    """
    Processes a single record from the dataset and updates the inventory DataFrame.
    
    Args:
        file (str): The file name associated with the record.
        record (pd.Series): A Pandas Series representing a single record.
    
    Modifies:
        - Updates or inserts the record in the inventory DataFrame.
    
    Returns:
        bool: True if the record should be skipped, False otherwise.
    """
    global object_inventory

    pid = record['PID']
    collection = handle_parent_id(record['RELS_EXT_isMemberOfCollection_uri_ms'])
    object_model = handle_parent_id(record['RELS_EXT_hasModel_uri_ms'])
    page = object_model in PAGE_MODELS
    skip = False

    if page:
        pid = record['RELS_EXT_isMemberOf_uri_ms'].replace('info:fedora/', '')

    if pid in object_inventory['PID'].values:
        row_index = object_inventory.index[object_inventory['PID'] == pid][0]
        inventory_file = object_inventory.at[row_index, 'File']

        if inventory_file == file:
            # If PID exists in inventory, update Num_Pages if it's a page
            if page:
                object_inventory.at[row_index, 'Num_Pages'] = int(
                    object_inventory.at[row_index, 'Num_Pages']
                ) + 1

            else:
                # Check if 'RELS_EXT_hasModel_uri_ms' is empty (NaN)
                if pd.isna(object_inventory.at[
                    row_index, 'RELS_EXT_hasModel_uri_ms'
                ]):
                    # Fill in remaining fields (not File, PID, and Num_Pages)
                    for field in OBJECT_INVENTORY_FIELDS[2:-1]:
                        value = record.get(field, None)
                        if 'RELS_EXT' in field:
                            value = handle_parent_id(value)
                        object_inventory.at[
                            row_index, field
                        ] = record.get(field, None)
        else:
            skip = True
    else:
        if page:
            new_entry = pd.DataFrame([{
                'File': file,
                'PID': pid,
                'Num_Pages': 1
            }])
            object_inventory = pd.concat(
                [object_inventory, new_entry], ignore_index=True
            )
        else:
            # Otherwise, add the full record but only keep the relevant fields
            fields = {
                field: record.get(field, None) \
                    for field in OBJECT_INVENTORY_FIELDS[2:-1]
            }
            fields['File'] = file
            fields['PID'] = pid
            fields['RELS_EXT_isMemberOfCollection_uri_ms'] = collection
            fields['RELS_EXT_hasModel_uri_ms'] = object_model
            fields['Num_Pages'] = 0
            new_entry = pd.DataFrame([fields])
            object_inventory = pd.concat(
                [object_inventory, new_entry], ignore_index=True
            )
    
    return skip


# Save the updated inventory to CSV
def save_inventory():
    object_inventory.to_csv(OBJECT_INVENTORY_FILE, index=False)
