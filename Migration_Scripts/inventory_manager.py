""" Modules """

import pandas as pd
import os


"""  LISTS """

BATCH_INVENTORY_FIELDS = [
    'File',
    'Collection',
    'Collection_Count'
    'Batch_Count'
]

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
    'pitt_collection_373.csv'
    'null_collection_objects.csv',
]

PAGE_MODELS = [
    'info:fedora/islandora:pageCModel', 
    'info:fedora/islandora:manuscriptPageCModel',
    'info:fedora/islandora:newspaperPageCModel'
]

""" VARIABLES """

# Load or create inventory DataFrame
OBJECT_INVENTORY_FILE = "object_inventory.csv"

# Initialize inventory variables
object_inventory = None


""" FUNCTIONS """

def load_inventories():
    global object_inventory
    global batch_inventory
    if os.path.exists(OBJECT_INVENTORY_FILE):
        object_inventory = pd.read_csv(
            OBJECT_INVENTORY_FILE, dtype=str
        ).fillna("")
    else:
        object_inventory = pd.DataFrame(columns=OBJECT_INVENTORY_FIELDS)


def order_files(files: list) -> list:
    """
    Reorders a list of filenames by moving specified files to the end while preserving their order.

    This function separates files that are listed in the `COLLECTIONS_TO_HOLD` 
    constant and moves them to the end of the list while preserving the order 
    of both the remaining files and the held files.

    Args:
        files (list): A list of filenames.

    Returns:
        list: A reordered list with `COLLECTIONS_TO_HOLD` files at the end in their original order.
    """
    hold_files = [f for f in COLLECTIONS_TO_HOLD if f in files]
    other_files = [f for f in files if f not in COLLECTIONS_TO_HOLD]
    return other_files + hold_files


def process_parent_id(value: str) -> str:
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
    collection = process_parent_id(record['RELS_EXT_isMemberOfCollection_uri_ms'])
    object_model = process_parent_id(record['RELS_EXT_hasModel_uri_ms'])
    page = object_model in PAGE_MODELS
    skip = False

    if page:
        pid = record['RELS_EXT_isMemberOf_uri_ms'].replace('info:fedora/', '')

    if pid in object_inventory['PID'].values:
        row_index = object_inventory.index[object_inventory['PID'] == pid][0]
        inv_file = object_inventory.at[row_index, 'File']

        if inv_file == file:
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
                            value = process_parent_id(value)
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
def save_inventories():
    object_inventory.to_csv(OBJECT_INVENTORY_FILE, index=False)
