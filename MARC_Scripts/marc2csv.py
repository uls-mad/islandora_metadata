#!/usr/bin/python3

"""MARC to MODS Batch Processing Tool.

This script provides a graphical interface to batch-process MARC records
(in .mrc or .xml format) from a source directory. It converts records to 
MARCXML, applies an XSLT transformation to produce MODS, and extracts 
specific metadata into CSV files.

Dependencies:
    - lxml, pymarc, pandas, tqdm (External libraries)
    - process_xml.py (Local module)
    - marc2mods.xsl (XSLT stylesheet located in the script directory)

Usage:
    Run the script and follow the directory selection prompts.
    1. Select the input directory containing MARC files.
    2. Select the output directory for processed CSVs and error logs.
"""

# --- Modules ---

# Import standard modules
import os
from io import BytesIO
import tkinter as tk
from tkinter import filedialog, messagebox
from typing import List, Optional, Tuple

# Import third-party modules
import pandas as pd
from lxml import etree
from pymarc import MARCReader, Record, XMLWriter, parse_xml_to_array
from tqdm import tqdm

# Import local module
from process_xml import process_xml


def convert_record_to_marcxml(record):
    if not isinstance(record, Record):
        raise TypeError(f"Unexpected record type: {type(record)}")

    # Write the record to an in-memory byte stream
    record_stream = BytesIO()
    writer = XMLWriter(record_stream)
    writer.write(record)
    writer.close(close_fh=False)
    record_stream.seek(0)

    # Parse the full <collection> element that wraps the record
    try:
        collection_tree = etree.parse(record_stream)
    except etree.XMLSyntaxError as e:
        raise ValueError(f"Could not parse MARCXML record: {e}")

    # Extract and return the single <record> element
    record_element = collection_tree.find('.//{http://www.loc.gov/MARC21/slim}record')
    if record_element is None:
        raise ValueError("No <record> element found inside <collection>.")

    return record_element

def main():
    # ---- XSLT Setup ----
    script_dir = os.path.dirname(os.path.abspath(__file__))
    xslt_path = os.path.join(script_dir, "marc2mods.xsl")

    xsl_parser = etree.XMLParser(load_dtd=True, resolve_entities=True)
    with open(xslt_path, "rb") as f:
        xsl_tree = etree.parse(f, parser=xsl_parser, base_url=xslt_path)
    transform = etree.XSLT(xsl_tree)

    # ---- Tkinter File Dialogs ----
    root = tk.Tk()
    root.withdraw()

    input_dir = filedialog.askdirectory(title="Select Input Directory with MARC files")
    if not input_dir:
        messagebox.showerror("No Directory Selected", "Please select an input directory.")
        exit()

    output_dir = filedialog.askdirectory(title="Select Output Directory for CSV and Logs")
    if not output_dir:
        messagebox.showerror("No Directory Selected", "Please select an output directory.")
        exit()

    logs = []

    # ---- Process MARC files ----
    files = [f for f in os.listdir(input_dir) if f.endswith(('.mrc', '.xml'))]
    file_bar = tqdm(files, desc="Processing Files", unit="file")

    for file_name in file_bar:
        file_path = os.path.join(input_dir, file_name)
        base_name = os.path.splitext(file_name)[0]
        records = []

        if file_name.endswith(".mrc"):
            with open(file_path, "rb") as f:
                marc_records = list(MARCReader(f, to_unicode=True, force_utf8=True))
        elif file_name.endswith(".xml"):
            with open(file_path, "rb") as f:
                marc_records = parse_xml_to_array(f)
        else:
            continue

        record_bar = tqdm(marc_records, desc=f"Records in {file_name}", leave=False, unit="record")

        for i, record in enumerate(record_bar):
            try:
                # Convert the record to a MARCXML <record> element
                record_element = convert_record_to_marcxml(record)

                # Transform the <record> element using XSLT
                mods_result = transform(record_element)

                # Get the resulting root element
                if isinstance(mods_result, etree._XSLTResultTree):
                    mods_root = mods_result.getroot()
                elif isinstance(mods_result, etree._Element):
                    mods_root = mods_result
                else:
                    raise ValueError("XSLT transformation did not return a valid result tree.")

                if mods_root is None or not isinstance(mods_root, etree._Element):
                    raise ValueError("XSLT transformation failed.")

                #print(etree.tostring(mods_root, pretty_print=True, encoding="unicode"))

                # Process MODS to dict
                processed_record = process_xml(mods_root)
                if processed_record:
                    records.append(processed_record)

            except Exception as e:
                logs.append((file_name, i, str(e)))
                print(f"Error processing record {i} in {file_name}: {e}")


        # Save records to CSV
        if records:
            df = pd.DataFrame(records)
            output_csv = os.path.join(output_dir, f"{base_name}_processed.csv")
            df.to_csv(output_csv, index=False, encoding='utf-8')

    # ---- Save Logs ----
    if logs:
        log_df = pd.DataFrame(logs)
        log_path = os.path.join(output_dir, "excluded_data_log.csv")
        log_df.to_csv(log_path, index=False, encoding='utf-8')

    messagebox.showinfo(
        "Processing Complete", 
        "All MARC files have been processed successfully."
        )
    
if __name__ == "__main__":
    main()
