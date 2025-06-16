# Import standard modules
import argparse
import traceback
import os
import sys
from datetime import datetime
try:
    from tkinter import filedialog
    import tkinter as tk
    TK_AVAILABLE = True
except ImportError:
    TK_AVAILABLE = False

# Import third-party modules
from lxml import etree

# Import local modules
from file_utils import get_directory, write_reports


""" Global Variables """

global transformations
transformations = []

global exceptions
exceptions = []


""" Functions """

def parse_arguments():
    """
    Parse command-line arguments to retrieve the batch directory.

    Returns:
        tuple: batch_path (str | None)
    """
    parser = argparse.ArgumentParser(
        description="Process CSV files to add media filenames to metadata records."
    )
    parser.add_argument(
        "--batch_path",
        type=str,
        default=None,
        help="Path to a batch directory (default: will prompt if not provided)."
    )
    args = parser.parse_args()
    return args.batch_path


def add_exception(
    input_filename: str, 
    output_filename: str, 
    exception: str
) -> None:
    """
    Add an exception record to the exceptions list.

    Args:
        input_filename (str): The filename of the input media datastream.
        output_filename (str): The filename of the output media datastream.
        exception (str): A description of the exception.
    """
    exceptions.append({
        "Input_File": input_filename,
        "Output File": output_filename,
        "Exception": exception
    })


def add_transformation(
    input_filename: str, 
    output_filename: str, 
    transformation: str
) -> None:
    """
    Add an transformation record to the transformations list.

    Args:
        input_filename (str): The filename of the input media datastream.
        output_filename (str): The filename of the output media datastream.
        current_file (str): Name of the current CSV file being processed.
    """
    transformations.append({
        "Input_File": input_filename,
        "Output File": output_filename,
        "Transformation": transformation
    })


def extract_body_text(file_path: str) -> str:
    """
    Extracts plain text from the <body> element of an XHTML file.

    Args:
        file_path (str): Path to the input .shtml (XHTML) file.

    Returns:
        str: Concatenated text content from the <body> element, or an empty string if not found.
    """
    parser = etree.XMLParser(recover=True)
    tree = etree.parse(file_path, parser)
    ns = {'html': 'http://www.w3.org/1999/xhtml'}
    body = tree.find('.//html:body', namespaces=ns)
    if body is not None:
        return ''.join(body.itertext()).strip()
    return ''


def process_directory(input_dir: str, log_dir: str):
    """
    Processes all .shtml files in the given directory:
    - Extracts text from the <body> element of each XHTML file.
    - Saves the text to a corresponding .asc file with 'HOCR' replaced by 'OCR' in the filename.
    - Skips any file if the corresponding .asc file already exists.
    - Tracks progress and logs transformations and exceptions.

    Args:
        input_dir (str): Path to the directory containing .shtml files.
        log_fir (str): Path to the directory for logs.

    Returns:
        tuple: (transformations, exceptions)
            transformations (list): Records of processed or skipped files.
            exceptions (list): Records of any exceptions that occurred.
    """
    print("Analyzing HOCR and OCR files...")

    # Scan the directory once
    all_files = os.listdir(input_dir)

    # Cache base names of existing .asc files
    asc_files = {
        os.path.splitext(f)[0]
        for f in all_files
        if f.lower().endswith('.asc')
    }

    # Filter .shtml files without a matching .asc file
    shtml_files = [
        f for f in all_files
        if f.lower().endswith(('.shtml', '.xml'))
        and os.path.splitext(f.replace("HOCR", "OCR"))[0] not in asc_files
    ]

    total_files = len(shtml_files)

    # End processing if no SHTML files
    if not shtml_files:
        print("No missing OCR files to generate from HOCR.")
        return transformations, exceptions

    # Process SHTML files
    print("Generating OCR from HOCR...")
    for idx, filename in enumerate(shtml_files, start=1):
        input_path = os.path.join(input_dir, filename)

        # Construct output filename and path
        basename = filename.replace("HOCR", "OCR")
        output_filename = os.path.splitext(basename)[0] + '.asc'
        output_path = os.path.join(input_dir, output_filename)

        # Report progress
        print(
            f"[{idx}/{total_files}] Processing: {filename}".ljust(100), 
            end='\r', 
            flush=True
        )

        try:
            # Extract and save text
            text = extract_body_text(input_path)
            if text:
                with open(output_path, 'w', encoding="utf-8") as f:
                    f.write(text)
            else:
                add_transformation(
                    filename,
                    output_filename,
                    "skipping, HOCR file empty"
                )

        except Exception as e:
            add_exception(
                filename,
                output_filename,
                f"{e.__class__.__name__}: {str(e)}\n{traceback.format_exc()}"
            )
        
    # Report completion of processing
    print(
            f"Processed {total_files} HOCR files.".ljust(100), 
            end='\r', 
            flush=True
        )

    # Report exceptions, if any
    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    write_reports(log_dir, timestamp, "ocr", transformations, exceptions)


""" Driver Code """

if __name__ == "__main__":
    batch_path = parse_arguments()
    
    try:
        if TK_AVAILABLE:
            # Set up tkinter window for GUI
            root = tk.Tk()
            root.withdraw()
            input_prompt = 'Select Batch Folder with Input CSV Files'
        else:
            input_prompt = 'Enter Batch Folder with Input CSV Files'

        # Get directories and timestamp for file handling
        if batch_path is None:
            batch_path = get_directory('input', input_prompt, TK_AVAILABLE)
        print(f"\nProcessing batch directory: {batch_path}")
        media_dir = os.path.join(batch_path, "import", "media")
        log_dir = os.path.join(batch_path, "logs")

        # Process CSV files
        process_directory(media_dir, log_dir)

    except Exception as e:
        print(f"Unexpected error: {e}")
        print(traceback.format_exc())
        sys.exit(1)

    finally:
        if TK_AVAILABLE:
            # Close GUI window
            root.destroy()

        sys.exit(0)

