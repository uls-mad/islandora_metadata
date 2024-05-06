# Files Found Here
The files found within this directory are primarily python scripts created to work with archival metadata using the [Metadata Object Descriptive Schema](http://www.loc.gov/standards/mods/)(MODS).
These scripts were developed for particular use cases for [Digital Collections](https://digital.library.pitt.edu/) at Pitt. 

The ["mods2csv.py"](https://github.com/uls-mad/islandora_metadata/blob/main/MODS_Scripts/mods2csv.py) script will flatten MODS XML into a CSV spreadsheet. However, BE AWARE that this script prioritizes certain MODS fields and attributes. There are special fields that are captured differently than all other fields, which can be modified in the `process_xml()` and `update_columns()` functions in the ["process_xml"](https://github.com/uls-mad/islandora_metadata/blob/main/MODS_Scripts/process_xml.py) file. Prioritized fields have fieldnames that are stadardized according to a template in the output CSV file, which can be modified in the `columns` dictionary and `fieldnames` list in the ["definitions.py"](https://github.com/uls-mad/islandora_metadata/blob/main/MODS_Scripts/definitions.py) file. 

To run the script, make sure that you have all of the packages in ["requirements.txt"](https://github.com/uls-mad/islandora_metadata/blob/main/MODS_Scripts/requirements.txt)
installed. You can run `pip install -r requirements.txt`.

- Requires: `gui.py`, `process_xml.py`

The ["gui.py"](https://github.com/uls-mad/islandora_metadata/blob/main/MODS_Scripts/gui.py) file contains a class for managing a graphical user interface (GUI) application using Tkinter with an additional level of abstraction for ease of use. The `GUI` class provides methods for creating and managing GUI elements such as windows, frames, labels, buttons, and progress bars. The GUI class also includes methods for centering the window on the screen and running the Tkinter main event loop.
  
The ["process_xml.py"](https://github.com/uls-mad/islandora_metadata/blob/main/MODS_Scripts/process_xml.py) file contains classes and functions for processing XML files containing Metadata Object Description Schema (MODS) data. It provides functionality to parse XML documents, extract relevant information based on XPath expressions, and construct dictionaries representing MODS records. The ModsElement class represents individual MODS elements and provides methods for retrieving element values and attributes. Additionally, the file includes helper functions for handling namespace prefixes, checking special fields, and extracting specific types of MODS data. The `process_xml()` function serves as the main entry point, processing XML files to extract MODS metadata and returning the extracted data as a dictionary.
  

The ["definitions.py"](https://github.com/uls-mad/islandora_metadata/blob/main/MODS_Scripts/definitions.py) file contains a list of prioritized fields and a dictionary used to rename target columns in an output CSV file generated from processing XML files containing Metadata Object Description Schema (MODS) data.

- Priority Field List (fieldnames): This list prioritizes fields to be included in the output CSV file. Each field represents a specific attribute or element extracted from the MODS data, such as identifiers, titles, creators, subjects, descriptions, dates, genres, formats, publishers, and various source-related information.
- Column Renaming Dictionary (columns): This dictionary maps original XPath expressions to more readable column names in the output CSV file. It includes mappings for identifiers, titles, types of resources, publishers, publication places, dates, languages, descriptions, formats, extents, genres, and various source-related information.
- Namespaces Information (namespaces): This dictionary provides namespace prefixes and corresponding URIs for XPath expressions used in parsing MODS XML documents. It includes namespaces for MODS elements and copyright-related metadata.

Additionally, the file defines namespace prefixes for MODS elements (mods_ns) and copyright metadata (copyright_ns) to ensure proper parsing of XML documents.

The ["utilities.py"](https://github.com/uls-mad/islandora_metadata/blob/main/MODS_Scripts/utilities.py) file contains helper functions used in ["mods2csv.py"](https://github.com/uls-mad/islandora_metadata/blob/main/MODS_Scripts/mods2csv.py) and ["process_xml.py"](https://github.com/uls-mad/islandora_metadata/blob/main/MODS_Scripts/process_xml.py), and can be used in other scripts.

