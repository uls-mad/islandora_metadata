# Files Found Here

The ["add_copyright_element.py"](https://github.com/uls-mad/islandora_metadata/blob/main/Heslin_Scripts/MODS_Scripts/add_copyright_element.py) script creates an <accessCondition> tag and a child <copyrightMD:copyright> tag with attributes "copyright.status" and "publication.status."
In our use case, we used this script to batch apply copyright status to archival objects missing this metadata field using a CSV spreadsheet. 
  
The ["add_identifier_element.py"](https://github.com/uls-mad/islandora_metadata/blob/main/Heslin_Scripts/MODS_Scripts/build_mods_for_books_dateCreated.py) functions in a similar way to the "add_copyright_element" but instead we were created an unique identifier for archival objects missing this field using a CSV spreadsheet. 

The ["build_mods_for_books_dateCreated.py"](https://github.com/uls-mad/islandora_metadata/blob/main/Heslin_Scripts/MODS_Scripts/build_mods_for_books_dateCreated.py) and ["build_mods_for_books_dateIssued.py"](https://github.com/uls-mad/islandora_metadata/blob/main/Heslin_Scripts/MODS_Scripts/build_mods_for_books_dateIssued.py) scripts allow us to extract exisiting data from our integrated library system (ILS), Alma, and reuse some of this metadata in MODS records. To work, these scripts require a CSV spreadsheet containing the fields you wish to collect in the MODS record (referenced in the script as "extended csv"), and a batch MODS XML file that contains MARC data for each object you wish to create a MODS file for (referenced in the script as "mods xml file").

# How to use "build_mods_for_books*.py" scripts

1. Download and save the file locally 
2. Open you terminal and invoke python (must be using python 3) followed by the full path to the downloaded script
3. You will be prompted by the script to supply the MODS xml file path. Make sure you supply the **FULL** path to the file. This should be an MODS xml file containing the metadata from an original MARC record **for all objects** you wish to create MODS for.
4. XML data will then be parsed
5. You will be prompted to supply the **FULL** path to the extended metadata CSV. 
6. You will then be promopted to supply the **FULL** path to a directory for where the first set of MODS should be saved.
7. Depending on the size of the batch, this may take several seconds. 
8. MODS files will be created, but the appropriate headers must be created. The script will prompt you to specify a new directory path. 
9. Once finished processing, check the second last directory specified for MODS files. Make sure they contain the data expected and the number of MODS files matches your expectation.
  