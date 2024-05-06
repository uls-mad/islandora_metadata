# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased] - 2024-05-06

### Changed
- Modularized `GUI` class and `process_xml()` function and helper functions so 
  they can be used in other scripts (now in their own Python files).
- Added states to control progression of Processor through steps.


### Added
- Added askyesno dialog box when a source or destination is not selected so that 
  program can continue without closing and re-running. 


## [Unreleased] - 2024-01-29

### Added
- Added exception logging feature:
    - reflects the number of files that were skipped due exceptions in the 
    progress reporting in the GUI dialog box
    - reports the files that were skipped due to exceptions and the traceback 
    of the exception in a CSV file
- The column header for container elements (e.g., name, subject, relatedItem)
  will now contain the authority attribute value, if any, for non-target fields

### Fixed
- Fixed bug that caused duplicate columns. 
    - `process_xml()` renames fields as they are added to the record.

