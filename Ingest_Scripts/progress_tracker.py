#!/bin/python3 

"""
Provides progress tracking and CLI feedback for multi-file processing tasks.

This module contains the ProgressTracker class, which manages state for record-level
and file-level progress, supporting real-time updates and user cancellation in 
threaded environments.
"""

# --- Modules ---

# Import standard modules
import os
import threading


# --- Class ---

class ProgressTracker:
    """
    Track and display processing progress for files and records in a CLI environment.

    The tracker manages internal counters for files and records and uses terminal 
    escape characters to provide dynamic, in-place updates to the user.
    """

    def __init__(self):
        """
        Initialize the ProgressTracker with default tracking variables.
        """
        # Tracking variables
        self.current_file = "No file is being processed."
        self.total_records = 0
        self.processed_records = 0
        self.total_records_processed = 0
        self.total_files = 0
        self.processed_files = 0
        self.cancel_requested = threading.Event()

    def update_progress_texts(self):
        """Update and render current progress metrics to the terminal.
        
        This method prints the ratio of processed files and records to the standard
        output without moving to a new line.
        """
        print(
            f"Files Processed: {self.processed_files}/{self.total_files} | " + 
            f"Records Processed: {self.processed_records}/{self.total_records}", 
            end="", 
            flush=True
        )

    def set_total_files(self, total_files):
        """
        Set the total number of files expected for the current batch.

        Args:
            total_files (int): The total number of files to be processed.
        """
        self.total_files = total_files
        self.processed_files = 0
        print(f"\nTotal files to process: {self.total_files}")

    def set_current_file(self, current_file, total_records):
        """
        Update the tracker to reflect the file currently under process.
        

        Args:
            current_file (str): The name of the file being processed.
            total_records (int): Total number of records in the file.
        """
        self.current_file = current_file
        self.total_records = total_records
        self.processed_records = 0
        print(
            f"\nProcessing file: {self.current_file} " + 
            f"({self.total_records} records)"
        )

    def update_processed_records(self, is_last=False):
        """
        Increment the record count and refresh the CLI progress line.

        Args:
            is_last (bool): Whether this is the last record in the current file.
            If True, clears the line and moves to a new line to prepare for the 
            next file or process completion.
        """
        self.processed_records += 1
        self.total_records_processed += 1 

        print(
            f"\rRecords Processed: {self.processed_records}/{self.total_records}   ",
            end="",
            flush=True
        )

        # If this is the last record, clear line and move to a new line
        if is_last:
            print()

    def update_processed_files(self):
        """
        Increment the completed file count and display a summary of work done.
        
        Finalizes the output with a completion message if the processed file 
        count matches the total files.
        """
        self.processed_files += 1
        print(
            f"\rFiles Processed: {self.processed_files}/{self.total_files} | "
            f"Records Processed: {self.total_records_processed} total   ",
            end="\n",
            flush=True
        )

        if self.processed_files == self.total_files:
            print("\nAll files have been processed.")

    def cancel_process(self):
        """
        Signal a cancellation event and terminate the process immediately.
        
        Sets the internal threading event and uses os._exit to forcefully 
        shut down the script and all associated threads.
        """
        self.cancel_requested.set()
        print("\nProcessing canceled by the user.")
        os._exit(0)
