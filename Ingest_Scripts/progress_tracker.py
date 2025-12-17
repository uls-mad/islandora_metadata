""" Modules """

# Import standard modules
import os
import threading


""" Class """

class ProgressTracker:
    def __init__(self):
        """
        Initialize the CLI-based ProgressTracker.
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
        """ 
        Update and print progress for files and records.
        """
        print(
            f"Files Processed: {self.processed_files}/{self.total_files} | " + 
            f"Records Processed: {self.processed_records}/{self.total_records}", 
            end="", 
            flush=True
        )

    def set_total_files(self, total_files):
        """
        Set the total number of files to process.

        Args:
            total_files (int): The total number of files to be processed.
        """
        self.total_files = total_files
        self.processed_files = 0
        print(f"\nTotal files to process: {self.total_files}")

    def set_current_file(self, current_file, total_records):
        """
        Set the current file and total records.

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
        Update the number of processed records.

        Args:
            is_last (bool): Whether this is the last record in the current file.
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
        Update the number of processed files and print final message if done.
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
        Signal to cancel the process and forcefully exit if needed.
        """
        self.cancel_requested.set()
        print("\nProcessing canceled by the user.")
        os._exit(0)
        