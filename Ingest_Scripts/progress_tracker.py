#!/usr/bin/env python3

"""Provide progress tracking and CLI feedback for processing tasks."""

# ---------------------------------------------------------------------------
# Modules
# ---------------------------------------------------------------------------

# Standard library imports
import os
import threading


# ---------------------------------------------------------------------------
# Classes
# ---------------------------------------------------------------------------

class ProgressTracker:
    """Track and display file and record progress in the CLI."""

    def __init__(self) -> None:
        """Initialize default progress counters."""
        self.current_file: str = "No file is being processed."
        self.total_records: int = 0
        self.processed_records: int = 0
        self.total_records_processed: int = 0
        self.total_files: int = 0
        self.processed_files: int = 0
        self.cancel_requested = threading.Event()

    def update_progress_texts(self) -> None:
        """Render current file and record progress to the terminal."""
        print(
            f"Files Processed: {self.processed_files}/{self.total_files} | "
            f"Records Processed: {self.processed_records}/{self.total_records}",
            end="",
            flush=True,
        )

    def set_total_files(self, total_files: int) -> None:
        """Set the total number of files expected for the current batch.

        Args:
            total_files: Total number of files to process.
        """
        self.total_files = total_files
        self.processed_files = 0

        print(f"\nTotal files to process: {self.total_files}")

    def set_current_file(
        self,
        current_file: str,
        total_records: int,
    ) -> None:
        """Set the current file and reset record-level counters.

        Args:
            current_file: Name of the file being processed.
            total_records: Total number of records in the current file.
        """
        self.current_file = current_file
        self.total_records = total_records
        self.processed_records = 0

        print(
            f"\nProcessing file: {self.current_file} "
            f"({self.total_records} records)"
        )

    def update_processed_records(self, is_last: bool = False) -> None:
        """Increment record progress counters and update terminal feedback.

        Args:
            is_last: Whether this is the final record in the current file.
        """
        self.processed_records += 1
        self.total_records_processed += 1

        print(
            f"\rRecords Processed: "
            f"{self.processed_records}/{self.total_records}   ",
            end="",
            flush=True,
        )

        if is_last:
            print()

    def update_processed_files(self) -> None:
        """Increment completed file count and display file-level progress."""
        self.processed_files += 1

        print(
            f"\rFiles Processed: {self.processed_files}/{self.total_files} | "
            f"Records Processed: {self.total_records_processed} total   ",
            end="\n",
            flush=True,
        )

    def cancel_process(self) -> None:
        """Signal cancellation and terminate the process immediately."""
        self.cancel_requested.set()
        print("\nProcess cancelled by user. Terminating immediately...")
        os._exit(1)
