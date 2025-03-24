""" Modules """

# Import standard modules
import os
import threading
from queue import Queue
try:
    import tkinter as tk
    from tkinter import ttk
    TK_AVAILABLE = True
except ImportError:
    TK_AVAILABLE = False


""" Class """

class ProgressTrackerGUI:
    def __init__(self, root, update_queue: Queue):
        """
        Initialize the ProgressTracker GUI.

        Args:
            root (tk.Tk): The root tkinter window.
            update_queue (queue.Queue): Queue to handle thread-safe GUI updates.
        """
        self.root = root
        self.update_queue = update_queue
        self.root.deiconify()
        self.root.title("Solr to I2 Transformation Progress Tracker")

        # Tracking variables
        self.current_file = tk.StringVar(value="No file is being processed.")
        self.total_records = tk.IntVar(value=0)
        self.processed_records = tk.IntVar(value=0)
        self.total_files = tk.IntVar(value=0)
        self.processed_files = tk.IntVar(value=0)
        self.cancel_requested = threading.Event()

        # Derived variables
        self.files_progress_text = tk.StringVar()
        self.records_progress_text = tk.StringVar()
        self.update_progress_texts()

        # Create UI components
        self.create_widgets()

    def create_widgets(self):
        """
        Set up the GUI layout and widgets.
        """
        # Current file label
        tk.Label(self.root, text="Current File:").grid(
            row=0, column=0, sticky="w", padx=10, pady=5
            )
        self.file_label = tk.Label(
            self.root, 
            textvariable=self.current_file, 
            wraplength=400, 
            anchor="w", 
            justify="left"
        )
        self.file_label.grid(row=0, column=1, sticky="w", padx=10, pady=5)

        # Total files label
        tk.Label(self.root, text="Files Processed:").grid(
            row=1, column=0, sticky="w", padx=10, pady=5
            )
        self.files_label = tk.Label(
            self.root, 
            textvariable=self.files_progress_text, 
            anchor="w", 
            justify="left"
        )
        self.files_label.grid(row=1, column=1, sticky="w", padx=10, pady=5)

        # Total records label
        tk.Label(self.root, text="Records Processed:").grid(
            row=2, column=0, sticky="w", padx=10, pady=5
            )
        self.records_label = tk.Label(
            self.root, 
            textvariable=self.records_progress_text, 
            anchor="w", justify="left"
        )
        self.records_label.grid(row=2, column=1, sticky="w", padx=10, pady=5)

        # File progress bar
        tk.Label(self.root, text="Progress:").grid(
            row=3, column=0, sticky="w", padx=10, pady=5
            )
        self.progress_bar = ttk.Progressbar(
            self.root, orient="horizontal", length=300, mode="determinate"
        )
        self.progress_bar.grid(row=3, column=1, sticky="w", padx=10, pady=5)

        # Cancel button
        self.cancel_button = tk.Button(
            self.root, text="Cancel", command=self.cancel_process
            )
        self.cancel_button.grid(row=4, column=0, columnspan=2, padx=10, pady=10)

    def update_progress_texts(self):
        """
        Update the derived text for files and records.
        """
        self.files_progress_text.set(
            f"{self.processed_files.get()}/{self.total_files.get()}"
            )
        self.records_progress_text.set(
            f"{self.processed_records.get()}/{self.total_records.get()}"
            )

    def set_total_files(self, total_files):
        """
        Set the total number of files to process.

        Args:
            total_files (int): The total number of files to be processed.
        """
        self.total_files.set(total_files)
        self.processed_files.set(0)
        self.update_progress_texts()

    def set_current_file(self, current_file, total_records):
        """
        Set the current file and total records.

        Args:
            current_file (str): The name of the file being processed.
            total_records (int): Total number of records in the file.
        """
        self.current_file.set(current_file)
        self.total_records.set(total_records)
        self.processed_records.set(0)
        self.update_progress_texts()

        # Reset the progress bar
        self.progress_bar["value"] = 0

    def update_processed_records(self):
        """
        Update the number of processed records.
        """
        self.processed_records.set(self.processed_records.get() + 1)

        # Update the progress bar
        progress_percentage = (
            int((self.processed_records.get() / self.total_records.get()) * 100)
            if self.total_records.get() > 0
            else 0
        )
        self.progress_bar["value"] = progress_percentage
        self.update_progress_texts()

    def update_processed_files(self):
        """
        Update the number of processed files and close the window if all files are processed.
        """
        self.processed_files.set(self.processed_files.get() + 1)
        self.update_progress_texts()

        # Check if all files have been processed
        if self.processed_files.get() == self.total_files.get():
            print("All files have been processed.")
            self.root.quit()

    def cancel_process(self):
        """
        Signal to cancel the process and forcefully exit if needed.
        """
        self.cancel_requested.set()
        print("Processing canceled by the user.")
        self.root.destroy()
        os._exit(0)


class ProgressTrackerCLI:
    def __init__(self):
        """
        Initialize the CLI-based ProgressTracker.
        """
        # Tracking variables
        self.current_file = "No file is being processed."
        self.total_records = 0
        self.processed_records = 0
        self.total_files = 0
        self.processed_files = 0
        self.cancel_requested = threading.Event()

    def update_progress_texts(self):
        """ 
        Update and print progress for files and records.
        """
        print(
            f"\rFiles Processed: {self.processed_files}/{self.total_files} | " + 
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

    def update_processed_records(self):
        """
        Update the number of processed records.
        """
        self.processed_records += 1

        # Print progress on the same line, overwriting previous
        print(
            f"\rFiles Processed: {self.processed_files}/{self.total_files} | " +
            f"Records Processed: {self.processed_records}/{self.total_records}   ",
            end="",
            flush=True
        )

    def update_processed_files(self):
        """
        Update the number of processed files and print final message if done.
        """
        self.processed_files += 1

        # Final update (in case records finished before the file finished)
        print(
            f"\rFiles Processed: {self.processed_files}/{self.total_files} | " +
            f"Records Processed: {self.processed_records}/{self.total_records}   ",
            end="",
            flush=True
        )

        # If done, move to a new line and print summary
        if self.processed_files == self.total_files:
            print()  # Move to next line
            print("All files have been processed.")

    def cancel_process(self):
        """
        Signal to cancel the process and forcefully exit if needed.
        """
        self.cancel_requested.set()
        print("\nProcessing canceled by the user.")
        os._exit(0)


""" Factory Function """

def ProgressTrackerFactory(root, update_queue: Queue):
    """
    Returns the appropriate progress tracker (GUI or CLI) based on system capability.

    Args:
        root (tk.Tk) | None: The root tkinter window or None, if tkinter is not available.
        update_queue (queue.Queue): 

    Returns:
        ProgressTrackerGUI or ProgressTrackerCLI instance
    """
    if TK_AVAILABLE:
        return ProgressTrackerGUI(root, update_queue)
    else:
        return ProgressTrackerCLI()
