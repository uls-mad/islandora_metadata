# External packages
import sys
from tkinter import filedialog, messagebox
from tkinter import *
from datetime import datetime
import traceback
import os
import glob
import zipfile
import pandas as pd

# Local packages
from gui import GUI
from process_xml import *
from utilities import get_pid
from definitions import fieldnames


""" CLASSES """

# Define states for the processor
class State:
    INIT = 0
    GET_SOURCE = 1
    GET_DESTINATION = 2
    PROCESSING = 3

class Processor:
    """
    Represents a processor for converting MODS files to CSV format.

    This class manages the processing steps involved in converting MODS files
    to CSV format. It interacts with a GUI for user input and feedback.

    Attributes:
        gui (GUI): An instance of the GUI class for displaying user interface elements.
        state (State): An enum representing the current state of the processor.
        files (list): A list of file paths to be processed.
        total_files (int): The total number of files to be processed.
        progress (int): The current progress of processing (number of files processed).
        progress_var (DoubleVar): A Tkinter variable for updating the progress bar.
        progress_label (int): A label indicating the progress of processing.
        processing_label (Label): A label indicating the processing status.
        complete_label (Label): A label indicating when processing is complete.
        exceptions_label (Label): A label indicating exceptions occurred during processing.
        close_button (Button): A button for closing the GUI window.
        source (str): The input source directory or file (ZIP).
        destination (str): The output destination file (CSV).
        records (list): A list of dictionaries representing processed records.
        exceptions (list): A list of dictionaries representing exceptions encountered during processing.

    Methods:
        __init__(root): Initializes the Processor instance with a GUI.
        update_state(state): Updates the state of the processor.
        run(): Manages the processing steps based on the current state.
        get_source(): Prompts the user to select the input source type.
        get_source_by_type(source_type): Handles input source selection based on the specified type.
        get_destination(): Prompts the user to select the output destination.
        start_processing(): Initiates the processing of files.
        manage_processor(): Manages the processing of individual files.
        log_exceptions(): Logs any exceptions that occurred during processing.
    """

    gui = None
    state = State.INIT
    files = []
    total_files = 0
    progress = 0
    progress_var = None
    progress_label = 0
    processing_label = None
    complete_label = None
    exceptions_label = None
    close_button = None
    source = None
    destination = None
    records = []
    exceptions = []

    def __init__(self, root):
        self.gui = GUI(root=root, 
                       title="MODS to CSV Converter", 
                       dimensions="300x110")
        self.gui.center_window()

    def update_state(self, state):
        self.state = state

    def run(self):
        if self.state == State.INIT:
            self.update_state(State.GET_SOURCE)
            self.get_source()
        
        elif self.state == State.GET_SOURCE:
            self.update_state(State.GET_DESTINATION)
            self.get_destination()

        elif self.state == State.GET_DESTINATION:
            self.update_state(State.PROCESSING)
            self.start_processing()

    def get_source(self):
        # Create text frame and add text to prompt user for source type
        self.gui.add_text_frame()
        self.gui.add_label("Do you need to import a Zip file or a folder?")

        # Create button frame and add buttons
        self.gui.add_button_frame()
        self.gui.add_button(text="Zip file", side=LEFT, padx=5,
                            command=lambda: self.get_source_by_type('Zip file'))
        self.gui.add_button(text="Folder", side=RIGHT, padx=5,
                            command=lambda: self.get_source_by_type('folder'))
        
    def get_source_by_type(self, source_type: str):
        # Update dialog box content
        self.gui.set_geom("300x65")
        self.gui.reset_text_frame()
        self.gui.add_label(f"Select an input {source_type}.")
        self.gui.reset_button_frame()
        
        # Set source       
        if source_type == 'Zip file':
            file = filedialog.askopenfilename(title='Select Input File')
            if file:
                self.gui.reset_text_frame()
                self.gui.add_label("Unzipping files...")
                self.source = extract_files(file)
        else:
            self.source = filedialog.askdirectory(title='Select Input Folder')

        # Check if a source was selected
        if not self.source:
            # If no source selected, ask the user if they want to proceed
            proceed = messagebox.askyesno("Input source required", 
                                          f"You did not select a {source_type}. " 
                                          + "Do you want to proceed?")
            if proceed:
                # If the user wants to proceed, call the run() function again
                self.get_source_by_type(source_type)
            else:
                # If the user doesn't want to proceed, terminate the app
                self.gui.close()
                return

        self.run()
        
    def get_destination(self):
        self.gui.set_geom("300x65")
        self.gui.reset_text_frame()
        self.gui.add_label("Select an output destination.")
        file_types = [('CSV UTF-8', '*.csv')]
        self.destination = filedialog.\
            asksaveasfilename(parent=self.gui.root, title="Save Output As", 
                              filetypes=file_types, defaultextension=file_types)
        
        # Check if a source was selected
        if not self.destination:
            # If no source selected, ask the user if they want to proceed
            proceed = messagebox.askyesno("Output destination required",
                                          "You did not select an output " + 
                                          "destination. Do you want to proceed?"
                                          )
            if proceed:
                # If the user wants to proceed, call the run() function again
                self.get_destination()
            else:
                # If the user doesn't want to proceed, terminate the app
                self.gui.close()
                return
        
        self.gui.remove_text_frame()
        self.gui.remove_button_frame()

        self.run()

    def start_processing(self):
        # Get files to be processed
        self.files = get_files(source=self.source)
        self.total_files = len(self.files)

        # Update GUI
        self.gui.set_title("Processing Files")
        self.gui.set_geom('300x155')
        self.gui.root.attributes("-topmost", True)

        # Create a progress bar
        self.progress_var = DoubleVar()
        self.progress_bar = self.gui.add_progres_bar(max=100, len=200, 
                                                     var=self.progress_var)

        # Label to display processing status
        self.complete_label = self.gui.add_label("Processing...", pady=0,
                                                 text_frame=False)

        # Create and place processed label
        self.processing_label = self.gui.add_label("0/0", pady=False,
                                                   text_frame=False)
        # self.processing_label.pack()
        # self.processing_label.update_idletasks() 

        # Create close button for GUI window
        self.gui.add_button_frame()
        self.gui.add_button("Cancel", side=RIGHT, pady=10, 
                            command=self.gui.close)

        # Update root to display components
        self.gui.root.update_idletasks() 

        self.manage_processor()

    def manage_processor(self):
        if self.progress < self.total_files:
            file = self.files[self.progress]
            try:
                # Process the MODS file
                record = process_xml(file)
                self.records.append(record)
            except:
                # Log the exception for the skipped file
                tb = reformat_traceback(traceback.format_exc())
                self.exceptions.append({'File': file, 'Traceback': tb})
                self.records.append({'identifier': get_pid(file)})
                self.progress_label -= 1

            # Update progress
            self.progress += 1
            self.progress_label += 1
            self.progress_var.set(int((self.progress / self.total_files) * 100))

            # Update processed label
            text = f"{self.progress_label}/{self.total_files} files"
            self.processing_label.config(text=text)
            self.processing_label.update_idletasks() 

            # Schedule the next file processing
            self.gui.root.after(1, self.manage_processor)
        else:
            # Notify user that processing is complete
            records_to_csv(records=self.records, destination=self.destination)
            self.complete_label.config(text="Complete!")
            if self.exceptions:
                self.log_exceptions()
                self.gui.set_geom('300x180')
                exception_msg = f"{len(self.exceptions)} exceptions occurred."
                self.exceptions_label = Label(self.gui.root, text=exception_msg)
                self.exceptions_label.pack()
            self.gui.reset_button_frame()
            self.gui.add_button("OK", side=RIGHT, pady=10, 
                                command=self.gui.close)
    
    def log_exceptions(self):
        # Create or append to a text file with exception information
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = f'exceptions_{current_datetime}.csv'
        exceptions_df = pd.DataFrame.from_dict(self.exceptions)
        exceptions_df.to_csv(filepath, index=False, encoding='utf-8')


""" Helper Functions """

# Show a given error and exit program
def show_error(title: str, message: str):
    messagebox.showerror(title=title, message=message)
    sys.exit(0)
    

# Extract files from compressed file (Zip) into a directory
def extract_files(filepath: str):
    output_dir = ""
    
    # Confirm that file is zip file
    if zipfile.is_zipfile(filepath):
        directory = os.path.dirname(filepath)
        filename = os.path.basename(filepath)
        output_dir = '%s\\%s' % (directory, os.path.splitext(filename)[0])

        # Extract files to output_dir
        with zipfile.ZipFile(filepath, 'r') as zip_archive:
            zip_archive.extractall(output_dir)
    else:
        # Display file format error
        show_error(title="Invalid File Format", 
                   message="Input file must be a ZIP file (*.zip). " + 
                   "Run the program and try again.")
    
    return output_dir


# Get list of files to be processed
def get_files(source):
    # Change working directory to source directory
    os.chdir(source)
    # Get list of XML files in directory
    files = glob.glob('*.xml')
    # Remove finding aids from list of files
    files = remove_finding_aids(files)
    return files


# Remove finding aids from input files based on filename patterns
def remove_finding_aids(files: list):
    fa_patterns = ['666980084','clp.','mss.','qss','rg04.201','ppi','us-qqs']
    files_to_remove = []

    # Generate list of finding aids identified by a finding aid filename pattern
    for filename in files:
        if any(pattern in filename.lower() for pattern in fa_patterns):
            files_to_remove.append(filename)

    # Remove finding aids from list of files
    for file in files_to_remove:
        files.remove(file)

    return files


# Modify column headers and add URL column for final output
def update_columns(df: pd.DataFrame):
    # Sort DataFrame by column name (alphabetical, ascending order)
    df = df[sorted(df.columns)]

    # Rename columns: Split each string on forward slashes, reverse the order, 
    # and rejoin. Replace at symbols with forward slash and spaces with underscore
    # Ex: physicalDescription/form@marcform >> form/marcform/physicalDescription
    df.columns = [
        '/'.join(header.split('/')[::-1]).replace('@', '/').replace(' ', '_') 
        for header in df.columns.tolist()
    ]
    
    # Add columns not in target fields
    for fieldname in df.columns.values:
        if fieldname not in fieldnames:
            fieldnames.append(fieldname)

    # Add column with URL for object
    url_prefix = "https://gamera.library.pitt.edu/islandora/object/pitt:"
    if 'identifier' in df.columns:
        df['url'] = url_prefix + df['identifier']

    # Reorder columns
    df = df.reindex(columns=fieldnames)
    return df


# Process records and export to a CSV file
def records_to_csv(records: list, destination: str):
    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame.from_dict(records)
    df = update_columns(df)

    # Remove empty values
    nan_value = float("NaN")
    df.replace({'': nan_value, '; ': nan_value, '; ; ': nan_value}, inplace=True)
    df.dropna(how='all', axis=1, inplace=True)

    # Write DataFrame to CSV file
    df.to_csv(destination, index=False, header=True, encoding='utf-8')


# Remove script filename from given traceback
def reformat_traceback(tb: str):
    script_path = os.path.abspath(__file__)
    return tb.replace(f'File "{script_path}", ', '').strip()


""" Driver Code """

if __name__ == "__main__":
    root = Tk()
    processor = Processor(root)
    processor.run()
    root.mainloop()
