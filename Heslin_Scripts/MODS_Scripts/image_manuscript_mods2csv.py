import sys
from tkinter import filedialog, messagebox, ttk
from tkinter import *
from lxml import etree as ET
import re
import os
import glob
import zipfile
import pandas as pd
from definitions import fieldnames, columns, namespaces


""" Global Variables """

global source 
source = None
global destination
destination = None
global mods_ns
mods_ns = '{http://www.loc.gov/mods/v3}'
global records
records = []
exceptions = []


""" Classes """

class DialogBox():
    parent = Tk()
    text_frame = None
    button_frame = None

    def __init__(self, title, dimensions):
        # Create dialog box
        self.parent.title(title)
        self.parent.geometry(dimensions)

    def create_text_frame(self):
        # Create a frame for dialog box buttons
        self.text_frame = Frame(self.parent)
        self.text_frame.pack()

    def reset_text_frame(self):
        self.text_frame.destroy()
        self.create_text_frame()
    
    def add_label(self, text):
        # Add label to text frame
        label = Label(self.text_frame, text=text)
        label.pack(pady=20)

    def create_button_frame(self):
        self.button_frame = Frame(self.parent)
        self.button_frame.pack()

    def reset_button_frame(self):
        self.button_frame.destroy()
        self.create_button_frame()

    def add_button(self, text, command, side, padx=0, pady=0):
        button = Button(self.button_frame, text=text, command=command, width=10)
        button.pack(side=side, padx=padx, pady=pady)

    def display(self):
        self.parent.mainloop()

    def close(self):
        self.parent.destroy()


class ModsElement:
    def __init__(self, root, xpath, namespace, elementname, **kwargs):
        self.root = root
        self.xpath = xpath
        self.namespace = namespace
        self.elementname = elementname
        self.additional_args = kwargs

    def get_element_value(self):
        if self.root.find(self.xpath, self.namespace) is not None:
            elementname = self.root.find(self.xpath, self.namespace).text
            return elementname
        else:
            elementname = ''
            return elementname

    def get_complex_element(self):
        value_list = []
        #if 'text' in self.additional_args.keys():
        for element in self.root.findall(self.xpath, self.namespace):
            if element is not None \
                and element.text == self.additional_args['text']:
                value_list.append(element.getparent().getprevious().text)
                return value_list

    def get_element_attrib(self):
        if self.root.find(self.xpath, self.namespace) is not None:
            elementattrib = 'yes'
            return elementattrib
        

""" Helper Functions """

# Show a given error and exit program
def show_error(title=str, message=str):
    messagebox.showerror(title=title, message=message)
    sys.exit(0)
    

# Extract files from compressed file (Zip) into a directory
def extract_files(filepath=str):
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
                   message="Input file must be a ZIP file (*.zip). Run the program and try again.")
    
    return output_dir


# Define on-click functions to get source for import
def get_source(dialog_box=DialogBox, source_type=str):
    # Update dialog box content
    dialog_box.parent.geometry("150x65")
    dialog_box.reset_text_frame()
    dialog_box.add_label(f"Select an input {source_type}.")
    dialog_box.reset_button_frame()
    
    # Set source
    global source
    
    if source_type == 'Zip file':
        file = filedialog.askopenfilename(title='Select Input File')
        source = extract_files(file)
    else:
        source = filedialog.askdirectory(title='Select Input Folder')

    # Close dialog box
    dialog_box.close()


# Run file dialog box to get input filepath
def run_source_dialog():
    # Create dialog box
    source_box = DialogBox(title="Select Source Type", dimensions="300x110")
    center_window(source_box.parent, "top")

    # Create text frame and add text to prompt user for source type
    source_box.create_text_frame()
    source_box.add_label("Do you need to import a Zip file or a folder?")

    # Create button frame and add buttons
    source_box.create_button_frame()
    source_box.add_button(text="Zip file", side=LEFT, padx=5,
                          command=lambda: get_source(source_box, 'Zip file'))
    source_box.add_button(text="Folder", side=RIGHT, padx=5,
                          command=lambda: get_source(source_box, 'folder'))

    # Get source for import
    source_box.display()


# Run file dialog box to get output filepath
def get_destination():
    global destination
    files = [('CSV UTF-8', '*.csv')]
    destination = filedialog.asksaveasfilename(filetypes = files, 
                                               defaultextension = files)


# Check if the given element is a special field
def check_special_field(element=ET.Element, tag=str):
    special_fields = ['accessCondition', 'namePart', 'roleTerm', 'subject']
    for field in special_fields:
        if f'{ mods_ns }{ field }' in [tag, element.getparent().tag]:
            return True
    return False


# Generate a list of an element's parents
def get_parents(root=ET.Element, element=ET.Element):
    parent_list = []
    while element.getparent() != root:
        parent_list.append(element.getparent().tag.replace(mods_ns, ''))
        element = element.getparent()
    return parent_list


# Get text values from given list of child elements
def get_child_text(children=list):
    child_text = [child.text for child in children \
                  if child.text is not None and child.text.strip()]
    return child_text


# Remove newline characters, trailing whitespaces, and multiple spaces from text
def remove_whitespaces(text):
    new_text = text.replace('\n    ', ' ').replace('\n', '').strip()
    new_text = re.sub(r'\s+', ' ', new_text)
    return new_text


def center_window(window, position):
    # Get the screen width and height
    screen_width = window.winfo_screenwidth()
    screen_height = window.winfo_screenheight()

    # Get the window width and height
    window_width = window.winfo_reqwidth()
    window_height = window.winfo_reqheight()

    # Calculate the x and y coordinates to center the window
    x = (screen_width - window_width) // 2
    y = (screen_height - window_height) // 2

    # Position window to side to center top-level window(s)
    if position == "bottom":
        x -= 100
        y -= 50

    # Set the window position
    window.geometry(f"+{x}+{y}")


def remove_finding_aids(files=list):
    fa_patterns = ['666980084', 'clp', 'mss', 'qss', 'rg04', 'ppi', 'qqs']
    files_to_remove = []
    # Generate list of finding aids identified by a finding aid filename pattern
    for filename in files:
        if any(pattern in filename.lower() for pattern in fa_patterns):
            files_to_remove.append(filename)
    # Remove finding aids from list of files
    for file in files_to_remove:
        files.remove(file)
    return files


""" Main Functions """

def process_xml(file):
    # Create an XML object that python can parse
    xml_object = ET.parse(file)
    # Get the root of that object
    root = xml_object.getroot()
    # Initialize dictionary for elements
    elements = {}

    # Create dictionary of dictionaries from parsed XML data
    # Top-level dictionary: Key = Xpath, value = child dictionary
    # Child dictionary: Key = field value, value = list of parent element(s)
    for element in root.xpath('.//*'):
        tag = element.tag
        text = element.text
        type_attribute = element.attrib.get('type')
        
        # Check that current element and its parent is not subject 
        # and that the element text is not empty
        if not check_special_field(element, tag) and text is not None:
            # Add XML dictionary with list of all parent elements
            if type_attribute:
                elements.setdefault(f'{tag}/{type_attribute}', []).append(
                    {remove_whitespaces(text):
                        [e for e in get_parents(root, element)]})
            else:
                elements.setdefault(tag, []).append(
                    {remove_whitespaces(text):
                        [e for e in get_parents(root, element)]})
    
    # Create dictionary from parsed XML data in elements dictionary
    record = {}

    for e1, e2 in elements.items():
        for value_tag in e2:
            for value, tag in value_tag.items():
                # Check if value has non-whitespace characters/is not empty
                if not value.strip():
                    continue
                if tag:
                    record.setdefault(f"{e1.replace(mods_ns, '')}/{tag[-1]}", 
                        []).append(value.replace('\r', ' '))
                else:
                    record.setdefault(e1.replace(mods_ns, ''),
                        []).append(value.replace('\r', ' '))

    # Get accessCondition elements
    publication_status = ""
    copyright_status = ""

    for access_conditions in root.iterfind('mods:accessCondition', 
                                            namespaces['mods_ns']):
        for copyrights in access_conditions.iterfind(
            'copyrightMD:copyright', namespaces['copyright_ns']):
            publication_status = copyrights.attrib['publication.status']
            copyright_status = copyrights.attrib['copyright.status']

    # Get subject elements
    for subject in root.iterfind('mods:subject', namespaces['mods_ns']):
        children = subject.getchildren()
        if not children:
            continue
        subject_field = f"subject_{children[0].tag.replace(mods_ns, '')}"
        record.setdefault(subject_field, [])
        values = '--'.join(get_child_text(children))
        if values:
            record[subject_field].append(values)
        if subject_field == 'subject_name':
            grandchildren = children[0].getchildren()
            if grandchildren:
                record[subject_field] += (get_child_text(grandchildren))
        
    # Create a dictionary for each targeted namePart roleTerm
    nameParts = {
        "creator": [],
        "contributor": [],
        "depositor": []
    }

    # Get all namePart elements and add values to corresponding role list
    for field in root.findall('.//mods:namePart', namespaces['mods_ns']):
        try:
            # below throws IndexError: list index out of range
            roleTerm = field.getnext().getchildren()[0].text
            nameParts[roleTerm].append(field.text)
        except:
            pass

    # Create a MODS element from Xpath
    date_qualifier = ModsElement(
        root=root,
        xpath=".//mods:dateCreated[@qualifier='approximate'][@encoding='iso8601'][@keyDate='yes']", 
        namespace=namespaces['mods_ns'], 
        elementname='date_qualifier'
        )
    
    # Add fields to second XML dictionary
    record.setdefault('copyright_status', copyright_status)
    record.setdefault('publication_status', publication_status)
    record.setdefault('contributor', nameParts['contributor'])
    record.setdefault('creator', nameParts['creator'])
    record.setdefault('depositor', nameParts['depositor'])
    record.setdefault('normalized_date_qualifier',
                        date_qualifier.get_element_attrib())
    
    # Convert field value from lists to strings
    for field, value in record.items(): 
        if type(value) is list:
            record[field] = '; '.join(value)

    # Add record dictionary to records
    records.append(record)


def manage_processor(files=list, total_files=int, progress=int, 
                  progress_var=DoubleVar, processed_label=Label):
    if progress < total_files:
        # Process the file
        file = files[progress]
        try:
            process_xml(file)
        except:
            records.append({'identifier/pitt': file.split('_')[1]})

        # Update progress
        progress += 1
        progress_var.set(int((progress / total_files) * 100))

        # Update processed label
        text = f"{progress}/{total_files} files"
        processed_label.config(text=text)
        processed_label.update_idletasks() 

        # Schedule the next file processing
        root.after(1, manage_processor, files, total_files, progress, 
                   progress_var, processed_label)
    else:
        # Processing is complete
        processing_complete_label.config(text="Complete!")


# Function to start processing files
def start_processing(root=Tk, files=list):
    # Initialize progress variable
    progress = 0

    # Update root to display components
    root.update_idletasks() 

    # Create and place processed label
    processed_label = Label(root, text="0/0")
    processed_label.pack()
    processed_label.update_idletasks() 
    
    # Start processing files
    manage_processor(files=files, total_files = len(files), progress=progress, 
                  progress_var=progress_var, processed_label=processed_label)


""" Driver Code """

if __name__ == "__main__":
    
    """ 
    Prompt user to select the source file or directory containing input data and 
    the destination for the output
    """

    # Start dialog to get source type and input file/folder 
    run_source_dialog()

    # Check if a source was selected
    # TO DO: Try a while loop that will ask if you want to close program or retry
    if not source:
        text = "You did not select an input source. Run the program and try again."
        messagebox.showerror(title="Try again", message=text)
        sys.exit(0)

    # Get destination for export
    get_destination()

    # Check if a source was selected
    if not destination:
        text = "You did not select an output folder. Run the program and try again."
        messagebox.showerror(title="Try again", message=text)
        sys.exit(0)


    """ Process data """

    # Change working directory to source directory
    os.chdir(source)

    # Get list of XML files in directory
    file_list = glob.glob('*.xml')

    # Remove finding aids from list of files
    file_list = remove_finding_aids(file_list)
        
    # Get records by parsing and processing data in XML files
    root = Tk()
    root.title("Processing Files")
    root.geometry('255x125')
    root.attributes("-topmost", True)
    center_window(root, "top")

    # Create a progress bar
    progress_var = DoubleVar()
    progress_bar = ttk.Progressbar(root, variable=progress_var, 
                                   maximum=100, len=200)
    progress_bar.pack(pady=20)

    # Label to display processing status
    processing_complete_label = Label(root, text="Processing...")
    processing_complete_label.pack()

    # Start processing files
    start_processing(root=root, files=file_list)

    root.mainloop()
    
    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame.from_dict(records)

    # Replace abbreviated Xpaths with standardized field names
    for fieldname in df.columns.values:
        if fieldname not in fieldnames:
            fieldnames.append(fieldname)

    # Add column with URL for object
    url_prefix = "https://gamera.library.pitt.edu/islandora/object/pitt:"
    if 'url' in df.columns:
        df['url'] =  url_prefix + df['identifier/pitt']

    # Reindex and rename columns
    df = df.reindex(columns=fieldnames)
    df.rename(columns=columns, inplace=True)

    # Remove empty values
    nan_value = float("NaN")
    df.replace({'': nan_value, '; ': nan_value, '; ; ': nan_value}, inplace=True)
    df.dropna(how='all', axis=1, inplace=True)


    """ Save Data """

    # Write DataFrame to CSV file
    df.to_csv(destination, index=False, header=True, encoding='utf-8')
