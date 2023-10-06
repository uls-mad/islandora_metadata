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

global mods_ns
mods_ns = '{http://www.loc.gov/mods/v3}'
exceptions = []


""" Classes """

class GUI:
    root = None
    text_frame = None
    button_frame = None

    def __init__(self, root, title, dimensions):
        self.root = root
        self.root.title(title)
        self.root.geometry(dimensions)

    def set_title(self, title):
        self.root.title = title

    def set_geom(self, geometry):
        self.root.geometry(geometry)

    def add_text_frame(self):
        self.text_frame = Frame(self.root)
        self.text_frame.pack()

    def remove_text_frame(self):
        self.text_frame.destroy()

    def reset_text_frame(self):
        self.text_frame.destroy()
        self.add_text_frame()
    
    def add_label(self, text):
        label = Label(self.text_frame, text=text)
        label.pack(pady=20)

    def add_button_frame(self):
        self.button_frame = Frame(self.root)
        self.button_frame.pack()

    def remove_button_frame(self):
        self.button_frame.destroy()

    def reset_button_frame(self):
        self.button_frame.destroy()
        self.add_button_frame()

    def add_button(self, text, command, side, padx=0, pady=0):
        button = Button(self.button_frame, text=text, command=command, width=10)
        button.pack(side=side, padx=padx, pady=pady)

    def center_window(self,position):
        # Get the screen width and height
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()

        # Get the window width and height
        window_width = self.root.winfo_reqwidth()
        window_height = self.root.winfo_reqheight()

        # Calculate the x and y coordinates to center the window
        x = (screen_width - window_width) // 2
        y = (screen_height - window_height) // 2

        # Position window to side to center top-level window(s)
        if position == "bottom":
            x -= 100
            y -= 50

        # Set the window position
        self.root.geometry(f"+{x}+{y}")

    def run(self):
        self.root.mainloop()

    def close(self):
        self.root.destroy()


class Processor:
    gui = None
    files = []
    total_files = 0
    progress = 0
    progress_var = None
    processing_label = None
    complete_label = None
    close_button = None
    source = None
    destination = None
    records = []

    def __init__(self, root):
        self.gui = GUI(root=root, 
                       title="MODS to CSV Converter", 
                       dimensions="300x110")
        self.gui.center_window("top")

    def run(self):
        # Create text frame and add text to prompt user for source type
        self.gui.add_text_frame()
        self.gui.add_label("Do you need to import a Zip file or a folder?")

        # Create button frame and add buttons
        self.gui.add_button_frame()
        self.gui.add_button(text="Zip file", side=LEFT, padx=5,
                            command=lambda: self.get_source('Zip file'))
        self.gui.add_button(text="Folder", side=RIGHT, padx=5,
                            command=lambda: self.get_source('folder'))
        
    def get_source(self, source_type=str):
        # Update dialog box content
        self.gui.set_geom("300x65")
        self.gui.reset_text_frame()
        self.gui.add_label(f"Select an input {source_type}.")
        self.gui.reset_button_frame()
        
        # Set source       
        if source_type == 'Zip file':
            file = filedialog.askopenfilename(title='Select Input File')
            self.source = extract_files(file)
        else:
            self.source = filedialog.askdirectory(title='Select Input Folder')

        # Check if a source was selected
        # TO DO: Try a while loop that will ask if you want to close program or retry
        text = "You did not select an input source. Run the program and try again."
        if not self.source:
            show_error(title="Try again", message=text)

        self.get_destination()
        
    def get_destination(self):
        self.gui.set_geom("300x65")
        self.gui.reset_text_frame()
        self.gui.add_label("Select an output destination.")
        file_types = [('CSV UTF-8', '*.csv')]
        self.destination = filedialog.\
            asksaveasfilename(parent=self.gui.root, title="Save Output As", 
                              filetypes=file_types, defaultextension=file_types)
        
        # Check if a source was selected
        text = "You did not select an output destination." + \
            "Run the program and try again."
        if not self.destination:
            show_error(title="Try again", message=text)
        
        self.gui.remove_text_frame()
        self.gui.remove_button_frame()

        self.start_processing()

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
        self.progress_bar = ttk.Progressbar(self.gui.root, maximum=100, len=200,
                                            variable=self.progress_var)
        self.progress_bar.pack(pady=20)

        # Label to display processing status
        self.complete_label = Label(self.gui.root, text="Processing...")
        self.complete_label.pack()

        # Create and place processed label
        self.processing_label = Label(self.gui.root, text="0/0")
        self.processing_label.pack()
        self.processing_label.update_idletasks() 

        # Create close button for GUI window
        self.gui.add_button_frame()
        self.gui.add_button("Cancel", side=RIGHT, pady=10, 
                            command=self.gui.close)

        # Update root to display components
        self.gui.root.update_idletasks() 

        self.manage_processor()

    def manage_processor(self):
        if self.progress < self.total_files:
            # Process the file
            file = self.files[self.progress]
            try:
                record = process_xml(file)
                self.records.append(record)
            except:
                self.records.append({'identifier/pitt': file.split('_')[1]})

            # Update progress
            self.progress += 1
            self.progress_var.set(int((self.progress / self.total_files) * 100))

            # Update processed label
            text = f"{self.progress}/{self.total_files} files"
            self.processing_label.config(text=text)
            self.processing_label.update_idletasks() 

            # Schedule the next file processing
            self.gui.root.after(1, self.manage_processor)
        else:
            # Notify user that processing is complete
            records_to_csv(records=self.records, destination=self.destination)
            self.complete_label.config(text="Complete!")
            self.gui.reset_button_frame()
            self.gui.add_button("OK", side=RIGHT, pady=10, 
                                command=self.gui.close)


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


# Get list of files to be processed
def get_files(source):
    # Change working directory to source directory
    os.chdir(source)
    # Get list of XML files in directory
    files = glob.glob('*.xml')
    # Remove finding aids from list of files
    files = remove_finding_aids(files)
    return files
    

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
                   message="Input file must be a ZIP file (*.zip). " + 
                   "Run the program and try again.")
    
    return output_dir


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


# Remove finding aids from input files based on filename patterns
def remove_finding_aids(files=list):
    fa_patterns = ['666980084', 'clp.', 'mss.', 'qss', 'rg04.201', 'ppi', 'us-qqs']
    files_to_remove = []

    # Generate list of finding aids identified by a finding aid filename pattern
    for filename in files:
        if any(pattern in filename.lower() for pattern in fa_patterns):
            files_to_remove.append(filename)

    # Remove finding aids from list of files
    for file in files_to_remove:
        files.remove(file)

    return files


def update_columns(df=pd.DataFrame):
    # Add columns not in standardized fields
    for fieldname in df.columns.values:
        if fieldname not in fieldnames:
            fieldnames.append(fieldname)

    # Add column with URL for object
    url_prefix = "https://gamera.library.pitt.edu/islandora/object/pitt:"
    if 'url' in df.columns:
        df['url'] =  url_prefix + df['identifier/pitt']

    # Update values in normalized_date_qualifier
    if 'dateOther/display/originInfo' in df.columns:
        df['normalized_date_qualifier'] = df['dateOther/display/originInfo'].\
            apply(lambda x: 'yes' if 'c.' in x or 'ca.' in x else float("NaN"))

    # Reindex columns
    df = df.reindex(columns=fieldnames)

    # Replace abbreviated Xpaths with standardized field names
    df.rename(columns=columns, inplace=True)

    return df


def records_to_csv(records=list, destination=str):
    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame.from_dict(records)
    df = update_columns(df)

    # Remove empty values
    nan_value = float("NaN")
    df.replace({'': nan_value, '; ': nan_value, '; ; ': nan_value}, 
                inplace=True)
    df.dropna(how='all', axis=1, inplace=True)

    # Write DataFrame to CSV file
    df.to_csv(destination, index=False, header=True, encoding='utf-8')


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

    return record


""" Driver Code """

if __name__ == "__main__":

    root = Tk()
    processor = Processor(root)
    processor.run()
    root.mainloop()
