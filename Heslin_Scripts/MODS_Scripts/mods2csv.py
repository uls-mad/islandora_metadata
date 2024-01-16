import sys
from tkinter import filedialog, messagebox, ttk
from tkinter import *
from lxml import etree as ET
import re
import os
import glob
import zipfile
import pandas as pd
from definitions import fieldnames, columns, namespaces, mods_ns


""" Global Variables """

exceptions = []


""" Classes """

class GUI:
    root = None
    text_frame = None
    button_frame = None
    dimensions = None

    def __init__(self, root, title, dimensions):
        self.root = root
        self.dimensions = dimensions.split('x')
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
            x -= int(self.dimensions[0])
            y -= int(self.dimensions[1])

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

# Check if the given root has the MODS namespace prefix and add if not
def ensure_mods_prefix(tree=ET.ElementTree, root=ET.Element):
    if not root.prefix:
        # Create a new root element with 'mods' namespace
        new_root = ET.Element('mods', nsmap=namespaces['mods_ns'])
        # Copy children from the original root to the new root (with 'mods' namespace)
        for child in root:
            new_root.append(child)
        # Create a new tree with the modified root
        new_tree = ET.ElementTree(new_root)
        new_root = new_tree.getroot()
        return new_tree, new_root
    return tree, root


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
    if isinstance(text, str):
        new_text = text.replace('\n    ', ' ').replace('\n', '').strip()
        new_text = re.sub(r'\s+', ' ', new_text)
        return new_text.strip()
    return ''


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


def check_date_qualifier(record=dict):
    if not record.get('normalized_date_qualifier') \
        and record.get('dateOther/display/originInfo'):
        if any(pattern in record.get('dateOther/display/originInfo') \
               for pattern in ['c.', 'ca.']):
            record.setdefault('normalized_date_qualifier', 'yes')
    return record


def update_columns(df=pd.DataFrame):
    # Sort DataFrame by column name (alphabetical, ascending order)
    df = df[sorted(df.columns)]
    
    # Get column headers and sort them alphabetically
    column_headers = df.columns.tolist()

    # Split each string on forward slashes, reverse the order, and rejoin
    # Replace at symbols with forward slash and spaces with underscore
    # Ex: physicalDescription/form@marcform >> form/marcform/physicalDescription
    column_headers = ['/'.join(header.split('/')[::-1]).replace('@', '/').\
                      replace(' ', '_') for header in column_headers]

    # Rename DataFrame columns with the updated headers
    df.columns = column_headers
    
    # Add columns not in standardized fields
    for fieldname in df.columns.values:
        if fieldname not in fieldnames:
            fieldnames.append(fieldname)

    # Add column with URL for object
    url_prefix = "https://gamera.library.pitt.edu/islandora/object/pitt:"
    if 'identifier/pitt' in df.columns:
        df['url'] =  url_prefix + df['identifier/pitt']

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
    # Ensure that XML tree elements have MODS namespace prefix
    xml_object, root = ensure_mods_prefix(xml_object, root)

    # Create dictionary with element xpath as key and text as value
    record = {}

    for element in root.xpath('.//*'):
        xpath = xml_object.getpath(element).\
            replace('mods:', '').replace('/mods/', '').replace('copyrightMD:', '')
        xpath = re.sub(r'\[\d+\]', '', xpath)
        tag = element.tag
        text = remove_whitespaces(element.text)
        type_attribute = element.attrib.get('type')
        authority_attribute = element.attrib.get('authority')
        
        # Check that current element and parent are not special/nested fields
        # and that the element text is not empty
        if not check_special_field(element, tag) and text:
            # Add attribute value to xpath
            if type_attribute:
                xpath += f'@{type_attribute}'
            elif authority_attribute:
                xpath += f'@{authority_attribute}'
            # Add element data to record dictionary
            record.setdefault(xpath, []).append(text.replace('\r', ' '))

    ### SPECIAL FIELDS ###
    
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
        authority_attribute = subject.attrib.get('authority')
        if not children:
            continue
        subject_field = f"subject_{children[0].tag.replace(mods_ns, '')}"
        if subject_field == 'subject_topic' and authority_attribute == 'local':
            subject_field = 'subject_local'
        record.setdefault(subject_field, [])
        values = '--'.join(get_child_text(children))
        if values:
            record[subject_field].append(values)
        if subject_field == 'subject_name':
            grandchildren = children[0].getchildren()
            if grandchildren:
                record[subject_field] += (get_child_text(grandchildren))
        
    # Create a dictionary for each targeted name roleTerm
    names = {
        "creator": [],
        "contributor": [],
        "depositor": [],
        "interviewer": [],
        "interviewee": [],
        "other_names": [],
    }

    # Get all namePart elements and add values to corresponding role list
    for field in root.findall('.//mods:namePart', namespaces['mods_ns']):
        roleTerm = None
        try:
            # below throws IndexError: list index out of range
            roleTerm = field.getnext().getchildren()[0].text
        except:
            pass
        
        if roleTerm is None:
            names['other_names'].append(field.text)
        elif roleTerm not in names:
            names['other_names'].append(f"{field.text} [{roleTerm}]")
        else:
            names[roleTerm].append(field.text)

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
    record.setdefault('contributor', names['contributor'])
    record.setdefault('creator', names['creator'])
    record.setdefault('depositor', names['depositor'])
    record.setdefault('interviewer', names['interviewer'])
    record.setdefault('interviewee', names['interviewee'])
    record.setdefault('normalized_date_qualifier',
                        date_qualifier.get_element_attrib())
    
    if names['other_names']:
        record.setdefault('other_names', names['other_names'])
    
    # Check normalized_date_qualifier
    record = check_date_qualifier(record)

    # Ensure that identifier[@type="pitt"] exists
    if not 'pitt@identifier' in record:
        pid = file.replace("pitt_", "").replace("_MODS", "").replace(".xml", "")
        record.setdefault('identifier@pitt', pid)

    # Convert field values from lists to strings
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
