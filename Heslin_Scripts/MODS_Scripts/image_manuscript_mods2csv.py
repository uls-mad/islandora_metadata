import sys
from tkinter import filedialog, messagebox
from tkinter import *
from lxml import etree as ET
import re
import glob
import csv
import pandas as pd
import os
import zipfile


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
        button = Button(self.button_frame, text=text, command=command)
        button.pack(side=side, padx=padx, pady=pady)

    # Display dialog box content and wait for user response
    def display(self):
        self.parent.mainloop()

    def close(self):
        self.parent.destroy()


class ModsElement:
    def __init__(self, xpath, namespace, elementname, **kwargs):
        self.xpath = xpath
        self.namespace = namespace
        self.elementname = elementname
        self.additional_args = kwargs

    def get_element_value(self):
        if root.find(self.xpath, namespaces) is not None:
            elementname = root.find(self.xpath, namespaces).text
            return elementname
        else:
            elementname = ''
            return elementname

    def get_complex_element(self):
        value_list = []
        #if 'text' in self.additional_args.keys():
        for element in root.findall(self.xpath, self.namespace):
            if element is not None and element.text == self.additional_args['text']:
                value_list.append(element.getparent().getprevious().text)
                return value_list

    def get_element_attrib(self):
        if root.find(self.xpath, namespaces) is not None:
            elementattrib = 'yes'
            return elementattrib


""" Helper functions """

# Show a given error and exit program
def show_error(title=str, message=str):
    messagebox.showerror(title=title, message=message)
    sys.exit(0)

# Extract files from compressed file (Zip) into a directory
def extract_files(filepath=str):
    output_dir = ""
    
    # confirm that file is zip file
    if zipfile.is_zipfile(filepath):
        directory = os.path.dirname(filepath)
        filename = os.path.basename(filepath)
        output_dir = '%s\\%s' % (directory, os.path.splitext(filename)[0])

        # extract files to output_dir
        with zipfile.ZipFile(filepath, 'r') as zip_archive:
            zip_archive.extractall(output_dir)
    else:
        show_error(title="Invalid File Format", 
                   message="Input file must be a ZIP file (*.zip). Run the program and try again.")
    
    return output_dir


# Define on-click functions to get source for import
def get_source(dialog_box=DialogBox, source_type=str):
    # Update dialog box content
    dialog_box.parent.geometry("150x65")
    dialog_box.reset_text_frame()
    dialog_box.add_label(f"Select a {source_type}.")
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


def run_source_dialog():
    # Create dialog box
    source_box = DialogBox(title="Select Source Type", dimensions="300x110")

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


def get_destination():
    global destination
    files = [
        #('All Files', '*.*'),
        ('CSV UTF-8', '*.csv'),
        #('Text Document', '*.txt')
        ]
    destination = filedialog.asksaveasfilename(filetypes = files, defaultextension = files)


def your_while_generator(e):
    parent_list = []
    while e.getparent() != root:
        parent_list.append(e.getparent().tag.replace('{http://www.loc.gov/mods/v3}', ''))
        e = e.getparent()
    return parent_list


""" Driver Code """

if __name__ == "__main__":
    
    """ 
    Prompt user to select the source file or directory containing input data and the 
    destination for the output
    """

    global source 
    source = None
    global destination
    destination = None

    # Start dialog to get source type and input file/folder 
    run_source_dialog()
    #source = filedialog.askdirectory(title='Select Input Folder')

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
    list_of_files = glob.glob('*.xml')

    master_dict = []

    for file in list_of_files:
        #print(file)
        xmlObject = ET.parse(file)  # create an xml object that python can parse
        root = xmlObject.getroot()  # get the root of that object
        namespaces = {
            'mods': 'http://www.loc.gov/mods/v3'}  # define your namespace
        copyright_ns = {'copyrightMD': 'http://www.cdlib.org/inside/diglib/copyrightMD'}

        xml_dictionary = {}

        elements = {}
        for item in root.xpath('.//*'):
            #print(item)
            if item.getparent().tag != '{http://www.loc.gov/mods/v3}subject' and item.tag != '{http://www.loc.gov/mods/v3}subject':
                if item.text is not None:
                    try:
                        elements.setdefault(item.tag + '/' + item.attrib.get('type'), []).append({item.text.replace('\n    ', '').replace('\n', '').strip() : [e for e in your_while_generator(item)]})
                    except TypeError:
                        if item.text is not None:
                            elements.setdefault(item.tag, []).append({item.text.replace('\n    ', ' ').replace('\n', '').strip() : [e for e in your_while_generator(item)]})
                        else:
                            pass

        #print(elements) #['{http://www.loc.gov/mods/v3}subject']
        #print(elements['{http://www.loc.gov/mods/v3}topic'])

        for i in elements:
            for dict2 in elements[i]:
                for key in dict2:
                    if key != '\n      ' and key != '\n         ':
                        if dict2[key] != []:
                            xml_dictionary.setdefault(i.replace('{http://www.loc.gov/mods/v3}', '') + '/' + dict2[key][-1] , []).append(key.replace('\r', ' '))
                        else:
                            xml_dictionary.setdefault(i.replace('{http://www.loc.gov/mods/v3}', ''), []).append(key.replace('\r', ' '))
        #print(xml_dictionary)

        xml_dict2 = {}

        for key2 in xml_dictionary:
            #print(xml_dictionary[key2])
            if '\n    ' in xml_dictionary[key2]:
                pass
            else:
                try:
                    if key2 != 'namePart/name' and key2 != 'roleTerm/text/name' and xml_dictionary[key2][0] is not None:
                        xml_dict2.setdefault(key2, '; '.join(xml_dictionary[key2])) #removed [0] index from the end of this statement
                    else:
                        pass
                except TypeError:
                    pass

        for access_conditions in root.iterfind('mods:accessCondition', namespaces):
            for copyrights in access_conditions.iterfind('copyrightMD:copyright', copyright_ns):
                publication_status = copyrights.attrib['publication.status']
                copyright_status = copyrights.attrib['copyright.status']

        
        for subject in root.iterfind('mods:subject', namespaces):
            # print([child.text for child in subject.getchildren()])
                                
            if subject.getchildren() != []:
                                                                                                                
                xml_dict2.setdefault(['subject_' + subject_type.tag.replace('{http://www.loc.gov/mods/v3}', '') for subject_type in subject.getchildren()][0], []).append(
                                                                
                    '--'.join([child.text for child in subject.getchildren() if child.text != '\n      ' and child.text is not None]))

        for key in xml_dict2: #get subjects for manuscripts and images
            if type(xml_dict2[key]) is list:
                xml_dict2[key] = '; '.join(xml_dict2[key])
            else:
                xml_dict2[key] = xml_dict2[key]

        creator_value_list = []
        contributor_value_list = []
        depositor_value_list = []
        for e in root.findall('.//mods:namePart', namespaces):
            try:
                # below throws IndexError: list index out of range
                if e.getnext().getchildren()[0].text == 'creator':
                    creator_value_list.append(e.text)
            except:
                pass
            try:
                # below throws IndexError: list index out of range
                if e.getnext().getchildren()[0].text == 'contributor':
                    contributor_value_list.append(e.text)
            except:
                pass
            try:
                # below throws IndexError: list index out of range
                if e.getnext().getchildren()[0].text == 'depositor':
                    depositor_value_list.append(e.text)
            except:
                pass

        creator = '; '.join(creator_value_list)
        contributor = '; '.join(contributor_value_list)
        depositor = '; '.join(depositor_value_list)
        date_qualifier = ModsElement(".//mods:dateCreated[@qualifier='approximate'][@encoding='iso8601'][@keyDate='yes']", namespaces, 'date_qualifier')

        xml_dict2.setdefault('copyright_status', copyright_status)
        xml_dict2.setdefault('publication_status', publication_status)
        xml_dict2.setdefault('contributor', contributor)
        xml_dict2.setdefault('creator', creator)
        xml_dict2.setdefault('depositor', depositor)
        xml_dict2.setdefault('normalized_date_qualifier', date_qualifier.get_element_attrib())

        master_dict.append(xml_dict2)

    df = pd.DataFrame.from_dict(master_dict)
    df.to_csv (destination, index = False, header=True, encoding='utf-8')


    #new_csv = input('CSV has been created but headers need to be renamed and reindexed. Provide full pathname for new csv:')
    new_csv = destination


    fieldnames = ['identifier/pitt', 'title/titleInfo', 'creator', 'subject_geographic', 'subject_topic', 'namePart/subject', 'abstract', 'dateCreated/originInfo', 'normalized_date_qualifier','dateOther/display/originInfo',
                'dateOther/sort/originInfo', 'publication_status', 'copyright_status', '{http://www.cdlib.org/inside/diglib/copyrightMD}name/accessCondition','typeOfResource', 'languageTerm/code/language', 'title/relatedItem',
                'identifier/relatedItem', 'depositor', 'contributor', 'genre', 'form/physicalDescription', 'extent/physicalDescription', 'publisher/originInfo', 'note/prefercite/relatedItem', 'placeTerm/text/originInfo',
                'note/series/relatedItem', 'note/subseries/relatedItem', 'note/container/relatedItem', 'dateCreated/relatedItem', 'identifier/local-asc/relatedItem', 'note/ownership/relatedItem', 'identifier/source']

    df = pd.read_csv(destination) #===> Include the headers
    correct_df = df.copy()
    #print(df.columns.values)
    for i in correct_df.columns.values:
        if i not in fieldnames:
            if i != 'subject_name':
                fieldnames.append(i)
        else:
            pass
    df_reorder = correct_df.reindex(columns=fieldnames)
    df_reorder.to_csv(new_csv, index=False, header=True, encoding='utf-8')

    new_df = pd.read_csv(new_csv)
    correct_df2 = new_df.copy()
    correct_df2.rename(columns={'title/titleInfo': 'title', 'typeOfResource': 'type_of_resource','publisher/originInfo': 'publisher','dateOther/display/originInfo': '[DELETE] display_date', 'dateOther/sort/originInfo': '[DELETE] sort_date',
    'languageTerm/code/language': 'language', 'form/physicalDescription': 'format', 'extent/physicalDescription': 'extent', 'identifier/pitt' : 'identifier',
    'title/relatedItem': 'source_collection', 'dateCreated/originInfo': 'normalized_date', 'note/prefercite/relatedItem': 'source_citation', 'identifier/relatedItem': 'source_collection_id', 'note/container/relatedItem': 'source_container',
    'note/series/relatedItem': 'source_series', 'note/subseries/relatedItem': 'source_subseries', 'placeTerm/text/originInfo': 'pub_place',
    'abstract': 'description', 'namePart/subject': 'subject_name', '{http://www.cdlib.org/inside/diglib/copyrightMD}name/accessCondition' : 'rights_holder', 'identifier/source' : 'source_id', 'note/address' : 'address', 'dateCreated/relatedItem' : "source_collection_date", 'identifier/local-asc/relatedItem' : 'source_collection_id', 'note/ownership/relatedItem' : 'source_ownership'}, inplace=True)

    #data cleaning
    nan_value = float("NaN")
    correct_df2.replace({'': nan_value, '; ': nan_value, '; ; ': nan_value}, inplace=True)
    correct_df2.dropna(how='all', axis=1, inplace=True)
    correct_df2.to_csv(new_csv, index=False, header=True, encoding='utf-8')
