# Extrenal packages
from lxml import etree as ET
import re

# Local packages
from utilities import *
from definitions import columns, namespaces, mods_ns


""" Classes """

class ModsElement:
    """
    Represents a MODS (Metadata Object Description Schema) element for extracting data from XML documents.

    Attributes:
        root (Element): The root element of the XML document.
        xpath (str): The XPath expression for locating the MODS element within the XML document.
        namespace (dict): A dictionary containing namespace prefixes and URIs for XPath expressions.
        elementname (str): The name of the MODS element.
        additional_args (dict): Additional keyword arguments for specifying parameters.

    Methods:
        get_element_value(): Retrieves the text value of the MODS element specified by the XPath expression.
        get_complex_element(): Retrieves values from sibling elements based on specified criteria.
        get_element_attrib(): Sets the attribute value to 'yes' if the XPath expression matches an element in the XML document.
    """

    def __init__(self, root, xpath, namespace, elementname, **kwargs):
        self.root = root
        self.xpath = xpath
        self.namespace = namespace
        self.elementname = elementname
        self.additional_args = kwargs

    # Get the text value of the MODS element
    def get_element_value(self):
        if self.root.find(self.xpath, self.namespace) is not None:
            elementname = self.root.find(self.xpath, self.namespace).text
            return elementname
        else:
            elementname = ''
            return elementname

    # Get values from data values from sibling elements
    def get_complex_element(self):
        value_list = []
        #if 'text' in self.additional_args.keys():
        for element in self.root.findall(self.xpath, self.namespace):
            if element is not None \
                and element.text == self.additional_args['text']:
                value_list.append(element.getparent().getprevious().text)
                return value_list

    # Set element attribute value to 'yes' if XPath is not null
    def get_element_attrib(self):
        if self.root.find(self.xpath, self.namespace) is not None:
            elementattrib = 'yes'
            return elementattrib
        

""" Helper Functions """

# Check if the given root has the MODS namespace prefix and add if not
def ensure_mods_prefix(tree: ET.ElementTree, root: ET.Element):
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


# Check if the given element is a special field
def check_special_field(element: ET.Element, xpath: str):
    special_fields = ['copyright', 'namePart', 'roleTerm', 'subject']
    for field in special_fields:
        if field in xpath and 'relatedItem' not in xpath:
            return True
    return False


# Get XPath for given element and proccess string to simplify
def get_xpath(xml_object, element: ET.Element):
    # Get xpath
    xpath = xml_object.getpath(element)
    # Remove namespace prefixes and root
    xpath = xpath.\
        replace('mods:', '').\
        replace('copyrightMD:', '').\
        replace('/mods/', '')
    # Remove index-like expressions (digits in brackets) from the XPath
    xpath = re.sub(r'\[\d+\]', '', xpath)
    return xpath


# Get tag attribute of element
def get_tag(element: ET.Element):
    return element.tag.replace(mods_ns, '')


# Generate a list of an element's parents
def get_parents(root: ET.Element, element: ET.Element):
    parent_list = []
    while element.getparent() != root:
        parent_list.append(element.getparent().tag.replace(mods_ns, ''))
        element = element.getparent()
    return parent_list


# Get text values from given list of child elements
def get_child_text(parent: ET.Element):
    child_text = []
    children = parent.getchildren()
    if children:
        child_text = [child.text for child in children \
                  if child.text is not None and child.text.strip()]
    return child_text


# Get namePart and roleTerm values (if any) from given name element
def get_name_value(name: ET.Element):
    name = ET.ElementTree(name)
    namePart = name.find(f"{mods_ns}namePart")
    if namePart is None:
        return None
    roleTerm = name.find(f'{mods_ns}role/{mods_ns}roleTerm')
    value = f"{namePart.text} [{roleTerm.text}]" \
        if roleTerm is not None else namePart.text
    return value


# Get publication status and copyright status from given accessCondition element
def get_copyright_data(accessCondition: ET.Element):
    copyright = accessCondition.find(
        'copyrightMD:copyright', namespaces['copyright_ns'])
    if copyright is None:
        return []
    data = [('publication_status', copyright.attrib.get('publication.status')),
            ('copyright_status', copyright.attrib.get('copyright.status'))]
    return [(key, value) for key, value in data if value]


# Get specified and non-specified subject data
def get_subject_data(subject: ET.Element):
    data = []
    values = []
    authority_attribute = subject.attrib.get('authority')
    children = subject.getchildren()
    if not children:
        return data
    main_child = children[0]
    child_tag = main_child.tag.replace(mods_ns, '')

    # Name field
    field = f"subject/{child_tag}"
    if child_tag == 'topic' and authority_attribute == 'local':
        field = 'subject_local'
    elif child_tag in ['geographic', 'name', 'temporal', 'topic']:
        field = f'subject_{child_tag}'
    elif authority_attribute is not None:
        field = f"subject@{authority_attribute}/{child_tag}"

    # Extract and transform data
    for child in children:
        cur_tag = child.tag.replace(mods_ns, '')
        if cur_tag == 'name':
            values.append(get_name_value(child))
        elif cur_tag == 'titleInfo':
            values.append(', '.join(get_child_text(child)))
        elif cur_tag == 'hierarchicalGeographic':
            values.append('--'.join(get_child_text(child))) 
        elif cur_tag == 'cartographics': 
            # Assumes there are no other children in subject
            for grandchild in child.getchildren():
                grandchild_tag = grandchild.tag.replace(mods_ns, '')
                data.append((grandchild_tag, grandchild.text))
        else:
            values.append(child.text)

    # Remove NoneType values
    values = [value for value in values if value is not None and value != '']
    if values:
        values_str = '--'.join(values)
        data.append((field, values_str))
    
    return data


# Get nameTerm and roleTerm (if any) values from name element
def get_name_data(name: ET.Element):
    data = []
    roles = ['creator', 'contributor', 'depositor', 
             'interviewer', 'interviewee', 'other_names']
    name = ET.ElementTree(name)
    try:
        namePart = name.find(f'{mods_ns}namePart').text
    except:
        return data
    try:
        roleTerm = name.find(f'{mods_ns}role/{mods_ns}roleTerm').text
    except:
        roleTerm = None
    if roleTerm in roles or roleTerm is None:
        data.append((roleTerm or 'other_names', namePart))
    else:
        data.append(('other_names', f'{namePart} [{roleTerm}]'))
    return data


# Add type attribute value for name element to XPath
def add_name_type(element: ET.Element, xpath: str):
    name = element.getparent()
    type_attribute = name.get('type')
    if type_attribute is not None:
        xpath = xpath.replace('name/', f'name@{type_attribute}/')
    return xpath


# Add type attribute value for relatedItem element to XPath
def add_relatedItem_type(element: ET.Element, xpath: str):
    parent = element.getparent()
    while get_tag(parent) != 'relatedItem':
        parent = parent.getparent()
    type_attribute = parent.get('type')
    if type_attribute is not None:
        xpath = xpath.replace('relatedItem/', f'relatedItem@{type_attribute}/')
    return xpath


# Add date qualifier value if circa (or abbreviations) in display date
def check_date_qualifier(record=dict):
    if not record.get('normalized_date_qualifier') \
        and record.get('dateOther/display/originInfo'):
        if any(pattern in record.get(columns['originInfo/dateOther@display']) \
               for pattern in ['c.', 'ca.', 'circa']):
            record.setdefault('normalized_date_qualifier', 'yes')
    return record


""" Main Function """

def process_xml(file):
    """
    Processes an XML file containing MODS (Metadata Object Description Schema) data and extracts relevant information.

    Args:
        file (str): The path to the XML file to be processed.

    Returns:
        dict: A dictionary containing the extracted MODS data, with field names as keys and corresponding values.

    This function parses the specified XML file and extracts all data elements.  It iterates through each XML element, checks for special cases, retrieves data, and constructs a dictionary
    representing the MODS record. Additional processing is performed to ensure data consistency and completeness.
    The resulting dictionary contains field-value pairs representing the MODS metadata.
    """

    # Create an XML object that python can parse
    xml_object = ET.parse(file)
    # Get the root of that object
    root = xml_object.getroot()
    # Ensure that XML tree elements have MODS namespace prefix
    xml_object, root = ensure_mods_prefix(xml_object, root)

    # Create dictionary with element xpath as key and text as value
    record = {}

    for element in root.xpath('.//*'):
        xpath = get_xpath(xml_object, element)
        special_field = check_special_field(element, xpath)
        tag = element.tag.replace(f'{mods_ns}', '')
        text = remove_whitespaces(element.text)
        data = []
        type_attribute = element.attrib.get('type')
        authority_attribute = element.attrib.get('authority')
        
        # Check that current element and parent are not special/nested fields
        # and that the element text is not empty
        if not special_field and text:
            # Set field and value
            field = xpath
            value = text.replace('\r', ' ')
            # Add attribute value to field
            if type_attribute:
                field += f'@{type_attribute}'
            elif authority_attribute:
                field += f'@{authority_attribute}'
            # Add type to name element
            if tag in ['namePart', 'roleTerm']:
                field = add_name_type(element, xpath)
            if 'relatedItem/' in field:
                field = add_relatedItem_type(element, field)
            # Update xpath to corresponding column name, if one exists
            field = columns[field] if field in columns else field
            # Add data to record
            data.append((field, value))
        elif xpath == 'accessCondition':
            data = get_copyright_data(element)
        elif xpath == 'subject':
            data = get_subject_data(element)
        elif xpath == 'name':
            data = get_name_data(element)

        # Add element data to record dictionary
        for field, value in data:
            if value:
                value = remove_whitespaces(value)
                record.setdefault(field, []).append(value)

    # Create a MODS element from Xpath
    date_qualifier = ModsElement(
        root=root,
        xpath=".//mods:dateCreated[@qualifier='approximate'][@encoding='iso8601'][@keyDate='yes']", 
        namespace=namespaces['mods_ns'], 
        elementname='date_qualifier'
        )
    record.setdefault('normalized_date_qualifier',
                        date_qualifier.get_element_attrib())

    # Check normalized_date_qualifier
    record = check_date_qualifier(record)

    # Ensure that identifier@pitt is in record
    if not 'identifier' in record:
        pid = get_pid(file)
        record.setdefault('identifier', pid)

    # Convert field values from lists to strings
    for field, value in record.items(): 
        if type(value) is list:
            record[field] = '; '.join(value)

    return record