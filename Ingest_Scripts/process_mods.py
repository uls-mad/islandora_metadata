#!/usr/bin/env python3

"""Processes MODS XML elements and transforms data into Workbench records.

Extracts fields from MARCXML/MODS structures, applies vocabulary mappings, 
and validates metadata formatting against controlled taxonomies.
"""

# ---------------------------------------------------------------------------
# Modules
# ---------------------------------------------------------------------------

# Standard library imports
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

# Third-party imports
import pandas as pd
from lxml import etree as ET

# Local imports
from definitions import (
    COUNTRIES,
    FORMATTED_FIELDS,
    LINKED_AGENT_TYPES,
    IGNORED_FIELDS,
    ISSUANCE_MAPPING,
    LANGUAGES,
    MARC_FIELD_MAPPING,
    MODS_NS,
    NAME_TYPES,
    NAMESPACES,
    RELATOR_CODES,
    RELATOR_TERMS,
    SPECIAL_FIELDS,
    TAXONOMIES,
    TYPE_IGNORED_FIELDS,
    TYPE_MAPPING,
)
from process_dates import (
    convert_marc_date_to_edtf,
    resolve_dates,
    validate_edtf_date,
)
from process_related_item import process_related_item
from utilities import (
    LogRegistry,
    cap_first,
    create_directory,
)

# ---------------------------------------------------------------------------
# Global
# ---------------------------------------------------------------------------

LOGGER_NAME = LogRegistry.MAKE_MARC_METADATA_SHEET


# ---------------------------------------------------------------------------
# Class
# ---------------------------------------------------------------------------

@dataclass
class MARCProcessingResult:
    mods_tree: ET.ElementTree
    root: ET._Element
    record_id: str
    record: dict = field(default_factory=dict)
    issues: list = field(default_factory=list)
    transformations: list = field(default_factory=list)

    def __post_init__(self) -> None:
        """Add the record identifier to the output record."""
        self.record.setdefault('record_identifier', self.record_id)

    def log_issue(
            self, 
            field: str, 
            value: Any, 
            issue: str,
            log_msg: str | None = None,
        ):
        """Records an error in the metadata."""
        self.issues.append({
            'record_id': self.record_id,
            'field': field,
            'value': value,
            'exception': issue,
        })
        if not log_msg:
            # Build log message
            log_msg = (
                f"{cap_first(issue)} in field '{field}' with value '{value}'."
            )
        logging.getLogger(LOGGER_NAME).error(
            "Record %s: %s", self.record_id, log_msg
        )

    def log_transformation(
            self, 
            field: str, 
            original_value: Any, 
            new_value: Any, 
            action: str,
            log_msg: str | None = None,
        ):
        """Records a change made to the input metadata (e.g., MARC to EDTF)."""
        self.transformations.append({
            'record_id': self.record_id,
            'field': field,
            'original_value': original_value,
            'new_value': new_value,
            'action': action,
        })
        if not log_msg:
            # Build log message
            log_msg = (
                f"{cap_first(action)} in field '{field}':" 
                f"'{original_value}' -> '{new_value}'."
            )
        logging.getLogger(LOGGER_NAME).info(
            "Record %s: %s", self.record_id, log_msg
        )

    def save_mods_xml(self, output_dir: Path) -> None:
        """Saves MODS XML tree to an XML file in given output directory"""
        mods_dir = create_directory(output_dir / 'MODS')
        self.mods_tree.write(
            mods_dir / f"{self.record_id}.xml",
            pretty_print=True,
            encoding='utf-8'
        )


# ---------------------------------------------------------------------------
#  Functions
# ---------------------------------------------------------------------------

# --- General Utilities ---

def normalize_text(
    value: str | None,
    empty: Literal['none', 'str'] = 'none'
) -> str | None:
    """Normalize text by stripping whitespace and handling false-y values.

    Args:
        value: Input text value (may be None or a string).
        empty: Desired output for empty/false-y values:
            - "none": return None
            - 'str': return empty string ("")
    
    Returns:
        A normalized string, or None/"" depending on `empty`.
    """
    if value is None:
        return None if empty == 'none' else ''

    normalized = str(value).strip()

    if not normalized:
        return None if empty == 'none' else ''

    return normalized


def remove_whitespaces(text: str) -> str:
    """Normalize string spacing by collapsing and stripping arbitrary whitespaces.

    Removes explicit line breaks, carriage returns, and deeply indented spaces, 
    then compresses any remaining consecutive whitespace characters into a single 
    standard space.

    Args:
        text: The raw input string containing potential multi-line gaps or trailing spaces.

    Returns:
        The fully normalized and cleaned plaintext string, or an empty string
    """
    if isinstance(text, str):
        new_text = text.replace('\n    ', ' ').replace('\n', '').strip()
        new_text = re.sub(r'\s+', ' ', new_text)
        new_text = new_text.replace('\r', ' ')
        return new_text.strip()
    return ''


# --- XML Helpers ---

def ensure_mods_prefix(
    tree: ET.ElementTree,
    root: ET._Element
) -> tuple[ET.ElementTree, ET._Element]:
    """Ensure the XML root element uses the explicit MODS namespace prefix.

    Checks if the current root element lacks an explicit namespace prefix. If none 
    is present, a new root element is constructed with the appropriate MODS namespace 
    map, existing child elements are migrated over, and a fresh element tree is built.

    Args:
        tree: The active XML element tree instance being evaluated.
        root: The current root element node of the XML document.

    Returns:
        A tuple containing the validated (or newly generated) ElementTree and 
        its corresponding root Element node.
    """
    if not root.prefix:
        # Create a new root element with 'mods' namespace
        new_root = ET.Element('mods', nsmap=NAMESPACES['mods_ns'])
        # Copy children from the original root to the new root (with 'mods' namespace)
        for child in root:
            new_root.append(child)
        # Create a new tree with the modified root
        new_tree = ET.ElementTree(new_root)
        new_root = new_tree.getroot()
        return new_tree, new_root
    return tree, root


def get_xpath(mods_tree: ET.ElementTree, element: ET._Element) -> str:
    """Extract and normalize the simplified XPath string for a target element.

    Retrieves the raw path from the document tree structure, strips common metadata 
    namespace prefixes, and clears away node index positioning values to aggregate 
    a structural layout signature.

    Args:
        mods_tree: The native XML tree context holding the target element.
        element: The individual node element whose path is being requested.

    Returns:
        The fully simplified and normalized XPath string expression.
    """
    # Get xpath
    xpath = mods_tree.getpath(element)
    # Remove namespace prefixes and root
    xpath = xpath.\
        replace('mods:', '').\
        replace('copyrightMD:', '').\
        replace('/mods/', '')
    # Remove index-like expressions (digits in brackets) from the XPath
    xpath = re.sub(r'\[\d+\]', '', xpath)
    return xpath


def get_tag(element: ET._Element | None) -> str | None:
    """Retrieve the core tag name of an XML element stripped of its namespace.

    Args:
        element: The target XML node element, or None.

    Returns:
        The clean plaintext tag identifier string, or None if the incoming element 
        reference evaluates to empty.
    """
    if element is not None:
        return element.tag.replace(MODS_NS, '')
    return None


def get_toplevel_parent(
    root: ET._Element,
    element: ET._Element
) -> ET._Element | None:
    """Return the highest ancestor below root for an element.

    Args:
        root: Root element of the tree.
        element: Element whose top-level parent should be found.

    Returns:
        The highest ancestor below root, or None if the element is already a
        direct child of root or is not a descendant of root.
    """
    parent = element.getparent()

    while parent is not None:
        if parent.getparent() == root:
            return parent
        parent = parent.getparent()

    return None


def get_child_text(
    parent: ET._Element,
) -> list[str]:
    """Extract and normalize text from all immediate child elements."""
    values = []

    for child in parent:
        value = normalize_text(child.text)
        if value:
            values.append(value)

    return values


# --- Validation/Mapping Helpers ---

def check_if_special_field(tag: str | None) -> bool:
    """Check if the given XML element is classified as a special field.

    Args:
        tag: The XML element node being evaluated.

    Returns:
        True if the element exists within the controlled special fields mapping, 
        otherwise False.
    """
    return tag in SPECIAL_FIELDS


def check_if_agent_field(field: str) -> bool:
    """Verify if a field identifier string matches an authorized agent profile.

    Splits the string on its final suffix to cross-examine whether the composite parts 
    align with controlled relator terms and validated name types.

    Args:
        field: The field string identifier requiring agent validation.

    Returns:
        True if the field accurately encapsulates a known agent structural pattern, 
        otherwise False.
    """
    if not field or '_' not in field:
        return False

    role_term, name_type = field.rsplit('_', 1)

    role_term = role_term.replace('_', ' ')
    
    return role_term in RELATOR_TERMS and name_type in NAME_TYPES.values()


def validate_term(
    result: MARCProcessingResult,
    field: str,
    value: str,
    taxonomy: str,
) -> bool:
    """Validate that a term exists in the specified taxonomy.

    This function checks whether ``value`` appears in the global ``TAXONOMIES``
    DataFrame with the given ``taxonomy`` (stored in the ``'Vocabulary'`` column).
    If the term cannot be found, an exception is logged via ``add_exception`` and
    the function returns ``False``.

    Args:
        pid: PID of the record being processed (used for logging).
        field: Name of the CSV column the value came from (used for logging).
        value: The term to validate.
        taxonomy: Name of the taxonomy/vocabulary in which the term must appear.

    Returns:
        ``True`` if the term is considered valid (found in the taxonomy), 
        ``False`` if it is missing from the taxonomy and an exception was logged.
    """
    mask = ((TAXONOMIES['Name'] == value) &
            (TAXONOMIES['Vocabulary'] == taxonomy))

    matching_rows = TAXONOMIES.loc[mask]

    if matching_rows.empty:
        result.log_issue(
            field,
            value,
            f"could not find term in {taxonomy} taxonomy",
            f"Could not find term '{value}' in {taxonomy} taxonomy",
        )
        return False

    return True


def get_mapped_field( 
    record_id: str,
    xpath: str, 
    value: str,
) -> tuple[str | None, str | None, str | None]:
    """Crosswalk a raw XML XPath to its target field mapping and taxonomy context.

    Looks up metadata rules within a mapping dataframe using the XPath expression. 
    If a record rule match is omitted, it falls back to dynamically evaluating 
    whether the location represents an agent relationship or flags an unmatched 
    field warning to the active session logger.

    Args:
        record_id: The unique identifier tracking the active metadata record.
        xpath: The modified XPath layout string representing the current node source.
        value: The string data payload associated with the target position.

    Returns:
        A tuple containing the target destination field string identifier, the 
        optionally prefixed or transformed string value, and an optional 
        associated taxonomy or vocabulary string constraint.
    """
    logger = logging.getLogger(LOGGER_NAME)
    template_field = xpath
    field = template_field
    prefix = None
    taxonomy = None
    match = MARC_FIELD_MAPPING.loc[
        MARC_FIELD_MAPPING['xpath'] == xpath, 
        ['field', 'prefix', 'taxonomy']
    ]
    
    if not match.empty:
        field = match['field'].iloc[0]
        if not pd.isna(field):
            template_field = field
        prefix = match['prefix'].iloc[0]
        if pd.isna(prefix):
            prefix = None
        taxonomy = match['taxonomy'].iloc[0]
    else:
        # Check if it is an role-based agent or name
        if check_if_agent_field(xpath):
            agent_type = LINKED_AGENT_TYPES.get(xpath.split('_')[-1])
            if agent_type:
                taxonomy = agent_type.replace('_', ' ').title()
        else:
            if logger.hasHandlers() or logging.getLogger().hasHandlers():
                logger.warning(
                    "Record %s: Could not find match for XPath %s", 
                    record_id, xpath
                )
    # Add prefix only for note field, as other prefixes are for ingest sheet
    if prefix and field == 'note':
        value = f"{prefix} {value}"

    return template_field, value, taxonomy


# --- Field Extractors ---

def get_record_id(root: ET._Element) -> str:
    """Extract the primary record identifier from the MODS metadata block.

    Locates the recordIdentifier element nested within the recordInfo container. 
    If found, its inner text is normalized and returned; otherwise, a fallback 
    default string indicator is provided.

    Args:
        root: The root XML element node of the MODS record.

    Returns:
        The normalized string identifier for the record, or "UNKNOWN" if the 
        identifier tag is missing or unreadable.
    """
    recordIdentifier = root.find(
        f"{MODS_NS}recordInfo/{MODS_NS}recordIdentifier"
    )
    record_id = (
        normalize_text(recordIdentifier.text) 
        if recordIdentifier is not None 
        else None
    )
    return record_id or 'UNKNOWN'


def get_accessCondition_data(
    accessCondition: ET._Element,
) -> list[tuple[str, str]]:
    """Extract and structure data from a MODS access condition element.

    Parses text descriptions along with contextual type and display labels, 
    and checks for embedded extension metadata schemas (such as copyrightMD) 
    to compile an inventory of fields like copyright status and rights holders.

    Args:
        accessCondition: The XML node containing data restrictions or terms of use.

    Returns:
        A list of key-value tuples representing non-empty mapped metadata elements.
    """
    data = []

    accessCondition_text = normalize_text(accessCondition.text)
    accessCondition_type = normalize_text(
        accessCondition.attrib.get('type'),
        'str'
    )
    displayLabel = normalize_text(
        accessCondition.attrib.get('displayLabel'),
        'str'
    )

    if accessCondition_text:
        parts = []

        if accessCondition_type:
            parts.append(f"{accessCondition_type.title()}:")

        if displayLabel:
            parts.append(f"{displayLabel.title()} -")

        parts.append(accessCondition_text)

        value = " ".join(parts)
        data.append(('accessCondition', value))

    copyright = accessCondition.find(
        'copyrightMD:copyright',
        NAMESPACES['copyright_ns']
    )

    if copyright is not None:
        rights_holder_name = copyright.findtext(
            'copyrightMD:rights.holder/copyrightMD:name',
            namespaces=NAMESPACES['copyright_ns']
        )
        rights_holder_note = copyright.findtext(
            'copyrightMD:rights.holder/copyrightMD:note',
            namespaces=NAMESPACES['copyright_ns']
        )
        general_note = copyright.findtext(
            'copyrightMD:general.note',
            namespaces=NAMESPACES['copyright_ns']
        )

        data.extend([
            (
                'copyright_status',
                normalize_text(copyright.attrib.get('copyright.status'))
            ),
            (
                'copyright_holder',
                normalize_text(rights_holder_name)
            ),
            (
                'copyright_note',
                normalize_text(rights_holder_note)
            ),
            (
                'copyright_note',
                normalize_text(general_note)
            ),
        ])

    return [(key, value) for key, value in data if value]


def get_title_data(
    result: MARCProcessingResult, 
    xpath: str,
    title: ET._Element, 
    title_type: str
) -> list[tuple[str, str]]:
    """Extract and format title data from a MODS <titleInfo> element.

    Builds a structured title string using the following optional subelements:
    nonSort, title, subTitle, partNumber, and partName.

    Args:
        title: The MODS <titleInfo> element.
        title_type: The type attribute of the title (e.g., "alternative"),
            used to construct the output field name.

    Returns:
        A list containing a single (field, value) tuple if a title is present,
        otherwise an empty list.
    """
    title_elements = [
        'nonSort',
        'title',
        'subTitle',
        'partNumber',
        'partName',
    ]

    title_parts = {}

    field = f"{title_type}_title" if title_type else 'title'

    for tag in title_elements:
        part = title.find(f"{MODS_NS}{tag}")
        text = normalize_text(part.text) if part is not None else None
        if text:
            title_parts[tag] = text

    if not title_parts.get('title'):
        # Log missing title
        result.log_issue(
            xpath,
            ', '.join(get_child_text(title)),
            "missing title"
        )
        return []

    parts = []

    if title_parts.get('nonSort'):
        parts.append(f"{title_parts['nonSort']} ")

    parts.append(title_parts['title'])

    if title_parts.get('subTitle'):
        parts.append(f": {title_parts['subTitle']}")

    if title_parts.get('partNumber'):
        parts.append(f", {title_parts['partNumber']}")

    if title_parts.get('partName'):
        parts.append(f", {title_parts['partName']}")

    title_str = ''.join(parts)

    return [(field, title_str)]


def get_name_data(
    result: MARCProcessingResult,
    xpath: str,
    name: ET._Element
) -> list[tuple[str, str]]:
    """Extract linked-agent data from a MODS <name> or <agent> element.

    Builds one or more field/value pairs by extracting namePart values,
    resolving all roleTerm values as relator terms or relator codes, and
    determining the name type. If multiple roleTerm values resolve to the same
    term, only one tuple is returned for that relator. If no relator is
    available, the field defaults to attributed_name.

    Args:
        result: Processing result object used to log issues and transformations.
        xpath: XPath-like location of the name element.
        name: MODS <name> or <agent> element.
    """
    def clean_name_part(text: str | None) -> str:
        """Remove leading/trailing commas and whitespace."""
        return re.sub(r'^[\s,]+|[\s,]+$', '', text or '')

    def log_relator_note(
        original_value: str,
        resolved_value: str | None,
        relator_data: dict | None
    ) -> None:
        """Log a relator note when lookup data includes one."""
        if not relator_data:
            return

        note = relator_data.get('note')
        if note:
            result.log_transformation(
                xpath,
                original_value,
                resolved_value,
                f"encountered note for relator: {note}"
            )

    def resolve_role_term(role_value: str | None) -> str | None:
        """Resolve a roleTerm value as either a relator term or relator code."""
        role_value = normalize_text(role_value)

        if not role_value:
            return None

        role_key = role_value.lower()

        term_data = RELATOR_TERMS.get(role_key)
        if term_data:
            log_relator_note(role_key, role_key, term_data)
            return role_key

        code_data = RELATOR_CODES.get(role_key)
        if code_data:
            resolved_term = code_data.get('term')
            log_relator_note(role_key, resolved_term, code_data)
            return resolved_term or role_key

        result.log_issue(
            xpath,
            role_value,
            "could not resolve relator as term or code"
        )
        return role_key

    def resolve_role_terms() -> list[str]:
        """Resolve all roleTerm values from all role elements."""
        resolved_terms = []

        for role in name.findall(f"{MODS_NS}role"):
            for role_term in role.findall(f"{MODS_NS}roleTerm"):
                resolved = resolve_role_term(role_term.text)
                if resolved:
                    resolved_terms.append(resolved)

        return list(dict.fromkeys(resolved_terms))

    def get_name_type(role_terms: list[str]) -> str:
        """Return mapped name type, inferring untyped publishers as corporate."""
        raw_type = normalize_text(name.get('type'))

        if raw_type:
            return NAME_TYPES.get(raw_type, raw_type)

        normalized_roles = {role.lower() for role in role_terms}

        if 'publisher' in normalized_roles:
            result.log_transformation(
                xpath,
                name_text,
                None,
                "missing name type; inferred 'corporate' for publisher"
            )
            return 'corporate'

        result.log_issue(
            xpath,
            name_text,
            "missing name type; using 'untyped'"
        )
        return 'untyped'

    name_text = ', '.join(
        cleaned
        for text in get_child_text(name)
        if (cleaned := clean_name_part(text))
    )

    name_parts = [
        cleaned
        for name_part in name.findall(f"{MODS_NS}namePart")
        if (cleaned := clean_name_part(name_part.text))
    ]

    if not name_parts:
        result.log_issue(
            xpath,
            name_text,
            "missing namePart value"
        )
        return []

    name_parts_str = ', '.join(name_parts)

    role_terms = resolve_role_terms()

    if not role_terms:
        usage = name.get('usage')
        if usage in {'primary', 'primaryDisplay'}:
            role_terms = ['creator']
            result.log_transformation(
                xpath,
                name_text,
                None,
                "assigned relator 'creator' based on primary usage"
            )

    name_type = get_name_type(role_terms)

    data = []

    for role_term in role_terms:
        normalized_role = '_'.join(role_term.lower().split())
        data.append((f"{normalized_role}_{name_type}", name_parts_str))

    if not data:
        if 'subject' in xpath:
            field = xpath
        else:
            field = f'attributed_name_{name_type}'
            result.log_transformation(
                xpath,
                name_text,
                None,
                "could not find relator; using 'attributed_name'"
            )
        data.append((field, name_parts_str))

    return data


def get_subject_data(
    result: MARCProcessingResult,
    xpath: str,
    subject: ET._Element,
) -> list[tuple[str, str]]:
    """Extract and parse structured metadata descriptors from a MODS subject element.

    Processes complex sub-elements sequentially (such as names, titles, geographic 
    hierarchies, and cartographics), updates contextual field mapping patterns, 
    and captures pre- or post-coordinated subject heading transformations in the 
    record log state.

    Args:
        result: The active processing record instance managing error tracking 
            and transformation logs.
        xpath: The simplified XPath locating the subject node within the DOM.
        subject: The subject XML element node being parsed.

    Returns:
        A list of field-name and value string tuples extracted from the subject elements.
    """
    data = []
    values = []
    children = subject.getchildren()
    if not children:
        return data
    
    # Extract and transform data
    for child in children:
        tag = get_tag(child)
        field = f"subject/{tag}"
        type_attribute = child.attrib.get('type')
        value = None
        if tag == 'name':
            if type_attribute:
                field = f"{field}@{type_attribute}"
            else:
                field = f"{field}@untyped"
                # Log error that a name type was not found
                result.log_issue(
                    field,
                    ', '.join(get_child_text(child)),
                    "could not identify name type; using 'untyped'"
                )
            name_data = get_name_data(result, xpath, child)
            if name_data:
                value = name_data[0][1]
        elif tag == 'titleInfo':
            title_data = get_title_data(result, xpath, child, None)
            if title_data:
                value = title_data[0][1]
        elif tag == 'hierarchicalGeographic':
            value = '--'.join(get_child_text(child))
        elif tag == 'cartographics': 
            for grandchild in child.getchildren():
                tag = get_tag(grandchild)
                field = f"subject/cartographics/{tag}"
                value = normalize_text(grandchild.text)
                if value:
                    values.append(value)
                    data.append((field, value))
            continue
        else:
            value = normalize_text(child.text)

        if value:
            values.append(value)
            data.append((field, value))

    # Remove NoneType values
    values = [value for value in values if value is not None and value != '']

    # Join values, separated by a double-hyphen
    if len(values) > 1:
        values_str = '--'.join(values)
        # Log pre-/post-coordinated heading as transformation
        result.log_transformation(
            xpath, 
            values_str, 
            None, 
            "split coordinated heading" 
        )
    
    return data


def get_language_data(
    result: MARCProcessingResult,
    xpath: str,    
    language_term: ET._Element
) -> list[tuple[str, str]] | None:
    """Validate or resolve a MODS languageTerm element.

    Depending on the xpath, either converts a language code to its
    corresponding term or validates a language term against a known list.

    Args:
        language_term: The <languageTerm> element.
        xpath: Indicates how to interpret the value:
            - "language/languageTerm@code"
            - "language/languageTerm@term"

    Returns:
        A list of tuples containing the xpath and the normalized language term
        if valid/resolved; otherwise, None.
    """
    value = normalize_text(language_term.text)

    if not value:
        return None

    if xpath == "language/languageTerm@code":
        code = value.lower()
        term = LANGUAGES.get(code)

        if not term:
            # Log warning: no language term found for code
            result.log_issue(
                xpath,
                value,
                "could not find matching term for code"
            )
            return None

        return [(xpath, term)]

    elif xpath == "language/languageTerm@term":
        term = value

        # Normalize for comparison
        normalized_values = {
            v.lower() for v in LANGUAGES.values()
        }

        if term.lower() not in normalized_values:
            # Log error: language term not recognized
            result.log_issue(
                xpath,
                term,
                "could not find term in Language taxonomy"
            )

        return [(xpath, term)]

    else:
        # Log error: unexpected xpath
        result.log_issue(
            xpath,
            value,
            "encountered unexpected xpath"
        )
        return [(xpath, value)]
    

def get_place_data(
    result: MARCProcessingResult,
    xpath: str,
    place_term: ET._Element
) -> list[tuple[str, str]] | None:
    """Validate or resolve a MODS placeTerm element.

    Depending on the xpath, either converts a place code to its
    corresponding term or validates a place term against a known list.

    Args:
        place_term: The <placeTerm> element.
        xpath: Indicates how to interpret the value:
            - "place/placeTerm@code"
            - "place/placeTerm@term"

    Returns:
        A list of tuples containing the xpath and the normalized place term if
            valid/resolved; otherwise, None.
    """
    value = normalize_text(place_term.text)

    if not value:
        return None

    code = value.lower()
    term = COUNTRIES.get(code, code)

    if not term:
        # Log warning: no place term found for code
        result.log_issue(
            xpath,
            value,
            "could not find matching term for code"
        )

    return [(xpath, term)]


def get_date_data(
    result: MARCProcessingResult,
    element: ET._Element,
    tag: str,
    field: str
) -> list[tuple[str, str]]:
    """Extract and normalize date data from a MODS originInfo date element.

    Combines start/end date pairs into EDTF intervals when applicable and
    converts MARC-style date syntax to EDTF using marc_date_to_edtf(). End-point
    date elements are skipped because they are processed with their matching
    start date.

    Args:
        element: MODS date element.
        tag: Local name of the MODS date element, such as "dateIssued".

    Returns:
        A list of (field, value) tuples. Returns an empty list if no usable
        date value is found.
    """
    logger = logging.getLogger(LOGGER_NAME)
    point = element.get('point')
    
    # Handle MODS start/end point pairs
    if point == 'end':
        return []

    raw_date = normalize_text(element.text)
    if not raw_date:
        return []
    
    if point == 'start':
        origin_info = element.getparent()
        end_point = origin_info.find(f"{MODS_NS}{tag}[@point='end']")
        if end_point is not None:
            end_date = normalize_text(end_point.text)
            if end_date:
                raw_date = f"{raw_date}/{end_date}"

    # Check for date qualifiers
    qualifier = element.get('qualifier')
    is_approximate, is_inferred, is_uncertain = (
        qualifier == q for q in ('approximate', 'inferred', 'questionable')
    )

    # Process date value(s)
    edtf_results = convert_marc_date_to_edtf(
        raw_date,
        is_approximate,
        is_inferred,
        is_uncertain
    )

    if not edtf_results:
        logger.error(
            "Record %s: Could not convert date %s to EDTF.", 
            result.record_id, raw_date
        )
        result.log_issue(
            field,
            raw_date,
            "could not convert MARC date to EDTF date"
        )
        return [(field, raw_date)]

    data = []

    # Loop through each tuple in the result list
    for edtf_date, is_copyright_date, used_fallback in edtf_results:
        # Log transformation
        if raw_date != edtf_date:
            result.log_transformation(
                field,
                raw_date,
                edtf_date,
                "converted MARC date to EDTF"
            )
        
        # Validate syntax
        if not validate_edtf_date(edtf_date):
            result.log_issue(
                field,
                edtf_date,
                "invalid EDTF date",
                (
                    f"MARC date '{raw_date}' was converted to an invalid EDTF" 
                    f"date'{edtf_date}'."
                )
            )

        # Handle data assignment based on copyright flag
        if is_copyright_date:
            data.append(('copyright_date', edtf_date))
            result.log_transformation(
                field,
                raw_date,
                edtf_date,
                "extracted copyright date"
            )
        else:
            data.append((f'originInfo/{tag}', edtf_date))

        # Flag that fallback date was used
        if used_fallback:
            result.log_transformation(
                field,
                raw_date,
                edtf_date,
                "fallback date was used; check for accuracy"
            )

    return data


# --- Record Finalization Helpers ---

def process_values_by_field(
    field: str,
    values: list[str],
) -> list[str]:
    """Apply specialized value formatting based on individual field restrictions.

    Interprets special business logic adjustments (such as formatting and resolving 
    EDTF dates, stripping punctuation from locations, or normalising and stripping 
    alphabetic prefixes from OCLC standard numbers) on grouped multi-valued elements.

    Args:
        field: The destination field identifier being formatted.
        values: A list of accumulated raw string elements belonging to this field.

    Returns:
        A list of cleaned and fully transformed string values.
    """
    if field == 'date':
        values = resolve_dates(values)

    elif field == 'publication_place':
        # Remove trailing colon, as in place of publication
        values = [val.strip(":") for val in values]

    elif field == 'oclc_number':
        values = [
            re.sub(
                r"^(?:\(OCoLC\)|ocm|ocn|on)", 
                "", 
                val, 
                flags=re.I
            ).lstrip('0')
            for val in values
        ]

    return values


def finalize_record_values(
    record: dict[str, list[str] | str]
) -> None:
    """Convert record field values from lists to pipe-delimited strings.

    Applies field-specific processing, removes duplicate values while
    preserving order, and joins multi-valued fields with a pipe character.

    Args:
        record: Record dictionary containing field names and values.
    """
    for field, values in record.items():
        if isinstance(values, list):
            values = process_values_by_field(field, values)
            values = list(dict.fromkeys(values))
            record[field] = '|'.join(values)


# --- Orchestrators ---

def process_field(
    result: MARCProcessingResult, 
    element: ET._Element, 
    logger: logging.Logger
) -> list[tuple[str, str]] | None:
    """Parse an individual XML element and route it to its specific data extraction handler.

    Evaluates structural conditions, checks exclusion policies, extracts contextual attributes 
    (such as type and event properties), and maps text contents or structural nodes into 
    standardized key-value data packets.

    Args:
        result: The active processing record instance containing state properties like the 
            document tree and root reference.
        element: The target XML DOM element node to be evaluated.
        logger: The localized system logger instance used to track schema warnings and metrics.

    Returns:
        A list of key-value mapping tuples extracted from the node, or None if the element 
        is explicitly bypassed by structural exclusions.
    """
    tag = get_tag(element)
    parent_tag = get_tag(get_toplevel_parent(result.root, element))

    # Skip elements that shouldn't be processed
    is_special_field = check_if_special_field(parent_tag)
    is_ignored_field = (
        tag in IGNORED_FIELDS
        or parent_tag in IGNORED_FIELDS
    )
    if is_special_field or is_ignored_field:
        return None

    # Get element's modified XPath and text
    xpath = get_xpath(result.mods_tree, element)
    text = element.text

    # Get type attribute value
    type_attribute = None
    if tag == 'originInfo':
        type_attribute = element.attrib.get('eventType')
    elif tag not in TYPE_IGNORED_FIELDS:
        type_attribute = element.attrib.get('type')
    elif element.attrib.get('type'):
        logger.info(
            (
                "Record %s: Ignoring type attribute '%s' for element %s "
                "with value '%s'"
            ), 
            result.record_id, element.attrib.get('type'), xpath, text
        )

    # Get data from field
    data = []

    if tag == 'relatedItem':
        data = process_related_item(result, element)
    elif tag == 'accessCondition':
        data = get_accessCondition_data(element)
    elif tag == 'titleInfo':
        data = get_title_data(result, xpath, element, type_attribute)
    elif xpath == 'subject':
        data = get_subject_data(result, xpath, element)
    elif tag in ['agent', 'name']:
        data = get_name_data(result, xpath, element)
    elif tag in ['dateCreated', 'dateIssued']:
        data = get_date_data(result, element, tag, xpath)
    elif tag == 'placeTerm' and type_attribute == 'code':
        xpath = f"{xpath}@code"
        data = get_place_data(result, xpath, element)
    elif tag == 'languageTerm':
        xpath = f"{xpath}@{type_attribute}"
        data = get_language_data(result, xpath, element)
    elif tag == 'issuance':
        data = [(xpath, ISSUANCE_MAPPING.get(text, text))]
    elif tag == 'typeOfResource':
        data = [(xpath, TYPE_MAPPING.get(text, text))]
    elif text and not check_if_special_field(tag):
        # Add type attribute to xpath
        if type_attribute:
            xpath += f'@{type_attribute}'
        # Set field and value
        field = xpath
        value = text
        # Remove non-single space whitespaces from non-formatted text fields
        if tag not in FORMATTED_FIELDS:
            value = remove_whitespaces(text)
        # Store data
        data.append((field, value))

    return data


def process_data(
    result: MARCProcessingResult, 
    data: list[tuple[str, str]], 
    logger: logging.Logger
) -> None:
    """Iterate over extracted metadata elements to map, validate, and store values.

    Validates that incoming data packets are structured as field-value pairs, normalizes 
    their string content, crosswalks them to their destination schema positions, 
    and verifies taxonomy rules before appending them to the tracking record.

    Args:
        result: The active processing record instance managing the destination storage 
            and validation logs.
        data: A collection of elements, typically key-value pairs, harvested from 
            the source document.
        logger: The system logger instance used to capture execution errors safely.
    """
    for item in data:
        is_field_value_pair = isinstance(item, tuple) and len(item) == 2
        if not is_field_value_pair:
            continue
        field, value = item
        try:
            if normalize_text(field) and normalize_text(value):
                i2_field, value, taxonomy = get_mapped_field(
                    result.record_id, field, value
                )
                if i2_field:
                    result.record.setdefault(i2_field, []).append(value)
                if taxonomy:
                    validate_term(
                        result,
                        i2_field,
                        value,
                        taxonomy
                    )
        except Exception:
            logger.exception(
                "Record %s: Could not add data for field %s with value '%s'.",
                result.record_id, field, data
            )


def process_mods(root: ET._Element) -> MARCProcessingResult:
    """Orchestrate the extraction, crosswalking, and validation of MODS XML elements.

    Normalizes the source XML namespace schema, traverses all descending XML nodes 
    to dispatch field-specific parsing handlers, translates extracted values to 
    their corresponding target identifiers, verifies vocabulary constraints, and 
    aggregates elements into a dictionary.

    Args:
        root: The root XML element node of the MODS record being processed.

    Returns:
        The updated processing result tracking instance containing the formatted 
        and vectorized field matrix.
    """
    # Get logger
    logger = logging.getLogger(LOGGER_NAME)
    
    # Convert to ElementTree
    mods_tree = ET.ElementTree(root)

    # Ensure that XML tree elements have MODS namespace prefix
    mods_tree, root = ensure_mods_prefix(mods_tree, root)

    # Get record identifier
    record_id = get_record_id(root)

    # Initialize result object
    result = MARCProcessingResult(
        mods_tree=mods_tree,
        root=root,
        record_id=record_id, 
    )

    # Process elements
    all_elements = root.xpath('.//*')

    for element in all_elements:
        data = process_field(result, element, logger)

        if data:
            process_data(result, data, logger)

    # Convert field values from lists to strings
    finalize_record_values(result.record)

    return result
