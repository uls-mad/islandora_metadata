#!/usr/bin/env python3

"""Process MODS relatedItem elements into Workbench field/value pairs."""

# ---------------------------------------------------------------------------
# Modules
# ---------------------------------------------------------------------------

# Standard library imports
import copy
import logging
import re
from typing import TYPE_CHECKING

# Third-party imports
from lxml import etree as ET

# Local imports
from definitions import (
    COUNTRIES,
    MODS_NS,
)
from utilities import LogRegistry

if TYPE_CHECKING:
    from process_mods import MARCProcessingResult


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOGGER_NAME = LogRegistry.MAKE_MARC_METADATA_SHEET

RELATED_ITEM_FIELD_MAPPING = {
    'constituent': 'related_title_constituent',
    'host': 'related_title_part_of',
    'isReferencedBy': 'related_title_referenced_by',
    'original': 'related_title_original',
    'otherFormat': 'related_title_other_format',
    'otherVersion': 'related_title_other_version',
    'preceding': 'related_title_preceding',
    'series': 'related_title_part_of',
    'succeeding': 'related_title_succeeding',
}

RELATED_ITEM_NOTE_TEMPLATE = {
    'displayLabel': {
        'prefix': "",
        'text': [],
        'suffix': ": ",
    },
    'namePart': {
        'prefix': "",
        'text': [],
        'suffix': ". ",
    },
    'nonSort': {
        'prefix': "",
        'text': [],
        'suffix': " ",
    },
    'title': {
        'prefix': "",
        'text': [],
        'suffix': ". ",
    },
    'subTitle': {
        'prefix': ": ",
        'text': [],
        'suffix': ". ",
    },
    'edition': {
        'prefix': ", ",
        'text': [],
        'suffix': ". ",
    },
    'partNumber': {
        'prefix': ", ",
        'text': [],
        'suffix': ". ",
    },
    'partName': {
        'prefix': ", ",
        'text': [],
        'suffix': ". ",
    },
    'text': {
        'prefix': ", ",
        'text': [],
        'suffix': ". ",
    },
    'number': {
        'prefix': " ",
        'text': [],
        'suffix': ". ",
    },
    'placeTerm[@type="text"]': {
        'prefix': "",
        'text': [],
        'suffix': ". ",
    },
    'placeTerm[@type="code"]': {
        'prefix': ", ",
        'text': [],
        'suffix': ". ",
    },
    'publisher': {
        'prefix': ": ",
        'text': [],
        'suffix': ". ",
    },
    'dateCreated[@encoding="marc"]': {
        'prefix': ", ",
        'text': [],
        'suffix': ". ",
    },
    'dateIssued[@encoding="marc"]': {
        'prefix': ", ",
        'text': [],
        'suffix': ". ",
    },
    'note[@type="date/sequential designation"]': {
        'prefix': "",
        'text': [],
        'suffix': ". ",
    },
    'identifier[@type="issn"]': {
        'prefix': ". ISSN: ",
        'text': [],
        'suffix': "; ",
    },
    'identifier[@type="issn-l"]': {
        'prefix': ". ISSN-L: ",
        'text': [],
        'suffix': "; ",
    },
    'identifier[@type="lccn"]': {
        'prefix': ". LCCN: ",
        'text': [],
        'suffix': "; ",
    },
    'identifier[@type="oclc"]': {
        'prefix': ". OCLCN: ",
        'text': [],
        'suffix': "; ",
    },
    'identifier[@type="local"]': {
        'prefix': ". Local Identifier: ",
        'text': [],
        'suffix': "; ",
    },
}

SKIPPED_VALUES = {
    '9999',
    'uuuu',
    '1uuu',
    '(OCoLC)',
}


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------

# --- Logging Helpers ---

def log_skipped_element(
    result: "MARCProcessingResult",
    tag: str,
    text: str,
) -> None:
    """Log a relatedItem component that was skipped.

    Args:
        result: Processing result object used for record-level logging.
        tag: Tag or tag-with-attribute label for the skipped component.
        text: Text value that was skipped.
    """
    result.log_issue(
        'relatedItem',
        text,
        f"skipped unsupported relatedItem component: {tag}",
    )


# --- XML Helpers ---

def get_tag(element: ET.Element) -> str:
    """Return an XML element tag without the namespace.

    Args:
        element: XML element.

    Returns:
        Local tag name without the namespace.
    """
    return element.tag.split('}')[-1]


def check_ancestors(
    parent: ET.Element,
    child: ET.Element,
    tag: str,
) -> bool:
    """Check whether a child has a matching ancestor before reaching parent.

    Args:
        parent: Parent element that acts as the search boundary.
        child: Child element whose ancestors should be inspected.
        tag: Local tag name to look for among the ancestors.

    Returns:
        True if a matching ancestor is found before parent; otherwise False.
    """
    for ancestor in child.iterancestors():
        if ancestor == parent:
            return False

        if get_tag(ancestor) == tag:
            return True

    return False


def should_skip_nested_component(
    parent: ET.Element,
    child: ET.Element,
    should_check: bool,
) -> bool:
    """Determine whether a nested subject or relatedItem component should skip.

    Args:
        parent: Parent relatedItem element being processed.
        child: Child element currently being inspected.
        should_check: Whether ancestor checking is currently needed.

    Returns:
        True if the component should be skipped; otherwise False.
    """
    if not should_check:
        return False

    return (
        check_ancestors(parent, child, 'subject')
        or check_ancestors(parent, child, 'relatedItem')
    )


def build_component_tag(child: ET.Element) -> str:
    """Build a tag label with selected MODS attributes.

    Args:
        child: XML element to label.

    Returns:
        Tag label used to match RELATED_ITEM_NOTE_TEMPLATE.
    """
    tag = get_tag(child)

    type_attr = child.get('type')
    if type_attr:
        tag = f'{tag}[@type="{type_attr}"]'

    if tag in {'dateCreated', 'dateIssued'}:
        encoding_attr = child.get('encoding')
        if encoding_attr == 'marc':
            tag = f'{tag}[@encoding="marc"]'

    return tag


def normalize_identifier_tag(tag: str, text: str) -> str:
    """Map local identifier values to more specific identifier types.

    Args:
        tag: Current component tag.
        text: Identifier text.

    Returns:
        Original or remapped identifier tag.
    """
    if tag != 'identifier[@type="local"]':
        return tag

    if 'OC' in text:
        return 'identifier[@type="oclc"]'

    if 'DLC' in text:
        return 'identifier[@type="lccn"]'

    return tag


# --- Note Construction Helpers ---

def process_dates(
    note_components: dict,
) -> None:
    """Combine paired relatedItem MARC dates into a single range value.

    Args:
        note_components: Related-item note component configuration and values.
    """
    date_tags = [
        'dateCreated[@encoding="marc"]',
        'dateIssued[@encoding="marc"]',
    ]

    for date_tag in date_tags:
        dates = note_components[date_tag]['text']

        if len(dates) == 2:
            note_components[date_tag]['text'] = ['-'.join(dates)]


def process_pub_info(
    note_components: dict,
) -> None:
    """Normalize relatedItem publication place and publisher values.

    Args:
        note_components: Related-item note component configuration and values.
    """
    place_text = note_components['placeTerm[@type="text"]']['text']
    place_code = note_components['placeTerm[@type="code"]']['text']
    publisher = note_components['publisher']['text']

    if place_text:
        note_components['placeTerm[@type="code"]']['text'] = []

    elif place_code or (publisher and ':' not in str(publisher)):
        note_components['placeTerm[@type="text"]']['text'] = ['S.l.']

        if place_code and place_code[0] in COUNTRIES:
            note_components['placeTerm[@type="code"]']['text'] = [
                COUNTRIES[place_code[0]]
            ]

    if ':' in str(publisher):
        note_components['publisher']['prefix'] = ""


def update_identifiers(
    note_components: dict,
) -> None:
    """Adjust identifier prefixes so only the first starts a new sentence.

    Args:
        note_components: Related-item note component configuration and values.
    """
    identifier_tags = [
        'identifier[@type="issn"]',
        'identifier[@type="issn-l"]',
        'identifier[@type="lccn"]',
        'identifier[@type="oclc"]',
        'identifier[@type="local"]',
    ]

    first_identifier_found = False

    for identifier_tag in identifier_tags:
        note_components[identifier_tag]['text'] = [
            text for text in note_components[identifier_tag]['text'] if text
        ]

        text_values = note_components[identifier_tag]['text']

        if text_values:
            if first_identifier_found:
                prefix = note_components[identifier_tag]['prefix']
                note_components[identifier_tag]['prefix'] = prefix.replace(
                    ". ",
                    "",
                )
            else:
                first_identifier_found = True


def clean_up_note_text(note_text: str) -> str:
    """Clean punctuation and spacing in a generated relatedItem note.

    Args:
        note_text: Generated note text.

    Returns:
        Cleaned note text.
    """
    note_text = re.sub(r"\.{2,}", ".", note_text)

    replacements = {
        '..': '.',
        ',.': '.',
        ';.': '.',
        ':.': '.',
    }

    for old, new in replacements.items():
        note_text = note_text.replace(old, new)

    note_text = note_text.lstrip('. ')
    note_text = note_text.rstrip('; ')

    unwanted_chars = ['ǂd ', 'ǂt ', 'ǂw ']

    for char in unwanted_chars:
        note_text = note_text.replace(char, "")

    note_text = re.sub(r"\s+", " ", note_text)

    return note_text.strip()


# --- Note Constructor ---

def create_note(
    result: "MARCProcessingResult",
    related_item: ET.Element,
) -> str:
    """Create a note string from a MODS relatedItem element.

    Args:
        result: Processing result object used for record-level logging.
        related_item: MODS relatedItem element.

    Returns:
        Generated relatedItem note text, or an empty string if none is created.
    """
    logger = logging.getLogger(LOGGER_NAME)

    try:
        note_components = copy.deepcopy(RELATED_ITEM_NOTE_TEMPLATE)
        note = ""
        check_for_elements_to_skip = False

        for child in related_item.iter():
            original_text = child.text or ""
            text = original_text.strip()
            tag = get_tag(child)

            if tag in {'subject', 'relatedItem'}:
                check_for_elements_to_skip = True

            if not text:
                continue

            skipped = should_skip_nested_component(
                related_item,
                child,
                check_for_elements_to_skip,
            )

            if skipped:
                log_skipped_element(result, tag, original_text)
                continue

            if check_for_elements_to_skip:
                check_for_elements_to_skip = False

            component_tag = build_component_tag(child)
            component_tag = normalize_identifier_tag(component_tag, text)

            if text in SKIPPED_VALUES:
                log_skipped_element(result, component_tag, original_text)
                continue

            if component_tag in note_components:
                note_components[component_tag]['text'].append(text)
            else:
                log_skipped_element(result, component_tag, original_text)

        process_dates(note_components)
        process_pub_info(note_components)
        update_identifiers(note_components)

        for _component, values in note_components.items():
            component_text = ', '.join(values['text'])

            if not component_text:
                continue

            if note.endswith('. ') and values['prefix']:
                note = note[:-2]

            note += values['prefix'] + component_text + values['suffix']

        return clean_up_note_text(note)

    except Exception as exc:
        logger.exception(
            "Record %s: Failed to create relatedItem note.",
            result.record_id,
        )
        result.log_issue(
            'relatedItem',
            ET.tostring(related_item, encoding='unicode'),
            "runtime exception while creating relatedItem note",
            str(exc),
        )
        return ""


# --- Main Workflow ---

def process_related_item(
    result: "MARCProcessingResult",
    related_item: ET.Element,
) -> list[tuple[str, str]]:
    """Process a MODS relatedItem element into Workbench field/value pairs.

    Args:
        result: Processing result object used for record-level logging.
        related_item: MODS relatedItem element.

    Returns:
        List of field/value tuples extracted from the relatedItem.
    """
    logger = logging.getLogger(LOGGER_NAME)

    try:
        field_values = []

        related_item_type = related_item.attrib.get('type')
        field = RELATED_ITEM_FIELD_MAPPING.get(
            related_item_type,
            'related_title',
        )

        note = create_note(result, related_item)

        if note:
            field_values.append((field, note))
        else:
            result.log_issue(
                'relatedItem',
                related_item_type,
                "could not create relatedItem note",
            )

        related_item_subelements = related_item.findall(
            f".//{MODS_NS}relatedItem"
        )

        for nested_related_item in related_item_subelements:
            nested_note = create_note(result, nested_related_item)

            if nested_note:
                field_values.append(('related_title', nested_note))
            else:
                result.log_issue(
                    'relatedItem',
                    related_item_type,
                    "could not create nested relatedItem note",
                )

        return field_values

    except Exception as exc:
        logger.exception(
            "Record %s: Failed to process relatedItem.",
            result.record_id,
        )
        result.log_issue(
            'relatedItem',
            related_item.attrib.get('type'),
            "runtime exception while processing relatedItem",
            str(exc),
        )
        return []
