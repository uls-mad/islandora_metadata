#!/usr/bin/env python3

# ---------------------------------------------------------------------------
# Modules
# ---------------------------------------------------------------------------

# Standard library imports
from pathlib import Path

# Local imports
from utilities import (
    create_df,
    csv_to_dict
)


# ---------------------------------------------------------------------------
# Sets
# ---------------------------------------------------------------------------

# --- Field Sets ---

SYSTEM_REQUIRED_FIELDS = {
    'id',
    'title',
    'field_model',
    'field_resource_type',
    'field_domain_access',
}

METADATA_REQUIRED_FIELDS = {
    'field_member_of',
    'field_depositor'
}

MANDATORY_FIELDS = SYSTEM_REQUIRED_FIELDS | METADATA_REQUIRED_FIELDS

MINIMAL_METADATA_FIELDS = MANDATORY_FIELDS | {
    'node_id',
    'field_pid',
    'field_description_long',
    'field_collection_text',
    'field_series_information',
    'field_source_citation',
    'field_source_collection',
    'field_source_collection_id',
    'field_source_location',
    'field_rights_statement',
    'field_rights_notes',
    'field_access_terms',
    'field_local_identifier',
    'field_preservica_date',
    'field_preservica_id',
    'field_main_banner',
    'field_media_oembed_video',
    'parent_id',
    'field_weight',
    'published',
    'file',
    'hocr',
    'thumbnail',
    'transcript',
    'extracted_text',
}

PUBLISH_FIELDS = {
    'node_id', 
    'published'
}

DISALLOWED_FIELDS_BY_INGEST_TASK = {
    'create': {'node_id'},
    'update': {'id', 'file'},
}

CONTROLLED_FIELDS = {
    'field_depositor',
    'field_display_hints',
    'field_domain_access',
    'field_genre',
    'field_geographic_subject',
    'field_language',
    'field_linked_agent',
    'field_member_of',
    'field_mode_of_issuance',
    'field_model',
    'field_physical_form',
    'field_place_published_pitt',
    'field_resource_type',
    'field_rights_statement',
    'field_source_collection',
    'field_source_collection_id',
    'field_source_repository',
    'field_subject',
    'field_subject_genre',
    'field_subject_title',
    'field_subjects_name',
    'field_temporal_subject',
    'field_type_of_resource',
}

VETTED_FIELDS = {
    'field_depositor',
    'field_genre',
    'field_geographic_subject',
    'field_language',
    'field_linked_agent',
    'field_mode_of_issuance',
    'field_physical_form',
    'field_place_published_pitt',
    'field_source_collection',
    'field_source_collection_id',
    'field_source_repository',
    'field_subject',
    'field_subject_genre',
    'field_subject_title',
    'field_subjects_name',
    'field_temporal_subject',
    'field_type_of_resource'
}

DATE_FIELDS = {
    'field_edtf_date', 
    'field_copyright_date',
    'field_date_str'
}

SPECIAL_FIELDS = {
    'accessCondition', # Captured by get_accessCondition_data()
    'relatedItem', # Captured by process_related_item.process_related_item()
    'subject', # Captured by get_subject_data()
    'titleInfo', # Captured by get_title_data()
    'name', # Captured by get_name_data()
    'agent', # Captured by get_name_data()
    'namePart', # Captured by get_name_data()
    'roleTerm', # Captured by get_name_data()
}

FORMATTED_FIELDS = {
    'abstract', 
    'note', 
    'statement_of_responsibility',
    'tableOfContents'
}

IGNORED_FIELDS = {
    'classification',
    'location',
    'physicalLocation',
    'recordInfo',
    # Children of physicalDescription -  Could be moved to notes with prefixes
        'internetMediaType',  
        'digitalOrigin',
        'reformattingQuality',
}

TYPE_IGNORED_FIELDS = {
    "abstract",
    "accessCondition",
    "form",
    "tableOfContents",
}


# --- Approved Value Sets ---

CONTENT_TYPES = {
    'av',
    'book',
    'image',
    'interview',
    'japanese_prints',
    'manuscript',
    'map',
    'marc',
    'musical_recording',
    'notated_music',
    'photograph',
    'serial',
}

DOMAINS = {
    'americanmusic_library_pitt_edu',
    'digital_library_pitt_edu',
    'documenting_pitt_edu',
    'historicpittsburgh_org',
}

PARENT_MODELS = {
    'Compound Object',
    'Paged Content',
    'Newspaper',
    'Publication Issue',
}


# ---------------------------------------------------------------------------
# Mappings
# ---------------------------------------------------------------------------

# --- Hard-coded Mappings ---

MODEL_MAPPING = {
    'Collection': {
        'resource_type': 'Collection',
        'display_hint': None
    },
    'Compound Object': {
        'resource_type': 'Collection',
        'display_hint': None
    },
    'Paged Content': {
        'resource_type': 'Collection',
        'display_hint': 'Mirador'
    },
    'Newspaper': {
        'resource_type': 'Collection',
        'display_hint': 'Mirador'
    },
    'Publication Issue': {
        'resource_type': 'Collection',
        'display_hint': 'Mirador'
    },
    'Page': {
        'resource_type': 'Text',
        'display_hint': 'Mirador'
    },
    'Digital Document': {
        'resource_type': 'Text',
        'display_hint': 'PDFjs'
    },
    'Image': {
        'resource_type': 'Still Image',
        'display_hint': 'Mirador'
    },
    'Video': {
        'resource_type': 'Moving Image',
        'display_hint': None
    },
    'Audio': {
        'resource_type': 'Sound',
        'display_hint': None
    },
}

TYPE_MAPPING = {
    'cartographic': 'Cartographic',
    'mixed material': 'Mixed material',
    'moving image': 'Moving image',
    'notated music': 'Notated music',
    'software, multimedia': 'Multimedia|Software',
    'software, mutimedia': 'Multimedia|Software',
    'sound recording': 'Audio',
    'Sound Recording': 'Audio',
    'sound recording-musical': 'Audio musical',
    'sound recording-nonmusical': 'Audio non-musical',
    'sound recordings-nonmusical': 'Audio non-musical',
    'still image': 'Still image',
    'still_image': 'Still image',
    'text': 'Text',
    'three dimensional object': 'Artifact'
}

COPYRIGHT_MAPPING = {
    'copyrighted': 'In Copyright',
    'pd': 'No Copyright - United States',
    'pd_usfed': 'No Copyright - United States',
    'pd_holder': 'No Copyright - United States',
    'pd_expired': 'No Copyright - United States',
    'unknown': 'Copyright Undetermined',
}

ISSUANCE_MAPPING = {
    'continuing': 'serial',
    'monographic': 'single unit',
    'serial': 'serial',
}

LINKED_AGENT_TYPES = {
    'conference': 'conference', 
    'corporate': 'corporate_body', 
    'family': 'family',
    'person': 'person'
}

NAME_TYPES = {
    'conference': 'conference', 
    'corporate': 'corporate', 
    'family': 'family',
    'personal': 'person'
}


# --- Imported Mappings ---
PROJECT_ROOT = Path(__file__).resolve().parent
UTILITY_FILES_DIR = PROJECT_ROOT / "Utility_Files"

# Read in mappings as pd.DataFrames
# I2 Fields
# TODO: Build this dynamically from a JSON view?
FIELDS = create_df(
    UTILITY_FILES_DIR / 'i2_field_schema.csv'
)

# Metadata Template-to-I2 CSV field mapping
TEMPLATE_FIELD_MAPPING = create_df(
    UTILITY_FILES_DIR / 'template_to_i2_field_mapping.csv'
)

# Manifest-to-I2 CSV field mapping
MANIFEST_FIELD_MAPPING = create_df(
    UTILITY_FILES_DIR / 'manifest_to_i2_field_mapping.csv'
)

# MARC metadata sheet to I2 field mapping
MARC_FIELD_MAPPING = create_df(
    UTILITY_FILES_DIR / 'marc_to_i2_field_mapping.csv'
)

# I7-to-I2 CSV field mapping
I7_to_I2_MAPPING = create_df(
    UTILITY_FILES_DIR / 'i7_to_i2_template_field_mapping.csv'
)

# Read in taxonomies info as pd.DataFrame
TAXONOMIES = create_df(
    UTILITY_FILES_DIR / 'taxonomies.csv'
)

# Read in MARC vocabularies as dictionaries
COUNTRIES = csv_to_dict(
    UTILITY_FILES_DIR / 'marc_countries.csv'
)

LANGUAGES = csv_to_dict(
    UTILITY_FILES_DIR / 'marc_languages.csv'
)

RELATOR_CODES = csv_to_dict(
    UTILITY_FILES_DIR / 'marc_relators_code.csv', key_col='code'
)

RELATOR_TERMS = csv_to_dict(
    UTILITY_FILES_DIR / 'marc_relators_term.csv', key_col='term'
)


# ---------------------------------------------------------------------------
# Namespaces Info
# ---------------------------------------------------------------------------

NAMESPACES = {
    'mods_ns': {
        'mods': 'http://www.loc.gov/mods/v3'
        },
    'copyright_ns': {
        'copyrightMD': 'http://www.cdlib.org/inside/diglib/copyrightMD'
        }
    }


MODS_NS = '{http://www.loc.gov/mods/v3}'
COPYRIGHTMD_NS = '{http://www.cdlib.org/inside/diglib/copyrightMD}'
