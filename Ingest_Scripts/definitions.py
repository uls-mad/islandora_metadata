#!/bin/python3 

""" Modules """

# Import standard module
import os

# Import local module
from utilities import create_df


""" FIELD LISTS """

REQUIRED_FIELDS = [
    'id',
    'title',
    'field_model',
    'field_resource_type',
    'field_member_of',
    'field_domain_access'
]

MANIFEST_FIELDS = [
    'id',
    'field_pid',
    'node_id',
    'title',
    'file',
    'field_model',
    'field_resource_type',
    'field_domain_access'
]

CONTROLLED_FIELDS = [
    "field_depositor",
    "field_display_hints",
    "field_domain_access",
    "field_genre",
    "field_geographic_subject",
    "field_language",
    "field_linked_agent",
    "field_member_of",
    "field_mode_of_issuance",
    "field_model",
    "field_physical_form",
    "field_place_published_pitt",
    "field_resource_type",
    "field_rights_statement",
    "field_source_collection",
    "field_source_collection_id",
    "field_source_repository",
    "field_subject",
    "field_subject_genre",
    "field_subject_title",
    "field_subjects_name",
    "field_temporal_subject",
    "field_type_of_resource",
]

VETTED_FIELDS = [
    "field_depositor",
    "field_genre",
    "field_geographic_subject",
    "field_language",
    "field_linked_agent",
    "field_mode_of_issuance",
    "field_physical_form",
    "field_place_published_pitt",
    "field_source_collection",
    "field_source_collection_id",
    "field_source_repository",
    "field_subject",
    "field_subject_genre",
    "field_subject_title",
    "field_subjects_name",
    "field_temporal_subject",
    "field_type_of_resource"
]

DATE_FIELDS = [
    'field_edtf_date', 
    'field_copyright_date',
    'field_date_str'
]

GEO_FIELDS = [
    'field_addresses',
    'field_geographic_features',
    'field_geographic_features_categories'
    'field_thorougfares'
]

SOURCE_FIELDS = [
    'field_source_collection_id',
    'field_source_collection',
    'field_source_repository',
    'field_source_citation'
]

SUBJECT_FIELDS = [
    'field_geographic_subject',
    'field_subject',
    'field_subject_genre',
    'field_subject_title',
    'field_subjects_name',
    'field_temporal_subject',
]

DELIMITED_FIELDS = [    
    'field_addresses',
    'field_coordinates',
    'field_copyright_date',
    'field_copyright_holder',
    'field_edition',
    'field_edtf_date',
    'field_extent',
    'field_frequency',
    'field_genre',
    'field_geographic_features',
    'field_geographic_subject',
    'field_isbn',
    'field_issn',
    'field_language',
    'field_linked_agent',
    'field_local_identifier',
    'field_physical_form',
    'field_scale',
    'field_subject',
    'field_subject_title',
    'field_subjects_name',
    'field_temporal_subject',
    'field_thoroughfares',
]    

PARENT_MODELS = [
    'Compound Object',
    'Paged Content',
    'Newspaper',
    'Publication Issue',
]


""" MAPPINGS """

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


# mods_typeOfResource_ms -> resource_types
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

COPYRIGHT_STATUS_MAPPING = {
    "copyrighted": "In Copyright",
    "pd": "No Copyright - United States",
    "pd_usfed": "No Copyright - United States",
    "pd_holder": "No Copyright - United States",
    "pd_expired": "No Copyright - United States",
    "unknown": "Copyright Undetermined",
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
    'personal': 'person'
}

# Subject Type -> I2 field
SUBJECT_FIELD_MAPPING = {
    'conference': 'field_subjects_name',
    'corporate': 'field_subjects_name',
    'family': 'field_subjects_name',
    'genre': 'field_subject_genre',
    'geographic': 'field_geographic_subject',
    'personal': 'field_subjects_name',
    'temporal': 'field_temporal_subject',
    'title': 'field_subject_title',
    'topic': 'field_subject',
    'address': 'field_addresses',
    'thoroughfare': 'field_thoroughfares',
    'feature': 'field_geographic_features',
}

# RELS_EXT_isMemberOfSite_uri_ms -> field_domain_access
DOMAIN_MAPPING = {
    'info:fedora/': 'digital_library_pitt_edu',
    'info:fedora/pitt:site.admin': 'digital_library_pitt_edu',
    'info:fedora/pitt:site.american-music': 'americanmusic_library_pitt_edu',
    'info:fedora/pitt:site.documenting-pitt': 'documenting_pitt_edu',
    'info:fedora/pitt:site.historic-pittsburgh': 'historicpittsburgh_org',
    'info:fedora/pitt:site.uls-digital-collections': 'digital_library_pitt_edu',
    'info:fedora/pitt:uls-digital-collections': 'digital_library_pitt_edu',
}

# dc.rights -> rights_statement
RIGHTS_MAPPING = {
    'http://rightsstatements.org/vocab/UND/1.0/': 'Copyright Undetermined',
    'http://rightsstatements.org/vocab/CNE/1.0/': 'Copyright Undetermined',
    'http://rightsstatements.org/vocab/NoC-US/1.0/': 'No Copyright - United States',
    'http://rightsstatements.org/vocab/InC/1.0/': 'In Copyright'
}


""" IMPORTED MAPPINGS """

# Read in Fields
fields_csv = os.path.join(
    "Utility_Files", "i2_field_schema.csv"
)
FIELDS = create_df(fields_csv)

# Read in Metadata Template-to-I2 CSV field mapping
template_field_mapping_csv =  os.path.join(
    "Utility_Files", "template_to_i2_field_mapping.csv"
)
TEMPLATE_FIELD_MAPPING = create_df(template_field_mapping_csv)

# Read in Manifest-to-I2 CSV field mapping
manifest_field_mapping_csv =  os.path.join(
    "Utility_Files", "manifest_to_i2_field_mapping.csv"
)
MANIFEST_FIELD_MAPPING = create_df(manifest_field_mapping_csv)

# Read in taxonomies
taxonomies_csv = os.path.join(
    "Utility_Files", "taxonomies.csv"
)
TAXONOMIES = create_df(taxonomies_csv)

# Read in collection node mapping
collection_node_mapping_csv = os.path.join(
    "Utility_Files", "collection_node_mapping.csv"
)
COLLECTION_NODE_MAPPING = create_df(collection_node_mapping_csv)

# Read in language mapping as Dict
language_mapping_csv = os.path.join(
    "Utility_Files", "language_mapping.csv"
)
language_mapping_df = create_df(language_mapping_csv)
LANGUAGE_MAPPING = dict(
    zip(
        language_mapping_df['field_code'], 
        language_mapping_df['term_name']
    )
)

