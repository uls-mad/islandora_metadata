#!/bin/python3 

""" Modules """

# Import standard module
import os

# Import local module
from file_utils import create_df


""" FIELD LISTS """

REQUIRED_FIELDS = [
    'id',
    'field_model',
    'field_resource_type',
    'field_member_of'
]

UNMAPPED_FIELDS = [
    'ancestors_ms',
    'mods_name_namePart_ms',
    'mods_name_personal_editor_ms',
]

TITLE_FIELDS = [
    'field_full_title', 
    'field_uniform_title', 
    'field_alternative_title_pitt'
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


""" MAPPINGS """

# RELS_EXT_hasModel_uri_ms -> islandora_models
OBJECT_MAPPING = {
    'info:fedora/islandora:collectionCModel': {
        'model': 'Collection', 
        'resource_type': 'Collection'
    },
    'info:fedora/islandora:compoundCModel': {
        'model': 'Compound Object', 
        'resource_type': 'Collection'
    },
    'info:fedora/islandora:oralhistoriesCModel': {
        'model': 'Compound Object', 
        'resource_type': 'Collection'
    },
    'info:fedora/islandora:bookCModel': {
        'model': 'Paged Content', 
        'resource_type': 'Collection'
    },
    'info:fedora/islandora:manuscriptCModel': {
        'model': 'Paged Content', 
        'resource_type': 'Collection'
    },
    'info:fedora/islandora:newspaperCModel': {
        'model': 'Newspaper', 
        'resource_type': 'Collection'
    },
    'info:fedora/islandora:newspaperIssueCModel': {
        'model': 'Publication Issue', 
        'resource_type': 'Collection'
    },
    'info:fedora/islandora:pageCModel': {
        'model': 'Page', 
        'resource_type': 'Text'
    },
    'info:fedora/islandora:manuscriptPageCModel': {
        'model': 'Page', 
        'resource_type': 'Text'
    },
    'info:fedora/islandora:newspaperPageCModel': {
        'model': 'Page', 
        'resource_type': 'Text'
    },
    'info:fedora/islandora:sp_pdf': {
        'model': 'Digital Document', 
        'resource_type': 'Text'
    },
    'info:fedora/islandora:sp_large_image_cmodel': {
        'model': 'Image', 
        'resource_type': 'Still Image'
    },
    'info:fedora/islandora:sp_videoCModel': {
        'model': 'Video', 
        'resource_type': 'Moving Image'
    },
    'info:fedora/islandora:sp-audioCModel': {
        'model': 'Audio', 
        'resource_type': 'Sound'
    },
}

DISPLAY_HINTS_MAPPING = {
    'Paged Content': 'Mirador',
    'Page': 'Mirador',
    'Image': 'Mirador',
    'Publication Issue': 'Mirador',
    'Newspaper': 'Mirador',
    'Digital Document': 'PDFjs',
}

# mods_typeOfResource_ms -> resource_types
TYPE_MAPPING = {
    'cartographic': 'Cartographic',
    'mixed material': 'Mixed material',
    'moving image': 'Moving image',
    'notated music': 'Notated music',
    'software, multimedia': 'Multimedia|Software',
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

# RELS_EXT_isMemberOfSite_uri_ms -> domain
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


DATASTREAMS_MAPPING = {
    'hocr': ['HOCR'],
    'file': ['JP2'],
    'transcript': ['TRANSCRIPT']
}


""" IMPORTED MAPPINGS """

# Read in Fields
fields_csv = os.path.join(
    "Schema_Mappings", "i2_field_schema.csv"
)
FIELDS = create_df(fields_csv)

# Read in Solr-to-I2 field mapping
field_mapping_csv =  os.path.join(
    "Schema_Mappings", "solr_to_i2_field_mapping.csv"
)
FIELD_MAPPING = create_df(field_mapping_csv)

# Read in subjects mapping
name_mapping_csv = os.path.join(
    "Remediation_Mappings", "name_mapping.csv"
)
NAME_MAPPING = create_df(name_mapping_csv)

# Read in dates mapping
edtf_dates_csv = os.path.join(
    "Remediation_Mappings", "edtf_dates.csv"
)
EDTF_DATES = create_df(edtf_dates_csv)

language_mapping_csv = os.path.join(
    "Remediation_Mappings", "language_mapping.csv"
)
LANGUAGE_MAPPING = create_df(language_mapping_csv)

country_mapping_csv = os.path.join(
    "Remediation_Mappings", "country_mapping.csv"
)
COUNTRY_MAPPING = create_df(country_mapping_csv)

# Read in subjects mapping
subject_mapping_csv = os.path.join(
    "Remediation_Mappings", "subject_mapping.csv"
)
SUBJECT_MAPPING = create_df(subject_mapping_csv)

# Read in genre mappings
genre_mapping_csv = os.path.join(
    "Remediation_Mappings", "genre_mapping.csv"
)
GENRE_MAPPING = create_df(genre_mapping_csv)

genre_jp_mapping_csv = os.path.join(
    "Remediation_Mappings", "genre_japanese_prints_mapping.csv"
)
GENRE_JP_MAPPING = create_df(genre_jp_mapping_csv)

# Read in physical form mapping
physical_form_mapping_csv = os.path.join(
    "Remediation_Mappings", "physical_form_mapping.csv"
)
FORM_MAPPING = create_df(physical_form_mapping_csv)

# Read in geographic fields mapping
geo_fields_mapping_csv = os.path.join(
    "Remediation_Mappings", "geo_fields_mapping.csv"
)
GEO_FIELDS_MAPPING = create_df(geo_fields_mapping_csv)

# Read in source collection mapping
source_collection_mapping_csv = os.path.join(
    "Remediation_Mappings", "source_collection_mapping.csv"
)
SOURCE_COLLECTION_MAPPING = create_df(source_collection_mapping_csv)

# Read in missing source collection mapping
source_collection_missing_csv = os.path.join(
    "Remediation_Mappings", "source_collection_missing.csv"
)
SOURCE_COLLECTION_MISSING = create_df(source_collection_missing_csv)

# Read in collection node mapping
collection_node_mapping_csv = os.path.join(
    "Remediation_Mappings", "collection_node_mapping.csv"
)
COLLECTION_NODE_MAPPING = create_df(collection_node_mapping_csv)
