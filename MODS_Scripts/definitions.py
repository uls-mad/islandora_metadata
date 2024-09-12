"""" List of prioritized fields in output CSV """

fieldnames = [
    'url',
    'identifier', 
    'title', 
    'creator', 
    'contributor', 
    'interviewer',
    'interviewee',
    'other_names',
    'subject_geographic', 
    'subject_topic', 
    'subject_local',
    'subject_name',
    'subject_temporal',
    'description', 
    'normalized_date',
    'normalized_date_qualifier',
    'DELETE_display_date',
    'DELETE_sort_date', 
    'date_digitized',
    'type_of_resource', 
    'language',
    'genre', 
    'genre_aat',
    'format', 
    'extent', 
    'publisher', 
    'pub_place', 
    'publication_status', 
    'copyright_status', 
    'rights_holder',
    'address',
    'gift_of',
    'depositor',
    'source_collection',
    'source_collection_id',
    'source_collection_date',
    'source_citation', 
    'source_container',
    'source_series', 
    'source_subseries', 
    'source_other_level',
    'source_ownership',
    'source_id',
    ]


""" Dictionary used to rename target  columns in output CSV """

columns = {
    'identifier@pitt': 'identifier',
    'titleInfo/title': 'title', 
    'typeOfResource': 'type_of_resource',
    'originInfo/publisher': 'publisher',
    'originInfo/placeTerm@text': 'pub_place',
    'originInfo/dateOther@display': 'DELETE_display_date', 
    'originInfo/dateOther@sort': 'DELETE_sort_date',
    'language/languageTerm@code': 'language', 
    'originInfo/dateCreated': 'normalized_date', 
    'originInfo/dateCaptured': 'date_digitized',
    'abstract': 'description', 
    'physicalDescription/form': 'format', 
    'physicalDescription/extent': 'extent',
    'genre@aat': 'genre_aat', 
    'relatedItem@host/titleInfo/title': 'CHECK_source_collection',
    #'relatedItem/titleInfo/title': 'source_collection', # Remove once CSV-to-MODS utility updated
    'relatedItem@host/dateCreated': 'source_collection_date', 
    'relatedItem@host/identifier': 'source_collection_id', 
    'relatedItem@host/identifier@local-asc': 'source_collection_id', 
    'relatedItem@host/note@ownership': 'source_ownership',
    'relatedItem@host/note@prefercite': 'source_citation', 
    'relatedItem/note@prefercite': 'source_citation', 
    'relatedItem@host/note@container': 'source_container',
    'relatedItem@host/note@series': 'source_series', 
    'relatedItem@host/note@subseries': 'source_subseries', 
    'relatedItem@host/note@otherlevel': 'source_other_level',
    'identifier@source': 'source_id', 
    'name/rights.holder/copyright/accessCondition': 'rights_holder', 
    'note@address': 'address', 
    'note@donor': 'gift_of',
    }


""" Namespaces Info """

namespaces = {
    'mods_ns': {
        'mods': 'http://www.loc.gov/mods/v3'
        },
    'copyright_ns': {
        'copyrightMD': 'http://www.cdlib.org/inside/diglib/copyrightMD'
        }
    }


mods_ns = '{http://www.loc.gov/mods/v3}'
copyrightMD_ns = '{http://www.cdlib.org/inside/diglib/copyrightMD}'
