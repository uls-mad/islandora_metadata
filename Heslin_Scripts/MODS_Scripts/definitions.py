fieldnames = [
    'url',
    'identifier/pitt', 
    'title/titleInfo', 
    'creator', 
    'subject_geographic', 
    'subject_topic', 
    'subject_local',
    'subject_name',
    'subject_temporal',
    'namePart/subject', 
    'abstract', 
    'dateCreated/originInfo',
    'normalized_date_qualifier',
    'dateOther/display/originInfo',
    'dateOther/sort/originInfo', 
    'dateCaptured/originInfo',
    'publication_status', 
    'copyright_status', 
    '{http://www.cdlib.org/inside/diglib/copyrightMD}name/accessCondition',
    'typeOfResource', 
    'languageTerm/code/language', 
    'title/relatedItem',
    'identifier/relatedItem', 
    'depositor', 
    'contributor', 
    'genre', 
    'genre/aat',
    'form/physicalDescription', 
    'extent/physicalDescription', 
    'publisher/originInfo', 
    'note/prefercite/relatedItem', 
    'placeTerm/text/originInfo', 
    'note/series/relatedItem', 
    'note/subseries/relatedItem', 
    'note/container/relatedItem', 
    'dateCreated/relatedItem', 
    'identifier/local-asc/relatedItem', 
    'note/ownership/relatedItem', 
    'identifier/source',
    'note/address',
    'note/donor'
]


columns = {
    'title/titleInfo': 'title', 
    'typeOfResource': 'type_of_resource',
    'publisher/originInfo': 'publisher',
    'dateOther/display/originInfo': '[DELETE] display_date', 
    'dateOther/sort/originInfo': '[DELETE] sort_date',
    'languageTerm/code/language': 'language', 
    'form/physicalDescription': 'format', 
    'extent/physicalDescription': 'extent',
    'genre/aat': 'genre_aat', 
    'identifier/pitt' : 'identifier',
    'title/relatedItem': 'source_collection', 
    'dateCreated/originInfo': 'normalized_date', 
    'dateCaptured/originInfo': 'date_digitized',
    'note/prefercite/relatedItem': 'source_citation', 
    'identifier/relatedItem': 'source_collection_id', 
    'note/container/relatedItem': 'source_container',
    'note/series/relatedItem': 'source_series', 
    'note/subseries/relatedItem': 'source_subseries', 
    'note/otherlevel/relatedItem': 'source_other_level',
    'placeTerm/text/originInfo': 'pub_place',
    'abstract': 'description', 
    'namePart/subject': 'subject_name', 
    '{http://www.cdlib.org/inside/diglib/copyrightMD}name/accessCondition' : 'rights_holder', 
    'identifier/source' : 'source_id', 
    'note/address' : 'address', 
    'note/donor': 'gift_of',
    'dateCreated/relatedItem' : 'source_collection_date', 
    'identifier/local-asc/relatedItem' : 'source_collection_id', 
    'note/ownership/relatedItem' : 'source_ownership'}


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
