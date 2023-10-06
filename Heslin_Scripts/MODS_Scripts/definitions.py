fieldnames = [
    'url',
    'identifier/pitt', 
    'title/titleInfo', 
    'creator', 
    'subject_geographic', 
    'subject_topic', 
    'subject_name',
    'namePart/subject', 
    'abstract', 
    'dateCreated/originInfo',
    'normalized_date_qualifier',
    'dateOther/display/originInfo',
    'dateOther/sort/originInfo', 
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
    'identifier/source'
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
    'identifier/pitt' : 'identifier',
    'title/relatedItem': 'source_collection', 
    'dateCreated/originInfo': 'normalized_date', 
    'note/prefercite/relatedItem': 'source_citation', 
    'identifier/relatedItem': 'source_collection_id', 
    'note/container/relatedItem': 'source_container',
    'note/series/relatedItem': 'source_series', 
    'note/subseries/relatedItem': 'source_subseries', 
    'placeTerm/text/originInfo': 'pub_place',
    'abstract': 'description', 
    'namePart/subject': 'subject_name', 
    '{http://www.cdlib.org/inside/diglib/copyrightMD}name/accessCondition' : 'rights_holder', 
    'identifier/source' : 'source_id', 
    'note/address' : 'address', 
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
