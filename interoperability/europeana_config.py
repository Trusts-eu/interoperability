EUROPEANA_DATA_DICT_MAPPING = {
    'name': 'string(edm:EuropeanaAggregation/edm:datasetName)',
    'title': 'string(ore:Proxy/dc:title)',
    'notes': 'string(ore:Proxy/dc:description)',
}

EUROPEANA_RESOURCES_MAPPING = {
    'created': 'string(dqv:QualityAnnotation/dcterms:created)',
    'dataProvider': 'string(ore:Aggregation/edm:rights)',
    'name': 'string(edm:EuropeanaAggregation/edm:datasetName)',
    'remoteId': 'string(ore:Proxy/dc:identifier)',
    'rights': 'string(ore:Aggregation/edm:rights)',
    'url': 'string(edm:WebResource/@rdf:about)',
}
FTP_HOST_EUROPEANA = 'download.europeana.eu'
