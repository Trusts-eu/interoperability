import glob
import json
import os

from lxml import etree
from tqdm import tqdm


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
    # 'remoteId': 'string(ore:Proxy/dc:identifier)',
    'rights': 'string(ore:Aggregation/edm:rights)',
    'url': 'string(edm:WebResource/@rdf:about)',
}


def main(path_staging_area):
    dir_unzipped = os.path.join(path_staging_area, 'unzipped')
    pathname = dir_unzipped + '/**/*.xml'
    gen_transform = __transform(pathname)
    __store_files(gen_transform)


def __transform(f_glob):
    """
    Iterate over a folder containing Europeana .xml files and transform them
    into TRUSTS data_dicts.
    """
    for xml_file in tqdm(glob.glob(f_glob), desc="Transforming"):
        yield (xml_file, __transform_single_xml_file(xml_file))


def __transform_single_xml_file(xml_file):
    """
    Turn a Europeana xml file into a TRUSTS version. This function filters out
    xml properties that are not represented in TRUSTS.
    """
    data_dict = __extract_data_dict(xml_file)
    data_dict.update({'owner_org': 'Europeana'})
    data_dict.update({'resources': __extract_resources(xml_file)})
    if 'resources' not in data_dict:
        data_dict['resources'] = __extract_europeana_id(xml_file)
    else:
        data_dict['resources'].update({'europeana_id':
                                       __extract_europeana_id(xml_file)})
    return data_dict


def __extract_data_dict(fpath):
    """
    Convert a Europeana xml file to a TRUSTS data dictionary.
    """
    tree = etree.parse(fpath)
    return {key: __get_value_from_xpath(xpath, tree)
            for key, xpath in EUROPEANA_DATA_DICT_MAPPING.items()}


def __extract_europeana_id(fname):
    """
    Read the Europeana ID from the file name.

    >>> fname = 'path/to/file/europeana_id.xml'
    >>> extract_europeana_id(fname)
    'europeana_id'
    """
    fname = os.path.splitext(fname)[0]
    return os.path.split(fname)[1]


def __extract_resources(fname):
    """
    Extract the resources, i.e. links to images, etc., from the XML tree
    """
    tree = etree.parse(fname)
    return {key: __get_value_from_xpath(xpath, tree)
            for key, xpath in EUROPEANA_RESOURCES_MAPPING.items()}


def __get_value_from_xpath(xpath, tree):
    """
    Extract the value of the given xpath from the tree, using the nsmap of the
    tree's root element as the namespace.
    """
    return tree.xpath(xpath, namespaces=tree.getroot().nsmap)


def __create_json_dir(base_folder):
    """
    Creates the folder structure needed to hold zipped and unzipped data and
    the extracted .json files.
    """
    dir_jsons = os.path.join(base_folder, 'jsons')
    if not os.path.exists(dir_jsons):
        os.makedirs(dir_jsons)


def __store_files(gen_transform):
    """
    """
    for fpath, data_dict in gen_transform:
        fpath = fpath.replace('unzipped', 'jsons')
        fpath = fpath.replace('xml', 'json')
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        with open(os.path.join(fpath), 'w') as f:
            json.dump(data_dict, f)
