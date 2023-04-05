import argparse
import json
import os
import zipfile

from ftplib import FTP
from lxml import etree
from tqdm import tqdm

from europeana_config import (EUROPEANA_DATA_DICT_MAPPING,
                              EUROPEANA_RESOURCES_MAPPING, FTP_HOST_EUROPEANA)


def europeana_file_iterable(path_to_dataset, until):
    """
    Iterate over the zipped files in ``path_to_dataset`` until ``until`` is
    reached, unzip them, and extract the relevant properties as json files.
    """
    ftp, zips = __get_list_of_zips_on_ftp()
    for _zip in zips[:until]:
        # Setup
        zip_name = os.path.splitext(_zip)[0]
        dir_zipped, dir_unzipped, dir_jsons = __create_folders(path_to_dataset,
                                                               zip_name)

        # Run the process
        __download_zipfile(ftp, dir_zipped, _zip)
        __unzip(dir_unzipped, os.path.join(dir_zipped, _zip))
        data_dicts = __transform(dir_unzipped)
        __store_files(data_dicts, dir_jsons)


def __get_list_of_zips_on_ftp():
    ftp = FTP(FTP_HOST_EUROPEANA)
    ftp.login()
    ftp.cwd('dataset/XML')

    return ftp, __remove_checksum_files(ftp.nlst())


def __create_folders(base_folder, zip_name):
    """
    Creates the folder structure needed to hold zipped and unzipped data and
    the extracted .json files.
    """
    dir_zipped = os.path.join(base_folder, 'zipped')
    dir_unzipped = os.path.join(base_folder, 'unzipped', zip_name)
    dir_jsons = os.path.join(base_folder, 'jsons', zip_name)
    for _dir in [dir_zipped, dir_unzipped, dir_jsons]:
        if not os.path.exists(_dir):
            os.makedirs(_dir)
    return dir_zipped, dir_unzipped, dir_jsons


def __download_zipfile(ftp, to_dir, fname):
    """
    Downloads and stores the given zipfile from the ftp server.
    """
    with open(os.path.join(to_dir, fname), 'wb') as f:
        ftp.retrbinary(f'RETR {fname}', f.write)


def __remove_checksum_files(zips):
    """
    >>> zips = ['file_1.zip', 'file_2.zip.md5sum',
    ...         'file_2.zip', 'file_2.zip.md5sum']
    >>> remove_checksum_files(zips)
    ['file_1.zip', 'file_2.zip']
    """
    return [_zip for _zip in zips if not _zip.endswith('md5sum')]


def __unzip(to_dir, _zip):
    with zipfile.ZipFile(_zip, 'r') as zip_ref:
        zip_ref.extractall(to_dir)


def __transform(dir_unzipped):
    """
    Iterate over a folder containing Europeana .xml files and transform them
    into TRUSTS data_dicts.
    """
    return [__transform_single_xml_file(os.path.join(dir_unzipped, xml_file))
            for xml_file
            in tqdm(os.listdir(dir_unzipped), desc="Transforming")]


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


def __store_files(data_dicts, dir_jsons):
    """
    """
    for data_dict in data_dicts:
        fname = data_dict['resources']['europeana_id']
        with open(os.path.join(dir_jsons, f'{fname}.json'), 'w') as f:
            json.dump(data_dict, f)


if __name__ == '__main__':
    # import doctest
    # doctest.testmod()
    parser = argparse.ArgumentParser(
        description='Acquire datasets from Europeana.'
    )
    parser.add_argument(
        '-b', '--base_folder',
        help='The folder to store all output'
    )
    parser.add_argument(
        '-u', '--until', type=int,
        help='The number of zip files acquired from Europeana.'
    )
    args = parser.parse_args()
    europeana_file_iterable(args.base_folder, args.until)
