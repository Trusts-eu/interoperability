import os
import zipfile

from ftplib import FTP
from toolz.itertoolz import partition_all


FTP_HOST_EUROPEANA = 'download.europeana.eu'


def europeana_file_iterable(path_staging_area, batch_size, url_ftp_host):
    """
    Iterate over the zipped files in ``path_to_dataset`` until ``until`` is
    reached, unzip them, and extract the relevant properties as json files.
    """
    ftp = ftp_login(url_ftp_host)
    zips = [x for x in ftp.nlst() if not x.endswith('md5sum')]

    for batch in partition_all(batch_size, zips):
        for _zip in batch:
            zip_name = os.path.splitext(_zip)[0]
            dir_zipped, dir_unzipped = \
                __create_folders(path_staging_area, zip_name)
            __download_zipfile(ftp, dir_zipped, _zip)
            __unzip(dir_unzipped, os.path.join(dir_zipped, _zip))
        yield


def ftp_login(url_ftp_host):
    ftp = FTP(url_ftp_host)
    ftp.login()
    ftp.cwd('dataset/XML')

    return ftp


def __create_folders(base_folder, zip_name):
    """
    Creates the folder structure needed to hold zipped and unzipped data and
    the extracted .json files.
    """
    dir_zipped = os.path.join(base_folder, 'zipped')
    dir_unzipped = os.path.join(base_folder, 'unzipped', zip_name)
    for _dir in [dir_zipped, dir_unzipped]:
        if not os.path.exists(_dir):
            print(_dir)
            os.makedirs(_dir)
    return dir_zipped, dir_unzipped


def __download_zipfile(ftp, to_dir, fname):
    """
    Downloads and stores the given zipfile from the ftp server.
    """
    with open(os.path.join(to_dir, fname), 'wb') as f:
        ftp.retrbinary(f'RETR {fname}', f.write)


def __unzip(to_dir, _zip):
    with zipfile.ZipFile(_zip, 'r') as zip_ref:
        zip_ref.extractall(to_dir)
