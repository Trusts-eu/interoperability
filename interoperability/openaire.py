import doctest
import glob
import gzip
import json
import os

from toolz.dicttoolz import get_in
import toolz


OPENAIRE_TO_TRUSTS_MAPPING = {
    'description': 'notes',
    'maintitle': 'name',
    'publisher': 'owner_org',
    'maintitle': 'title',
}


def main():
    read_path = ('path/to/file'
                 'dataset')
    store_path = ('path/to/file'
                  'dataset_trusts_metadata')

    if not os.path.exists(store_path):
        os.makedirs(store_path)
    for i, json_dict in enumerate(openaire_file_iterable(read_path)):
        fname = json_dict['resources']['remoteId']
        with open(os.path.join(store_path, f"{fname}.json"), 'w',
                  encoding='utf8') as f:
            json.dump(json_dict, f, ensure_ascii=False)
        if i == 199:
            break


def openaire_file_iterable(path_to_dataset='.'):
    """
    Iterate over the OpenAIRE files (in gzipped format) in the folder
    ``path_to_dataset``, gunzip them, read them line by line and turn each json
    line into a ``dict``.
    """

    for _gzip in glob.glob(f"{path_to_dataset}/*.gz"):
        with gzip.open(_gzip) as f:
            for line in f:
                yield toolz.functoolz.pipe(
                    line, json.loads, __map_openaire_to_trusts,
                    # __extract_resources,
                )


def __map_openaire_to_trusts(content_dict):
    """
    """
    return {
        'name': content_dict['maintitle'],
        'title': content_dict['maintitle'],
        'notes': content_dict['description'],
        'owner_org': 'OpenAIRE',
        'resources': {
            'created': content_dict.get('publicationdate', 'None availabe'),
            'dataProvider': content_dict.get('publisher', 'None available'),
            'remoteId': content_dict['id'].split('::')[1],
            'rights': get_in(['instance', 0, 'license'], content_dict),
            'url': content_dict['instance'][0].get('url', 'None available'),
            'name': content_dict['maintitle'],
        },
    }


def __map_openaire_to_trusts_depr(content_dict):
    """
    Replace all keys in ``content_dict`` with the TRUSTS equivalents.
    >>> content_dict = {'maintitle': 'The title',
                        'publisher': 'Publisher name'}
    >>> __map_openaire_to_trusts(content_dict)
        {'name': 'The title', 'owner_org': 'Publisher name'}
    """
    return toolz.dicttoolz.keymap(__map_openaire_key, content_dict)


def __map_openaire_key(key):
    """
    Swap ``key`` with its equivalent, as specified in
    ``OPENAIRE_TRUSTS_MAPPING``.
    >>> __map_openaire_key("maintitle")
    'name'
    >>> __map_openaire_key("unmappable")
    'unmappable'
    """
    return OPENAIRE_TO_TRUSTS_MAPPING.get(key, key)


def __extract_resources(content_dict):
    """
    Extract values from the .json dictionaries relevant for TRUSTS and store
    them in a separate dictionary attached to ``content_dict``.
    """
    content_dict['resources'] = {
        # 'rights': content_dict['instance'][0].get('license', 'None available'),
        'created': content_dict.get('publicationdate', 'None availabe'),
        'dataProvider': content_dict.get('owner_org', 'None available'),
        'remoteId': content_dict['id'],
        'rights': get_in(['instance', 0, 'license'], content_dict),
        'url': content_dict['instance'][0].get('url', 'None available'),
        'name': content_dict['bestaccessright']['code'],
    }
    return content_dict


def __get_bestaccessright_code(content_dict):
    """
    """
    return get_in(['bestaccessright', 'code'], content_dict, default='None available.')


if __name__ == '__main__':
    main()
    # doctest.testmod()
