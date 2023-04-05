import json
import logging
import rdflib
import requests
import sys
import traceback

from copy import deepcopy
from datetime import date, datetime, time, timedelta
from dotenv import dotenv_values
from os.path import join as pathjoin
from requests.auth import HTTPBasicAuth
from typing import Tuple, Dict

sys.path.append('path/to/file')

from trusts_platform_client import trustsckan
from trusts_platform_client.trustsckan import helper_create_contract_data, helper_load_europeana_dataset

log = logging.getLogger("test")

class ConnectorException(Exception):
    def __init__(self, m):
        self.message = m

    def __str__(self):
        return "CONNECTOR_EXCEPTION " + self.message

def URI(somestr: str):
    if isinstance(somestr, rdflib.URIRef):
        return somestr
    if somestr.startswith("<"):
        somestr = somestr[1:]
    if somestr.endswith(">"):
        somestr = somestr[:-1]
    return rdflib.URIRef(somestr)


empty_result = {
    "author": None,
    "author_email": None,
    "creator_user_id": "__MISSING__", #  (/)
    "id": "__MISSING__",   # (/)
    "isopen": None,    # (/)
    "license_id": "__MISSING__",  # (/)
    "license_title": "__MISSING__",  # (/)
    "license_url": "__MISSING__",   # (/)
    "maintainer": None, # (/)
    "maintainer_email": None,  # (/)
    "metadata_created": "__MISSING__",   # (/)
    "metadata_modified": "__MISSING__",   # (/)
    "name": "__MISSING__", # (/)
    "notes": None,  # (/)
    "num_resources": 0,  # (/)
    "num_tags": 0, # (/)
    "owner_org": "__MISSING__",   # (/)
    "private": None, # (/)
    "state": "active", # (/)
    "theme": "__MISSING__",   # (/)
    "title": "__MISSING__",   # (/)
    "type": "__MISSING__",   # (/)
    "url": None,  # (/)
    "version": "__MISSING__",  # (/)
    "tags": [],  # (/)
    "groups": [],  # (/)
    "dataset_count": 0,   # (/)
    "service_count": 0,  # (/)
    "application_count": 0,  # (/)
    "relationships_as_object": [], # (/)
    "relationships_as_subject": [], # (/)
    "resources": [ ],
    "organization": {}  # (/)
}

def query_broker(query_string: str, connector_url, broker_url, auth):

    params = {"recipient": broker_url}
    url = pathjoin(connector_url, "api/ids/query")
    data = query_string.encode("utf-8")

    response = requests.post(url=url,
                              params=params,
                              data=data,
                              auth=HTTPBasicAuth(auth[0],
                                                 auth[1]))
    if response.status_code > 299 or response.text is None:
        log.error("Got code " + str(response.status_code) + " in search")
        log.error("Provided Data: " + data.decode("utf-8"))
        raise ConnectorException("Code: " + str(response.status_code) +
                                  " Text: " + str(response.text))

    return response.text

def sparl_get_all_resources(resource_type: str,
                             type_pred="https://www.trusts-data.eu/ontology/asset_type"):


    query = """
      PREFIX owl: <http://www.w3.org/2002/07/owl#>
      PREFIX ids: <https://w3id.org/idsa/core/>
      SELECT ?resultUri ?type ?externalname
      WHERE
      { ?resultUri a ?type .
        ?conn <https://w3id.org/idsa/core/offeredResource> ?resultUri .
        ?resultUri owl:sameAs ?externalname .
        """
    if resource_type is None or resource_type == "None":
        query += "\n ?resultUri " + URI(type_pred).n3() + " ?assettype."
    else:
        typeuri = URI("https://www.trusts-data.eu/ontology/" + \
                      resource_type.capitalize())
        query += "\n ?resultUri " + URI(
            type_pred).n3() + " " + typeuri.n3() + "."
        query += "\nBIND( " + typeuri.n3() + " AS ?assettype) ."
    query += "\n}"
    return query

def parse_broker_tabular_response(raw_text, sep="\t"):
    readrows = 0
    result = []
    for irow, row in enumerate(raw_text.split("\n")):
        rows = row.strip()
        if len(rows) < 1:
            continue
        vals = rows.split(sep)
        if irow == 0:
            colnames = [x.replace("?", "") for x in vals]
            continue
        d = {cname: vals[ci].strip()
             for ci, cname in enumerate(colnames)}
        result.append(d)

    return result


def ask_broker_for_description(element_uri: str,
                               broker_url : str,
                               connector_url : str,
                               auth : Tuple[str,str]):
    resource_contract_tuples = []

    if len(element_uri) < 5 or ":" not in element_uri:
        return {}
    params = {"recipient": broker_url,
              "elementId": element_uri}
    url = pathjoin(connector_url, "api/ids/description")
    response = requests.post(url=url,
                              params=params,
                              auth=HTTPBasicAuth(auth[0], auth[1]))
    if response.status_code > 299 or response.text is None:
        log.error("Got code " + str(response.status_code) + " in describe")
        raise ConnectorException("Code: " + str(response.status_code) +
                                  " Text: " + str(response.text))

    graphs = response.json()
    return graphs

def graphs_to_artifacts(raw_jsonld: Dict):
    g = raw_jsonld["@graph"]
    artifact_graphs = [x for x in g if x["@type"] == "ids:Artifact"]
    return [x["sameAs"] for x in artifact_graphs]

def graphs_to_ckan_result_format(raw_jsonld: Dict):
    g = raw_jsonld["@graph"]
    resource_graphs = [x for x in g if x["@type"] == "ids:Resource"]
    if "theme" not in resource_graphs[0].keys():
        return None
    representation_graphs = [x for x in g if
                             x["@type"] == "ids:Representation"]
    artifact_graphs = [x for x in g if x["@type"] == "ids:Artifact"]

    resource_uri = resource_graphs[0]["sameAs"]

    """
    print(10*"\n"+"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~>>")
    print("\nresource_graphs = \n",json.dumps(resource_graphs,
                                            indent=1).replace("\n","\n\t"))
    print("\nartifact_graphs = \n", json.dumps(artifact_graphs,
                                             indent=1).replace("\n", "\n\t"))
    print("\nrepresentation_graphs = \n", json.dumps(representation_graphs,
                                             indent=1).replace("\n", "\n\t"))
    print("\n<<~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"+10 * "\n")
    """

    # ToDo get this from the central core as well
    theirname = resource_uri
    organization_name = theirname.split("/")[2].split(":")[0]
    providing_base_url = "/".join(organization_name.split("/")[:3])
    organization_data = {
        "id": "52bc9332-2ba1-4c4f-bf85-5a141cd68423",
        "name": organization_name,
        "title": "Orga1",
        "type": "organization",
        "description": "",
        "image_url": "",
        "created": "2022-02-02T16:32:58.653424",
        "is_organization": True,
        "approval_status": "approved",
        "state": "active"
    }
    resources = []

    packagemeta = deepcopy(empty_result)
    packagemeta["id"] = resource_uri
    packagemeta["license_id"] = resource_graphs[0][
        "standardLicense"] if "standardLicense" in resource_graphs[0] else None
    packagemeta["license_url"] = resource_graphs[0][
        "standardLicense"] if "standardLicense" in resource_graphs[0] else None
    packagemeta["license_title"] = resource_graphs[0][
        "standardLicense"] if "standardLicense" in resource_graphs[0] else None
    packagemeta["metadata_created"] = resource_graphs[0]["created"]
    packagemeta["metadata_modified"] = resource_graphs[0]["modified"]
    packagemeta["name"] = clean_multilang(resource_graphs[0]["title"])
    packagemeta["title"] = clean_multilang(resource_graphs[0]["title"])
    packagemeta["type"] = resource_graphs[0]["asset_type"].split("/")[
        -1].lower()
    packagemeta["theme"] = resource_graphs[0]["theme"].split("/")[-1]
    packagemeta["version"] = resource_graphs[0]["version"]

    # These are the values we will use in succesive steps
    packagemeta["external_provider_name"] = organization_name
    #packagemeta["to_process_external"] = config.get(
    #    "ckan.site_url") + "/ids/processExternal?uri=" + \
    #                                     urllib.parse.quote_plus(
    #                                         resource_uri)
    packagemeta["provider_base_url"] = providing_base_url

    packagemeta["creator_user_id"] = "X"
    packagemeta["isopen"]: None
    packagemeta["maintainer"] = None
    packagemeta["maintainer_email"] = None
    packagemeta["notes"] = None
    packagemeta["num_tags"] = 0
    packagemeta["private"] = False
    packagemeta["state"] = "active"
    packagemeta["relationships_as_object"] = []
    packagemeta["relationships_as_subject"] = []
    packagemeta["url"] = providing_base_url
    packagemeta["tags"] = []  # (/)
    packagemeta["groups"] = []  # (/)

    packagemeta["dataset_count"] = 0
    packagemeta["service_count"] = 0
    packagemeta["application_count"] = 0
    packagemeta[packagemeta["type"] + "count"] = 1

    for rg in representation_graphs:
        artifact_this_res = [x for x in artifact_graphs
                             if x["@id"] == rg["instance"]][0]
        # logging.error(json.dumps(artifact_this_res,indent=1)+
        #              "\n~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
        empty_ckan_resource = {
            "artifact": artifact_this_res["@id"],
            "cache_last_updated": None,
            "cache_url": None,
            "created": resource_graphs[0]["created"],
            "description": clean_multilang(resource_graphs[0]["description"]),
            "format": "EXTERNAL",
            "hash": artifact_this_res["checkSum"],
            "id": rg["@id"],
            "last_modified": resource_graphs[0]["modified"],
            "metadata_modified": rg["modified"],
            "mimetype": rg["mediaType"],
            "mimetype_inner": None,
            "name": artifact_this_res["fileName"],
            "package_id": resource_graphs[0]["sameAs"],
            "position": 0,
            "representation": rg["sameAs"],
            "resource_type": "resource",
            "size": artifact_this_res["ids:byteSize"],
            "state": "active",
            "url": rg["sameAs"],
            "url_type": "upload"
        }
        resources.append(empty_ckan_resource)

    packagemeta["organization"] = organization_data
    packagemeta["owner_org"] = organization_data["id"]
    packagemeta["resources"] = resources
    packagemeta["num_resources"] = len(artifact_graphs)

    return packagemeta

def clean_multilang(astring: str):
    if isinstance(astring, str):
        return astring
    if isinstance(astring, dict) and "@value" in astring.keys():
        return str(astring["@value"])
    return str(astring)


def main(connector_url, broker_url, admin, password, ckan_token, trusts_url):
    # Retrieving data from clone (i.e. its broker)
    _trustsckan = trustsckan.TRUSTSCKAN(trusts_url, apikey=ckan_token)
    contract_data = helper_create_contract_data()

    auth = (admin, password)
    response = query_broker(query_string=sparl_get_all_resources(None),
                            connector_url=connector_url,
                            broker_url=broker_url,
                            auth=auth)

    # Parsing the data from clone
    broker_response_json = parse_broker_tabular_response(response)
    resource_uris = set([URI(x["resultUri"])
                                for x in broker_response_json
                                if URI(x["type"]) == broker_response_json])
    already_prcessed_externalnames = set()

    # Loading the data into TRUSTS main
    for asset in broker_response_json:
        try:
            print(asset)
            externalname = asset["externalname"][1:-1]
            if externalname in already_prcessed_externalnames:
                continue
            already_prcessed_externalnames.add(externalname)
            description = ask_broker_for_description(element_uri=externalname,
                                    broker_url=broker_url,
                                    connector_url=connector_url,
                                    auth=auth,
                                    )
            artifacts = graphs_to_artifacts(description)
            ckan_result = graphs_to_ckan_result_format(description)
            dataset_name = ckan_result["name"].lower()
            dataset_name = dataset_name.replace(' ', '_')

            # Mapping the data into the TRUSTS format
            json_for_client = {"name": dataset_name + "_v1",
            "title": ckan_result['title'] + "test_clone_v1",
            "theme": "https://trusts.poolparty.biz/Themes/18",
            "notes": str(ckan_result["notes"]),
            "owner_org": "Clone_node".lower(),
            "keywords": ckan_result["tags"] + ckan_result.get("keywords",[]),
            "resources": {"rights": ckan_result["license_url"],
            "url": ckan_result["resources"][0]["url"] + "__v1",
            "name":ckan_result["resources"][0]["name"]+ "test_clone_resource",
            "dataProvider": "Interoperability Provider with the Clone",
            "created": ckan_result["resources"][0]["created"],
            "remoteId": ckan_result["resources"][0]['id']}}
            print(json.dumps(json_for_client,indent=1))

            now = datetime.now()
            then = datetime.now() + timedelta(weeks=52)

            contract_data['contract_start_date'] = str(now.date())
            contract_data['contract_start_time'] = str(now.time())
            contract_data['contract_end_date'] = str(then.date())
            contract_data['contract_end_time'] = str(then.time())
            _trustsckan.post_dataset(json_for_client, contract_data)
        except:
            traceback.print_exc()


if __name__ == '__main__':
    config = dotenv_values(".env")
    main(config['CONNECTOR_URL'],
         config['BROKER_URL'],
         config['ADMIN'],
         config['PASSWORD'],
         config['CKAN_TOKEN'],
         config['TRUSTS_URL'],
         )
