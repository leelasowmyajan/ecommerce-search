import requests
import os
import re
import pandas as pd
from solr import SolrEngine
from IPython.display import display,HTML
from os import path
from tqdm import tqdm
from contextlib import contextmanager


# Running Solr in Docker. Setting the host and zookeeper host to the docker one.
SEARCH_SOLR_HOST = "search-solr"
SEARCH_NOTEBOOK_HOST="search-notebook"
SEARCH_ZK_HOST="search-zk"
SEARCH_SOLR_PORT = os.getenv('SEARCH_SOLR_PORT') or '8983'
SEARCH_NOTEBOOK_PORT= os.getenv('SEARCH_NOTEBOOK_PORT') or '8888' # Jupyter notebook
SEARCH_ZK_PORT= os.getenv('SEARCH_ZK_PORT') or '2181'
SEARCH_WEBSERVER_HOST = os.getenv('SEARCH_WEBSERVER_HOST') or 'localhost'
SEARCH_WEBSERVER_PORT = os.getenv('SEARCH_WEBSERVER_PORT') or '2345'
ENGINE = SolrEngine()

SOLR_URL = f'http://{SEARCH_SOLR_HOST}:{SEARCH_SOLR_PORT}/solr'
SOLR_COLLECTIONS_URL = f'{SOLR_URL}/admin/collections'
WEBSERVER_URL = f'http://{SEARCH_WEBSERVER_HOST}:{SEARCH_WEBSERVER_PORT}'

def healthcheck():
  status_url = f'{SOLR_URL}/admin/zookeeper/status'

  try:
    if (get_engine().health_check()):
      print ("Solr is up and responding.")
      print ("Zookeeper is up and responding.\n")
      print ("All Systems are ready. Happy Searching!")
  except:
      print ("Error! One or more containers are not responding.\nPlease follow the instructions in Appendix A.")

# Initialising the Solr Enginer
def get_engine():
  return ENGINE

def print_status(solr_response):
  if solr_response["responseHeader"]["status"] == 0:
      print("Status: Success")
  else:
      print("Status: Failure; Response:[ " + str(solr_response) + " ]")

def create_collection(collection_name):
  # Clear any collections
  wipe_collection_params = [
      ('action', "delete"),
      ('name', collection_name)
  ]

  print(f"Wiping '{collection_name}' collection")
  response = requests.post(SOLR_COLLECTIONS_URL, data=wipe_collection_params).json()

  #Create collection
  create_collection_params = [
      ('action', "CREATE"),
      ('name', collection_name),
      ('numShards', 1),
      ('replicationFactor', 1) ]

  print(create_collection_params)

  print(f"Creating '{collection_name}' collection")
  response = requests.post(SOLR_COLLECTIONS_URL, data=create_collection_params).json()
  print_status(response)

def enable_ltr(collection_name):

    collection_config_url = f'{SOLR_URL}/{collection_name}/config'

    del_ltr_query_parser = { "delete-queryparser": "ltr" }
    add_ltr_q_parser = {
     "add-queryparser": {
        "name": "ltr",
            "class": "org.apache.solr.ltr.search.LTRQParserPlugin"
        }
    }

    print(f"Del/Adding LTR QParser for {collection_name} collection")
    response = requests.post(collection_config_url, json=del_ltr_query_parser)
    print(response)
    print_status(response.json())
    response = requests.post(collection_config_url, json=add_ltr_q_parser).json()
    print_status(response)

    del_ltr_transformer = { "delete-transformer": "features" }
    add_transformer =  {
      "add-transformer": {
        "name": "features",
        "class": "org.apache.solr.ltr.response.transform.LTRFeatureLoggerTransformerFactory",
        "fvCacheName": "QUERY_DOC_FV"
    }}

    print(f"Adding LTR Doc Transformer for {collection_name} collection")
    response = requests.post(collection_config_url, json=del_ltr_transformer).json()
    print_status(response)
    response = requests.post(collection_config_url, json=add_transformer).json()
    print_status(response)
    


def upsert_text_field(collection_name, field_name):
    #clear out old field to ensure this function is idempotent
    delete_field = {"delete-field":{ "name":field_name }}
    response = requests.post(f"{SOLR_URL}/{collection_name}/schema", json=delete_field).json()

    print("Adding '" + field_name + "' field to collection")
    add_field = {"add-field":{ "name":field_name, "type":"text_general", "stored":"true", "indexed":"true", "multiValued":"false" }}
    response = requests.post(f"{SOLR_URL}/{collection_name}/schema", json=add_field).json()
    print_status(response)

    
def upsert_integer_field(collection_name, field_name):
    #clear out old field to ensure this function is idempotent
    delete_field = {"delete-field":{ "name":field_name }}
    response = requests.post(f"{SOLR_URL}/{collection_name}/schema", json=delete_field).json()


    
def num2str(number):
  return str(round(number,4)) #round to 4 decimal places for readibility

def vec2str(vector):
  return "[" + ", ".join(map(num2str,vector)) + "]"

def tokenize(text):
  return text.replace(".","").replace(",","").lower().split()

def display_search(query, documents):
  display(HTML(f"<strong>Query</strong>: <i>{query}</i><br/><br/><strong>Results:</strong>"))
  display(HTML(documents))
  
def render_search_results(query, results):
    file_path = os.path.dirname(os.path.abspath(__file__))
    search_results_template_file = os.path.join(file_path + "/data/templates/", "search-results.html")
    with open(search_results_template_file) as file:
        file_content = file.read()

        template_syntax = "<!-- BEGIN_TEMPLATE[^>]*-->(.*)<!-- END_TEMPLATE[^>]*-->"
        header_template = re.sub(template_syntax, "", file_content, flags=re.S)

        results_template_syntax = "<!-- BEGIN_TEMPLATE: SEARCH_RESULTS -->(.*)<!-- END_TEMPLATE: SEARCH_RESULTS[^>]*-->"
        x = re.search(results_template_syntax, file_content, flags=re.S)
        results_template = x.group(1)

        separator_template_syntax = "<!-- BEGIN_TEMPLATE: SEPARATOR -->(.*)<!-- END_TEMPLATE: SEPARATOR[^>]*-->"
        x = re.search(separator_template_syntax, file_content, flags=re.S)
        separator_template = x.group(1)

        rendered = header_template.replace("${QUERY}", query)
        for result in results:
            rendered += results_template.replace("${NAME}", result['name'] if 'name' in result else "UNKNOWN") \
                .replace("${MANUFACTURER}", result['manufacturer'] if 'manufacturer' in result else "UNKNOWN") \
                .replace("${DESCRIPTION}", result['shortDescription'] if 'shortDescription' in result else "")

            rendered += separator_template

        return rendered


def render_judged(products, judged, grade_col='ctr', label=""):
    """ Render the computed judgments alongside the productns and description data"""
    w_prods = judged.merge(products, left_on='doc_id', right_on='upc', how='left')

    w_prods = w_prods[[grade_col, 'image', 'upc', 'name', 'shortDescription']]

    return HTML(f"<h1>{label}</h1>" + w_prods.to_html(escape=False))


def download(uris, dest='data/'):
    for uri in uris:
        download_one(uri=uri, dest=dest, force=False, fancy=False)

def download_one(uri, dest='data/', force=False, fancy=False):
    import os

    if not os.path.exists(dest):
        os.makedirs(dest)

    if not os.path.isdir(dest):
        raise ValueError("dest {} is not a directory".format(dest))

    filename = uri[uri.rfind('/') + 1:]
    filepath = os.path.join(dest, filename)
    if path.exists(filepath):
        if not force:
            print(filepath + ' already exists')
            return
        print("exists but force=True, Downloading anyway")

    if not fancy:
        with open(filepath, 'wb') as out:
            print('GET {}'.format(uri))
            resp = requests.get(uri, stream=True)
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:
                    out.write(chunk)
    else:
        resp = requests.get(uri, stream=True)
        total = int(resp.headers.get('content-length', 0))
        with open(filepath, 'wb') as file, tqdm(
                desc=filepath,
                total=total,
                unit='iB',
                unit_scale=True,
                unit_divisor=1024,
        ) as bar:
            for data in resp.iter_content(chunk_size=1024):
                size = file.write(data)
                bar.update(size)


@contextmanager
def judgments_open(path=None, mode='r'):
    """ Work with judgments from the filesystem,
        either in a read or write mode"""
    try:
        f=open(path, mode)
        if mode[0] == 'r':
            yield JudgmentsReader(f)
        elif mode[0] == 'w':
            writer = JudgmentsWriter(f)
            yield writer
            writer.flush()
    finally:
        f.close()

