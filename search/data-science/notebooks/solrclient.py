import os
import requests
import re

class SolrClient():
    def __init__(self, solr_base=None):
        self.solr = requests.Session()
        if solr_base:
            self.solr_base_ep=solr_base
            if solr_base.endswith('/'):
                self.solr_base_ep=solr_base[:len(solr_base)-1]
            self.host = re.sub(r"https?://([^/:]+)((:[0-9]+)?/.*)?$", "\\1", solr_base)
        else:
            self.docker = os.environ.get('LTR_DOCKER') != None

            if self.docker:
                self.host = 'solr'
            else:
                self.host = 'localhost'

            self.solr_base_ep = f'http://{self.host}:8983/solr'



    def log_query(self, index, featureset, ids, options={}, id_field='id'):
        efi_options = []
        for key, val in options.items():
            efi_options.append(f'efi.{key}="{val}"')

        efi_str = ' '.join(efi_options)

        if ids == None:
            query = "*:*"
        else:
            query = "{{!terms f={}}}{}".format(id_field, ','.join(ids))
            print(query)

        params = {
            'fl': f"{id_field},[features store={featureset} {efi_str}]",
            'q': query,
            'rows': 1000,
            'wt': 'json'
        }
        resp = requests.post(f'{self.solr_base_ep}/{index}/select', data=params)
        resp_msg(msg=f'Searching {index}', resp=resp)
        resp = resp.json()

        def parseFeatures(features):
            fv = []

            all_features = features.split(',')

            for feature in all_features:
                elements = feature.split('=')
                fv.append(float(elements[1]))

            return fv

        # Clean up features to consistent format
        for doc in resp['response']['docs']:
            if ('[features]' not in doc):
                print("No features in doc")
                continue
            doc['ltr_features'] = parseFeatures(doc['[features]'])

        return resp['response']['docs']

def resp_msg(msg, resp, throw=True):
    print('{} [Status: {}]'.format(msg, resp.status_code))
    if resp.status_code >= 400:
        print(resp.text)
        if throw:
            raise RuntimeError(resp.text)  