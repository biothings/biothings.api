# pylint: disable=unexpected-keyword-arg
"""
    Manage sample dataset and mapping for testing

    - Run this file directly to (re)generate sample files.
    - Define pytest session level fixture.

"""
import json
import os

import elasticsearch
import pytest

TEST_INDEX = 'bts_test'
TEST_DOC_TYPE = 'gene'

def prepare():
    """
    Create two files in this folder that defines a test index.
    Data is a random set of 100 mygene documents from an ES6 index.

        ./test_data_index.json # includes type name
        ./test_data_query.json # the query to generate test data
        ./test_data.ndjson  # used for bulk api

        Envs: ES_SOURCE Assume source cluster has only one index.
    """
    client = elasticsearch.Elasticsearch(os.environ['ES_SOURCE'])
    dirname = os.path.dirname(__file__)

    mapping = client.indices.get('_all')
    with open(os.path.join(dirname, 'test_data_index.json'), 'w') as file:
        setting = next(iter(mapping.values()))
        setting["settings"]["index"] = {"analysis": setting["settings"]["index"]["analysis"]}
        json.dump(setting, file, indent=2)

    with open(os.path.join(dirname, 'test_data_query.json'), 'r') as file:
        query = json.load(file)
        docs = client.search(body=query, size=100)

    with open(os.path.join(dirname, 'test_data.ndjson'), 'w') as file:
        for hit in docs['hits']['hits']:
            json.dump({"index": {"_id": hit['_id']}}, file)
            file.write('\n')
            json.dump(hit['_source'], file)
            file.write('\n')

@pytest.fixture(scope="session", autouse=True)
def setup_es():
    """
    Populate ES with test index and documents.
    Index to localhost:9200 only.
    """
    client = elasticsearch.Elasticsearch()
    dirname = os.path.dirname(__file__)

    server_major_version = client.info()['version']['number'].split('.')[0]
    client_major_version = str(elasticsearch.__version__[0])

    # NOTE: Temporary comment to bypass this check.
    # Because we still use elasticsearch library ver under 8
    # if server_major_version != client_major_version:
    #     pytest.exit('ES version does not match its python library.')

    try:
        if not client.indices.exists(TEST_INDEX):


            mapping_file = 'test_data_index.json'
            if int(server_major_version) >= 8:
                mapping_file = 'test_data_index_for_es8.json'
            with open(os.path.join(dirname, mapping_file), 'r') as file:
                mapping = json.load(file)

            with open(os.path.join(dirname, 'test_data.ndjson'), 'r') as file:
                ndjson = file.read()

            if int(server_major_version) >= 8:
                client.indices.create(TEST_INDEX, mapping)
                client.bulk(ndjson, TEST_INDEX)
            elif elasticsearch.__version__[0] > 6:
                client.indices.create(TEST_INDEX, mapping, include_type_name=True)
                client.bulk(ndjson, TEST_INDEX)
            else:
                client.indices.create(TEST_INDEX, mapping)
                client.bulk(ndjson, TEST_INDEX, TEST_DOC_TYPE)

            client.indices.refresh()
            yield
            client.indices.delete(index=TEST_INDEX)

        else:
            yield

    except FileNotFoundError:
        pytest.exit('Error Loading Testing Data')
    except elasticsearch.exceptions.ImproperlyConfigured:
        pytest.exit('ES Configuration Error')
    except elasticsearch.exceptions.ElasticsearchException:
        pytest.exit('ES Setup Error')


if __name__ == "__main__":
    prepare()
