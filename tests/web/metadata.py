from biothings.web.services.metadata import *
from biothings.web import connections
from pprint import pprint as print

def test_es():
    client = connections.get_es_client("localhost:9200")
    indices = {None: "bts_test", "gene": "bts_test"}

    metadata = BiothingsESMetadata(indices, client)
    metadata.refresh()

    print(metadata.biothing_metadata)
    print(metadata.biothing_mappings)
    print(metadata.biothing_licenses)

def test_mongo():
    client = connections.get_mongo_client("mongodb://su05:27017/genedoc")
    collections = {
        "old": "mygene_allspecies_20210510_yqynv8db",
        "new": "mygene_allspecies_20210517_04usbghm"
    }

    metadata = BiothingsMongoMetadata(collections, client)
    metadata.refresh()

    print(metadata.get_metadata("old"))
    print(metadata.get_mappings("old"))
    print(metadata.get_licenses("old"))


if __name__ == "__main__":
    test_mongo()
