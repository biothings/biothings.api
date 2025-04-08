"""
Tests for evaluating the module biothings.web.services.metadata
"""

import logging

from biothings.web import connections
from biothings.web.services.metadata import BiothingsESMetadata, BiothingsMongoMetadata

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def test_es():
    client = connections.get_es_client("http://localhost:9200")
    indices = {}

    metadata = BiothingsESMetadata(indices, client)
    metadata.refresh()

    logging.info(metadata.biothing_metadata)
    logging.info(metadata.biothing_mappings)
    logging.info(metadata.biothing_licenses)


def test_mongo():
    client = connections.get_mongo_client("mongodb://su05:27017/genedoc")
    collections = {
        "old": "mygene_allspecies_20210510_yqynv8db",
        "new": "mygene_allspecies_20210517_04usbghm",
    }

    metadata = BiothingsMongoMetadata(collections, client)
    metadata.refresh(biothing_type=None)

    logging.info(metadata.get_metadata("old"))
    logging.info(metadata.get_mappings("old"))
    logging.info(metadata.get_licenses("old"))
