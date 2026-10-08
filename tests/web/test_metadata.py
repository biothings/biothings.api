"""
Tests for evaluating the module biothings.web.services.metadata
"""

import logging

import mongomock
import pytest

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


@pytest.mark.asyncio
async def test_mongo():
    # Use mongomock to test document counts without a MongoDB server.
    collections = {
        "old": "mygene_allspecies_20210510_yqynv8db",
        "new": "mygene_allspecies_20210517_04usbghm",
    }
    database = mongomock.MongoClient()["genedoc"]
    database[collections["old"]].insert_many([{"_id": str(i)} for i in range(7)])
    database[collections["new"]].insert_many([{"_id": str(i)} for i in range(11)])

    metadata = BiothingsMongoMetadata(collections, database)

    for biothing_type in collections:
        await metadata.refresh(biothing_type)

    assert metadata.types == ("old", "new")
    assert metadata.get_metadata("old")["stats"]["total"] == 7
    assert metadata.get_metadata("new")["stats"]["total"] == 11
    assert metadata.get_metadata("old")["biothing_type"] == "old"

    # This backend does not provide mappings or licenses.
    assert metadata.get_mappings("old") == {"__N/A__": True}
    assert metadata.get_licenses("old") == {}

    logging.info(metadata.get_metadata("old"))


class TestExistsAliasMetadata:
    """
    Test alias validation against mappings and agreement across indices.
    """

    MAPPING = {
        "properties": {
            "chrom": {"type": "keyword"},
            "gnomad_genome": {
                "properties": {
                    "chrom": {"type": "keyword"},
                    "hom": {"properties": {"hom": {"type": "integer"}}},
                }
            },
        }
    }

    def _extract(self, aliases):
        from biothings.web.services.metadata import _ESIndexMappings

        mapping = dict(self.MAPPING, _meta={"src": {}, "exists_field_aliases": aliases})
        return _ESIndexMappings(mapping).extract_exists_aliases()

    @pytest.mark.parametrize(
        "entry, expected",
        [
            (
                {"gnomad_genome": "gnomad_genome.chrom", "gnomad_genome.hom": "gnomad_genome.hom.hom"},
                {"gnomad_genome": "gnomad_genome.chrom", "gnomad_genome.hom": "gnomad_genome.hom.hom"},
            ),
            ({"gnomad_genome": 1}, {}),  # value is not a field name
            ({1: "gnomad_genome.chrom"}, {}),  # key is not a field name
            ({"gnomad_genome": "somewhere.else"}, {}),  # not a subfield of the key
            ({"gnomad_genome": "gnomad_genome"}, {}),  # alias is the object itself
            ({"chrom": "chrom.x"}, {}),  # key is a leaf, not an object
            ({"absent": "absent.x"}, {}),  # key is not in this mapping
            ({"gnomad_genome": "gnomad_genome.gone"}, {}),  # target is not in this mapping
        ],
        ids=[
            "valid_at_any_depth",
            "non_string_value",
            "non_string_key",
            "alias_outside_the_object",
            "alias_is_the_object",
            "key_is_a_leaf",
            "key_absent_from_mapping",
            "target_absent_from_mapping",
        ],
    )
    def test_extract_exists_aliases(self, entry, expected):
        assert self._extract(entry) == expected

    @pytest.mark.parametrize("aliases", [{}, None, "not-a-dict"], ids=["empty", "missing", "malformed"])
    def test_absent_or_malformed_map_disables_the_rewrite(self, aliases):
        assert self._extract(aliases) == {}

    @staticmethod
    def _index_info(properties, aliases):
        """Return one index entry in the format returned by client.indices.get()."""
        return {
            "aliases": {},
            "settings": {"index": {"creation_date": "1700000000000", "version": {"created": "8000099"}}},
            "mappings": {"properties": properties, "_meta": {"exists_field_aliases": aliases}},
        }

    @pytest.mark.parametrize(
        "per_index, expected",
        [
            # An index without the object field does not block its alias.
            (
                {
                    "curated": (
                        {
                            "msigdb": {"properties": {"id": {"type": "keyword"}}},
                            "go": {"properties": {"id": {"type": "keyword"}}},
                        },
                        {"msigdb": "msigdb.id", "go": "go.id"},
                    ),
                    "other": ({"name": {"type": "text"}}, {}),  # no 'msigdb', no 'go'
                },
                {"msigdb": "msigdb.id", "go": "go.id"},
            ),
            # Both indices map the field, so both must provide the same alias.
            (
                {
                    "curated": ({"genes": {"properties": {"taxid": {"type": "integer"}}}}, {"genes": "genes.taxid"}),
                    "other": ({"genes": {"properties": {"taxid": {"type": "integer"}}}}, {}),
                },
                {},
            ),
            # Conflicting aliases are excluded.
            (
                {
                    "one": ({"x": {"properties": {"a": {"type": "keyword"}}}}, {"x": "x.a"}),
                    "two": ({"x": {"properties": {"a": {"type": "keyword"}}}}, {"x": "x.b"}),
                },
                {},
            ),
        ],
        ids=["missing_field_cannot_veto", "unverified_alias_still_vetoes", "disagreeing_indices_still_veto"],
    )
    def test_get_exists_aliases_veto_rules(self, per_index, expected):
        from biothings.web.services.metadata import _BiothingsESMetadataReader

        info = {name: self._index_info(props, aliases) for name, (props, aliases) in per_index.items()}
        reader = _BiothingsESMetadataReader("geneset", info, count={"count": 10})
        assert reader.get_exists_aliases() == expected
