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
    # mongomock stands in for a real server: BiothingsMongoMetadata only needs
    # estimated_document_count(), and requiring a reachable mongod (it used to
    # point at su05) made this untestable off the internal network.
    collections = {
        "old": "mygene_allspecies_20210510_yqynv8db",
        "new": "mygene_allspecies_20210517_04usbghm",
    }
    database = mongomock.MongoClient()["genedoc"]
    database[collections["old"]].insert_many([{"_id": str(i)} for i in range(7)])
    database[collections["new"]].insert_many([{"_id": str(i)} for i in range(11)])

    metadata = BiothingsMongoMetadata(collections, database)

    # refresh() is a coroutine, so it has to be awaited -- and per
    # biothing_type, since it looks the name up in 'collections'
    for biothing_type in collections:
        await metadata.refresh(biothing_type)

    assert metadata.types == ("old", "new")
    assert metadata.get_metadata("old")["stats"]["total"] == 7
    assert metadata.get_metadata("new")["stats"]["total"] == 11
    assert metadata.get_metadata("old")["biothing_type"] == "old"

    # a document database has no schema to report, and licence info is left to
    # the metadata storage
    assert metadata.get_mappings("old") == {"__N/A__": True}
    assert metadata.get_licenses("old") == {}

    logging.info(metadata.get_metadata("old"))


class TestExistsAliasMetadata:
    """
    The hub records the '_exists_' alias map it derived at build time in the
    index _meta; the web tier reads it back and only honors entries it can
    verify against the mapping. A stale or hand-edited _meta must not be able
    to redirect a query somewhere else -- that would silently return the wrong
    documents, which is worse than the slow query being optimized away.
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
            # valid, at any depth: kept as is
            (
                {"gnomad_genome": "gnomad_genome.chrom", "gnomad_genome.hom": "gnomad_genome.hom.hom"},
                {"gnomad_genome": "gnomad_genome.chrom", "gnomad_genome.hom": "gnomad_genome.hom.hom"},
            ),
            ({"gnomad_genome": 1}, {}),  # value is not a field name
            ({1: "gnomad_genome.chrom"}, {}),  # key is not a field name
            ({"gnomad_genome": "somewhere.else"}, {}),  # not a subfield of the key
            ({"gnomad_genome": "gnomad_genome"}, {}),  # the object itself, no gain
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
        """One entry of the dict '_BiothingsESMetadataReader' expects, shaped
        like the per-index value 'client.indices.get()' returns."""
        return {
            "aliases": {},
            "settings": {"index": {"creation_date": "1700000000000", "version": {"created": "8000099"}}},
            "mappings": {"properties": properties, "_meta": {"exists_field_aliases": aliases}},
        }

    @pytest.mark.parametrize(
        "per_index, expected",
        [
            # a field missing entirely from an index (mygeneset-shaped: a
            # curated index plus a structurally different one) can't veto
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
            # both map the field, but only one verified an alias for it
            (
                {
                    "curated": ({"genes": {"properties": {"taxid": {"type": "integer"}}}}, {"genes": "genes.taxid"}),
                    "other": ({"genes": {"properties": {"taxid": {"type": "integer"}}}}, {}),
                },
                {},
            ),
            # two indices proposing different subfields for the same field
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
