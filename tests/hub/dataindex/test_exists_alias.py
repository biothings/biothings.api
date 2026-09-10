"""
Tests for deriving '_exists_:<object field>' aliases.

TestMappingWalk covers picking the candidate object fields out of a mapping;
TestDeriveAliases checks, against a real Elasticsearch, that a derived alias
really matches the same documents as the object query it replaces.
"""

import pytest

from biothings.hub.dataindex.exists_alias import (
    META_KEY,
    _collect_subfields,
    _objects_to_scan,
    _pick_alias,
    derive_exists_field_aliases,
    store_exists_field_aliases,
)

# one top-level leaf, two top-level objects, and nesting three levels deep
PROPERTIES = {
    "chrom": {"type": "keyword"},
    "gnomad_genome": {
        "properties": {
            "chrom": {"type": "keyword"},
            "alt": {"type": "keyword"},
            "notindexed": {"type": "keyword", "index": False},
            "af": {
                "properties": {
                    "af": {"type": "float"},
                    "af_afr": {"type": "float"},
                    "nested": {"properties": {"x": {"type": "float"}, "y": {"type": "float"}}},
                }
            },
        }
    },
    "clinvar": {"properties": {"chrom": {"type": "keyword"}, "type": {"type": "keyword"}}},
    "disabled": {"enabled": False, "properties": {"a": {"type": "keyword"}}},
}


class TestMappingWalk:
    def test_subfields_are_collected_at_every_depth(self):
        subfields = _collect_subfields(PROPERTIES["gnomad_genome"]["properties"], "gnomad_genome.")
        assert sorted(subfields) == [
            "gnomad_genome.af.af",
            "gnomad_genome.af.af_afr",
            "gnomad_genome.af.nested.x",
            "gnomad_genome.af.nested.y",
            "gnomad_genome.alt",
            "gnomad_genome.chrom",
        ]
        # 'index: false' cannot be matched by an exists query, so it is useless
        # as an alias
        assert "gnomad_genome.notindexed" not in subfields

    @pytest.mark.parametrize(
        "max_depth, expected",
        [
            # 0: root level only. top-level leaves ('chrom') and disabled
            # objects are never candidates. biggest object first.
            (0, ["gnomad_genome", "clinvar"]),
            # 1: nested objects are candidates in their own right, since
            # '_exists_:gnomad_genome.af' is a separate query from its parent
            (1, ["gnomad_genome", "gnomad_genome.af", "clinvar"]),
            # 2 and None both reach the deepest object in this mapping
            (2, ["gnomad_genome", "gnomad_genome.af", "gnomad_genome.af.nested", "clinvar"]),
            (None, ["gnomad_genome", "gnomad_genome.af", "gnomad_genome.af.nested", "clinvar"]),
        ],
        ids=["root_only", "one_nested_level", "two_nested_levels", "all_levels"],
    )
    def test_depth_limit(self, max_depth, expected):
        scanned = [field for field, _ in _objects_to_scan(PROPERTIES, 2, max_depth)]
        # ties in subfield count keep mapping order, so compare as sets plus
        # the "biggest first" property that makes the log readable
        assert set(scanned) == set(expected)
        assert scanned[0] == "gnomad_genome"

    @pytest.mark.parametrize(
        "min_subfields, expected",
        [(2, 4), (3, 2), (7, 0)],  # 7 > the 6 subfields of the largest object
        ids=["keep_all", "skip_the_small_ones", "skip_everything"],
    )
    def test_min_subfields_keeps_the_scan_off_cheap_objects(self, min_subfields, expected):
        assert len(_objects_to_scan(PROPERTIES, min_subfields, None)) == expected

    @pytest.mark.parametrize(
        "candidates, expected",
        [
            (["a.b.c", "a.z", "a.b"], "a.b"),  # shallowest wins
            (["a.bbb", "a.cc"], "a.cc"),  # then shortest
            (["a.zz", "a.yy"], "a.yy"),  # then alphabetical
        ],
        ids=["shallowest", "shortest", "alphabetical"],
    )
    def test_alias_choice_is_deterministic(self, candidates, expected):
        assert _pick_alias(candidates) == expected


class TestDeriveAliases:
    """Against a real Elasticsearch, via the es_client/index_name fixtures."""

    @staticmethod
    async def _load(es_client, index_name, docs):
        for doc in docs:
            await es_client.index(index=index_name, id=doc.pop("_id"), document=doc, refresh=True)

    @staticmethod
    async def _ids(es_client, index_name, field):
        response = await es_client.search(index=index_name, query={"exists": {"field": field}}, size=10)
        return [hit["_id"] for hit in response["hits"]["hits"]]

    @pytest.mark.asyncio
    async def test_only_a_subfield_present_on_every_document_is_chosen(self, es_client, index_name):
        # 'chrom' is on every doc holding the object; 'alt' is missing on doc 2,
        # so choosing it would silently drop that doc from results
        await self._load(
            es_client,
            index_name,
            [
                {"_id": "1", "gnomad_genome": {"chrom": "1", "alt": "A"}},
                {"_id": "2", "gnomad_genome": {"chrom": "2"}},
                {"_id": "3", "other": 1},
            ],
        )
        aliases = await derive_exists_field_aliases(es_client, index_name, min_subfields=2, max_depth=0)

        assert aliases == {"gnomad_genome": "gnomad_genome.chrom"}
        # the whole point: the same documents, in the same order
        assert await self._ids(es_client, index_name, "gnomad_genome") == ["1", "2"]
        assert await self._ids(es_client, index_name, "gnomad_genome.chrom") == ["1", "2"]

    @pytest.mark.asyncio
    async def test_nested_object_gets_its_own_alias(self, es_client, index_name):
        # doc 3 has the parent object but not 'hom', so parent and child match
        # genuinely different documents and each needs its own alias
        await self._load(
            es_client,
            index_name,
            [
                {"_id": "1", "gnomad_genome": {"chrom": "1", "hom": {"hom": 1, "hom_afr": 1}}},
                {"_id": "2", "gnomad_genome": {"chrom": "2", "hom": {"hom": 2, "hom_afr": 2}}},
                {"_id": "3", "gnomad_genome": {"chrom": "3"}},
            ],
        )

        assert await derive_exists_field_aliases(es_client, index_name, 2, max_depth=0) == {
            "gnomad_genome": "gnomad_genome.chrom"
        }
        aliases = await derive_exists_field_aliases(es_client, index_name, 2, max_depth=1)
        assert aliases == {
            "gnomad_genome": "gnomad_genome.chrom",
            "gnomad_genome.hom": "gnomad_genome.hom.hom",
        }

        for object_field, alias in aliases.items():
            assert await self._ids(es_client, index_name, object_field) == await self._ids(es_client, index_name, alias)
        # and the child really is narrower than the parent
        assert await self._ids(es_client, index_name, "gnomad_genome") == ["1", "2", "3"]
        assert await self._ids(es_client, index_name, "gnomad_genome.hom") == ["1", "2"]

    @pytest.mark.asyncio
    async def test_no_alias_when_no_subfield_qualifies(self, es_client, index_name):
        # every subfield is missing on at least one document holding the
        # object, so the field is left alone rather than rewritten unsafely
        await self._load(
            es_client,
            index_name,
            [
                {"_id": "1", "src": {"a": 1, "b": 2}},
                {"_id": "2", "src": {"a": 1}},
                {"_id": "3", "src": {"b": 2}},
            ],
        )
        assert await derive_exists_field_aliases(es_client, index_name, min_subfields=2) == {}

    @pytest.mark.asyncio
    async def test_storing_the_map_preserves_the_rest_of_meta(self, es_client, index_name):
        await es_client.indices.put_mapping(index=index_name, body={"_meta": {"build_version": "20260101"}})
        await store_exists_field_aliases(es_client, index_name, {"src": "src.a"})

        meta = (await es_client.indices.get_mapping(index=index_name))[index_name]["mappings"]["_meta"]
        assert meta == {"build_version": "20260101", META_KEY: {"src": "src.a"}}
