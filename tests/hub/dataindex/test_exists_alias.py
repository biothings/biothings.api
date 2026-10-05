"""
Test exists alias derivation and storage against Elasticsearch.
"""

import pytest

from biothings.hub.dataindex.exists_alias import (
    META_KEY,
    derive_exists_field_aliases,
    store_exists_field_aliases,
)


class TestDeriveAliases:
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
        # Only 'chrom' is present in every document containing 'gnomad_genome'.
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
        # The object and its alias must match the same documents.
        assert await self._ids(es_client, index_name, "gnomad_genome") == ["1", "2"]
        assert await self._ids(es_client, index_name, "gnomad_genome.chrom") == ["1", "2"]

    @pytest.mark.asyncio
    async def test_alias_choice_is_deterministic(self, es_client, index_name):
        # All subfields qualify; depth, length, and alphabetical order select src.yy.
        await self._load(
            es_client,
            index_name,
            [
                {"_id": "1", "src": {"zz": 1, "yy": 2, "bbb": 3, "nested": {"a": 4}}},
                {"_id": "2", "src": {"zz": 1, "yy": 2, "bbb": 3, "nested": {"a": 4}}},
            ],
        )
        aliases = await derive_exists_field_aliases(es_client, index_name, min_subfields=2, max_depth=0)
        assert aliases == {"src": "src.yy"}

    @pytest.mark.asyncio
    async def test_nested_object_gets_its_own_alias(self, es_client, index_name):
        # Document 3 has the parent but no 'hom', so they need separate aliases.
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
        # The child object matches only the first two documents.
        assert await self._ids(es_client, index_name, "gnomad_genome") == ["1", "2", "3"]
        assert await self._ids(es_client, index_name, "gnomad_genome.hom") == ["1", "2"]

    @pytest.mark.asyncio
    async def test_min_subfields_skips_small_objects(self, es_client, index_name):
        # 'big' has three leaf fields; 'small' has two.
        await self._load(
            es_client,
            index_name,
            [
                {"_id": "1", "big": {"a": 1, "b": 2, "c": 3}, "small": {"a": 1, "b": 2}},
                {"_id": "2", "big": {"a": 1, "b": 2, "c": 3}, "small": {"a": 1, "b": 2}},
            ],
        )
        assert set(await derive_exists_field_aliases(es_client, index_name, min_subfields=2, max_depth=0)) == {
            "big",
            "small",
        }
        assert set(await derive_exists_field_aliases(es_client, index_name, min_subfields=3, max_depth=0)) == {"big"}
        assert await derive_exists_field_aliases(es_client, index_name, min_subfields=4, max_depth=0) == {}

    @pytest.mark.asyncio
    async def test_no_alias_when_no_subfield_qualifies(self, es_client, index_name):
        # Neither subfield is present in every document containing the object.
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
