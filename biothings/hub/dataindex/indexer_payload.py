import asyncio
from collections import UserDict

__all__ = [
    "DEFAULT_INDEX_SETTINGS",
    "DEFAULT_INDEX_MAPPINGS",
    "IndexMappings",
    "IndexSettings"
]


# the hub may create an "all" field for all fields
# we want to enable query string query by default

DEFAULT_INDEX_SETTINGS = {
    "query": {
        "default_field": "_id,all"
    },
    "codec": "best_compression",
    "analysis": {
        "analyzer": {
            # Sebastian's Note:
            # soon deprecated in favor of keyword_lowercase_normalizer
            "string_lowercase": {
                "tokenizer": "keyword",
                "filter": "lowercase"
            },
            "whitespace_lowercase": {
                "tokenizer": "whitespace",
                "filter": "lowercase"
            },
        },
        "normalizer": {
            "keyword_lowercase_normalizer": {
                "filter": ["lowercase"],
                "type": "custom",
                "char_filter": []
            },
        }
    },
}

DEFAULT_INDEX_MAPPINGS = {
    "dynamic": "false",
    "properties": {"all": {'type': 'text'}}
}


class _IndexPayload(UserDict):

    async def finalize(self, client):
        """ Generate the ES payload format of the corresponding entities
        originally in Hub representation. May require querying the ES client
        for certain metadata to determine the compatible data format. """

class IndexMappings(_IndexPayload):

    async def finalize(self, client):
        version = int((yield from client.info())['version']['number'].split('.')[0])
        if version < 7:  # inprecise
            doc_type = self.pop("__hub_doc_type", "doc")
            return {doc_type: dict(self)}
        else:
            self.pop("__hub_doc_type", None)
        return dict(self)

class IndexSettings(_IndexPayload):

    async def finalize(self, client):
        return {"index": dict(self)}


def test_01():
    import asyncio
    from elasticsearch import AsyncElasticsearch

    client = AsyncElasticsearch()
    mappings = IndexMappings(DEFAULT_INDEX_MAPPINGS)
    mappings["dynamic"] = True

    async def finalize_mapping():
        print(await mappings.finalize(client))
        await client.close()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(finalize_mapping())

def test_02():
    import asyncio

    settings = IndexSettings(DEFAULT_INDEX_SETTINGS)
    settings["codec"] = "meow"

    async def finalize_settings():
        print(await settings.finalize(None))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(finalize_settings())


if __name__ == "__main__":
    test_02()
