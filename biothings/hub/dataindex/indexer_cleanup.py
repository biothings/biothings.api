import collections
import datetime
import itertools
import logging
from collections import UserDict, UserList
from dataclasses import dataclass

from elasticsearch import AsyncElasticsearch

_TAB = " " * 2

class _Index(UserDict):

    def __str__(self):
        return (
            f"<Index"
            f" env={repr(self.get('environment'))}"
            f" name={repr(self.get('_id'))}"
            f" ts={repr(self.get('created_at'))}"
            f"/>"
        )

class _Indices(UserList):
    group = NotImplemented

    def __init__(self, initlist=None):
        super().__init__(initlist)
        self.data = [_Index(x) for x in self]

    def __str__(self):
        return '\n'.join((
            f"<{self.group} len={len(self)}>",
            *(_TAB + str(index) for index in self),
            f"<{self.group}/>"
        ))

class _IndicesToKeep(_Indices):
    group = "Keep"

class _IndicesToRemove(_Indices):
    group = "Remove"

@dataclass
class _BuildConfig():
    name: str
    remove: _IndicesToRemove
    keep: _IndicesToKeep

    def __str__(self):
        return '\n'.join((
            f"<BuildConfig {repr(self.name)}>",
            *(_TAB + line for line in str(self.remove).split('\n')),
            *(_TAB + line for line in str(self.keep).split('\n')),
            f"<BuildConfig/>"
        ))

    def __iter__(self):
        return iter(self.remove)

class _CleanUpList(UserList):

    def __str__(self):
        lines = map(lambda x: str(x).split('\n'), self)
        lines = itertools.chain.from_iterable(lines)
        return "\n".join((
            "<CleanUp>",
            *(_TAB + line for line in lines),
            "<CleanUp/>"
        ))


def find(collection, env, keep=3, logger=None):
    logger = logger or logging.getLogger(__name__)
    results = list(collection.aggregate([
        {'$project': {
            'build_config': '$build_config._id',
            'index': {'$objectToArray': '$index'}}},
        {'$unwind': {'path': '$index'}},
        {'$addFields': {
            'index.v.build_config': '$build_config',
            'index.v._id': '$index.k'}},
        {'$replaceRoot': {'newRoot': '$index.v'}},
        {'$match': {
            'environment': env or {'$exists': True},
            'archive': {'$not': {'$eq': True}}}},  # TODO
        {'$project': {
            'build_config': 1,
            'environment': 1,
            'created_at': 1}},
        {'$sort': {'created_at': 1}},
        {'$group': {
            '_id': "$build_config",
            'indices': {"$push": "$$ROOT"}
        }}
    ]))
    return _CleanUpList([
        _BuildConfig(
            doc["_id"],  # ↓ -0 slicing does not yield the desired result
            _IndicesToRemove(doc["indices"][:len(doc["indices"])-keep]),
            _IndicesToKeep(doc["indices"][len(doc["indices"])-keep:])
        ) for doc in results
    ])

async def delete(collection, cleanups, indexers, logger=None):
    logger = logger or logging.getLogger(__name__)
    logging.info(cleanups)

    for index in itertools.chain.from_iterable(cleanups):
        args = indexers[index["environment"]]["args"]

        async with AsyncElasticsearch(**args) as client:
            resposne = await client.indices.delete(index["_id"], ignore_unavailable=True)
            logging.info(("DELETE", str(index), resposne))

            collection.update_many(
                {f"index.{index['_id']}.environment": index["environment"]},
                {"$unset": {f"index.{index['_id']}": 1}}
            )

class Cleaner():

    def __init__(self, collection, indexers, logger):
        self.collection = collection
        self.indexers = indexers
        self.logger = logger

    def find(self, env, keep=3):
        return find(self.collection, env, keep, self.logger)

    def execute(self, cleanups):
        return delete(self.collection, cleanups, self.indexers, self.logger)


def test_():
    return _CleanUpList([_BuildConfig(
        "mynews",
        _IndicesToRemove([
            {'_id': 'mynews_20210811_test',
             'build_config': 'mynews',
             'created_at': datetime.datetime(2021, 8, 11, 19, 27, 25, 141000),
             'environment': 'local'},
            {'_id': 'mynews_202105261855_5ffxvchx',
             'build_config': 'mynews',
             'created_at': datetime.datetime(2021, 8, 16, 9, 26, 56, 221000),
             'environment': 'local'},
            {'_id': 'mynews_202012280220_vsdevjdk',
             'build_config': 'mynews',
             'created_at': datetime.datetime(2021, 8, 17, 0, 23, 11, 374000),
             'environment': 'local'}
        ]),
        _IndicesToKeep([
            {'_id': 'mynews_202009170234_fjvg7skx',
             'build_config': 'mynews',
             'created_at': datetime.datetime(2020, 9, 17, 2, 35, 12, 800000),
             'environment': 'local'},
            {'_id': 'mynews_202009222133_6rz3vljq',
             'build_config': 'mynews',
             'created_at': datetime.datetime(2020, 9, 22, 21, 33, 40, 958000),
             'environment': 'local'},
            {'_id': 'mynews_202010060100_ontyofuv',
             'build_config': 'mynews',
             'created_at': datetime.datetime(2020, 10, 6, 1, 0, 11, 237000),
             'environment': 'local'}
        ])
    )])

def test_00():
    print(test_())

def test_01():
    from pymongo import MongoClient
    logging.basicConfig(level="DEBUG")

    client = MongoClient()
    collection = client["biothings"]["src_build"]
    obj = find(collection, None)
    print(type(obj))
    print(obj)

def test_02():
    import asyncio
    from pymongo import MongoClient
    logging.basicConfig(level="INFO")

    client = MongoClient()
    collection = client["biothings"]["src_build"]
    cleanups = find(collection, None, 1)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(delete(collection, cleanups, {"local": {"args": {}}}))


if __name__ == '__main__':
    test_01()
