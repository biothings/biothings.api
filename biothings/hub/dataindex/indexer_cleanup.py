
import datetime
import itertools
import logging
from collections import UserDict, UserList
from dataclasses import dataclass

from elasticsearch import AsyncElasticsearch


# NOTE
# Throughout this module, an XML-like serialization
# format is utilized, just like the python default:
#
# >>> object()
# <object object at 0x7f7b4882a110>
#
# This format helps preserve hierarchical structure
# in the object it represents, is intuitive to read,
# and closely represent the underlying programming
# concepts, each tagname corresponds to one class.
#
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

# OUTPUT EXAMPLE
# ---------------------
# >>> _CleanUpList(...)
# <CleanUp>
#   <BuildConfig 'mygene_allspecies'>
#     <Remove len=6>
#       <Index env='prod' name='mygene_xbuo6d' ts=datetime(2019, 8, 27, ...)/>
#       <Index env='prod' name='mygene_vxia0r' ts=datetime(2020, 1, 8, ...)/>
#       <Index env='prod' name='mygene_25wlt4' ts=datetime(2020, 1, 20, ...)/>
#       <Index env='local' name='mygene_ufkw79' ts=datetime(2020, 8, 28, ...)/>
#       <Index env='local' name='mygene_uq3chc' ts=datetime(2021, 4, 6, ...)/>
#       <Index env='local' name='mygene_mnkct5' ts=datetime(2021, 4, 15, ...)/>
#     <Remove/>
#     <Keep len=3>
#       <Index env='su10' name='mygene_ibjpha' ts=datetime(2021, 6, 30, ...)/>
#       <Index env='su10' name='mygene_osyzmt' ts=datetime(2021, 8, 9, ...)/>
#       <Index env='su10' name='mygene_test' ts=datetime(2021, 8, 9, ...)/>
#     <Keep/>
#   <BuildConfig/>
#   <BuildConfig 'demo_allspecies'>
#     <Remove len=3>
#       <Index env='test' name='demo_mygene_ngupjv' ts=datetime(2018, 3, 12, ...)/>
#       <Index env='test' name='demo_mygene_rpguqe' ts=datetime(2018, 8, 6, ...)/>
#       <Index env='test' name='demo_mygene_vqpur6' ts=datetime(2019, 1, 29, ...)/>
#     <Remove/>
#     <Keep len=3>
#       <Index env='test' name='demo_mygene_irvwa0' ts=datetime(2019, 3, 25, ...)/>
#       <Index env='test' name='demo_mygene_cpoldl' ts=datetime(2020, 1, 8, ...)/>
#       <Index env='local' name='demo_mygene_qekeic' ts=datetime(2020, 4, 22, ...)/>
#     <Keep/>
#   <BuildConfig/>
# <CleanUp/>
#


class Cleaner():

    def __init__(self, collection, indexers, logger=None):

        self.collection = collection  # pymongo.collection.Collection
        self.indexers = indexers  # hub.dataindex.IndexManager
        self.logger = logger or logging.getLogger(__name__)

    def find(self, env, keep=3):
        results = list(self.collection.aggregate([
            {'$project': {
                'build_config': '$build_config._id',
                'index': {'$objectToArray': '$index'}}},
            {'$unwind': {'path': '$index'}},
            {'$addFields': {
                'index.v.build_config': '$build_config',
                'index.v._id': '$index.k'}},
            {'$replaceRoot': {'newRoot': '$index.v'}},
            {'$match': {'environment': env or {'$exists': True}}},
            {'$project':  # ...............: {X
                dict.fromkeys((  # ........:    '_id': 'mynews_202012280220_vsdevjdk',
                    'build_config',  # ....:    'build_config': 'mynews',  ──────────┐
                    'environment',  # .....:    'environment': 'local',              │
                    'created_at'), 1)},  # :    'created_at': datetime(...)          │
            {'$sort': {'created_at': 1}},  # }Y                                      │
            {'$group': {  # ...............: {                              GROUP BY │
                '_id': "$build_config",  # :    '_id': 'mynews',  <──────────────────┘
                'indices': {  # ...........:    'indices': [
                    "$push": "$$ROOT"  # ..:        {X ... }Y, ...
                }}}  # ....................:    ]
        ]))  # ............................: }
        return _CleanUpList([
            _BuildConfig(
                doc["_id"],  # ↓ -0 in slicing does not yield the desired result
                _IndicesToRemove(doc["indices"][:-keep or len(doc["indices"])]),
                _IndicesToKeep(doc["indices"][-keep or len(doc["indices"]):])
            ) for doc in results
        ])

    async def clean(self, cleanups):
        self.logger.debug(cleanups)

        actions = []
        for index in itertools.chain.from_iterable(cleanups):
            args = self.indexers[index["environment"]]["args"]

            async with AsyncElasticsearch(**args) as client:
                resposne = await client.indices.delete(
                    index=index["_id"], ignore_unavailable=True)
                action = ("DELETE", str(index), resposne)

                actions.append(action)
                logging.info(action)

                self.collection.update_many(
                    {f"index.{index['_id']}.environment": index["environment"]},
                    {"$unset": {f"index.{index['_id']}": 1}}
                )
        return actions

def test_str():
    print(_CleanUpList([_BuildConfig(
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
    )]))

def test_find():
    from pymongo import MongoClient
    logging.basicConfig(level="DEBUG")

    client = MongoClient("su04")
    collection = client["mychem_hubdb"]["src_build"]
    cleaner = Cleaner(collection, {"local": {"args": {}}})
    obj = cleaner.find("local")
    print(type(obj))
    print(obj)
    return cleaner, obj

def test_clean():
    import asyncio
    cleaner, cleanups = test_find()
    loop = asyncio.get_event_loop()
    print(loop.run_until_complete(cleaner.clean(cleanups)))


if __name__ == '__main__':
    test_find()
