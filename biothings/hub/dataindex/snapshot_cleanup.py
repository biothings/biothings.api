import logging
from xml.etree import ElementTree


class Cleaner():

    def __init__(self, collection, snapenvs, indexers, logger=None):

        self.collection = collection  # pymongo.collection.Collection
        self.snapenvs = snapenvs  # hub.dataindex.snapshooter.SnapshotManager
        self.indexers = indexers  # hub.dataindex.indexer.IndexManager
        self.logger = logger or logging.getLogger(__name__)


def find(collection, env=None, keep=3, group_by="build_config", **filters):
    if isinstance(group_by, str):
        group_by = "$" + group_by
    elif isinstance(group_by, (list, tuple)):
        group_by = {k.replace('.', '_'): "$" + k for k in group_by}

    results = list(collection.aggregate([
        {'$project': {
            'build_config': '$build_config._id',
            'snapshot': {'$objectToArray': '$snapshot'}}},
        {'$unwind': {'path': '$snapshot'}},
        {'$addFields': {
            'snapshot.v.build_config': '$build_config',
            'snapshot.v.build_name': '$_id',
            'snapshot.v._id': '$snapshot.k'}},
        {'$replaceRoot': {'newRoot': '$snapshot.v'}},
        {'$match': {'environment': env, **filters} if env else filters},
        {'$sort': {'created_at': 1}},
        {'$group': {'_id': group_by, 'items': {"$push": "$$ROOT"}}}
    ]))

    return (
        "CleanUps", {},
        [("Group", _expand(doc["_id"], group_by),
          [("Remove", {},
            [("Snapshot", _asattr(_doc, group_by), [])
             for _doc in _remove(doc, keep)]),
           ("Keep", {},
            [("Snapshot", _asattr(_doc, group_by), [])
             for _doc in _keep(doc, keep)])]
          ) for doc in results
         ])

def _expand(doc_id, group_by):
    if isinstance(group_by, str):
        return _asattr({group_by.strip("$"): doc_id})
    if isinstance(group_by, dict):
        return _asattr(doc_id)
    raise TypeError()

def _keep(doc, keep):
    return doc["items"][-keep or len(doc["items"]):]

def _remove(doc, keep):
    return doc["items"][:-keep or len(doc["items"])]

def _asattr(doc, filters=None):
    _doc = {}
    for k, v in doc.items():

        # firstly, remove irrelevant information
        # -----------------------------------------
        # snapshot step result, only indicate step success.
        if k in ("pre", "snapshot", "post"):
            continue
        # this key, if present, is a duplicate in conf key.
        if k == "repository":
            continue
        # no need to repeat filters, they must be the same.
        if isinstance(filters, dict):
            if k in filters:
                continue
        elif isinstance(filters, str):
            if k == filters.strip("$"):
                continue

        # secondly, make special objects concise
        # ------------------------------------------
        if v or not filters:
            if isinstance(v, dict):
                v = "{...}"
            elif v is None:
                v = ""
            else:  # like datetime ...
                v = str(v)
            _doc[k] = v
    return _doc


def to_xml(x):
    x[1]["len"] = str(len(x[2]))
    root = ElementTree.Element(x[0], x[1])
    for _x in x[2]:
        root.append(to_xml(_x))
    return root


def test_find():
    from pymongo import MongoClient
    logging.basicConfig(level="DEBUG")

    # mychem
    # -------
    # "su04"
    # "mychem_hubdb", "src_build"

    client = MongoClient("su06")
    collection = client["outbreak_hubdb"]["src_build"]
    # cleaner = Cleaner(collection, {"local": {"args": {}}}, {})
    obj = find(collection)
    printxml(to_xml(obj))
    # print(obj)
    # return cleaner, obj

def test_print():
    printxml(to_xml((
        "A", {}, [
            ("AA", {}, []),
            ("AB", {"ABC": "D"}, [])
        ]
    )))

def printxml(element):
    import xml.dom.minidom
    dom = xml.dom.minidom.parseString(ElementTree.tostring(element))
    with open("output.xml", "w") as file:
        file.write(dom.toprettyxml(indent=" "*2))
    # print(dom.toprettyxml(indent=" "*2))


if __name__ == '__main__':
    test_find()
