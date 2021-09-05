import logging
import xml.dom.minidom
from typing import NamedTuple
from xml.etree import ElementTree

from elasticsearch import Elasticsearch


class _Ele(NamedTuple):  # Cleanup Element
    tag: str
    attrs: dict
    elems: list

    @classmethod
    def ment(cls, tag, attrs, content):  # _Ele.ment(..) :)
        return _Ele(tag, attrs, [_Ele.ment(*e) for e in content])

    def to_xml(self):
        attrs = self.attrs.copy()

        if self.tag in ("CleanUps", "Remove", "Keep"):
            attrs["size"] = str(len(self.elems))

        if self.tag == "Snapshot":
            attrs = {
                "_id": attrs["_id"],
                "build_name": attrs["build_name"],
                "created_at": str(attrs["created_at"]),
                "env": attrs.get("environment") or "N/A"
            }

        root = ElementTree.Element(self.tag, attrs)
        for elem in self.elems:
            root.append(elem.to_xml())
        return root

    def __str__(self):
        ets = ElementTree.tostring(self.to_xml())
        dom = xml.dom.minidom.parseString(ets)
        return dom.toprettyxml(indent=" "*2)


def find(collection, env=None, keep=3, group_by=None, **filters):
    if isinstance(group_by, (str, type(None))):
        group_by = "$" + (group_by or "build_config")
    elif isinstance(group_by, (list, tuple)):
        group_by = {k.replace('.', '_'): "$" + k for k in group_by}

    groups = list(collection.aggregate([
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

    return _Ele.ment(
        "CleanUps", {},
        [("Group", _expand(group["_id"], group_by),
          [("Remove", {},
            [("Snapshot", _doc, [])
             for _doc in _remove(group, keep)]),
           ("Keep", {},
            [("Snapshot", _doc, [])
             for _doc in _keep(group, keep)])]
          ) for group in groups
         ])

def _expand(group_id, group_by):
    if isinstance(group_id, str):
        return {group_by.strip("$"): group_id}
    if isinstance(group_id, dict):
        return group_id
    raise TypeError()

def _keep(doc, keep):
    return doc["items"][-keep or len(doc["items"]):]

def _remove(doc, keep):
    return doc["items"][:-keep or len(doc["items"])]


# the operations below are not made async
# because SnapshotEnv.client is not async

def delete(collection, element, envs):
    cnt = 0
    assert element.tag == "CleanUps"
    for group in element.elems:
        for catagory in group.elems:
            if catagory.tag == "Remove":
                for snapshot in catagory.elems:
                    _delete(collection, snapshot, envs)
                    cnt += 1
    return cnt


def _delete(collection, snapshot, envs):
    assert snapshot.tag == "Snapshot"

    if "environment" in snapshot.attrs:
        env = snapshot.attrs["environment"]
        client = envs[env].client
    else:  # legacy format
        env = snapshot.attrs["conf"]["indexer"]["env"]
        env = envs.index_manager[env]
        client = Elasticsearch(**env["args"])

    client.snapshot.delete(
        snapshot.attrs["conf"]["repository"]["name"],
        snapshot.attrs["_id"])

    collection.update_one(
        {"_id": snapshot.attrs["build_name"]},
        {"$unset": {f"snapshot.{snapshot.attrs['_id']}": 1}}
    )

def test_find():
    from pymongo import MongoClient
    logging.basicConfig(level="DEBUG")

    # mychem
    # -------
    # "su04"
    # "mychem_hubdb", "src_build"

    client = MongoClient("su06")
    collection = client["outbreak_hubdb"]["src_build"]

    print(find(collection))


def test_print():
    print(_Ele.ment(
        "A", {}, [
            ("AA", {}, []),
            ("AB", {"ABC": "D"}, [])
        ]
    ))


if __name__ == '__main__':
    test_find()
