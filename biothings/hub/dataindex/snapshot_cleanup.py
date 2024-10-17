import logging
import xml.dom.minidom
from typing import NamedTuple
from xml.etree import ElementTree

from elasticsearch import Elasticsearch
from pymongo.collection import Collection
from config import logger as logging

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
                "env": attrs.get("environment") or "N/A",
            }

        root = ElementTree.Element(self.tag, attrs)
        for elem in self.elems:
            root.append(elem.to_xml())
        return root

    def __str__(self):
        ets = ElementTree.tostring(self.to_xml())
        dom = xml.dom.minidom.parseString(ets)
        return dom.toprettyxml(indent=" " * 2)


def find(collection, *, env=None, keep=3, group_by=None, return_db_cols=False, **filters):
    if not isinstance(collection, Collection):
        raise NotImplementedError("Require MongoDB Hubdb.")

    if isinstance(group_by, (str, type(None))):
        group_by = "$" + (group_by or "build_config")
    elif isinstance(group_by, (list, tuple)):
        group_by = {k.replace(".", "_"): "$" + k for k in group_by}

    groups = list(
        collection.aggregate(
            [
                {
                    "$project": {
                        "build_config": "$build_config._id",
                        "snapshot": {"$objectToArray": "$snapshot"},
                    }
                },
                {"$unwind": {"path": "$snapshot"}},
                {
                    "$addFields": {
                        "snapshot.v.build_config": "$build_config",
                        "snapshot.v.build_name": "$_id",
                        "snapshot.v._id": "$snapshot.k",
                    }
                },
                {"$replaceRoot": {"newRoot": "$snapshot.v"}},
                {"$match": {"environment": env, **filters} if env else filters},
                # Exclude cloud credentials
                {"$unset": ["conf.cloud.access_key", "conf.cloud.secret_key"]},
                {"$sort": {"created_at": 1}},
                {"$group": {"_id": group_by, "items": {"$push": "$$ROOT"}}},
            ]
        )
    )

    if return_db_cols:
        return groups

    return _Ele.ment(
        "CleanUps",
        {},
        [
            (
                "Group",
                _expand(group["_id"], group_by),
                [
                    ("Remove", {}, [("Snapshot", _doc, []) for _doc in _remove(group, keep)]),
                    ("Keep", {}, [("Snapshot", _doc, []) for _doc in _keep(group, keep)]),
                ],
            )
            for group in groups
        ],
    )


def _expand(group_id, group_by):
    if isinstance(group_id, str):
        return {group_by.strip("$"): group_id}
    if isinstance(group_id, dict):
        return group_id
    raise TypeError()


def _keep(doc, keep):
    return doc["items"][-keep or len(doc["items"]) :]


def _remove(doc, keep):
    return doc["items"][: -keep or len(doc["items"])]


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
        try:
            env = envs.index_manager[env]
        except KeyError:
            raise ValueError(f"Environment '{env}' is not registered and connection details are unavailable. Manual deletion is required.")
        client = Elasticsearch(**env["args"])

    client.snapshot.delete(
        repository=snapshot.attrs["conf"]["repository"]["name"],
        snapshot=snapshot.attrs["_id"],
    )

    collection.update_one(
        {"_id": snapshot.attrs["build_name"]},
        {"$unset": {f"snapshot.{snapshot.attrs['_id']}": 1}},
    )


def plain_text(element):
    plain_texts = []
    assert element.tag == "CleanUps"
    for group in element.elems:
        assert group.tag == "Group"

        plain_texts.append("Snapshots filtered by:")
        for k, v in group.attrs.items():
            plain_texts.append(f"      {k}={repr(v)}")
        plain_texts.append("")

        removes = group.elems[0].elems
        plain_texts.append(f"    Found {len(removes)} snapshots to remove:")
        for snapshot in removes:
            plain_texts.append(" " * 8 + _plain_text(snapshot))

        keeps = group.elems[1].elems
        plain_texts.append(f"    Found {len(keeps)} snapshots to keep:")
        for snapshot in keeps:
            plain_texts.append(" " * 8 + _plain_text(snapshot))
        plain_texts.append("")

    return "\n".join(plain_texts)


def _plain_text(snapshot):
    assert snapshot.tag == "Snapshot"
    return "".join(
        (
            snapshot.attrs["_id"],
            " (",
            f'env={snapshot.attrs.get("environment") or "N/A"}',
            ", "
            #
            # "build_name" generally agrees with the snapshot _id,
            # although technically snapshots can be named anything.
            # since in most use cases, the snapshot name at least
            # indicates which build it is created from, even when
            # it is not named exactly the same as the build, for
            # presentation concision, build_name is not shown here.
            # uncomment the following line if this assumption is
            # no longer true in the future.
            #
            # f'build_name={repr(snapshot.attrs["build_name"])}', ", ",
            f'created_at={str(snapshot.attrs["created_at"])}',
            ")",
        )
    )


# Feature Specification ↑
# https://suwulab.slack.com/archives/CC19LHAF2/p1631126588023700?thread_ts=1631063247.003700&cid=CC19LHAF2

# Snapshots filtered by:
#       build_config="demo_allspecies"
#        ...

#    Found 8 snapshots to remove:
#        ...
#    Found 3 snapshots to keep:
#        ...


def test_find():
    from pymongo import MongoClient

    logging.basicConfig(level="DEBUG")

    # mychem
    # -------
    # "su04"
    # "mychem_hubdb", "src_build"

    client = MongoClient("su06")
    collection = client["outbreak_hubdb"]["src_build"]

    print(plain_text(find(collection)))


def test_print():
    print(
        _Ele.ment(
            "A",
            {},
            [
                ("AA", {}, []),
                ("AB", {"ABC": "D"}, []),
            ],
        )
    )


if __name__ == "__main__":
    test_find()
