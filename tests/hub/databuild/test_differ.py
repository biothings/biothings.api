import importlib
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.fixture(scope="module")
def differ():
    with patch("biothings.utils.es.Elasticsearch") as es_class:
        client = es_class.return_value
        client.indices.exists.return_value = True
        client.exists.return_value = True
        client.get.return_value = {"_source": {}}
        yield importlib.import_module("biothings.hub.databuild.differ")


class StubBackend:
    def __init__(self, target_name, existing_ids=()):
        self.target_name = target_name
        self.existing_ids = existing_ids
        self.existing_id_requests = []

    def get_existing_ids(self, ids):
        self.existing_id_requests.append(list(ids))
        return iter(self.existing_ids)

    def mget_from_ids(self, ids, asiter=False):
        raise AssertionError("The worker must not load full documents to classify IDs")


def test_diff_worker_new_vs_old_uses_existing_ids(differ, monkeypatch, tmp_path):
    old = StubBackend("old", existing_ids=["common-b", "common-a", "common-b"])
    new = StubBackend("new")
    backends = {"old": old, "new": new}
    dumped = {}
    diff_call = {}

    monkeypatch.setattr(
        differ,
        "create_backend",
        lambda backend_name, follow_ref: backends[backend_name],
    )

    def diff_func(old_backend, new_backend, ids, exclude_attrs):
        diff_call.update(
            old=old_backend,
            new=new_backend,
            ids=ids,
            exclude_attrs=exclude_attrs,
        )
        return [{"_id": "common-a", "patch": [{"op": "replace"}]}]

    def dump(data, path):
        dumped.update(data)
        Path(path).write_bytes(b"diff")

    monkeypatch.setattr(differ, "dump", dump)
    monkeypatch.setattr(differ, "md5sum", lambda path: "test-md5")

    summary = differ.diff_worker_new_vs_old(
        ["common-a", "new", "common-b", "common-a"],
        "old",
        "new",
        1,
        str(tmp_path),
        diff_func,
        exclude=["ignored"],
    )

    assert old.existing_id_requests == [["common-a", "new", "common-b"]]
    assert diff_call == {
        "old": old,
        "new": new,
        "ids": ["common-a", "common-b"],
        "exclude_attrs": ["ignored"],
    }
    assert dumped["add"] == ["new"]
    assert dumped["update"] == [{"_id": "common-a", "patch": [{"op": "replace"}]}]
    assert summary["add"] == 1
    assert summary["update"] == 1
    assert summary["diff_file"]["md5sum"] == "test-md5"
