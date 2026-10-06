"""
A merge needs all the sources of its build configuration uploaded successfully: when they aren't, it
tells which ones at once, and why ("merge <configuration> --check" checks without merging)
"""

from functools import partial
from types import SimpleNamespace

import mongomock
import pytest

from biothings.hub.databuild import builder
from biothings.hub.databuild.builder import BuilderException, BuilderManager, DataBuilder
from biothings.hub.dataload.uploader import ResourceNotReady


@pytest.fixture
def fake_builder(monkeypatch):
    database = mongomock.MongoClient()["hubdb"]
    database["src_build_config"].insert_one(
        {"_id": "disease", "sources": ["mondo", "hpo", "umls", "ctd", "disgenet", "nope"]}
    )
    database["src_dump"].insert_many(
        [
            {"_id": "mondo", "upload": {"jobs": {"mondo": {"status": "success"}}}},
            {"_id": "hpo", "upload": {"jobs": {"hpo": {"status": "uploading"}}}},
            {
                "_id": "umls",
                "upload": {"jobs": {"umls": {"status": "failed", "err": "Traceback...\nHTTPError: 500 Server Error"}}},
            },
            {"_id": "ctd", "download": {"status": "success"}},  # downloaded, never uploaded
        ]
    )
    # main source of an uploaded collection (None: never uploaded, or unknown)
    fullnames = {"mondo": "mondo", "hpo": "hpo", "umls": "umls", "ctd": "ctd", "disgenet": "disgenet"}
    monkeypatch.setattr(builder, "get_source_fullname", fullnames.get)
    fake = SimpleNamespace(
        build_config={"name": "disease"},
        source_backend=SimpleNamespace(build_config=database["src_build_config"], dump=database["src_dump"]),
    )
    fake.unready_sources = partial(DataBuilder.unready_sources, fake)
    fake.check_ready = partial(DataBuilder.check_ready, fake)
    return fake


def test_unready_sources(fake_builder):
    assert fake_builder.unready_sources() == {
        "hpo": "being uploaded",
        "umls": "its last upload failed: HTTPError: 500 Server Error",
        "ctd": "never uploaded",
        "disgenet": "never downloaded nor uploaded",
        "nope": "unknown source",
    }
    with pytest.raises(ResourceNotReady) as error:  # all of them at once
        fake_builder.check_ready()
    assert str(error.value).startswith("hpo (being uploaded); umls (its last upload failed: HTTPError: 500")
    fake_builder.check_ready(force=True)  # merges anyway


def test_merge_check(fake_builder):
    class Manager:
        src_build_config = fake_builder.source_backend.build_config
        list_sources = BuilderManager.list_sources

        def __getitem__(self, build_name):
            return fake_builder

    with pytest.raises(BuilderException, match=r"aren't ready for the merge: hpo \(being uploaded\); umls"):
        BuilderManager.merge(Manager(), "disease", check=True)
    fake_builder.source_backend.build_config.update_one({"_id": "disease"}, {"$set": {"sources": ["mondo"]}})
    assert BuilderManager.merge(Manager(), "disease", check=True) == "Ready to merge 'disease': mondo"
