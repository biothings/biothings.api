"""
Publishing a snapshot waits for the build's release note, created in background after the snapshot
"""

import asyncio
import logging
from functools import partial
from types import SimpleNamespace

import mongomock
import pytest

from biothings.hub.datarelease import publisher
from biothings.hub.datarelease.publisher import PublisherException, SnapshotPublisher

BUILD = "disease_20261001_ilfkgzsv"
NOTE = {"changes": {"new": {"_version": "20261001"}}, "release_folder": "/data/releases/previous-new"}


@pytest.fixture
def src_build(monkeypatch):
    collection = mongomock.MongoClient()["hubdb"]["src_build"]
    collection.insert_one({"_id": BUILD, "snapshot": {BUILD: {}}, "pending": ["release_note"]})
    monkeypatch.setattr(publisher, "get_src_build", lambda: collection)
    return collection


def fake_publisher():
    """What SnapshotPublisher.publish() needs from its publisher, until it publishes"""
    calls = []

    async def publish(*args, **kwargs):
        calls.append((args, kwargs))
        return "published"

    fake = SimpleNamespace(
        calls=calls,
        logger=logging.getLogger("test_publish_release_note"),
        load_build=lambda name, stage=None: publisher.get_src_build().find_one({"_id": name}),
        setup_log=lambda name: None,
        template_out_conf=lambda bdoc: {"release": {"bucket": "releases", "folder": "mydisease.info"}},
        job_manager=SimpleNamespace(loop=asyncio.get_running_loop()),
        publish=publish,
    )
    fake.wait_for_release_note = partial(SnapshotPublisher.wait_for_release_note, fake, interval=0.01)
    return fake


@pytest.mark.asyncio
async def test_publish_snapshot_waits_for_the_release_note(src_build):
    fake = fake_publisher()
    asyncio.get_running_loop().call_later(
        0.05, lambda: src_build.update_one({"_id": BUILD}, {"$set": {"release_note": {"previous": NOTE}}})
    )
    task = SnapshotPublisher.publish(fake, BUILD)
    assert not fake.calls  # waiting for the release note
    assert await task == "published"
    assert fake.calls == [((BUILD,), {"build_name": BUILD, "previous_build": None, "steps": ["pre", "meta", "post"]})]


@pytest.mark.asyncio
async def test_wait_for_a_failed_release_note(src_build):
    fake = fake_publisher()
    # once failed, the release note is registered without its changes
    src_build.update_one({"_id": BUILD}, {"$set": {"release_note": {"previous": {"created_at": "2026-10-01"}}}})
    with pytest.raises(
        PublisherException, match="failed .* create it again with create_release_note previous %s" % BUILD
    ):
        await fake.wait_for_release_note(BUILD)


@pytest.mark.asyncio
async def test_wait_for_a_missing_release_note(src_build):
    fake = fake_publisher()
    with pytest.raises(PublisherException, match="create it with create_release_note <previous build> %s" % BUILD):
        await fake.wait_for_release_note(BUILD, timeout=0.05)
