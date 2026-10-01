"""
Tests for the various dumper classes
"""

import asyncio
import logging
import pathlib
import tempfile

import pytest
import requests

from biothings.hub.autoupdate import BiothingsDumper
from biothings.hub.dataload.dumper import BaseDumper, DumperManager, HTTPDumper
from biothings.hub.dataload.manager import SourcesFailed


def test_base_dumper():
    """
    Builds and tests the BaseDumper class instance
    """
    dumper_instance = BaseDumper()

    # verify default class property values
    assert dumper_instance.SRC_NAME is None
    assert dumper_instance.SRC_ROOT_FOLDER is None
    assert dumper_instance.AUTO_UPLOAD
    assert dumper_instance.SUFFIX_ATTR == "release"
    assert dumper_instance.MAX_PARALLEL_DUMP is None
    assert dumper_instance.SLEEP_BETWEEN_DOWNLOAD == 0.0
    assert dumper_instance.ARCHIVE
    assert dumper_instance.SCHEDULE is None

    # verify the initial dumper state
    assert dumper_instance._state["client"] is None
    assert dumper_instance._state["src_dump"] is None
    assert dumper_instance._state["logger"] is None
    assert dumper_instance._state["src_doc"] is None


def test_http_dumper_properties():
    """
    Builds and tests the HTTPDumper class instance
    """
    dumper_instance = HTTPDumper()

    # verify default class property values
    assert dumper_instance.VERIFY_CERT
    assert dumper_instance.IGNORE_HTTP_CODE == []
    assert not dumper_instance.RESOLVE_FILENAME

    assert isinstance(dumper_instance.client, requests.Session)
    assert dumper_instance.VERIFY_CERT
    assert not dumper_instance.need_prepare()

    # client generation
    dumper_instance.release_client()
    assert dumper_instance._state["client"] is None

    dumper_instance.prepare_client()
    assert isinstance(dumper_instance.client, requests.Session)
    assert dumper_instance.VERIFY_CERT
    assert not dumper_instance.need_prepare()


@pytest.mark.parametrize(
    "remoteurl,resolve_filepath",
    [
        # this URL contains a "content-disposition" header with a filename as "biothings.api-0.12.5.zip"
        # if resolve_filepath is True, the dumper should resolve the filename from this header
        ("https://codeload.github.com/biothings/biothings.api/zip/refs/tags/v0.12.5", True),
        # this URL does not contain a "content-disposition" header, and resolve_filepath is False
        ("https://biothings.io/static/img/transition.svg", False),
    ],
)
def test_http_dumper_download(remoteurl: str, resolve_filepath: bool):
    """
    Tests the download functionality with various valid link
    """
    with tempfile.NamedTemporaryFile() as temp_local_file:
        dumper_instance = HTTPDumper()
        HTTPDumper.RESOLVE_FILENAME = resolve_filepath
        assert dumper_instance.remote_is_better(remoteurl, temp_local_file.name)
        download_headers = {}
        response = dumper_instance.download(
            remoteurl=remoteurl, localfile=temp_local_file.name, headers=download_headers
        )
        assert isinstance(response, requests.models.Response)
        if resolve_filepath:
            assert response.headers.get("content-disposition")
            assert response.headers["content-disposition"].endswith("biothings.api-0.12.5.zip")
            local_file = pathlib.Path(pathlib.Path(temp_local_file.name).parent, "biothings.api-0.12.5.zip")
            assert local_file.exists()
            assert local_file.stat().st_size > 0
        else:
            local_file = pathlib.Path(pathlib.Path(temp_local_file.name))
            assert local_file.exists()
            assert local_file.stat().st_size > 0


@pytest.mark.asyncio
async def test_dump_all_skips_private_dumpers(monkeypatch):
    """
    dump_all() runs data sources' dumpers, not the private ones ("__" names), eg. the code
    upgrade dumpers, which would pull new code for the hub application and the BioThings SDK,
    nor the ones downloading data releases to install (eg. on mydisease's hub, which installs its
    releases in its API's indices)
    """

    class ReleaseDumper(BiothingsDumper):
        pass

    manager = DumperManager(job_manager=None)
    manager.register = {
        "mondo": [],
        "hpo": [],
        "__biothings_sdk": [],
        "__application": [],
        "mydisease-disease__es9": [ReleaseDumper],
    }
    dumped = []
    monkeypatch.setattr(manager, "dump_src", lambda src, **kwargs: dumped.append(src) or [])
    await manager.dump_all()
    assert dumped == ["mondo", "hpo"]


@pytest.mark.asyncio
async def test_dump_all_tells_which_sources_failed(monkeypatch, caplog):
    manager = DumperManager(job_manager=None)
    manager.register = {"mondo": [], "mydisease-disease__es9": [], "disgenet": []}

    async def dumped():
        await asyncio.sleep(0.05)  # still running when the other one fails

    async def failed():
        raise TypeError("string indices must be integers, not 'str'")

    jobs = {"mondo": [dumped], "mydisease-disease__es9": [failed], "disgenet": []}  # disgenet: disabled dumper
    monkeypatch.setattr(manager, "dump_src", lambda src, **kwargs: [asyncio.ensure_future(job()) for job in jobs[src]])
    with caplog.at_level(logging.ERROR), pytest.raises(SourcesFailed) as error:
        await manager.dump_all()
    assert str(error.value) == (
        "dump failed for 1 of 3 sources: mydisease-disease__es9 (TypeError: string indices must be integers, "
        "not 'str'). Done: mondo. Skipped: disgenet"
    )
    assert error.value.summary["mondo"] == "done"  # wasn't stopped by the other failing
    assert "dump failed for mydisease-disease__es9: TypeError" in caplog.text
    assert "(still running: mondo)" in caplog.text
    jobs["mydisease-disease__es9"] = [dumped]
    assert await manager.dump_all() == {"mondo": "done", "mydisease-disease__es9": "done", "disgenet": "skipped"}
