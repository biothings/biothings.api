"""
Tests for the various dumper classes
"""

import tempfile

import pytest
import requests

from biothings.hub.dataload.dumper import BaseDumper, HTTPDumper


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
        ("https://github.com/biothings/biothings.api/archive/refs/tags/v0.12.5.zip", True),
        ("https://www.gnu.org/software/gzip/manual/gzip.html.gz", False),
    ],
)
def test_http_dumper_download(remoteurl: str, resolve_filepath: bool):
    """
    Tests the download functionality with various valid link
    """
    with tempfile.NamedTemporaryFile() as temp_local_file:
        dumper_instance = HTTPDumper()
        HTTPDumper.RESOLVE_FILENAME = resolve_filepath
        assert dumper_instance.remote_is_better(remoteurl, temp_local_file)
        download_headers = {}
        response = dumper_instance.download(
            remoteurl=remoteurl, localfile=temp_local_file.name, headers=download_headers
        )
        assert isinstance(response, requests.models.Response)
