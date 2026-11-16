import io
import tarfile

import pytest

from biothings.utils.common import sanitize_tarfile


def _make_tar(member_name):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        info = tarfile.TarInfo(member_name)
        info.size = 1
        tar.addfile(info, io.BytesIO(b"x"))
    buf.seek(0)
    return tarfile.open(fileobj=buf)


def test_sanitize_tarfile_allows_members_inside_folder(tmp_path):
    folder = tmp_path / "data"
    folder.mkdir()
    with _make_tar("sub/file.txt") as tar:
        sanitize_tarfile(tar, str(folder))


def test_sanitize_tarfile_blocks_parent_folder(tmp_path):
    folder = tmp_path / "data"
    folder.mkdir()
    with _make_tar("../file.txt") as tar:
        with pytest.raises(ValueError):
            sanitize_tarfile(tar, str(folder))


def test_sanitize_tarfile_blocks_sibling_folder_with_same_prefix(tmp_path):
    folder = tmp_path / "data"
    folder.mkdir()
    with _make_tar("../data_other/file.txt") as tar:
        with pytest.raises(ValueError):
            sanitize_tarfile(tar, str(folder))
