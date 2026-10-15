import tomllib
from pathlib import Path

import pytest

import biothings
from biothings.utils.version import get_biothings_commit

REPO_URL = "https://github.com/biothings/biothings.api.git"
COMMIT_HASH = "45f55504a4074883a950d8385062e16c93e144e9"


@pytest.fixture
def package_dir(tmp_path, monkeypatch):
    """Point get_biothings_commit() at a temporary package directory."""
    monkeypatch.setattr(biothings, "__file__", str(tmp_path / "__init__.py"))
    get_biothings_commit.cache_clear()
    yield tmp_path
    get_biothings_commit.cache_clear()


def test_get_biothings_commit_reads_git_info(package_dir):
    (package_dir / ".git-info").write_text(f"{REPO_URL}\n{COMMIT_HASH}\n3954", encoding="utf-8")

    assert get_biothings_commit() == {
        "repository-url": REPO_URL,
        "commit-hash": COMMIT_HASH,
        "master-commits": "3954",
        "version": biothings.__version__,
    }


def test_get_biothings_commit_without_commit_count(package_dir):
    # what setup.py writes without a local master branch, e.g. in CI tag checkouts
    (package_dir / ".git-info").write_text(f"{REPO_URL}\n{COMMIT_HASH}\n", encoding="utf-8")

    commit = get_biothings_commit()

    assert commit["repository-url"] == REPO_URL
    assert commit["commit-hash"] == COMMIT_HASH
    assert commit["master-commits"] == ""


def test_get_biothings_commit_without_git_info(package_dir):
    assert get_biothings_commit() == {
        "repository-url": "",
        "commit-hash": "",
        "master-commits": "",
        "version": biothings.__version__,
    }


def test_git_info_is_package_data():
    """setup.py writes biothings/.git-info at build time, but it only ships if package-data lists it."""
    pyproject = tomllib.loads((Path(__file__).parents[2] / "pyproject.toml").read_text(encoding="utf-8"))

    assert ".git-info" in pyproject["tool"]["setuptools"]["package-data"]["*"]
