import datetime
from types import SimpleNamespace

import mongomock
import pytest

from biothings.hub import overview
from biothings.hub.autoupdate import BiothingsDumper, BiothingsUploader
from biothings.utils.redact import REDACTED

CREATED = datetime.datetime(2026, 10, 1, 8, 0)
SNAPSHOT_CONF = {
    "cloud": {"type": "aws", "access_key": "AKIA123", "secret_key": "s3cr3t", "region": "us-west-2"},
    "repository": {
        "name": "disease_repository",
        "type": "s3",
        "settings": {"bucket": "biothings-es8-snapshots", "region": "us-west-2", "base_path": "mydisease.info"},
    },
}


@pytest.fixture
def hub_db(monkeypatch):
    database = mongomock.MongoClient()["hubdb"]
    monkeypatch.setattr(overview, "get_src_build", lambda: database["src_build"])
    monkeypatch.setattr(overview, "get_src_build_config", lambda: database["src_build_config"])
    monkeypatch.setattr(overview, "get_src_dump", lambda: database["src_dump"])
    return database


def test_build_summary(hub_db):
    hub_db["src_build"].insert_one(
        {
            "_id": "disease_20261001_ilfkgzsv",
            "build_config": {"_id": "disease", "doc_type": "disease", "sources": ["mondo", "hpo"]},
            "status": "success",
            "started_at": CREATED,
            "_meta": {
                "build_version": "20261001",
                "stats": {"total": 1234},
                "src": {"mondo": {"version": "2026-09-01", "stats": {"mondo": 1000}}, "hpo": {"version": "2026-08-01"}},
            },
            "mapping": {"mondo": {"properties": {}}},  # bulky, left out
            "jobs": [
                {"step": "merge", "status": "success", "time": "1m", "step_started_at": CREATED},
                {"step": "index", "status": "failed", "err": "Traceback (most recent call last):\nValueError: boom\n"},
                {
                    "step": "pre-snapshot",
                    "status": "failed",
                    "detail": [{"error": "repository_verification_exception"}],
                },
            ],
            "index": {"disease_20261001_ilfkgzsv": {"environment": "local", "count": 1234, "created_at": CREATED}},
            "snapshot": {
                "disease_20261001_ilfkgzsv": {
                    "environment": "s3_es8",
                    "index_name": "disease_20261001_ilfkgzsv",
                    "created_at": CREATED,
                    "conf": SNAPSHOT_CONF,
                }
            },
            "release_note": {
                "disease_20260901_1eekoufo": {
                    "changes": {"new": {"_version": "20261001"}},
                    "release_folder": "/data/releases/disease_20260901_1eekoufo-disease_20261001_ilfkgzsv",
                }
            },
            "publish": {
                "full": {
                    "disease_20261001_ilfkgzsv": {
                        "conf": {"cloud": SNAPSHOT_CONF["cloud"]},
                        "metadata": {"url": "https://biothings-releases.s3.amazonaws.com/mydisease.info/20261001.json"},
                    },
                    "created_at": CREATED,  # when it was last published
                }
            },
            "pending": ["publish"],
        }
    )
    summary = overview.build_summary("disease_20261001_ilfkgzsv")
    assert summary["build_config"] == "disease"
    assert summary["version"] == "20261001"
    assert summary["documents"] == 1234
    assert summary["sources"] == {"mondo": "2026-09-01", "hpo": "2026-08-01"}
    assert summary["steps"][0] == {"step": "merge", "status": "success", "time": "1m", "started_at": CREATED}
    assert summary["steps"][1] == {"step": "index", "status": "failed", "error": "ValueError: boom"}
    assert summary["steps"][2]["error"] == [{"error": "repository_verification_exception"}]
    assert summary["index"] == {
        "disease_20261001_ilfkgzsv": {"environment": "local", "count": 1234, "created_at": CREATED}
    }
    assert summary["snapshot"]["disease_20261001_ilfkgzsv"] == {
        "environment": "s3_es8",
        "index": "disease_20261001_ilfkgzsv",
        "created_at": CREATED,
        "repository": {
            "name": "disease_repository",
            "type": "s3",
            "bucket": "biothings-es8-snapshots",
            "region": "us-west-2",
            "base_path": "mydisease.info",
        },
    }
    assert summary["release_note"] == {
        "disease_20260901_1eekoufo": {"folder": "/data/releases/disease_20260901_1eekoufo-disease_20261001_ilfkgzsv"}
    }
    assert summary["publish"] == {
        "full": {
            "disease_20261001_ilfkgzsv": {
                "metadata": {"url": "https://biothings-releases.s3.amazonaws.com/mydisease.info/20261001.json"}
            },
            "created_at": CREATED,
        }
    }
    assert summary["pending"] == ["publish"]
    assert "mapping" not in summary and "diff" not in summary  # empty: left out
    assert "s3cr3t" not in str(summary) and "AKIA123" not in str(summary)
    with pytest.raises(ValueError, match="No build named 'nope'"):
        overview.build_summary("nope")


def test_build_config(hub_db):
    hub_db["src_build_config"].insert_many(
        [
            {"_id": "disease", "doc_type": "disease", "sources": ["mondo", "hpo"], "root": ["mondo"]},
            {"_id": "test", "doc_type": "disease", "sources": ["hpo"], "token": "abc"},
        ]
    )
    hub_db["src_build"].insert_many(
        [
            {"_id": "disease_2", "build_config": {"_id": "disease"}},
            {"_id": "disease_1", "build_config": {"_id": "disease"}},
            {"_id": "disease_0", "build_config": {"_id": "disease"}, "archived": CREATED},
        ]
    )
    disease = overview.build_config("disease")
    assert disease["sources"] == ["mondo", "hpo"]
    assert disease["builds"] == ["disease_1", "disease_2"]  # not the archived ones
    configs = overview.build_config()
    assert sorted(configs) == ["disease", "test"]
    assert configs["test"]["builds"] == [] and configs["test"]["token"] == REDACTED
    with pytest.raises(ValueError, match="No build configuration named 'nope'"):
        overview.build_config("nope")


def manager(**register):
    return SimpleNamespace(register=register)


class ReleaseDumper(BiothingsDumper):
    pass


class ReleaseUploader(BiothingsUploader):
    pass


def test_data_sources():
    dump_manager = manager(mondo=[object], hpo=[object], __application=[object], **{"disease__es8": [ReleaseDumper]})
    upload_manager = manager(mondo=[object], hpo_phenotype=[object], **{"disease__es8": [ReleaseUploader]})
    assert overview.data_sources(dump_manager, upload_manager) == ["hpo", "hpo_phenotype", "mondo"]
    assert overview.data_sources() == []


def test_source_summary(hub_db):
    hub_db["src_dump"].insert_many(
        [
            {
                "_id": "mondo",
                "download": {"release": "2026-09-01", "status": "success", "started_at": CREATED},
                "upload": {
                    "jobs": {"mondo": {"status": "success", "count": 1000, "started_at": CREATED, "step": "mondo"}}
                },
            },
            {
                "_id": "hpo",
                "download": {
                    "status": "failed",
                    "started_at": CREATED,
                    "error": "Traceback...\nConnectionError: https://user:pass@example.com unreachable",
                },
            },
            {"_id": "disease__es8", "download": {"status": "success"}},  # not a data source
        ]
    )
    summary = overview.source_summary(manager(mondo=[object], hpo=[object], ctd=[object]), manager(mondo=[object]))
    assert summary == {
        "ctd": {},  # never downloaded
        "hpo": {
            "download": {
                "status": "failed",
                "at": CREATED,
                "error": "ConnectionError: https://%s@example.com unreachable" % REDACTED,
            }
        },
        "mondo": {
            "release": "2026-09-01",
            "download": {"status": "success", "at": CREATED},
            "upload": {"mondo": {"status": "success", "documents": 1000, "at": CREATED}},
        },
    }


def test_envs(monkeypatch):
    monkeypatch.setattr(
        overview,
        "config",
        SimpleNamespace(
            INDEX_CONFIG={"env": {"su12_es8": {"host": "http://su12:9200", "indexer": {"args": {"http_auth": "u:p"}}}}},
            SNAPSHOT_CONFIG={
                "env": {
                    "s3_es8": {
                        "cloud": SNAPSHOT_CONF["cloud"],
                        "repository": SNAPSHOT_CONF["repository"],
                        "indexer": {"env": "su12_es8"},
                    }
                }
            },
            RELEASE_CONFIG={
                "env": {
                    "s3_mydisease": {
                        "cloud": SNAPSHOT_CONF["cloud"],
                        "release": {"bucket": "biothings-releases", "folder": "mydisease.info", "auto": True},
                        "diff": {"bucket": "biothings-diffs", "folder": "mydisease.info"},
                    }
                }
            },
        ),
    )
    summary = overview.envs(release_installers=lambda: [{"name": "mydisease-disease"}])
    assert summary == {
        "index": {"su12_es8": {"host": "http://su12:9200"}},
        "snapshot": {
            "s3_es8": {
                "repository": {
                    "name": "disease_repository",
                    "type": "s3",
                    "bucket": "biothings-es8-snapshots",
                    "region": "us-west-2",
                    "base_path": "mydisease.info",
                },
                "index_env": "su12_es8",
            }
        },
        "release": {
            "s3_mydisease": {
                "release": {"bucket": "biothings-releases", "folder": "mydisease.info"},
                "diff": {"bucket": "biothings-diffs", "folder": "mydisease.info"},
            }
        },
        "release_installers": [{"name": "mydisease-disease"}],
    }
    monkeypatch.setattr(overview, "config", SimpleNamespace())
    assert overview.envs() == {"index": {}, "snapshot": {}, "release": {}}
