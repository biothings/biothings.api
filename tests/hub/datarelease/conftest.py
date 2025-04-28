from pathlib import Path
import copy
import logging
import sys

import pytest
from pytest_mock import MockerFixture


logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def releasenote_configuration(root_configuration: "TestConfig"):
    releasenote_configuration = {
        "HUB_DB_BACKEND": {"module": "biothings.utils.sqlite3", "sqlite_db_folder": "./dummy_db"},
        "DATA_HUB_DB_DATABASE": "mock_releasenote.hubdb",
    }
    root_configuration.override(releasenote_configuration)

    prior_config = sys.modules.get("config", None)
    prior_biothings_config = sys.modules.get("biothings.config", None)

    sys.modules["config"] = root_configuration
    sys.modules["biothings.config"] = root_configuration

    yield root_configuration

    root_configuration.reset()
    sys.modules["config"] = prior_config
    sys.modules["biothings.config"] = prior_biothings_config

    hub_db_folder = Path(releasenote_configuration["HUB_DB_BACKEND"].get("sqlite_db_folder", ".")).resolve().absolute()
    hub_db_filepath = hub_db_folder.joinpath(releasenote_configuration["DATA_HUB_DB_DATABASE"])

    hub_db_filepath.unlink(missing_ok=True)
    logger.info("%s unlinked", hub_db_filepath)

    hub_db_folder.rmdir()
    logger.info("%s deleted", hub_db_folder)


@pytest.fixture
def cold_src_build_doc():
    src_build_doc = {
        "_id": "warm_hg19_20220127_fpgrwjzd",
        # "build_config":{
        #     "cold_collection" : ""  # should not exist
        # },
        "_meta": {
            "build_version": "20220127",
            "stats": {
                "hg19": 1367771989,
                "vcf": 1369768159,
                "observed": 1251513292,
                "total": 1414567901,
            },
            "src": {
                "cosmic": {
                    "version": "68",
                    "stats": {"cosmic": 1024494},
                },
            },
        },
        "merge_stats": {
            "cosmic": 1024494,
        },
        "mapping": {
            "cosmic": {
                "properties": {
                    "chrom": {"type": "text", "analyzer": "string_lowercase"},
                    "hg19": {
                        "properties": {
                            "start": {"type": "integer"},
                            "end": {"type": "integer"},
                        }
                    },
                    "tumor_site": {"type": "text"},
                    "cosmic_id": {
                        "type": "text",
                        "analyzer": "string_lowercase",
                    },
                    "mut_nt": {
                        "type": "text",
                        "analyzer": "string_lowercase",
                    },
                    "mut_freq": {"type": "float"},
                    "ref": {
                        "type": "text",
                        "analyzer": "string_lowercase",
                    },
                    "alt": {
                        "type": "text",
                        "analyzer": "string_lowercase",
                    },
                }
            },
        },
    }
    return src_build_doc


@pytest.fixture
def hot_src_build_doc():
    src_build_doc = {
        "_id": "superhot_hg19_20220205_0v6out4e",
        "build_config": {"cold_collection": "warm_hg19_20220127_fpgrwjzd"},
        "_meta": {
            "build_version": "20220205",
            "stats": {
                "total": 1420966502,
                "hg19": 1367771989,
                "vcf": 1369768159,
                "observed": 1258678351,
            },
            "src": {
                "geno2mp": {
                    "version": "2021-09-17",
                    "stats": {"geno2mp": 37885228},
                },
                "clinvar": {
                    "version": "2022-01",
                    "stats": {"clinvar_hg19": 1126823},
                },
            },
        },
        "merge_stats": {
            "clinvar_hg19": 1126823,
            "geno2mp": 37885228,
        },
        "mapping": {
            "geno2mp": {
                "properties": {
                    "hpo_count": {
                        "type": "integer",
                    }
                }
            },
            "clinvar": {
                "properties": {
                    "hg19": {
                        "properties": {
                            "start": {"type": "integer"},
                            "end": {"type": "integer"},
                        }
                    },
                    "gene": {
                        "properties": {
                            "symbol": {
                                "type": "text",
                                "analyzer": "string_lowercase",
                                "copy_to": ["all"],
                            },
                            "id": {"type": "long"},
                        }
                    },
                    "type": {
                        "type": "text",
                        "analyzer": "string_lowercase",
                    },
                    "rsid": {
                        "type": "text",
                        "analyzer": "string_lowercase",
                    },
                },
            },
        },
    }
    return src_build_doc


@pytest.fixture
def old_hot_src_build_docs(hot_src_build_doc):
    # Do not use dict(hot_src_build_doc), that's a shallow copy
    src_build_doc = copy.deepcopy(hot_src_build_doc)

    # change the contents on purpose to forge a doc for a previous build

    src_build_doc["_id"] = "superhot_hg19_20210906_uthzjvjk"
    src_build_doc["build_config"]["cold_collection"] = "warm_hg19_20210722_yz5tmmal"
    src_build_doc["_meta"]["build_version"] = "20210906"

    src_build_doc["_meta"]["stats"] = {
        "total": 1420907501,
        "hg19": 1367771989,
        "vcf": 1369768159,
        "observed": 1258602315,
    }

    src_build_doc["_meta"]["src"] = {
        "geno2mp": {
            "version": "2021-01-28",
            "stats": {
                "geno2mp": 37557266,
            },
        },
        "clinvar": {
            "version": "2021-08",
            "stats": {
                "clinvar_hg19": 990233,
            },
        },
    }

    src_build_doc["merge_stats"] = {
        "clinvar_hg19": 990233,
        "geno2mp": 37557266,
    }

    # delete a field
    del src_build_doc["mapping"]["clinvar"]["properties"]["gene"]["properties"]["id"]
    # add a field
    src_build_doc["mapping"]["clinvar"]["properties"]["gene"]["properties"]["gene_id"] = {"type": "long"}
    # modify a field
    src_build_doc["mapping"]["clinvar"]["properties"]["type"]["type"] = "keyword"

    return src_build_doc


@pytest.fixture
def old_cold_src_build_docs(cold_src_build_doc):
    # Do not use dict(cold_src_build_doc), that's a shallow copy
    src_build_doc = copy.deepcopy(cold_src_build_doc)

    # change the contents on purpose to forge a doc for a previous build

    src_build_doc["_id"] = "warm_hg19_20210722_yz5tmmal"
    src_build_doc["_meta"]["build_version"] = "20210722"

    src_build_doc["_meta"]["stats"] = {
        "total": 1414567743,
        "hg19": 1367771989,
        "vcf": 1369768159,
        "observed": 1251513134,
    }

    src_build_doc["_meta"]["src"] = {
        "cosmic": {"version": "67", "stats": {"cosmic": 1000000}},
    }

    src_build_doc["merge_stats"] = {"cosmic": 1000000}

    # delete a field
    del src_build_doc["mapping"]["cosmic"]["properties"]["mut_freq"]
    # add a field
    src_build_doc["mapping"]["cosmic"]["properties"]["mutation_frequency"] = {"type": "float"}
    # modify a field
    src_build_doc["mapping"]["cosmic"]["properties"]["ref"]["type"] = "keyword"

    return src_build_doc


@pytest.fixture
def mock_get_source_fullname(mocker: MockerFixture):
    def mock_fn(col_name):
        source_fullname_map = {
            # build stat names
            "hg19": "hg19",
            "vcf": "vcf",
            "observed": "observed",
            "total": "total",
            # datasource names
            "cosmic": "cosmic",
            "geno2mp": "geno2mp",
            "clinvar_hg19": "clinvar.clinvar_hg19",
        }

        if col_name in source_fullname_map:
            return source_fullname_map[col_name]

        raise ValueError(f"behavior is not mocked when input col_name is {col_name}.")

    # should not be "biothings.utils.hub_db.get_source_fullname", nor "biothings.utils.sqlite3.get_source_fullname"
    return mocker.patch("biothings.hub.datarelease.releasenote.get_source_fullname", mock_fn)


@pytest.fixture
def release_note_source(
    mock_get_source_fullname, cold_src_build_doc, hot_src_build_doc, old_cold_src_build_docs, old_hot_src_build_docs
):
    from biothings.hub.datarelease.releasenote import (
        ReleaseNoteSource,
        ReleaseNoteSrcBuildReader,
    )

    old_src_build_reader = ReleaseNoteSrcBuildReader(old_hot_src_build_docs)
    old_src_build_reader.attach_cold_src_build_reader(ReleaseNoteSrcBuildReader(old_cold_src_build_docs))

    new_src_build_reader = ReleaseNoteSrcBuildReader(hot_src_build_doc)
    new_src_build_reader.attach_cold_src_build_reader(ReleaseNoteSrcBuildReader(cold_src_build_doc))

    diff_stats_from_metadata_file = {"foo": "bar", "baz": "qux"}
    addon_note = "FooBarBazQux"

    source = ReleaseNoteSource(
        old_src_build_reader=old_src_build_reader,
        new_src_build_reader=new_src_build_reader,
        diff_stats_from_metadata_file=diff_stats_from_metadata_file,
        addon_note=addon_note,
    )
    return source
