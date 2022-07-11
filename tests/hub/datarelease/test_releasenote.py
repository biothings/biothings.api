from copy import deepcopy
import pytest
from pytest_mock import MockerFixture
from biothings.hub.datarelease.releasenote import ReleaseNoteSrcBuildReader, ReleaseNoteSrcBuildReaderAdapter, \
    ReleaseNoteSource, ReleaseNoteTxt


pytestmark = pytest.mark.skipif(True, reason="Skip this test_releasenote module till it's fixed")


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
                "total": 1414567901
            },
            "src": {
                "cosmic": {
                    "version": "68",
                    "stats": {
                        "cosmic": 1024494
                    }
                },
            },
        },
        "merge_stats": {
            "cosmic": 1024494,
        },
        "mapping": {
            "cosmic": {
                "properties": {
                    "chrom": {
                        "type": "text",
                        "analyzer": "string_lowercase"
                    },
                    "hg19": {
                        "properties": {
                            "start": {
                                "type": "integer"
                            },
                            "end": {
                                "type": "integer"
                            }
                        }
                    },
                    "tumor_site": {
                        "type": "text"
                    },
                    "cosmic_id": {
                        "type": "text",
                        "analyzer": "string_lowercase"
                    },
                    "mut_nt": {
                        "type": "text",
                        "analyzer": "string_lowercase"
                    },
                    "mut_freq": {
                        "type": "float"
                    },
                    "ref": {
                        "type": "text",
                        "analyzer": "string_lowercase"
                    },
                    "alt": {
                        "type": "text",
                        "analyzer": "string_lowercase"
                    }
                }
            },
        }
    }
    return src_build_doc


@pytest.fixture
def hot_src_build_doc():
    src_build_doc = {
        "_id": "superhot_hg19_20220205_0v6out4e",
        "build_config": {
            "cold_collection": "warm_hg19_20220127_fpgrwjzd"
        },
        "_meta": {
            "build_version": "20220205",
            "stats": {
                "total": 1420966502,
                "hg19": 1367771989,
                "vcf": 1369768159,
                "observed": 1258678351
            },
            "src": {
                "geno2mp": {
                    "version": "2021-09-17",
                    "stats": {
                        "geno2mp": 37885228
                    }
                },
                "clinvar": {
                    "version": "2022-01",
                    "stats": {
                        "clinvar_hg19": 1126823
                    }
                },
            }
        },
        "merge_stats": {
            "clinvar_hg19": 1126823,
            "geno2mp": 37885228
        },
        "mapping": {
            "geno2mp": {
                "properties": {
                    "hpo_count": {
                        "type": "integer"
                    }
                }
            },
            "clinvar": {
                "properties": {
                    "hg19": {
                        "properties": {
                            "start": {
                                "type": "integer"
                            },
                            "end": {
                                "type": "integer"
                            }
                        }
                    },
                    "gene": {
                        "properties": {
                            "symbol": {
                                "type": "text",
                                "analyzer": "string_lowercase",
                                "copy_to": [
                                    "all"
                                ]
                            },
                            "id": {
                                "type": "long"
                            }
                        }
                    },
                    "type": {
                        "type": "text",
                        "analyzer": "string_lowercase"
                    },
                    "rsid": {
                        "type": "text",
                        "analyzer": "string_lowercase"
                    },
                },
            },
        }
    }
    return src_build_doc


@pytest.fixture
def old_hot_src_build_docs(hot_src_build_doc):
    # Do not use dict(hot_src_build_doc), that's a shallow copy
    src_build_doc = deepcopy(hot_src_build_doc)

    # change the contents on purpose to forge a doc for a previous build

    src_build_doc["_id"] = "superhot_hg19_20210906_uthzjvjk"
    src_build_doc["build_config"]["cold_collection"] = "warm_hg19_20210722_yz5tmmal"
    src_build_doc["_meta"]["build_version"] = "20210906"

    src_build_doc["_meta"]["stats"] = {
        "total": 1420907501,
        "hg19": 1367771989,
        "vcf": 1369768159,
        "observed": 1258602315
    }

    src_build_doc["_meta"]["src"] = {
        "geno2mp": {
            "version": "2021-01-28",
            "stats": {
                "geno2mp": 37557266
            }
        },
        "clinvar": {
            "version": "2021-08",
            "stats": {
                "clinvar_hg19": 990233
            }
        },
    }

    src_build_doc["merge_stats"] = {
        "clinvar_hg19": 990233,
        "geno2mp": 37557266
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
    src_build_doc = deepcopy(cold_src_build_doc)

    # change the contents on purpose to forge a doc for a previous build

    src_build_doc["_id"] = "warm_hg19_20210722_yz5tmmal"
    src_build_doc["_meta"]["build_version"] = "20210722"

    src_build_doc["_meta"]["stats"] = {
        "total": 1414567743,
        "hg19": 1367771989,
        "vcf": 1369768159,
        "observed": 1251513134
    }

    src_build_doc["_meta"]["src"] = {
        "cosmic": {
            "version": "67",
            "stats": {
                "cosmic": 1000000
            }
        },
    }

    src_build_doc["merge_stats"] = {
        "cosmic": 1000000
    }

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
            "clinvar_hg19": "clinvar.clinvar_hg19"
        }

        if col_name in source_fullname_map:
            return source_fullname_map[col_name]

        raise ValueError(f"behavior is not mocked when input col_name is {col_name}.")

    # should not be "biothings.utils.hub_db.get_source_fullname", nor "biothings.utils.sqlite3.get_source_fullname"
    return mocker.patch("biothings.hub.datarelease.releasenote.get_source_fullname", mock_fn)


@pytest.mark.ReleaseNoteSrcBuildReader
def test_read_cold_src_build(cold_src_build_doc):
    cold_doc = cold_src_build_doc
    cold_reader = ReleaseNoteSrcBuildReader(cold_doc)

    assert cold_reader.build_id == cold_doc["_id"]
    assert cold_reader.build_version == cold_doc["_meta"]["build_version"]
    assert cold_reader.build_stats == cold_doc["_meta"]["stats"]

    assert cold_reader.cold_src_build_reader is None
    assert cold_reader.cold_collection_name is None
    with pytest.raises(ValueError):
        cold_reader.attach_cold_src_build_reader(ReleaseNoteSrcBuildReader(cold_src_build_doc))

    assert cold_reader.datasource_versions == {"cosmic": "68"}
    assert cold_reader.datasource_stats == cold_doc["merge_stats"]
    assert cold_reader.datasource_mapping == cold_doc["mapping"]


@pytest.mark.ReleaseNoteSrcBuildReader
def test_read_hot_src_build(hot_src_build_doc, cold_src_build_doc):
    hot_doc = hot_src_build_doc
    hot_reader = ReleaseNoteSrcBuildReader(hot_doc)

    cold_doc = cold_src_build_doc
    cold_reader = ReleaseNoteSrcBuildReader(cold_doc)

    hot_reader.attach_cold_src_build_reader(cold_reader)

    assert hot_reader.build_id == hot_doc["_id"]
    assert hot_reader.build_version == hot_doc["_meta"]["build_version"]
    assert hot_reader.build_stats == hot_doc["_meta"]["stats"]

    assert hot_reader.cold_src_build_reader is not None
    assert hot_reader.cold_collection_name == hot_doc["build_config"]["cold_collection"]

    assert hot_reader.datasource_versions == {'clinvar': hot_doc["_meta"]["src"]["clinvar"]["version"],
                                              "cosmic": cold_doc["_meta"]["src"]["cosmic"]["version"],
                                              "geno2mp": hot_doc["_meta"]["src"]["geno2mp"]["version"]}
    assert hot_reader.datasource_stats == {**hot_doc["merge_stats"], **cold_doc["merge_stats"]}
    assert hot_reader.datasource_mapping == {**hot_doc["mapping"], **cold_doc["mapping"]}


@pytest.mark.ReleaseNoteSrcBuildReaderAdapter
def test_read_cold_datasource_info(mock_get_source_fullname, cold_src_build_doc):
    cold_doc = cold_src_build_doc
    cold_reader = ReleaseNoteSrcBuildReader(cold_doc)
    cold_adapter = ReleaseNoteSrcBuildReaderAdapter(cold_reader)

    info = cold_adapter.datasource_info
    assert len(info) == 1
    assert info["cosmic"] == {'_version': cold_doc["_meta"]["src"]["cosmic"]["version"],
                              '_count': cold_doc["merge_stats"]["cosmic"]}


@pytest.mark.ReleaseNoteSrcBuildReaderAdapter
def test_read_hot_datasource_info(mock_get_source_fullname, cold_src_build_doc, hot_src_build_doc):
    hot_doc = hot_src_build_doc
    cold_doc = cold_src_build_doc

    hot_reader = ReleaseNoteSrcBuildReader(hot_doc)
    cold_reader = ReleaseNoteSrcBuildReader(cold_doc)
    hot_reader.attach_cold_src_build_reader(cold_reader)

    hot_adapter = ReleaseNoteSrcBuildReaderAdapter(hot_reader)

    info = hot_adapter.datasource_info
    assert len(info) == 3
    assert info["geno2mp"] == {'_version': hot_doc["_meta"]["src"]["geno2mp"]["version"],
                               '_count': hot_doc["merge_stats"]["geno2mp"]}
    assert info["cosmic"] == {'_version': cold_doc["_meta"]["src"]["cosmic"]["version"],
                              '_count': cold_doc["merge_stats"]["cosmic"]}
    assert info["clinvar"] == {'_version': hot_doc["_meta"]["src"]["clinvar"]["version"],
                               'clinvar_hg19': {'_count': hot_doc["merge_stats"]["clinvar_hg19"]}}


@pytest.mark.ReleaseNoteSrcBuildReaderAdapter
def test_read_hot_build_stats(mock_get_source_fullname, cold_src_build_doc, hot_src_build_doc):
    hot_doc = hot_src_build_doc
    cold_doc = cold_src_build_doc

    hot_reader = ReleaseNoteSrcBuildReader(hot_doc)
    cold_reader = ReleaseNoteSrcBuildReader(cold_doc)
    hot_reader.attach_cold_src_build_reader(cold_reader)

    hot_adapter = ReleaseNoteSrcBuildReaderAdapter(hot_reader)

    stats = hot_adapter.build_stats

    assert len(stats) == 4
    assert stats["total"] == {'_count': hot_doc["_meta"]["stats"]["total"]}
    assert stats["hg19"] == {'_count': hot_doc["_meta"]["stats"]["hg19"]}
    assert stats["vcf"] == {'_count': hot_doc["_meta"]["stats"]["vcf"]}
    assert stats["observed"] == {'_count': hot_doc["_meta"]["stats"]["observed"]}


@pytest.fixture
def release_note_source(mock_get_source_fullname, cold_src_build_doc, hot_src_build_doc,
                        old_cold_src_build_docs, old_hot_src_build_docs):
    old_src_build_reader = ReleaseNoteSrcBuildReader(old_hot_src_build_docs)
    old_src_build_reader.attach_cold_src_build_reader(ReleaseNoteSrcBuildReader(old_cold_src_build_docs))

    new_src_build_reader = ReleaseNoteSrcBuildReader(hot_src_build_doc)
    new_src_build_reader.attach_cold_src_build_reader(ReleaseNoteSrcBuildReader(cold_src_build_doc))

    diff_stats_from_metadata_file = {"foo": "bar", "baz": "qux"}
    addon_note = "FooBarBazQux"

    source = ReleaseNoteSource(old_src_build_reader=old_src_build_reader,
                               new_src_build_reader=new_src_build_reader,
                               diff_stats_from_metadata_file=diff_stats_from_metadata_file,
                               addon_note=addon_note)
    return source


@pytest.mark.ReleaseNoteSource
def test_diff_datasource_mapping(release_note_source):
    diff = release_note_source.diff_datasource_mapping()

    assert len(diff) == 3
    assert set(diff["add"]) == set(['clinvar.gene.id', 'cosmic.mut_freq'])
    assert set(diff["remove"]) == set(['clinvar.gene.gene_id', 'cosmic.mutation_frequency'])
    assert set(diff["replace"]) == set(['clinvar.type.type', 'cosmic.ref.type'])


@pytest.mark.ReleaseNoteSource
def test_diff_build_stats(release_note_source):
    new_doc = release_note_source.new_src_build_reader.src_build_doc
    old_doc = release_note_source.old_src_build_reader.src_build_doc

    diff = release_note_source.diff_build_stats()
    assert len(diff) == 3
    assert diff["added"] == {}
    assert diff["deleted"] == {}

    assert len(diff["updated"]) == 2
    assert diff["updated"]["total"] == {"new": {"_count": new_doc["_meta"]["stats"]["total"]},
                                        "old": {"_count": old_doc["_meta"]["stats"]["total"]}}
    assert diff["updated"]["observed"] == {"new": {"_count": new_doc["_meta"]["stats"]["observed"]},
                                           "old": {"_count": old_doc["_meta"]["stats"]["observed"]}}


@pytest.mark.ReleaseNoteSource
def test_diff_datasource_info(release_note_source):
    new_hot_doc = release_note_source.new_src_build_reader.src_build_doc
    new_cold_doc = release_note_source.new_src_build_reader.cold_src_build_reader.src_build_doc
    old_hot_doc = release_note_source.old_src_build_reader.src_build_doc
    old_cold_doc = release_note_source.old_src_build_reader.cold_src_build_reader.src_build_doc

    diff = release_note_source.diff_datasource_info()

    assert len(diff) == 3
    assert diff["added"] == {}
    assert diff["deleted"] == {}

    assert len(diff["updated"]) == 3
    assert len(diff["updated"]["geno2mp"]) == 2
    assert len(diff["updated"]["clinvar"]) == 2
    assert len(diff["updated"]["cosmic"]) == 2
    assert diff["updated"]["geno2mp"]["new"] == {'_version': new_hot_doc["_meta"]["src"]["geno2mp"]["version"],
                                                 '_count': new_hot_doc["merge_stats"]["geno2mp"]}
    assert diff["updated"]['geno2mp']["old"] == {'_version': old_hot_doc["_meta"]["src"]["geno2mp"]["version"],
                                                 '_count': old_hot_doc["merge_stats"]["geno2mp"]}
    assert diff["updated"]["clinvar"]["new"] == {'_version': new_hot_doc["_meta"]["src"]["clinvar"]["version"],
                                                 'clinvar_hg19': {'_count': new_hot_doc["merge_stats"]["clinvar_hg19"]}}
    assert diff["updated"]['clinvar']["old"] == {'_version': old_hot_doc["_meta"]["src"]["clinvar"]["version"],
                                                 'clinvar_hg19': {'_count': old_hot_doc["merge_stats"]["clinvar_hg19"]}}
    assert diff["updated"]["cosmic"]["new"] == {'_version': new_cold_doc["_meta"]["src"]["cosmic"]["version"],
                                                '_count': new_cold_doc["merge_stats"]["cosmic"]}
    assert diff["updated"]['cosmic']["old"] == {'_version': old_cold_doc["_meta"]["src"]["cosmic"]["version"],
                                                '_count': old_cold_doc["merge_stats"]["cosmic"]}


@pytest.mark.ReleaseNoteTxt
def test_format_number():
    assert ReleaseNoteTxt._format_number(0) == "0"
    assert ReleaseNoteTxt._format_number(0, sign=True) == "0"

    assert ReleaseNoteTxt._format_number(1, sign=True) == "+1"
    assert ReleaseNoteTxt._format_number(-1, sign=True) == "-1"

    assert ReleaseNoteTxt._format_number("one", sign=True) == "N.A"
