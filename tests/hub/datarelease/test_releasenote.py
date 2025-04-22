import pytest

from biothings.hub.datarelease.releasenote import (
    ReleaseNoteSrcBuildReader,
    ReleaseNoteSrcBuildReaderAdapter,
    ReleaseNoteTxt,
)


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

    assert hot_reader.datasource_versions == {
        "clinvar": hot_doc["_meta"]["src"]["clinvar"]["version"],
        "cosmic": cold_doc["_meta"]["src"]["cosmic"]["version"],
        "geno2mp": hot_doc["_meta"]["src"]["geno2mp"]["version"],
    }
    assert hot_reader.datasource_stats == {**hot_doc["merge_stats"], **cold_doc["merge_stats"]}
    assert hot_reader.datasource_mapping == {**hot_doc["mapping"], **cold_doc["mapping"]}


@pytest.mark.ReleaseNoteSrcBuildReaderAdapter
def test_read_cold_datasource_info(mock_get_source_fullname, cold_src_build_doc):
    cold_doc = cold_src_build_doc
    cold_reader = ReleaseNoteSrcBuildReader(cold_doc)
    cold_adapter = ReleaseNoteSrcBuildReaderAdapter(cold_reader)

    info = cold_adapter.datasource_info
    assert len(info) == 1
    assert info["cosmic"] == {
        "_version": cold_doc["_meta"]["src"]["cosmic"]["version"],
        "_count": cold_doc["merge_stats"]["cosmic"],
    }


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
    assert info["geno2mp"] == {
        "_version": hot_doc["_meta"]["src"]["geno2mp"]["version"],
        "_count": hot_doc["merge_stats"]["geno2mp"],
    }
    assert info["cosmic"] == {
        "_version": cold_doc["_meta"]["src"]["cosmic"]["version"],
        "_count": cold_doc["merge_stats"]["cosmic"],
    }
    assert info["clinvar"] == {
        "_version": hot_doc["_meta"]["src"]["clinvar"]["version"],
        "clinvar_hg19": {"_count": hot_doc["merge_stats"]["clinvar_hg19"]},
    }


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
    assert stats["total"] == {"_count": hot_doc["_meta"]["stats"]["total"]}
    assert stats["hg19"] == {"_count": hot_doc["_meta"]["stats"]["hg19"]}
    assert stats["vcf"] == {"_count": hot_doc["_meta"]["stats"]["vcf"]}
    assert stats["observed"] == {"_count": hot_doc["_meta"]["stats"]["observed"]}


@pytest.mark.ReleaseNoteSource
def test_diff_datasource_mapping(release_note_source):
    diff = release_note_source.diff_datasource_mapping()

    assert len(diff) == 3
    assert set(diff["add"]) == set(["clinvar.gene.id", "cosmic.mut_freq"])
    assert set(diff["remove"]) == set(["clinvar.gene.gene_id", "cosmic.mutation_frequency"])
    assert set(diff["replace"]) == set(["clinvar.type.type", "cosmic.ref.type"])


@pytest.mark.ReleaseNoteSource
def test_diff_build_stats(release_note_source):
    new_doc = release_note_source.new_src_build_reader.src_build_doc
    old_doc = release_note_source.old_src_build_reader.src_build_doc

    diff = release_note_source.diff_build_stats()
    assert len(diff) == 3
    assert diff["added"] == {}
    assert diff["deleted"] == {}

    assert len(diff["updated"]) == 2
    assert diff["updated"]["total"] == {
        "new": {"_count": new_doc["_meta"]["stats"]["total"]},
        "old": {"_count": old_doc["_meta"]["stats"]["total"]},
    }
    assert diff["updated"]["observed"] == {
        "new": {"_count": new_doc["_meta"]["stats"]["observed"]},
        "old": {"_count": old_doc["_meta"]["stats"]["observed"]},
    }


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
    assert diff["updated"]["geno2mp"]["new"] == {
        "_version": new_hot_doc["_meta"]["src"]["geno2mp"]["version"],
        "_count": new_hot_doc["merge_stats"]["geno2mp"],
    }
    assert diff["updated"]["geno2mp"]["old"] == {
        "_version": old_hot_doc["_meta"]["src"]["geno2mp"]["version"],
        "_count": old_hot_doc["merge_stats"]["geno2mp"],
    }
    assert diff["updated"]["clinvar"]["new"] == {
        "_version": new_hot_doc["_meta"]["src"]["clinvar"]["version"],
        "clinvar_hg19": {"_count": new_hot_doc["merge_stats"]["clinvar_hg19"]},
    }
    assert diff["updated"]["clinvar"]["old"] == {
        "_version": old_hot_doc["_meta"]["src"]["clinvar"]["version"],
        "clinvar_hg19": {"_count": old_hot_doc["merge_stats"]["clinvar_hg19"]},
    }
    assert diff["updated"]["cosmic"]["new"] == {
        "_version": new_cold_doc["_meta"]["src"]["cosmic"]["version"],
        "_count": new_cold_doc["merge_stats"]["cosmic"],
    }
    assert diff["updated"]["cosmic"]["old"] == {
        "_version": old_cold_doc["_meta"]["src"]["cosmic"]["version"],
        "_count": old_cold_doc["merge_stats"]["cosmic"],
    }


@pytest.mark.ReleaseNoteTxt
def test_format_number():
    assert ReleaseNoteTxt._format_number(0) == "0"
    assert ReleaseNoteTxt._format_number(0, sign=True) == "0"

    assert ReleaseNoteTxt._format_number(1, sign=True) == "+1"
    assert ReleaseNoteTxt._format_number(-1, sign=True) == "-1"

    assert ReleaseNoteTxt._format_number("one", sign=True) == "N.A"
