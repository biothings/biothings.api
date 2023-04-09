import math
from pprint import pformat

from biothings.utils.common import splitstr
from biothings.utils.inspect import DeepStatsMode, inspect, inspect_docs, merge_record, merge_scalar_list


class InspectorTest(object):
    def test_01_not_order_specific(self):
        d1 = {"id": "124", "lofd": [{"val": 34.3}, {"ul": "bla"}], "d": {"start": 134, "end": 5543}}
        d2 = {"id": "5", "lofd": {"oula": "mak", "val": 34}, "d": {"start": 134, "end": 5543}}
        d3 = {"id": "890", "lofd": [{"val": 34}], "d": {"start": 134, "end": 5543}}

        # merge either ways in the same
        m12 = inspect_docs([d1, d2])["type"]
        m21 = inspect_docs([d2, d1])["type"]
        # if undordered list, then:
        assert m21 == m12, "\nm21=%s\n!=\nm12=%s" % (pformat(m21), pformat(m12))

    def test_02_same_key_different_types(self):
        # val can be an int and a float
        m1 = inspect_docs([{"val": 34}, {"val": 1.2}])["type"]
        # set: types can be in any order
        assert set(m1["val"]) == {int, float}

    def test_03_same_key_different_types_with_list(self):
        # even if val is in a list
        m2 = inspect_docs([{"val": 34}, [{"val": 1.2}]])["type"]
        # list and val not merged
        assert set(m2.keys()) == {"val", list}

    def test_04_same_key_different_types_with_list_and_dict(self):
        # another example with a mix a dict and list (see "p")
        od1 = {"id": "124", "d": [{"p": 123}, {"p": 456}]}
        od2 = {"id": "124", "d": [{"p": 123}, {"p": [456, 789]}]}
        m12 = inspect_docs([od1, od2], mode="type")["type"]
        m21 = inspect_docs([od2, od1], mode="type")["type"]
        assert m12 == m21
        # "p" is a integer or a list of integer
        assert m12["d"][list]["p"].keys() == {list, int}

    def test_05_stats(test):
        d1 = {"id": "124", "lofd": [{"val": 34.3}, {"ul": "bla"}], "d": {"start": 134, "end": 5543}}
        d2 = {"id": "5", "lofd": {"oula": "mak", "val": 34}, "d": {"start": 134, "end": 5543}}
        # stats
        m = {}
        inspect(d1, mapt=m, mode="stats")
        # some simple check
        assert set(m["id"].keys()) == {str}
        assert m["id"][str]["_stats"]["_count"] == 1
        assert m["id"][str]["_stats"]["_max"] == 3
        assert m["id"][str]["_stats"]["_min"] == 3
        assert m["lofd"].keys() == {list}
        # list's stats
        assert m["lofd"][list]["_stats"]["_count"] == 1
        assert m["lofd"][list]["_stats"]["_max"] == 2
        assert m["lofd"][list]["_stats"]["_min"] == 2
        # one list's elem stats
        assert m["lofd"][list]["val"][float]["_stats"]["_count"] == 1
        assert m["lofd"][list]["val"][float]["_stats"]["_max"] == 34.3
        assert m["lofd"][list]["val"][float]["_stats"]["_min"] == 34.3
        # again (should see increment in counts for instance)
        inspect(d1, mapt=m, mode="stats")
        assert m["id"][str]["_stats"]["_count"] == 2
        assert m["id"][str]["_stats"]["_max"] == 3
        assert m["id"][str]["_stats"]["_min"] == 3
        assert m["lofd"][list]["_stats"]["_count"] == 2
        assert m["lofd"][list]["_stats"]["_max"] == 2
        assert m["lofd"][list]["_stats"]["_min"] == 2
        assert m["lofd"][list]["val"][float]["_stats"]["_count"] == 2
        assert m["lofd"][list]["val"][float]["_stats"]["_max"] == 34.3
        assert m["lofd"][list]["val"][float]["_stats"]["_min"] == 34.3
        # mix with d2
        inspect(d2, mapt=m, mode="stats")
        assert m["id"][str]["_stats"]["_count"] == 3
        assert m["id"][str]["_stats"]["_max"] == 3
        assert m["id"][str]["_stats"]["_min"] == 1  # new min
        assert m["lofd"][list]["_stats"]["_count"] == 2  # not incremented as in d2 it's not a list
        assert m["lofd"][list]["_stats"]["_max"] == 2
        assert m["lofd"][list]["_stats"]["_min"] == 2
        # now float & int
        assert m["lofd"][list]["val"][float]["_stats"]["_count"] == 2
        assert m["lofd"][list]["val"][float]["_stats"]["_max"] == 34.3
        assert m["lofd"][list]["val"][float]["_stats"]["_min"] == 34.3
        # val{int} wasn't merged
        assert m["lofd"]["val"][int]["_stats"]["_count"] == 1
        assert m["lofd"]["val"][int]["_stats"]["_max"] == 34
        assert m["lofd"]["val"][int]["_stats"]["_min"] == 34
        # d2 again
        inspect(d2, mapt=m, mode="stats")
        assert m["id"][str]["_stats"]["_count"] == 4
        assert m["id"][str]["_stats"]["_max"] == 3
        assert m["id"][str]["_stats"]["_min"] == 1
        assert m["lofd"][list]["_stats"]["_count"] == 2
        assert m["lofd"][list]["_stats"]["_max"] == 2
        assert m["lofd"][list]["_stats"]["_min"] == 2
        assert m["lofd"][list]["val"][float]["_stats"]["_count"] == 2
        assert m["lofd"][list]["val"][float]["_stats"]["_max"] == 34.3
        assert m["lofd"][list]["val"][float]["_stats"]["_min"] == 34.3
        assert m["lofd"]["val"][int]["_stats"]["_count"] == 2
        assert m["lofd"]["val"][int]["_stats"]["_max"] == 34
        assert m["lofd"]["val"][int]["_stats"]["_min"] == 34

        # all counts should be 10
        m = inspect_docs([d1] * 10, mode="stats")["stats"]
        assert m["d"]["end"][int]["_stats"]["_count"] == 10
        assert m["d"]["start"][int]["_stats"]["_count"] == 10
        assert m["id"][str]["_stats"]["_count"] == 10
        assert m["lofd"][list]["_stats"]["_count"] == 10
        assert m["lofd"][list]["ul"][str]["_stats"]["_count"] == 10
        assert m["lofd"][list]["val"][float]["_stats"]["_count"] == 10

    def test_06_merge_stats(self):
        nd1 = {"id": "124", "lofd": [{"val": 34.3}, {"ul": "bla"}]}
        nd2 = {"id": "5678", "lofd": {"val": 50.2}}
        m = {}
        inspect(nd1, mapt=m, mode="deepstats")
        inspect(nd2, mapt=m, mode="deepstats")
        assert set(m["lofd"].keys()) == {list, "val", "_stats"}, "%s" % m["lofd"].keys()
        assert m["lofd"][list]["val"][float]["_stats"] == {
            "__vals": [34.3],
            "_count": 1,
            "_max": 34.3,
            "_min": 34.3,
        }, m["lofd"][list]["val"][float]["_stats"]
        # merge stats into the left param
        DeepStatsMode().merge(m["lofd"][list]["val"][float]["_stats"], m["lofd"]["val"][float]["_stats"])
        assert m["lofd"][list]["val"][float]["_stats"] == {
            "__vals": [34.3, 50.2],
            "_count": 2,
            "_max": 50.2,
            "_min": 34.3,
        }

    def test_07_mapping_simple(self):
        # mapping mode (splittable strings)
        # "bla" is splitable in one case, not in the other
        # "oula" is splitable, "arf" is not
        sd1 = {"_id": "124", "vals": [{"oula": "this is great"}, {"bla": "I am splitable", "arf": "ENS355432"}]}
        sd2 = {"_id": "5678", "vals": {"bla": "rs45653", "void": 654}}
        sd3 = {"_id": "124", "vals": [{"bla": "thisisanid"}]}
        m = {}
        inspect(sd3, mapt=m, mode="mapping")
        # bla not splitable here
        assert m["vals"][list]["bla"][str] == {}
        inspect(sd1, mapt=m, mode="mapping")
        # now it is
        assert m["vals"][list]["bla"][splitstr] == {}
        inspect(sd2, mapt=m, mode="mapping")
        # not splitable in sd2
        assert m["vals"]["bla"][str] == {}

    def test_08_mapping_with_list_of_list_of_integer(self):
        # mapping with type of type
        sd1 = {"_id": "123", "homologene": {"id": "bla", "gene": [[123, 456], [789, 102]]}}
        m = inspect_docs([sd1], mode="mapping")["mapping"]
        assert m == {
            "homologene": {
                "properties": {
                    "gene": {"type": "integer"},
                    "id": {"normalizer": "keyword_lowercase_normalizer", "type": "keyword"},
                }
            }
        }, (
            "mapping %s" % m
        )

    def test_09_mapping_scalar_or_list(self):
        # ok, "bla" is either a scalar or in a list, test merge
        md1 = {"_id": "124", "vals": [{"oula": "this is great"}, {"bla": "rs24543", "arf": "ENS355432"}]}
        md2 = {"_id": "5678", "vals": {"bla": "I am splitable in a scalar", "void": 654}}
        # bla is a different type here
        md3 = {"_id": "5678", "vals": {"bla": 1234}}
        m = inspect_docs([md1, md2], mode="mapping", pre_mapping=True)["mapping"]  # "mapping" implies merge=True
        assert not "bla" in m["vals"]
        assert m["vals"][list]["bla"] == {splitstr: {}}, m["vals"][list]["bla"]  # splittable str from md2 merge to list
        m = inspect_docs([md1, md3], mode="mapping", pre_mapping=True)["mapping"]
        assert not "bla" in m["vals"]
        assert m["vals"][list]["bla"] == {int: {}, str: {}}  # keep as both types
        m = inspect_docs([md1, md2, md3], mode="mapping", pre_mapping=True)["mapping"]
        assert not "bla" in m["vals"]
        assert m["vals"][list]["bla"] == {int: {}, splitstr: {}}, m["vals"][list][
            "bla"
        ]  # splittable kept + merge int to keep both types

    def test_10_merge_mix_scalar_list_with_stats(self):
        # not implemented, will raise NotImplementedError
        return
        # test merge scalar/list with stats
        # unmerged is a inspect-doc with mode=stats, structure is:
        # id and name keys are both as root keys and in [list]
        insdoc = {
            list: {
                "_stats": {"_count": 10, "_max": 200, "_sum": 1000, "_min": 2},
                "id": {str: {"_stats": {"_count": 100, "_max": 10, "_sum": 1000, "_min": 1}}},
                "name": {str: {"_stats": {"_count": 500, "_max": 5, "_sum": 500, "_min": 0.5}}},
            },
            "id": {
                str: {"_stats": {"_count": 300, "_max": 30, "_sum": 300, "_min": 3}},
                int: {"_stats": {"_count": 1, "_max": 1, "_sum": 1, "_min": 1}},
            },
            "name": {str: {"_stats": {"_count": 400, "_max": 40, "_sum": 4000, "_min": 4}}},
        }
        merge_scalar_list(insdoc, mode="stats")
        # root keys have been merged into [llist] (even id as an integer, bc it's merged based on
        # key name, not key name *and* type
        assert list(insdoc) == [list]
        # check merged stats for "id"
        assert insdoc[list]["id"][str]["_stats"]["_count"] == 400  # 300 + 100
        assert insdoc[list]["id"][str]["_stats"]["_max"] == 30  # from root key
        assert insdoc[list]["id"][str]["_stats"]["_min"] == 1  # from list key
        assert insdoc[list]["id"][str]["_stats"]["_sum"] == 1300  # 1000 + 300
        # "id" as in integer is also merged, stats are kept
        assert insdoc[list]["id"][int]["_stats"]["_count"] == 1
        assert insdoc[list]["id"][int]["_stats"]["_max"] == 1
        assert insdoc[list]["id"][int]["_stats"]["_min"] == 1
        assert insdoc[list]["id"][int]["_stats"]["_sum"] == 1
        # check merged stats for "name"
        assert insdoc[list]["name"][str]["_stats"]["_count"] == 900  # 500 + 400
        assert insdoc[list]["name"][str]["_stats"]["_max"] == 40  # from root key
        assert insdoc[list]["name"][str]["_stats"]["_min"] == 0.5  # from list key
        assert insdoc[list]["name"][str]["_stats"]["_sum"] == 4500  # 4000 + 500
        # [list] stats unchanged
        assert insdoc[list]["_stats"]["_count"] == 10
        assert insdoc[list]["_stats"]["_max"] == 200
        assert insdoc[list]["_stats"]["_min"] == 2
        assert insdoc[list]["_stats"]["_sum"] == 1000

    def test_11_stats_with_same_docs(self):
        d1 = {
            "go": {
                "BP": {
                    "term": "skeletal muscle fiber development",
                    "qualifier": "NOT",
                    "pubmed": 1234,
                    "id": "GO:0048741",
                    "evidence": "IBA",
                }
            },
            "_id": "101362076",
        }
        d2 = {
            "go": {
                "BP": [
                    {
                        "term": "ubiquitin-dependent protein catabolic process",
                        "pubmed": 5678,
                        "id": "GO:0006511",
                        "evidence": "IEA",
                    },
                    {"term": "protein deubiquitination", "pubmed": [2222, 3333], "id": "GO:0016579", "evidence": "IEA"},
                ]
            },
            "_id": "101241878",
        }
        m = inspect_docs([d1, d1, d2, d2], mode="stats")["stats"]
        # no test, but just run

    def test_12_merge_deeply_nested(self):
        # more merge tests involving real case, deeply nested
        # here, go.BP contains a list and some scalars that should be merge
        # together, but also go.BP.pubmed also contains list and scalars
        # needed to be merged together
        insdocdeep = {
            "_id": {str: {}},
            "go": {
                "BP": {
                    "evidence": {str: {}},
                    "id": {str: {}},
                    "pubmed": {list: {int: {}}, int: {}},
                    "qualifier": {str: {}},
                    "term": {str: {}},
                    list: {
                        "evidence": {str: {}},
                        "id": {str: {}},
                        "pubmed": {list: {int: {}}, int: {}},
                        "qualifier": {str: {}},
                        "term": {str: {}},
                    },
                }
            },
        }
        merge_scalar_list(insdocdeep, mode="type")
        # we merge the first level
        assert list(insdocdeep["go"]["BP"].keys()) == [list]
        # and also the second one
        assert list(insdocdeep["go"]["BP"][list]["pubmed"].keys()) == [list]

    def test_13_merge_with_splitstr(self):
        # merge_scalar_list when str split involved (?) in list of list
        doc = {"_id": "1", "f": ["b", ["a 0", "b 1"]]}
        # merge list of str and splitstr
        docb = {"_id": "1", "f": ["a 0"]}
        docg = {"_id": "1", "f": ["a0"]}
        m = inspect_docs([docb, docg], mode="mapping")
        assert m["mapping"]["f"] == {"type": "text"}  # splitstr > str
        # same when strings (not list)
        docb = {"_id": "1", "f": "a 0"}
        docg = {"_id": "1", "f": "a0"}
        m = inspect_docs([docb, docg], mode="mapping")
        assert m["mapping"]["f"] == {"type": "text"}  # splitstr > str
        # same when strings and list of strings
        doc1 = {"_id": "1", "f": ["a 0"]}
        doc2 = {"_id": "1", "f": ["a0"]}
        doc3 = {"_id": "1", "f": "a 0"}
        doc4 = {"_id": "1", "f": "a0"}
        m = inspect_docs([doc1, doc2, doc3, doc4], mode="mapping")
        assert m["mapping"]["f"] == {"type": "text"}  # splitstr > str
        doc1 = {"_id": "1", "f": ["a0"]}
        doc2 = {"_id": "1", "f": ["a 0"]}
        m = inspect_docs([doc1, doc2], mode="mapping")
        assert m["mapping"]["f"] == {"type": "text"}  # splitstr > str
        # splitstr > str whatever the order they appear while inspected (here: splitstr,str,str, in list,list,dict)
        d1 = {"_id": "a", "r": {"k": [{"id": "one", "rel": "is"}, {"id": "two", "rel": "simil to"}]}}
        d2 = {"_id": "b", "r": {"k": [{"id": "three", "rel": "is"}, {"id": "four", "rel": "is"}]}}
        d3 = {"_id": "c", "r": {"k": {"id": "five", "rel": "is"}}}
        m = inspect_docs([d1, d2, d3], mode="mapping")
        assert "errors" not in m["mapping"]

    def test_14_merge_record(self):
        # merge_record splitstr > str
        d1 = {"_id": {str: {}}, "k": {"a": {list: {"i": {str: {}}, "r": {str: {}}}}}}
        d2 = {"_id": {str: {}}, "k": {"a": {list: {"i": {str: {}}, "r": {splitstr: {}}}}}}
        m = {}
        m = merge_record(m, d1, "mapping")
        m = merge_record(m, d2, "mapping")
        assert m["k"]["a"][list]["r"] == {splitstr: {}}

    def test_15_mapping_with_int_float(self):
        # allow int & float in mapping (keep float)
        t1 = {"_id": "a", "f": [1, 2]}
        t2 = {"_id": "a", "f": [1.1, 2.2]}
        m = inspect_docs([t1, t2], mode="mapping")
        assert m["mapping"]["f"]["type"] == "float"

    def test_16_mapping_with_nan_inf(self):
        # NaN/Inf not allowed (if mode is mapping)
        n1 = {"_id": "a", "v1": "oula", "v2": math.nan}
        n2 = {"_id": "b", "v1": "arf", "v2": 13.4}
        n3 = {"_id": "c", "v1": "mak", "v2": math.nan, "v3": math.inf}
        m = inspect_docs([n1, n2, n3], mode="mapping")
        assert "errors" in m["mapping"]
