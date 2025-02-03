"""
Tests for verifying the schema generation methods
for our data type translations
"""

import json

from biothings.hub.datainspect.doc_inspect import (
    DeepStatsMode,
    inspect,
    inspect_docs,
    merge_record,
    merge_scalar_list,
    typify_inspect_doc,
)
from biothings.hub.datainspect.schema import generate_json_schema
import biothings.utils.jsondiff


def test_schema_generation(temporary_data_storage):
    """
    Tests a variety of scenarios for our schema generation
    """

    biothings.utils.jsondiff.UNORDERED_LIST = True
    jsondiff = biothings.utils.jsondiff.make

    # object
    td1 = {"i": {"a": 456}}
    s1 = {
        "properties": {
            "i": {
                "properties": {"a": {"type": "integer"}},
                "type": "object",
            }
        },
        "type": "object",
    }
    m = inspect_docs([td1], mode="type")["type"]
    gs = generate_json_schema(m)
    assert jsondiff(gs, s1) == [], "%s  !=\n%s" % (gs, s1)

    td5 = {"i": [1, 2, 3]}
    s5 = {
        "properties": {
            "i": {
                "items": {"type": "integer"},
                "type": "array",
            }
        },
        "type": "object",
    }
    m = inspect_docs([td5], mode="type")["type"]
    gs = generate_json_schema(m)
    assert jsondiff(gs, s5) == [], "%s  !=\n%s" % (gs, s5)

    # array of object
    td2 = {"i": [{"a": 123}]}
    s2 = {
        "properties": {
            "i": {
                "items": {
                    "properties": {"a": {"type": "integer"}},
                    "type": "object",
                },
                "type": "array",
            }
        },
        "type": "object",
    }
    m = inspect_docs([td2], mode="type")["type"]
    gs = generate_json_schema(m)
    assert jsondiff(gs, s2) == [], "%s  !=\n%s" % (gs, s2)

    # object in object
    td3 = {"i": {"a": {"b": 123}}}
    s3 = {
        "properties": {
            "i": {
                "properties": {
                    "a": {
                        "properties": {"b": {"type": "integer"}},
                        "type": "object",
                    }
                },
                "type": "object",
            }
        },
        "type": "object",
    }
    m = inspect_docs([td3], mode="type")["type"]
    gs = generate_json_schema(m)
    assert jsondiff(gs, s3) == [], "%s  !=\n%s" % (gs, s3)

    # mixed str/float in array
    td6 = {"i": [1, 2, "a"]}
    s6 = {
        "properties": {
            "i": {
                "items": {"type": ["integer", "string"]},
                "type": "array",
            }
        },
        "type": "object",
    }
    m = inspect_docs([td6], mode="type")["type"]
    gs = generate_json_schema(m)
    assert jsondiff(gs, s6) == [], "%s  !=\n%s" % (gs, s6)

    # mixed array/object
    td1 = {"i": {"a": 456}}
    td2 = {"i": [{"a": 123}]}
    s12 = {
        "properties": {
            "i": {
                "items": {
                    "properties": {"a": {"type": "integer"}},
                    "type": "object",
                },
                "properties": {"a": {"type": "integer"}},
                "type": ["array", "object"],
            }
        },
        "type": "object",
    }
    m = inspect_docs([td1, td2], mode="type")["type"]
    gs = generate_json_schema(m)
    assert jsondiff(gs, s12) == [], "%s  !=\n%s" % (gs, s12)

    # list of integer (list of things which are not objects)
    td4 = {"a": [5, 5, 3]}
    s4 = {
        "properties": {
            "a": {
                "items": {"type": "integer"},
                "type": "array",
            }
        },
        "type": "object",
    }
    m = inspect_docs([td4], mode="type")["type"]
    gs = generate_json_schema(m)
    assert jsondiff(gs, s4) == [], "%s  !=\n%s" % (gs, s4)

    td7 = {"i": {"a": 1, "b": 2}}
    s7 = {
        "type": "object",
        "properties": {
            "i": {
                "type": "object",
                "properties": {
                    "a": {"type": "integer"},
                    "b": {"type": "integer"},
                },
            }
        },
    }
    m = inspect_docs([td7], mode="type")["type"]
    gs = generate_json_schema(m)
    assert jsondiff(gs, s7) == [], "%s  !=\n%s" % (gs, s7)

    # int or list of int (not a list of dict, testing scalar there)
    td81 = {"i": 1}
    td82 = {"i": [2, 3]}
    s812 = {
        "properties": {
            "i": {
                "items": {"type": "integer"},
                "type": ["array", "integer"],
            }
        },
        "type": "object",
    }
    m = inspect_docs([td81, td82], mode="type")["type"]
    gs = generate_json_schema(m)
    assert jsondiff(gs, s812) == [], "%s  !=\n%s" % (gs, s812)

    # run from app folder, biothings as symlink

    # small real-life collection
    cgi_schema_file = temporary_data_storage.joinpath("cgi_schema.json")
    cgi_map_file = temporary_data_storage.joinpath("cgi_map.json")
    with open(cgi_schema_file, "r", encoding="utf-8") as schema_handle, open(
        cgi_map_file, "r", encoding="utf-8"
    ) as map_handle:
        cgi_schema = json.load(schema_handle)
        cgi_map = json.load(map_handle)
        cgi_map = typify_inspect_doc(cgi_map)
        schema = generate_json_schema(cgi_map)
        assert jsondiff(cgi_schema, schema) == []

    clinvar_schema_file = temporary_data_storage.joinpath("clinvar_schema.json")
    clinvar_map_file = temporary_data_storage.joinpath("clinvar_map.json")
    with open(clinvar_schema_file, "r", encoding="utf-8") as schema_handle, open(
        clinvar_map_file, "r", encoding="utf-8"
    ) as map_handle:
        clinvar_schema = json.load(schema_handle)
        clinvar_map = json.load(map_handle)
        clinvar_map = typify_inspect_doc(clinvar_map)
        schema = generate_json_schema(clinvar_map)
        assert jsondiff(clinvar_schema, schema) == []

    mygene_schema_file = temporary_data_storage.joinpath("mygene_schema.json")
    mygene_map_file = temporary_data_storage.joinpath("mygene_map.json")
    with open(mygene_schema_file, "r", encoding="utf-8") as schema_handle, open(
        mygene_map_file, "r", encoding="utf-8"
    ) as map_handle:
        mygene_schema = json.load(schema_handle)
        mygene_map = json.load(map_handle)
        mygene_map = typify_inspect_doc(mygene_map)
        schema = generate_json_schema(mygene_map)
        assert jsondiff(mygene_schema, schema) == []
