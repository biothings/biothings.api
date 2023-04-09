import json

import bson

from biothings.utils.common import is_str


def generate_json_schema(dmap):
    scalarmap = {
        str: "string",
        int: "integer",
        float: "number",
        bool: "boolean",
        bson.int64.Int64: "number",
        None: "null",
    }

    def merge_type(typ1, typ2):
        if isinstance(typ1, list):
            if isinstance(typ2, list):
                typ1.extend(typ2)
            else:
                typ1.append(typ2)
        elif isinstance(typ2, list):
            typ1 = [typ1] + typ1
        else:
            typ1 = [typ1, typ2]

        return list(set(typ1))

    schema = {}

    if isinstance(dmap, dict):
        for k in dmap:
            if is_str(k):
                esch = generate_json_schema(dmap[k])
                if schema:
                    if schema["type"] == "object":
                        # we just complete 'properties', key already defined previously
                        pass
                    elif schema["type"] == "array":
                        if not schema.get("properties"):
                            schema["properties"] = {}
                            schema["type"] = merge_type(schema["type"], "object")
                    elif isinstance(schema["type"], list):
                        assert set(schema["type"]) == {"object", "array"}
                    else:
                        raise Exception("Previous schema type not expected: %s" % schema["type"])

                else:
                    schema = {"type": "object", "properties": {}}
                schema["properties"][k] = esch
            # elif type(k) == type:    # TODO: remove this line
            elif isinstance(k, type):
                if k == list:
                    if schema:
                        # already defined for this key, mixed types
                        schema.update({"items": {}})
                        schema["type"] = merge_type(schema["type"], "array")
                    else:
                        schema = {"type": "array", "items": {}}
                    esch = generate_json_schema(dmap[k])
                    schema["items"] = generate_json_schema(dmap[k])
                else:
                    if schema:
                        schema["type"] = merge_type(schema["type"], scalarmap[k])
                    else:
                        schema = {"type": scalarmap[k]}
            elif k is None:
                schema = {"type": None}
            else:
                raise Exception("no not here, k: %s" % k)
    else:
        pass

    return schema


def test():
    # TODO: Move these test to tests folder, or maybe already moved over? Chunlei
    # can't use assert directly, as we can't ensure the order of types (for instance)
    import biothings.utils.jsondiff
    from biothings.utils.inspect import inspect_docs, typify_inspect_doc

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
    cgi_schema = json.load(open("biothings/tests/cgi_schema.json"))
    cgi_map = typify_inspect_doc(json.load(open("biothings/tests/cgi_map.json")))
    schema = generate_json_schema(cgi_map)
    assert jsondiff(cgi_schema, schema) == []

    clinvar_schema = json.load(open("biothings/tests/clinvar_schema.json"))
    clinvar_map = typify_inspect_doc(json.load(open("biothings/tests/clinvar_map.json")))
    schema = generate_json_schema(clinvar_map)
    assert jsondiff(clinvar_schema, schema) == []

    mygene_schema = json.load(open("biothings/tests/mygene_schema.json"))
    mygene_map = typify_inspect_doc(json.load(open("biothings/tests/mygene_map.json")))
    schema = generate_json_schema(mygene_map)
    assert jsondiff(mygene_schema, schema) == []

    print("All test OK")


if __name__ == "__main__":
    test()
