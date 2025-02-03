import json
from typing import Dict

import bson

from biothings.utils.common import is_str


def generate_json_schema(dmap) -> Dict:
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
