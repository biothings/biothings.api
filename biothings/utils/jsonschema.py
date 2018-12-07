import json, jsonschema
from biothings.utils.common import is_str
from pprint import pprint,pformat


def generate_json_schema(dmap):

    scalarmap = {
            str: "string",
            int: "integer",
            float: "number",
            bool: "boolean",
            }

    def merge_type(typ1,typ2):
        if type(typ1) == list:
            if type(typ2) == list:
                typ1.extend(typ2)
            else:
                typ1.append(typ2)
        elif type(typ2) == list:
            typ1 = [typ1] + typ1
        else:
            typ1 = [typ1,typ2]

        return list(set(typ1))

    schema = {}

    if type(dmap) == dict:
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
                            schema["type"] = merge_type(schema["type"],"object")
                        #typ = esch.pop("type")
                        #schema.update(esch)
                        #schema["type"] = merge_type(schema["type"],typ)
                    elif type(schema["type"]) == list:
                        assert set(schema["type"]) == {"object","array"}
                        pass
                    else:
                        raise Exception("Previous schema type not expected: %s" % schema["type"])

                else:
                    schema = {"type" : "object", "properties" : {}}
                schema["properties"][k] = esch
            elif type(k) == type:
                if k == list:
                    if schema:
                        # already defined for this key, mixed types
                        # since here k is a list, previous schema must be about
                        # a dict/object, so we can safely update()
                        assert "properties" in schema
                        schema.update({"items" : {}})
                        schema["type"] = merge_type(schema["type"],"array")
                    else:
                        schema = {"type" : "array", "items" : {}}
                    esch = generate_json_schema(dmap[k])
                    schema["items"] = generate_json_schema(dmap[k])
                else:
                    if schema:
                        schema["type"] = merge_type(schema["type"],scalarmap[k])
                    else:
                        schema = {"type" : scalarmap[k]}
            else:
                raise Exception("no not here")
    else:
        pass

    return schema


def test():
    from biothings.utils.inspect import typify_inspect_doc, inspect_docs
    import pickle
    from pprint import pprint,pformat

    # object
    td1 = {"i" : {"a":456}}
    s1 = {'properties': {'i': {'properties': {'a': {'type': 'integer'}},
       'type': 'object'}},
     'type': 'object'}
    m = inspect_docs([td1],mode="type")["type"]
    gs = generate_json_schema(m)
    assert gs == s1, "%s  !=\n%s" % (gs,s1)

    td5 = {"i" : [1,2,3]}
    s5 = {'properties': {'i': {'items': {'type': 'integer'}, 'type': 'array'}},
            'type': 'object'}
    m = inspect_docs([td5],mode="type")["type"]
    gs = generate_json_schema(m)
    assert gs == s5, "%s  !=\n%s" % (gs,s5)

    # array of object
    td2 = {"i" : [{"a":123}]}
    s2 = {'properties': {'i': {'items': {'properties': {'a': {'type': 'integer'}},
        'type': 'object'},
       'type': 'array'}},
     'type': 'object'}
    m = inspect_docs([td2],mode="type")["type"]
    gs = generate_json_schema(m)
    assert gs == s2, "%s  !=\n%s" % (gs,s2)

    # object in object
    td3 = {"i" : {"a":{"b":123}}}
    s3 = {'properties': {'i': {'properties': {'a': {'properties': {'b': {'type': 'integer'}},
         'type': 'object'}},
       'type': 'object'}},
     'type': 'object'}
    m = inspect_docs([td3],mode="type")["type"]
    gs = generate_json_schema(m)
    assert gs == s3, "%s  !=\n%s" % (gs,s3)

    # mixed str/float in array
    td6 = {"i" : [1,2,"a"]}
    s6 = {'properties': {'i': {'items': {'type': ['integer','string']}, 'type': 'array'}},
            'type': 'object'}
    m = inspect_docs([td6],mode="type")["type"]
    gs = generate_json_schema(m) 
    assert gs == s6, "%s  !=\n%s" % (gs,s6)

    # mixed array/object
    td1 = {"i" : {"a":456}}
    td2 = {"i" : [{"a":123}]}
    s12 = {'properties': {'i': {'items': {'properties': {'a': {'type': 'integer'}},
        'type': 'object'},
       'properties': {'a': {'type': 'integer'}},
       'type': ['array', 'object']}},
     'type': 'object'}
    m = inspect_docs([td1,td2],mode="type")["type"] 
    gs = generate_json_schema(m)
    assert gs == s12, "%s  !=\n%s" % (gs,s12)

    # list of integer (list of things which are not objects)
    #td4 = {'i': {'a': [5, 5, 3]}}
    td4 = {'a': [5, 5, 3]}
    #s4 = {'properties': {'i': {'properties': {'a': {'items': {'type': 'integer'},
    #    'type': 'array'}},
    #  'type': 'object'}},
    #'type' : 'object'}
    s4 = {'properties': {'a': {'items': {'type': 'integer'},
        'type': 'array'}},
      'type': 'object'}
    m = inspect_docs([td4],mode="type")["type"]
    gs = generate_json_schema(m)
    assert gs == s4, "%s  !=\n%s" % (gs,s4)

    td7 = {"i" : {"a":1,"b":2}}

    # small real-life collection
    cgi_schema = \
{'properties': {'cgi': {'items': {'properties': {'association': {'type': 'string'},
     'cdna': {'type': 'string'},
     'drug': {'type': 'string'},
     'evidence_level': {'type': 'string'},
     'gene': {'type': 'string'},
     'primary_tumor_type': {'type': 'string'},
     'protein_change': {'type': 'string'},
     'region': {'type': 'string'},
     'source': {'type': 'string'},
     'transcript': {'type': 'string'}},
    'type': 'object'},
   'properties': {'association': {'type': 'string'},
    'cdna': {'type': 'string'},
    'drug': {'type': 'string'},
    'evidence_level': {'type': 'string'},
    'gene': {'type': 'string'},
    'primary_tumor_type': {'type': 'string'},
    'protein_change': {'type': 'string'},
    'region': {'type': 'string'},
    'source': {'type': 'string'},
    'transcript': {'type': 'string'}},
   'type': ['array', 'object']}},
 'type': 'object'}

	# generated from inspect_docs
    cgi_dmap = \
{
 'cgi': {'gene': {str: {}},
  'evidence_level': {str: {}},
  'primary_tumor_type': {str: {}},
  'association': {str: {}},
  list: {'association': {str: {}},
   'cdna': {str: {}},
   'drug': {str: {}},
   'evidence_level': {str: {}},
   'gene': {str: {}},
   'primary_tumor_type': {str: {}},
   'protein_change': {str: {}},
   'region': {str: {}},
   'source': {str: {}},
   'transcript': {str: {}}},
  'drug': {str: {}},
  'transcript': {str: {}},
  'source': {str: {}},
  'region': {str: {}},
  'cdna': {str: {}},
  'protein_change': {str: {}}}}

    schema = generate_json_schema(cgi_dmap)
    assert cgi_schema == schema, "%s\n!=\n%s" % (pformat(cgi_schema),pformat(schema))

    # from app folder, biothings as symlink
    clinvar_schema = json.load(open("biothings/tests/clinvar_schema.json"))
    clinvar_map = typify_inspect_doc(json.load(open("biothings/tests/clinvar_map.json")))
    schema = generate_json_schema(clinvar_map)
    assert clinvar_schema == schema

    print("All test OK")


if __name__ == "__main__":
    test()
