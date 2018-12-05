import json, jsonschema
from biothings.utils.common import is_str

def generate_json_schema(dmap,level=0):

    schtypesmap = {
            str: "string",
            int: "integer",
            float: "number",
            list: "array",
            bool: "boolean",
            dict: "object",
            }

    schema = {
            "type" : schtypesmap[type(dmap)],
            "properties" : {}
        }

    errors = []

    for rootk in dmap:
        typekeys = [k for k in dmap[rootk].keys() if type(k) is type]
        typesonly = len(typekeys) == len(dmap[rootk])
        if not typesonly:
            typekeys.append(dict)
        if not is_str(rootk) or rootk.startswith("_"):
            # _stats
            continue
        if len(typekeys) > 1:
            schema["properties"][rootk] = {"type" : [schtypesmap[k] for k in typekeys]}
        elif typekeys:
            schema["properties"][rootk] = {"type" : [schtypesmap[k] for k in typekeys][0]}

        if list in typekeys:
            typekeys.remove(list)
            schema["properties"][rootk]["items"] = generate_json_schema(dmap[rootk][list],level=level+1)
            dmap[rootk].pop(list)

        if typekeys:
            sch = generate_json_schema(dmap[rootk],level=level+1)
            sch.pop("type")
            #schema["properties"][rootk] = generate_json_schema(dmap[rootk],level=level+1)
            schema["properties"][rootk].update(sch)
            pass

    if schema["properties"] == {}:
        schema.pop("properties")
    return schema


if __name__ == "__main__":
    from biothings.utils.inspect import typify_inspect_doc, inspect_docs
    import pickle
    from pprint import pprint,pformat

    # object
    td1 = {"i" : {"a":456}}
    s1 = {'properties': {'i': {'properties': {'a': {'type': 'integer'}},
       'type': 'object'}},
     'type': 'object'}
    m = inspect_docs([td1],mode="stats")["stats"]
    gs = generate_json_schema(m)
    assert gs == s1, "%s  !=\n%s" % (gs,s1)

    # array of object
    td2 = {"i" : [{"a":123}]}
    s2 = {'properties': {'i': {'items': {'properties': {'a': {'type': 'integer'}},
        'type': 'object'},
       'type': 'array'}},
     'type': 'object'}
    m = inspect_docs([td2],mode="stats")["stats"]
    gs = generate_json_schema(m)
    assert gs == s2, "%s  !=\n%s" % (gs,s2)

    # object in object
    td3 = {"i" : {"a":{"b":123}}}
    s3 = {'properties': {'i': {'properties': {'a': {'properties': {'b': {'type': 'integer'}},
         'type': 'object'}},
       'type': 'object'}},
     'type': 'object'}
    m = inspect_docs([td3],mode="stats")["stats"]
    gs = generate_json_schema(m)
    assert gs == s3, "%s  !=\n%s" % (gs,s3)

    # mixed array/object
    td1 = {"i" : {"a":456}}
    td2 = {"i" : [{"a":123}]}
    s12 = {'properties': {'i': {'items': {'properties': {'a': {'type': 'integer'}},
        'type': 'object'},
       'properties': {'a': {'type': 'integer'}},
       'type': ['array', 'object']}},
     'type': 'object'}
    m = inspect_docs([td1,td2],mode="stats")["stats"] 
    gs = generate_json_schema(m)
    assert gs == s12, "%s  !=\n%s" % (gs,s12)

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
{'_id': {str: {'_stats': {'_count': 323, '_max': 36, '_min': 17}}},
 '_stats': {'_count': 323, '_max': 1, '_min': 1},
 'cgi': {list: {'_stats': {'_count': 111, '_max': 32, '_min': 2},
   'association': {str: {'_stats': {'_count': 401, '_max': 39, '_min': 9}}},
   'cdna': {str: {'_stats': {'_count': 401, '_max': 21, '_min': 7}}},
   'drug': {str: {'_stats': {'_count': 401, '_max': 91, '_min': 14}}},
   'evidence_level': {str: {'_stats': {'_count': 401,
      '_max': 31,
      '_min': 11}}},
   'gene': {str: {'_stats': {'_count': 401, '_max': 6, '_min': 2}}},
   'primary_tumor_type': {str: {'_stats': {'_count': 401,
      '_max': 78,
      '_min': 4}}},
   'protein_change': {str: {'_stats': {'_count': 401, '_max': 13, '_min': 8}}},
   'region': {str: {'_stats': {'_count': 401, '_max': 23, '_min': 22}}},
   'source': {str: {'_stats': {'_count': 401, '_max': 119, '_min': 3}}},
   'transcript': {str: {'_stats': {'_count': 401, '_max': 15, '_min': 15}}}},
  'evidence_level': {str: {'_stats': {'_count': 212, '_max': 31, '_min': 11}}},
  'region': {str: {'_stats': {'_count': 212, '_max': 23, '_min': 22}}},
  'drug': {str: {'_stats': {'_count': 212, '_max': 61, '_min': 14}}},
  '_stats': {'_count': 212, '_max': 1, '_min': 1},
  'gene': {str: {'_stats': {'_count': 212, '_max': 6, '_min': 2}}},
  'transcript': {str: {'_stats': {'_count': 212, '_max': 15, '_min': 15}}},
  'protein_change': {str: {'_stats': {'_count': 212, '_max': 12, '_min': 8}}},
  'association': {str: {'_stats': {'_count': 212, '_max': 38, '_min': 9}}},
  'source': {str: {'_stats': {'_count': 212, '_max': 69, '_min': 3}}},
  'primary_tumor_type': {str: {'_stats': {'_count': 212,
     '_max': 37,
     '_min': 4}}},
  'cdna': {str: {'_stats': {'_count': 212, '_max': 21, '_min': 7}}}}}

    schema = generate_json_schema(cgi_dmap)
    assert cgi_schema == schema

    print("All test OK")

