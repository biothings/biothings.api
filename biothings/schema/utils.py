""" Starting some utils for biothings schema. """

import temp_schema
SCHEMA = temp_schema.SCHEMA

#def generate_avro_schema(schema):

#def generate_web_schema(schema):

#def generate_jsonld_schema(schema):

def generate_es_mappings(schema):
    
    def _traverse(d):
        r = {}
        if '@esmappings' in d:
            r.update({ d['name'] : d['@esmappings'] })
        if 'fields' in d and len(d['fields']) > 0:
            r.setdefault('properties', {})
            for i in d['fields']:
                if isinstance(i, dict):
                    r['properties'].update(dict(_traverse(i)))
        else:
            r.setdefault( d['name'], {} )
            for i in d['type']:
                if isinstance(i, dict) and 'fields' in i:
                    r[ d['name'] ].update(dict(_traverse(i)))
        return list(r.items())
        
    g = _traverse(schema)
    # need to re-think the recursion to prevent this....
    # TODO: remove empties....
    ret = dict([g[0]])
    ret[g[0][0]].update(dict([g[1]]))
    return ret
