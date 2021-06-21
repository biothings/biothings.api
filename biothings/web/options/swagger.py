'''
    Utils for creating swagger spec from a config_web module.

'''
from collections import OrderedDict
import yaml

def setup_yaml():
    """ https://stackoverflow.com/a/8661021 """
    represent_dict_order = lambda self, data:  self.represent_mapping('tag:yaml.org,2002:map', data.items())
    yaml.add_representer(OrderedDict, represent_dict_order)

all_params = {}

def _get_common_parameters(settings, biothing_object, url):
    _all = _get_all_params(settings)
    _ret = OrderedDict([])
    # get all params except for q, scopes, ids -> these are added to the request body, and not the query string
    _common_params_list = [(k,v) for (k,v) in _all.items() if (k != 'q') and (k != 'scopes') and (k != 'ids') and (k in getattr(settings, 'KWARG_DESCRIPTIONS', {}))]
    # add 'special' parameters not controlled by main biothings web pipeline
    _common_params_list.extend([('callback', {'type': str, 'default': None}),
                                ('email', {'type': str, 'default': None})])
    _description_templates = getattr(settings, 'KWARG_DESCRIPTIONS', {})
    for (param, description) in _common_params_list:
        _description_template_object = _description_templates.get(param)
        _param_display = _description_template_object.get('name')
        _ret.update(OrderedDict([
            (_param_display, OrderedDict([
                ('name', _param_display),
                ('in', 'query'),
                ('description', _description_template_object.get('text_template', '').format(
                    biothing_object=biothing_object, 
                    param_type=_string_types(_all.get(param, {}).get('type', '')),
                    param_default_value=_param_default_values(_all.get(param, {}).get('default', '')), 
                    param_max=_param_max(_all.get(param, {}).get('max', '')))),
                ('schema', {'type': _schema_types(_all.get(param, {}).get('type', ''))})
                ]))
            ]))
    return _ret
              
def _get_schemas(settings, biothing_object, url):
    return OrderedDict([
        ("string_or_array", {
            "oneOf": [
                {"type": "array", "items": {"type": "string"}},
                {"type": "string"}
            ]
        }),
        ("int64_or_array", {
            "oneOf": [
                {"type": "array", "items": {"type": "integer", "format": "int64"}},
                {"type": "integer", "format": "int64"}
            ]
        }),
        ("QueryResult", {
            "type": "object",
            "properties": OrderedDict([
                ("max_score", {"type": "number", "format": "float"}),
                ("took", {"type": "integer"}),
                ("total", {"type": "integer"}),
                ("hits", {"type": "array", "items": {"$ref": "#/components/schemas/{}".format(biothing_object.title())}})
            ])
        }),
        ("QueryPOSTResult", {
            "type": "array",
            "items": {"allOf": [
                {"$ref": '#/components/schemas/{}'.format(biothing_object.title())},
                {"type": "object", "properties": {"_score": {"type": "number", "format": "float"}, "query": {"type": "string"}}}
            ]}}),
        ("ErrorResult", {"type": "object",
            "properties": {"success": {"type": "boolean"}, "message": {"type": "string"}}}),
        ("{}".format(biothing_object.title()), {"type": "object", "required": ["_id"], "properties": {"_id":{"type": "string"}}})
        ])

def _schema_types(t):
    if t == type(bool()):
        return 'boolean'
    if t == type(int()):
        return 'integer'
    return 'string'

def _string_types(t):
    if t == type(str()):
        return ' Type: string.'
    elif t == type(int()):
        return ' Type: integer.'
    elif t == type([]):
        return ' Type: string (list).'
    elif t == type(bool()):
        return ' Type: boolean.'
    return ' Type: string.'

def _param_default_values(d):
    if not d:
        return ' Default: None.'
    if type(d) == type(int()) or type(d) == type(str()):
        return ' Default: {}.'.format(d)
    elif type(d) == type(True) and d == True:
        return ' Default: true.'
    elif type(d) == type(False) and d == False:
        return ' Default: false.'
    return ' Default: None.'

def _param_max(m):
    if not m:
        return ''
    return ' Max: {}.'.format(m)

def _query_get_params(settings):
    _ret = {}
    for d in [settings.QUERY_GET_CONTROL_KWARGS, settings.QUERY_GET_ES_KWARGS,
        settings.QUERY_GET_ESQB_KWARGS, settings.QUERY_GET_TRANSFORM_KWARGS]:
        for k, v in d.items():
            if k != 'q' and k in getattr(settings, 'KWARG_DESCRIPTIONS', {}):
                _ret.setdefault(k, v)

    _ret['callback'] = {'type': str, 'default': None}
    _ret['email'] = {'type': str, 'default': None}

    return _ret

def _metadata_params(settings):
    _ret = {}
    for d in [settings.METADATA_GET_CONTROL_KWARGS, settings.METADATA_GET_ES_KWARGS,
        settings.METADATA_GET_ESQB_KWARGS, settings.METADATA_GET_TRANSFORM_KWARGS]:
        for k, v in d.items():
            if k in getattr(settings, 'KWARG_DESCRIPTIONS', {}):
                _ret.setdefault(k, v)

    _ret['callback'] = {'type': str, 'default': None}

    return _ret

def _annotation_get_params(settings):
    _ret = {}
    for d in [settings.ANNOTATION_GET_CONTROL_KWARGS, settings.ANNOTATION_GET_ES_KWARGS,
        settings.ANNOTATION_GET_ESQB_KWARGS, settings.ANNOTATION_GET_TRANSFORM_KWARGS]:
        for k, v in d.items():
            if k != 'q' and k in getattr(settings, 'KWARG_DESCRIPTIONS', {}):
                _ret.setdefault(k, v)

    _ret['callback'] = {'type': str, 'default': None}
    _ret['email'] = {'type': str, 'default': None}

    return _ret

def _query_post_params(settings):
    _ret = {}
    for d in [settings.QUERY_POST_CONTROL_KWARGS, settings.QUERY_POST_ES_KWARGS,
        settings.QUERY_POST_ESQB_KWARGS, settings.QUERY_POST_TRANSFORM_KWARGS]:
        for k, v in d.items():
            if k != 'q' and k != 'scopes' and k in getattr(settings, 'KWARG_DESCRIPTIONS', {}):
                _ret.setdefault(k, v)

    _ret['email'] = {'type': str, 'default': None}

    return _ret

def _annotation_post_params(settings):
    _ret = {}
    for d in [settings.ANNOTATION_POST_CONTROL_KWARGS, settings.ANNOTATION_POST_ES_KWARGS,
        settings.ANNOTATION_POST_ESQB_KWARGS, settings.ANNOTATION_POST_TRANSFORM_KWARGS]:
        for k, v in d.items():
            if k != 'ids' and k in getattr(settings, 'KWARG_DESCRIPTIONS', {}):
                _ret.setdefault(k, v)

    _ret['email'] = {'type': str, 'default': None}

    return _ret

def _get_all_params(settings):
    # returns all possible parameters for a biothing web settings object
    if all_params:
        return all_params

    for d in [settings.ANNOTATION_GET_CONTROL_KWARGS, settings.ANNOTATION_GET_ES_KWARGS,
        settings.ANNOTATION_GET_ESQB_KWARGS, settings.ANNOTATION_GET_TRANSFORM_KWARGS,
        settings.ANNOTATION_POST_CONTROL_KWARGS, settings.ANNOTATION_POST_ES_KWARGS,
        settings.ANNOTATION_POST_ESQB_KWARGS, settings.ANNOTATION_POST_TRANSFORM_KWARGS,
        settings.QUERY_GET_CONTROL_KWARGS, settings.QUERY_GET_ES_KWARGS,
        settings.QUERY_GET_ESQB_KWARGS, settings.QUERY_GET_TRANSFORM_KWARGS,
        settings.QUERY_POST_CONTROL_KWARGS, settings.QUERY_POST_ES_KWARGS,
        settings.QUERY_POST_ESQB_KWARGS, settings.QUERY_POST_TRANSFORM_KWARGS,
        settings.METADATA_GET_CONTROL_KWARGS, settings.METADATA_GET_ES_KWARGS,
        settings.METADATA_GET_ESQB_KWARGS, settings.METADATA_GET_TRANSFORM_KWARGS]:
        for k, v in d.items():
            all_params.setdefault(k, v)

    return all_params

def _query_get_description(settings, biothing_object, url):
    _descriptions = getattr(settings, 'KWARG_DESCRIPTIONS', {})
    _parameters = [OrderedDict([
        ('name', 'q'),
        ('in', 'query'),
        ('description', _descriptions.get('q',{}).get('text_template').format(doc_query_syntax_url='')),
        ('required', True),
        ('example', getattr(settings, 'STATUS_CHECK', {}).get('id', 'EXAMPLE')),
        ('schema', {'type': 'string'})
    ])]
    for k in _query_get_params(settings).keys():
        _parameters.append(
            OrderedDict([
                ('name', _descriptions.get(k).get('name')),
                ('$ref', '#/components/parameters/{}'.format(_descriptions.get(k).get('name')))
            ]))
    return OrderedDict([
        ('tags', ["query"]),
        ('summary', 'Make {biothing_object} queries and return matching {biothing_object} hits. Supports JSONP and CORS as well.'.format(biothing_object=biothing_object)),
        ('parameters', _parameters),
        ('responses', OrderedDict([
            ('200', OrderedDict([
                ('description', 'A query response with the "hits" property'),
                ('content', {'application/json': {'schema': {'$ref': '#/components/schemas/QueryResult'}}})
            ])),
            ('400', OrderedDict([
                ('description', 'A response indicating an improperly formatted query'),
                ('content', {'application/json': {'schema': {'$ref': '#/components/schemas/ErrorResult'}}})
            ]))    
        ]))])

def _query_post_description(settings, biothing_object, url):
    _descriptions = getattr(settings, 'KWARG_DESCRIPTIONS', {})
    _parameters = []
    for k in _query_post_params(settings).keys():
        _parameters.append(
            OrderedDict([
                ('name', _descriptions.get(k).get('name')),
                ('$ref', '#/components/parameters/{}'.format(_descriptions.get(k).get('name')))
            ]))
    return OrderedDict([
        ('tags', ["query"]),
        ('summary', 'Make {biothing_object} batch queries and return matching {biothing_object} hits'.format(biothing_object=biothing_object)),
        ('requestBody', {'content': {'application/x-www-form-urlencoded': {'schema': {'properties': OrderedDict([
            ('q', OrderedDict([
                ('description', 'multiple query terms separated by comma (also "+" or whitespace).  Does not support wildcard queries'),
                ('type', 'string')
            ])),
            ('scopes', OrderedDict([
                ('description', _descriptions.get('scopes', {}).get('text_template', '').format(
                    param_type='string', param_max='')),
                ('type', 'string')
            ]))
        ])}}}}),
        ('parameters', _parameters),
        ('responses', OrderedDict([
            ('200', OrderedDict([
                ('description', 'Query response objects with the "hits" property'),
                ('content', {'application/json': {'schema': {'$ref': '#/components/schemas/QueryPOSTResult'}}})
            ])),
            ('400', OrderedDict([
                ('description', 'A response indicating an improperly formatted query'),
                ('content', {'application/json': {'schema': {'$ref': '#/components/schemas/ErrorResult'}}})
            ]))]))    
    ])

def _annotation_get_description(settings, biothing_object, url):
    _descriptions = getattr(settings, 'KWARG_DESCRIPTIONS', {})
    _parameters = [
        OrderedDict([
            ('name', '{biothing_object}id'.format(biothing_object=biothing_object)),
            ('in', 'path'),
            ('description', '{biothing_object} id'.format(biothing_object=biothing_object)),
            ('required', True),
            ('example', getattr(settings, 'STATUS_CHECK', {}).get('id', 'EXAMPLE')),
            ('schema', {'type': 'string'})
        ])
    ]
    for k in _annotation_get_params(settings).keys():
        _parameters.append(
            OrderedDict([
                ('name', _descriptions.get(k).get('name')),
                ('$ref', '#/components/parameters/{}'.format(_descriptions.get(k).get('name')))
            ]))
    return OrderedDict([
        ('tags', ["{}".format(biothing_object)]),
        ('summary', 'Retrieve {biothing_object} objects based on {biothing_object} id.  Supports JSONP and CORS as well.'.format(biothing_object=biothing_object)),
        ('parameters', _parameters),
        ('responses', OrderedDict([
            ('200', OrderedDict([
                ('description', 'A matching {biothing_object} object'.format(biothing_object=biothing_object)),
                ('content', {'application/json': {'schema': {'$ref': '#/components/schemas/{}'.format(biothing_object.title())}}})
            ])),
            ('404', OrderedDict([
                ('description', 'A response indicating an unknown {biothing_object} id'.format(biothing_object=biothing_object))
            ]))]))    
    ])

def _annotation_post_description(settings, biothing_object, url):
    _descriptions = getattr(settings, 'KWARG_DESCRIPTIONS', {})
    _parameters = []
    for k in _annotation_post_params(settings).keys():
        _parameters.append(
            OrderedDict([
                ('name', _descriptions.get(k).get('name')),
                ('$ref', '#/components/parameters/{}'.format(_descriptions.get(k).get('name')))
            ]))
    return OrderedDict([
        ('tags', ["{biothing_object}".format(biothing_object=biothing_object)]),
        ('summary', 'For a list of {biothing_object} ids, return the matching {biothing_object} object'.format(biothing_object=biothing_object)),
        ('requestBody', {'content': {'application/x-www-form-urlencoded': {'schema': OrderedDict([
            ('properties', {'ids': OrderedDict([
                ('description', _descriptions.get('ids', {}).get('text_template', '').format(
                    biothing_object=biothing_object, param_type=' Type: string (list).', param_default_value='',
                    param_max = _param_max(getattr(settings, 'ANNOTATION_POST_CONTROL_KWARGS', {}).get('max', 1000)))),
                ('type', 'string')
            ])}),
            ('required', ['ids'])])}}}),
        ('parameters', _parameters),
        ('responses', OrderedDict([
            ('200', OrderedDict([
                ('description', 'A list of matching {biothing_object} objects'.format(biothing_object=biothing_object)),
                ('content', {'application/json': {'schema': {'type': 'array', 'items': {'$ref': '#/components/schemas/{}'.format(biothing_object.title())}}}})
            ])),
            ('400', OrderedDict([
                ('description', 'A response indicating an improperly formatted query'),
                ('content', {'application/json': {'schema': {'$ref': '#/components/schemas/ErrorResult'}}})
            ]))]))    
    ])

def _metadata_description(settings, biothing_object, url):
    return OrderedDict([
        ('tags', ['metadata']),
        ('summary', 'Get metadata about the data available from {url}'.format(url=url)),
        ('parameters', [OrderedDict([('name', 'callback'), ('in', 'query'), ('$ref', '#/components/parameters/callback')])]),
        ('responses', {'200': {'description': '{url} metadata object'.format(url=url)}})
    ])

def _metadata_fields_description(settings, biothing_object, url):
    _descriptions = getattr(settings, 'KWARG_DESCRIPTIONS', {})
    _parameters = []
    for k in _metadata_params(settings).keys():
        _parameters.append(
            OrderedDict([
                ('name', _descriptions.get(k).get('name')),
                ('$ref', '#/components/parameters/{}'.format(_descriptions.get(k).get('name')))
            ]))
    return OrderedDict([
        ('tags', ['metadata']),
        ('summary', 'Get metadata about the data fields available from a {url} {biothing_object} object'.format(biothing_object=biothing_object, url=url)),
        ('parameters', _parameters),
        ('responses', {'200': {'description': '{url} metadata fields object'.format(url=url)}})
    ])

def create_openapi_json(_settings, **kwargs):
    _url = getattr(_settings, 'GA_TRACKER_URL', 'MyBiothing.info')
    _biothing_object = getattr(_settings, 'ES_DOC_TYPE', 'biothing')
    _doc = OrderedDict([
        ("openapi", kwargs.get('openapi', '3.0.0')),
        ("info", OrderedDict([
            ("version", kwargs.get('info__version', '{:.1f}'.format(float(getattr(_settings, 'API_VERSION', 'v1').lstrip('v'))))),
            ("title", kwargs.get('info__title', '{} API'.format(getattr(_settings,'GA_TRACKER_URL', 'MyBiothing.info')))),
            ("description", kwargs.get('info__description', getattr(_settings, 'KWARG_DESCRIPTION', {}).get('_root', 'Documentation of the {url} {biothing_object} query web services.  Learn more about [{url}](http://{url}/)'.format(url=_url, biothing_object=_biothing_object)))),
            ("termsOfService", kwargs.get('info__termsOfService', 'http://{url}/terms'.format(url=_url))),
            ("contact", OrderedDict([
                ("name", kwargs.get('info__contact__name', 'Chunlei Wu')),
                ("x-role", kwargs.get('info__contact__x_role', 'responsible developer')),
                ("email", kwargs.get('info__contact__email', 'help@biothings.io')),
                ("x-id", kwargs.get('info__contact__x_id', 'https://github.com/newgene'))
            ]))
        ])),
        ("servers", [OrderedDict([
            ("url", "http://{url}/{version}".format(url=_url, version=getattr(_settings, 'API_VERSION', 'v1'))),
            ("description", "Production server")
        ]),
        OrderedDict([
            ("url", "https://{url}/{version}".format(url=_url, version=getattr(_settings, 'API_VERSION', 'v1'))),
            ("description", "Encrypted Production server")
        ])]),
        ("tags", [{"name": "{biothing_object}".format(biothing_object=_biothing_object)},
                  {"name": "query"},
                  {"name": "metadata"}]),
        ("paths", OrderedDict([
            ("/{biothing_object}/{{{biothing_object}id}}".format(biothing_object=_biothing_object), 
            # annotation GET
            OrderedDict([
                ("get", _annotation_get_description(settings=_settings, biothing_object=_biothing_object, url=_url))
            ])),
            # annotation POST
            ("/{biothing_object}".format(biothing_object=_biothing_object), OrderedDict([
                ("post", _annotation_post_description(settings=_settings, biothing_object=_biothing_object, url=_url))
            ])),
            # query GET/POST
            ("/query", OrderedDict([
                ("get", _query_get_description(settings=_settings, biothing_object=_biothing_object, url=_url)),
                ("post", _query_post_description(settings=_settings, biothing_object=_biothing_object, url=_url))
            ])),
            # metadata GET
            ("/metadata", OrderedDict([
                ("get", _metadata_description(settings=_settings, biothing_object=_biothing_object, url=_url))
            ])),
            # metadata/fields GET
            ("/metadata/fields", OrderedDict([
                ("get", _metadata_fields_description(settings=_settings, biothing_object=_biothing_object, url=_url))
            ]))
        ])),
        ("components", OrderedDict([
            ("parameters", _get_common_parameters(settings=_settings, biothing_object=_biothing_object, url=_url)),
            ("schemas", _get_schemas(settings=_settings, biothing_object=_biothing_object, url=_url))
        ]))
    ])
    return _doc

def create_openapi_yaml(settings, yaml_file, **kwargs):
    ''' Create an openapi compliant spec for the biothing project whose settings are input.

        settings - a biothings web settings object
        yaml_file - the output file
    '''
    setup_yaml()
    _json = create_openapi_json(settings, **kwargs)
    with open(yaml_file, 'w') as outf:
        yaml.dump(_json, outf, default_flow_style=False)
