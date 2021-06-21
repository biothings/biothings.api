from copy import deepcopy

import pytest
from biothings.web.options.openapi import *

_base_document = {
    'openapi': '3.0.3',
    'paths': {},
}


def test_create():
    o = OpenAPIDocumentBuilder()
    r = deepcopy(_base_document)
    assert o.document == r


def test_create_child_context():
    o = OpenAPIDocumentBuilder()
    o.info()
    r = deepcopy(_base_document)
    r['info'] = {}
    assert o.document == r


def test_create_child_context_set_attrib():
    o = OpenAPIDocumentBuilder()
    o.info()\
        .title('t')\
        .version('v')
    r = deepcopy(_base_document)
    r['info'] = {'title': 't', 'version': 'v'}
    assert o.document == r


def test_create_child_context_with_params():
    o = OpenAPIDocumentBuilder()
    o.info(title='t', version='v')
    r = deepcopy(_base_document)
    r['info'] = {'title': 't', 'version': 'v'}
    assert o.document == r


def test_explicit_end_context():
    o = OpenAPIDocumentBuilder()
    c = o.info().end()
    assert isinstance(c, OpenAPIContext)


def test_implicit_context_change_one_level():
    o = OpenAPIDocumentBuilder()
    c = o.info()\
        .server('http://example.org/')
    assert isinstance(c, OpenAPIContext)


def test_implicit_change_multi_level():
    o = OpenAPIDocumentBuilder()
    c = o.path('/api')\
        .get()\
        .parameter('p', in_='query', required=False)\
        .path('/api2')
    assert isinstance(c, OpenAPIPathItemContext)


def test_non_allowed_multilevel_raises():
    o = OpenAPIDocumentBuilder()
    with pytest.raises(AttributeError):
        o.path('/api')\
            .get()\
            .parameter('p', in_='query', required=False)\
            .info(title='t')


def test_bad_kwarg_raises():
    o = OpenAPIDocumentBuilder()
    with pytest.raises(AttributeError):
        o.info(non_existent_attrib_as_param='v')


def test_set_server():
    o = OpenAPIDocumentBuilder()
    o.server('http://example.org/api', description='Example1')
    o.server('https://example.org/api')
    r = deepcopy(_base_document)
    r['servers'] = [
        {'url': 'http://example.org/api',
         'description': 'Example1'},
        {'url': 'https://example.org/api'}
    ]
    assert o.document == r


def test_add_path():
    o = OpenAPIDocumentBuilder()
    o.path('/endpoint1', summary='s', description='d')
    r = deepcopy(_base_document)
    r['paths'] = {
        '/endpoint1': {'summary': 's', 'description': 'd'},
    }
    assert o.document == r


def test_operation_init():
    o = OpenAPIDocumentBuilder()
    o.path('/endpoint1', summary='s', description='d')\
        .get()
    r = deepcopy(_base_document)
    r['paths'] = {
        '/endpoint1': {
            'summary': 's',
            'description': 'd',
            'get': {
                'responses': {
                    '200': {
                        'description': "Success",
                    },
                },
            }
        },
    }
    assert o.document == r


def test_parameter_init():
    o = OpenAPIDocumentBuilder()
    o.path('/e/{p}', summary='s', description='d')\
        .parameter('p', in_='path', required=True)
    r = deepcopy(_base_document)
    r['paths'] = {
        '/e/{p}': {
            'summary': 's',
            'description': 'd',
            'parameters': [
                {'name': 'p',
                 'in': 'path',
                 'required': True, },
            ]
        },
    }
    assert o.document == r


def test_parameter_type():
    o = OpenAPIDocumentBuilder()
    o.path('/e/{p}', summary='s', description='d')\
        .parameter('p', in_='path', required=True)\
        .type('integer', default=0)
    r = deepcopy(_base_document)
    r['paths'] = {
        '/e/{p}': {
            'summary': 's',
            'description': 'd',
            'parameters': [
                {'name': 'p',
                 'in': 'path',
                 'required': True,
                 'schema': {
                     'type': 'integer',
                     'default': 0,
                 }},
            ]
        },
    }
    assert o.document == r


def test_tags():
    o = OpenAPIDocumentBuilder()
    o.path('/api', summary='s', description='d')\
        .get()\
        .tag('t1')\
        .tag('t2')
    r = deepcopy(_base_document)
    r['paths'] = {
        '/api': {
            'summary': 's',
            'description': 'd',
            'get': {
                'tags': ['t1', 't2'],
                'responses': {
                    '200': {
                        'description': "Success",
                    },
                },
            },
        },
    }
    assert o.document == r


def test_method_name_different_field():
    o = OpenAPIDocumentBuilder()
    o.path('/api', summary='s', description='d')\
        .get()\
        .operation_id('opid')
    r = deepcopy(_base_document)
    r['paths'] = {
        '/api': {
            'summary': 's',
            'description': 'd',
            'get': {
                'operationId': 'opid',
                'responses': {
                    '200': {
                        'description': "Success",
                    },
                },
            },
        },
    }
    assert o.document == r


def test_x_extension():
    o = OpenAPIDocumentBuilder()
    o.info().x_test('test')
    r = deepcopy(_base_document)
    r['info'] = {'x-test': 'test'}
    assert o.document == r


def test_invalid_extension_field():
    o = OpenAPIDocumentBuilder()
    with pytest.raises(ValueError):
        o.info().x_test('test', 'bad-field')
