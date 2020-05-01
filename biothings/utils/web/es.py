import logging
from collections import OrderedDict

import elasticsearch
import elasticsearch_dsl

from biothings.utils.common import is_seq
from biothings.utils.doc_traversal import depth_first_traversal


async def get_es_versions(client):
    es_version = 'unknown'
    es_cluster = 'unknown'
    try:
        info = await client.info(request_timeout=3)
        version = info['version']['number']
        cluster = info['cluster_name']
        health = await client.cluster.health(request_timeout=3)
        status = health['status']
    except elasticsearch.TransportError as exc:
        logger = logging.getLogger(__name__)
        logger.error('Error reading elasticsearch status.')
        logger.debug(exc)
    else:
        es_version = version
        es_cluster = f"{cluster} ({status})"
    return {
        "elasticsearch_version": es_version,
        "elasticsearch_cluster": es_cluster
    }

def exists_or_null(doc, field, val=None):
    def _helper(doc, _list, val):
        if isinstance(doc, dict):
            if len(_list) > 1:
                if _list[0] not in doc:
                    doc[_list[0]] = {}
                _helper(doc[_list[0]], _list[1:], val)
            else:
                if _list[0] not in doc:
                    doc[_list[0]] = val
        elif is_seq(doc):
            for o in doc:
                _helper(o, _list, val)

    _helper(doc, list(field.split('.')), val)

    return doc


def flatten_doc_2(doc, outfield_sep='.', sort=True):
    _ret = {}
    for _path, _val in depth_first_traversal(doc):
        if not isinstance(_val, dict) and not is_seq(_val):
            if outfield_sep:
                _new_path = outfield_sep.join(_path)
            else:
                _new_path = _path
            _ret.setdefault(_new_path, []).append(_val)
    if sort and outfield_sep:
        return OrderedDict(sorted([(k, v[0]) if len(v) == 1 else (k, v) for (k, v) in _ret.items()], key=lambda x: x[0]))
    return dict([(k, v[0]) if len(v) == 1 else (k, v) for (k, v) in _ret.items()])


def flatten_doc(doc, outfield_sep='.', sort=True):
    ''' This function will flatten an elasticsearch document (really any json object).
        outfield_sep is the separator between the fields in the return object.
        sort specifies whether the output object should be sorted alphabetically before returning
            (otherwise output will remain in traveral order) '''

    def _recursion_helper(_doc, _ret, out):
        if isinstance(_doc, dict):
            for key in _doc:
                if outfield_sep:
                    new_key = key if not out else outfield_sep.join([out, key])
                else:
                    new_key = tuple([key]) if not out else tuple(list(tuple(out)) + [key])
                _recursion_helper(_doc[key], _ret, new_key)
        elif is_seq(_doc):
            for _obj in _doc:
                _recursion_helper(_obj, _ret, out)
        else:
            # this is a leaf
            _ret.setdefault(out, []).append(_doc)

    ret = {}
    _recursion_helper(doc, ret, '')
    if sort and outfield_sep:
        return OrderedDict(sorted([(k, v[0]) if len(v) == 1 else (k, v) for (k, v) in ret.items()], key=lambda x: x[0]))
    return dict([(k, v[0]) if len(v) == 1 else (k, v) for (k, v) in ret.items()])
