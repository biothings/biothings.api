import datetime
from collections import OrderedDict, UserString, UserDict, UserList
from urllib.parse import parse_qs, unquote_plus, urlencode, urlparse, urlunparse

import orjson
import yaml


def to_json_0(data):
    '''deprecated'''
    import json
    from biothings.utils.common import BiothingsJSONEncoder

    return json.dumps(data, cls=BiothingsJSONEncoder)


def orjson_default(o):
    '''The default function passed to orjson to serialize non-serializable objects'''
    if isinstance(o, (UserDict, UserList)):
        return o.data     # o.data is the actual dictionary of list to store the data
    raise TypeError(f"Type {type(o)} not serializable")


def to_json(data, indent=False, sort_keys=False):
    # default option:
    #    OPT_NON_STR_KEYS: non string dictionary key, e.g. integer
    #    OPT_NAIVE_UTC: use UTC as the timezone when it's missing
    option = orjson.OPT_NON_STR_KEYS | orjson.OPT_NAIVE_UTC
    if indent:
        option |= orjson.OPT_INDENT_2
    if sort_keys:
        option |= orjson.OPT_SORT_KEYS
    return orjson.dumps(data, default=orjson_default, option=option).decode()


def to_yaml(data, stream=None, Dumper=yaml.SafeDumper, default_flow_style=False):
    # Author: Cyrus Afrasiabi

    class OrderedDumper(Dumper):
        pass

    def _dict_representer(dumper, data):
        return dumper.represent_mapping(
            yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
            data.items())

    OrderedDumper.add_representer(OrderedDict, _dict_representer)
    return yaml.dump(data, stream, OrderedDumper, default_flow_style=default_flow_style)

def to_msgpack(data):
    import msgpack
    return msgpack.packb(data, use_bin_type=True, default=_msgpack_encode_datetime)


def _msgpack_encode_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return {
            '__datetime__': True,
            'as_str': obj.strftime("%Y%m%dT%H:%M:%S.%f")
        }
    return obj

class URL(UserString):
    def remove(self, param='format'):
        urlparsed = urlparse(str(self))
        qs = parse_qs(urlparsed.query)
        qs.pop(param, None)
        qs = urlencode(qs, True)
        urlparsed = urlparsed._replace(query=qs)
        url = urlunparse(urlparsed)
        return unquote_plus(url)
