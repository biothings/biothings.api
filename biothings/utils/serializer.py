import datetime
import json
from collections import OrderedDict, UserString
from urllib.parse import parse_qs, unquote_plus, urlencode, urlparse, urlunparse

import yaml
from biothings.utils.common import BiothingsJSONEncoder


def to_json(data):
    return json.dumps(data, cls=BiothingsJSONEncoder)

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
