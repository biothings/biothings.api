import datetime
from collections import OrderedDict, UserDict, UserList, UserString
from typing import Any, Union
from urllib.parse import parse_qs, unquote_plus, urlencode, urlparse, urlunparse

import msgspec
import yaml


def load_json(json_str: Union[bytes, str]) -> Any:
    """Load a JSON string or bytes using msgspec"""
    return msgspec.json.decode(json_str)


def to_json_0(data):
    """deprecated"""
    import json

    from biothings.utils.common import BiothingsJSONEncoder

    return json.dumps(data, cls=BiothingsJSONEncoder)


def json_enc_hook(o):
    """The hook passed to msgspec to serialize otherwise non-serializable objects"""
    if isinstance(o, (UserDict, UserList)):
        return o.data  # o.data is the actual dictionary of list to store the data
    raise TypeError(f"Type {type(o)} not serializable")


# deprecated alias, kept for backward compatibility (pre-msgspec name)
orjson_default = json_enc_hook


# orjson's OPT_NON_STR_KEYS coerced any non-string dict key to a string. msgspec
# stringifies int/float/datetime/uuid/enum keys natively but rejects bool/None
# keys (and rejects ANY non-str key when order="sorted"). When that happens we
# coerce all non-string keys ourselves, matching orjson (None->"null",
# True/False->"true"/"false", everything else via str()).
def _coerce_key(k):
    if isinstance(k, str):
        return k
    if k is None:
        return "null"
    if isinstance(k, bool):
        return "true" if k else "false"
    return str(k)


def _stringify_keys(obj):
    """Recursively replace non-string dict keys with their string form.
    Only invoked as a fallback after a failed encode, so it never touches the
    common all-string-keys payload."""
    if isinstance(obj, dict):
        return {_coerce_key(k): _stringify_keys(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_stringify_keys(v) for v in obj]
    return obj


def to_json(data, indent=False, sort_keys=False, return_bytes=False):
    # msgspec handles non-string dictionary keys (e.g. integer), inf/nan
    # (encoded as null) and datetimes natively. Note: unlike orjson with
    # OPT_NAIVE_UTC, naive datetimes are encoded without a UTC offset suffix.
    order = "sorted" if sort_keys else None
    try:
        byte_dump = msgspec.json.encode(data, enc_hook=json_enc_hook, order=order)
    except TypeError:
        # a non-string dict key msgspec won't take (bool/None, or any non-str
        # key when sorting); coerce keys like orjson's OPT_NON_STR_KEYS, retry
        byte_dump = msgspec.json.encode(_stringify_keys(data), enc_hook=json_enc_hook, order=order)
    if indent:
        byte_dump = msgspec.json.format(byte_dump, indent=2)
    if return_bytes:
        return byte_dump

    return byte_dump.decode()


def to_json_file(data, fobj, indent=False, sort_keys=False):
    json_str = to_json(data, indent=indent, sort_keys=sort_keys)
    fobj.write(json_str)


# define aliases close to json.loads and json.dumps for convenience
json_loads = load_json
json_dumps = to_json


def to_yaml(data, stream=None, Dumper=yaml.SafeDumper, default_flow_style=False):
    # Author: Cyrus Afrasiabi

    class OrderedDumper(Dumper):
        pass

    def _dict_representer(dumper, data):
        return dumper.represent_mapping(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, data.items())

    OrderedDumper.add_representer(OrderedDict, _dict_representer)
    return yaml.dump(data, stream, OrderedDumper, default_flow_style=default_flow_style)


def to_msgpack(data):
    import msgpack  # import here so that we don't have to install msgpack if we don't use it

    return msgpack.packb(data, use_bin_type=True, default=_msgpack_encode_datetime)


def _msgpack_encode_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return {"__datetime__": True, "as_str": obj.strftime("%Y%m%dT%H:%M:%S.%f")}
    return obj


class URL(UserString):
    def remove(self, param="format"):
        urlparsed = urlparse(str(self))
        qs = parse_qs(urlparsed.query)
        qs.pop(param, None)
        qs = urlencode(qs, True)
        urlparsed = urlparsed._replace(query=qs)
        url = urlunparse(urlparsed)
        return unquote_plus(url)


# keep it here to be used by other modules
# (unlike orjson's, msgspec's DecodeError is NOT a subclass of json.JSONDecodeError:
# always catch serializer.JSONDecodeError around load_json, not json.JSONDecodeError)
JSONDecodeError = msgspec.DecodeError
