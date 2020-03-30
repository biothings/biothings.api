import datetime
import json
import logging
import re
from collections import OrderedDict
from itertools import chain
from pprint import pformat
from urllib.parse import (parse_qs, unquote_plus, urlencode, urlparse,
                          urlunparse)

import tornado.web
from biothings.utils.common import dotdict, is_seq, is_str, split_ids
from tornado.escape import json_decode

from biothings.utils.common import split_ids, DateTimeJSONEncoder
from biothings.utils.web.analytics import GAMixIn
from biothings.utils.web.tracking import StandaloneTrackingMixin
from tornado.escape import json_decode
from tornado.web import Finish, RequestHandler


try:
    from raven.contrib.tornado import SentryMixin
except ImportError:
    # dummy class mixin
    class SentryMixin(object):
        pass

# TODO: remove this unused import
# try:
#     from re import fullmatch as match
# except ImportError:
#     from re import match

try:
    import msgpack

    def msgpack_encode_datetime(obj):
        if isinstance(obj, datetime.datetime):
            return {'__datetime__': True, 'as_str': obj.strftime("%Y%m%dT%H:%M:%S.%f")}
        return obj
    SUPPORT_MSGPACK = True
except ImportError:
    SUPPORT_MSGPACK = False

try:
    import yaml
    SUPPORT_YAML = True
except ImportError:
    SUPPORT_YAML = False

# TODO: remove it. dup of biothings.commons.DateTimeJSONEncoder
# class DateTimeJSONEncoder(json.JSONEncoder):
#     def default(self, obj):
#         if isinstance(obj, datetime.datetime):
#             return obj.isoformat()
#         else:
#             return super(DateTimeJSONEncoder, self).default(obj)

class BiothingParameterTypeError(Exception):

    def __init__(self, error='', param=''):
        message = f'Cannot process parameter {param}. ' if param else ''  # TODO reword
        message += error
        if message:
            super().__init__(message)
        else:
            super().__init__()


class BiothingsQueryParamInterpreter():

    def __init__(self, jsoninput=False):
        '''
            Parameter sets the support for json string list
        '''
        if isinstance(jsoninput, str):
            self.jsoninput = self.str_to_bool(jsoninput)
        else:
            self.jsoninput = bool(jsoninput)

    def convert(self, value, to_type):
        if isinstance(value, to_type):
            return value
        if isinstance(value, str) and to_type == bool:  # TODO use dispatch syntax
            return self.str_to_bool(value)
        elif isinstance(value, str) and to_type == list:
            return self.str_to_list(value)
        elif isinstance(value, (int, float)):
            return self.convert(str(value), to_type)
        else:
            return self.obj_to_type(value, to_type)

    @staticmethod
    def str_to_bool(val):
        return val.lower() in ('1', 'true', 'y', 't', '')  # TODO maybe set a switch

    def str_to_list(self, val, param=''):
        if self.jsoninput:
            try:
                val = json.loads(val)
            except Exception:
                pass
        if not isinstance(val, list):
            try:
                val = split_ids(str(val))
            except ValueError as e:
                raise BiothingParameterTypeError(str(e), param)
        return val

    @staticmethod
    def obj_to_type(val, type_, param=''):
        try:
            result = (type_)(val)
        except ValueError:
            raise BiothingParameterTypeError(f"Expect type {type_.__name__}.", param)
        return result

        # TODO the two above can be put to utils

class GoogleAnalyticsMixIn(GAMixIn):  # TODO maybe not necessary as we are putting most features into the base_handler
    pass


class BaseHandler(SentryMixin, RequestHandler, GAMixIn, StandaloneTrackingMixin):
    """
        Parent class of all biothings handlers, only direct descendant of
        `tornado.web.RequestHandler <http://www.tornadoweb.org/en/stable/web.html#tornado.web.RequestHandler>`_,
        contains the common functions in the biothings handler universe:

            * return `self` as JSON
            * set CORS and caching headers
            * typify the URL keyword arguments
            * optionally send tracking data to google analytics and integrate with sentry monitor
    """
    json_arguments = {}

    @classmethod
    def setup(cls, web_settings):
        '''
        Override me to extend class level setup.
        Called in generate API.
        '''
        cls.web_settings = web_settings

    def prepare(self):
        '''
        Extract body and url query parameters.
        Override to add more customizations.
        Typify predefined user inputs patterns here.
        '''
        if self.request.headers.get('Content-Type') in ('application/x-json', 'application/json'):
            try:
                self.json_arguments = json_decode(self.request.body)
            except Exception:
                self.send_error(400, reason='Invalid JSON body.')
                raise Finish()

    def initialize(self):
        """
        Tornado handler `initialize() <http://www.tornadoweb.org/en/stable/web.html#tornado.web.RequestHandler.initialize>`_,
        Override to add settings for *this* biothing API.  Assumes that the ``web_settings`` kwarg exists in APP_LIST
        """
        self.ga_event_object_ret = {
            'category': '{}_api'.format(self.web_settings.API_VERSION)
            # 'action': 'query_get', 'gene_post', 'fetch_all', etc.
            # 'label': 'total', 'qsize', etc.
            # 'value': 0, corresponds to label ...
        }

    def on_finish(self):
        """
        This is a tornado lifecycle hook.
        Override to provide tracking features.
        """
        self.ga_track(event=self.ga_event_object_ret)
        self.self_track(data=self.ga_event_object_ret)

    def get_sentry_client(self):
        """
        Override the default behavior and retrieve from app setting instead.
        """
        return self.settings.get('sentry_client')

    def log_exception(self, *args, **kwargs):
        """
        Only attempt to report to Sentry when the client key is provided.
        """
        if self.settings.get('sentry_client'):
            return super(BaseHandler, self).log_exception(*args, **kwargs)
        else:
            return super(SentryMixin, self).log_exception(*args, **kwargs)

    def ga_event_object(self, data=None):
        ''' Create the data object for google analytics tracking. '''
        # Most of the structure of this object is formed during self.initialize
        if data and isinstance(data, dict):
            self.ga_event_object_ret['label'] = list(data.keys()).pop()
            self.ga_event_object_ret['value'] = list(data.values()).pop()
        return self.ga_event_object_ret

    def return_html(self, data, status_code=200):
        self.set_status(status_code)
        if not self.web_settings.DISABLE_CACHING:
            #get etag if data is a dictionary and has "etag" attribute.
            etag = data.get('etag', None) if isinstance(data, dict) else None
            self.set_cacheable(etag=etag)
        self.support_cors()
        self.set_header("Content-Type", "text/html; charset=utf-8")
        _link = self.request.full_url()
        d = urlparse(_link)
        if 'metadata' in d.path:
            _docs = self.web_settings.METADATA_DOCS_URL
        elif 'query' in d.path:
            _docs = self.web_settings.QUERY_DOCS_URL
        else:
            _docs = self.web_settings.ANNOTATION_DOCS_URL
        q = parse_qs(d.query)
        q.pop('format', None)
        d = d._replace(query=urlencode(q, True))
        _link = urlunparse(d)
        self.write(
            self.web_settings.HTML_OUT_TEMPLATE.format(
                data=json.dumps(data),
                img_src=self.web_settings.HTML_OUT_HEADER_IMG,
                link=_link,
                link_decode=unquote_plus(_link),
                title_html=self.web_settings.HTML_OUT_TITLE,
                docs_link=_docs
            )
        )
        return

    def return_yaml(self, data, status_code=200):

        if not SUPPORT_YAML:
            self.set_status(500)
            self.write({"success": False, "error": "Extra requirements for biothings.web needed."})
            return

        def ordered_dump(data, stream=None, Dumper=yaml.Dumper, **kwds):
            class OrderedDumper(Dumper):
                pass

            def _dict_representer(dumper, data):
                return dumper.represent_mapping(
                    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
                    data.items())
            OrderedDumper.add_representer(OrderedDict, _dict_representer)
            return yaml.dump(data, stream, OrderedDumper, **kwds)

        self.set_status(status_code)
        self.set_header("Content-Type", "text/x-yaml; charset=UTF-8")
        self.support_cors()
        self.write(ordered_dump(
            data=data, Dumper=yaml.SafeDumper, default_flow_style=False))

    def return_object(self, data, encode=True, indent=None, status_code=200, _format='json'):
        '''Return passed data object as the proper response.

        :param data: object to return as JSON
        :param encode: if encode is False, assumes input data is already a JSON encoded string.
        :param indent: number of indents per level in JSON string
        :param status_code: HTTP status code for response
        :param _format: output format - currently supports "json", "html", "yaml", or "msgpack"
        '''
        if _format == 'html':
            self.return_html(data=data, status_code=status_code)
            return
        elif _format == 'yaml':
            self.return_yaml(data=data, status_code=status_code)
            return
        elif SUPPORT_MSGPACK and self.web_settings.ENABLE_MSGPACK and _format == 'msgpack':
            self.return_json(
                data=data,
                encode=encode,
                indent=indent,
                status_code=status_code,
                is_msgpack=True)
            return
        else:
            self.return_json(data=data, encode=encode, indent=indent, status_code=status_code)

    def return_json(self, data, encode=True, indent=None, status_code=200, is_msgpack=False):
        '''Return passed data object as JSON response.
        If **jsonp** parameter is set in the  request, return a valid
        `JSONP <https://en.wikipedia.org/wiki/JSONP>`_ response.

        :param data: object to return as JSON
        :param encode: if encode is False, assumes input data is already a JSON encoded string.
        :param indent: number of indents per level in JSON string
        :param status_code: HTTP status code for response
        :param is_msgpack: should this object be compressed before return?
        '''
        indent = indent or 2   # tmp settings
        self.set_status(status_code)
        if is_msgpack:
            _json_data = msgpack.packb(data, use_bin_type=True, default=msgpack_encode_datetime)
            self.set_header("Content-Type", "application/x-msgpack")
        else:
            _json_data = json.dumps(data, cls=DateTimeJSONEncoder,
                                    indent=indent) if encode else data
            self.set_header("Content-Type", "application/json; charset=UTF-8")
        if not self.web_settings.DISABLE_CACHING:
            #get etag if data is a dictionary and has "etag" attribute.
            etag = data.get('etag', None) if isinstance(data, dict) else None
            self.set_cacheable(etag=etag)
        self.support_cors()
        if getattr(self, 'jsonp', False):
            self.write('%s(%s)' % (self.jsonp, _json_data))
        else:
            self.write(_json_data)

    def set_cacheable(self, etag=None):
        '''set proper header to make the response cacheable.
           set etag if provided.
        '''
        self.set_header("Cache-Control",
                        "max-age={}, public".format(self.web_settings.CACHE_MAX_AGE))
        if etag:
            self.set_header('Etag', etag)

    def support_cors(self, *args, **kwargs):
        '''Provide server side support for CORS request.'''
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Methods",
                        "{}".format(self.web_settings.ACCESS_CONTROL_ALLOW_METHODS))
        self.set_header("Access-Control-Allow-Headers",
                        "{}".format(self.web_settings.ACCESS_CONTROL_ALLOW_HEADERS))
        self.set_header("Access-Control-Allow-Credentials", "false")
        self.set_header("Access-Control-Max-Age", "60")

    def options(self, *args, **kwargs):
        pass  # TODO

    def set_default_headers(self):
        pass  # TODO
