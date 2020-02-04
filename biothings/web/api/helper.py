import datetime
import json
import logging
import re
from collections import OrderedDict
from urllib.parse import (parse_qs, unquote_plus, urlencode, urlparse,
                          urlunparse)

import tornado.web
from tornado.escape import json_decode

from biothings.utils.common import is_seq, is_str, split_ids
from biothings.utils.web.analytics import GAMixIn
from biothings.utils.web.tracking import StandaloneTrackingMixin

try:
    from raven.contrib.tornado import SentryMixin
except ImportError:
    # dummy class mixin
    class SentryMixin(object):
        pass

try:
    from re import fullmatch as match
except ImportError:
    from re import match

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

class DateTimeJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        else:
            return super(DateTimeJSONEncoder, self).default(obj)

class BiothingParameterTypeError(Exception):
    def __init__(self, error='', param=''):
        message = f'Cannot process parameter {param}. ' if param else ''
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

    @staticmethod
    def str_to_bool(val, param=''):
        return val.lower() in ('1', 'true', 'y', 't')

    def str_to_list(self, val, param=''):
        if self.jsoninput:
            try:
                val = json.loads(val)
            except Exception:
                pass
        if not isinstance(val, list):
            try:
                val = split_ids(val)
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


class BaseHandler(SentryMixin, tornado.web.RequestHandler, GAMixIn, StandaloneTrackingMixin):
    ''' Parent class of all biothings handlers, only direct descendant of
        `tornado.web.RequestHandler <http://www.tornadoweb.org/en/stable/web.html#tornado.web.RequestHandler>`_, 
        contains the common functions in the biothings handler universe:

            * return `self` as JSON
            * set CORS and caching headers
            * typify the URL keyword arguments
            * optionally send tracking data to google analytics and integrate with sentry monitor'''

    def initialize(self, web_settings):
        """ Tornado handler `initialize() <http://www.tornadoweb.org/en/stable/web.html#tornado.web.RequestHandler.initialize>`_, 
        Override to add settings for *this* biothing API.  Assumes that the ``web_settings`` kwarg exists in APP_LIST """
        self.web_settings = web_settings
        self.ga_event_object_ret = {'category': '{}_api'.format(self.web_settings.API_VERSION)}
        self.kwarg_settings = {}

    def _format_log_exception_message(self, msg='', delim="-"*30):
        return "{msg}\n\nError message:\n{delim}\n{msg}\n\nRequest parameters:\n{delim}\n{req}\n\nTraceback:\n{delim}\n".format(msg=msg, delim=delim, req=self.request)

    def log_exceptions(self, exception_msg=''):
        """ Logs the current exception in tornado logs.
            This must be called in an exception handler """
        _msg = self._format_log_exception_message(exception_msg)
        logging.exception(_msg)

    def ga_event_object(self, data={}):
        ''' Create the data object for google analytics tracking. '''
        # Most of the structure of this object is formed during self.initialize
        if data:
            self.ga_event_object_ret['label'] = list(data.keys()).pop()
            self.ga_event_object_ret['value'] = list(data.values()).pop()
        return self.ga_event_object_ret

    def _sanitize_params(self, args):
        ''' Subclass to implement custom parameter sanitization '''
        self.jsonp = args.pop(self.web_settings.JSONP_PARAMETER, None)
        return args

    def get_query_params(self):
        '''Extract, typify, and sanitize the parameters from the URL query string. '''

        # extract and combine url parameters and body parameters

        args_json = {}
        if self.request.headers.get('Content-Type') in ('application/x-json', 'application/json'):
            try:
                args_json = json_decode(self.request.body)
            except Exception:
                raise BiothingParameterTypeError('Json decoder error')

        args_query = {k: self.get_argument(k) for k in self.request.arguments}
        args = dict(args_json)
        args.update(args_query)

        # apply key alias transformation
        
        aliases = {}
        for _arg, _setting in self.kwarg_settings.items():
            if 'alias' in _setting:
                if isinstance(_setting['alias'], str):
                    aliases[_setting['alias']] = _arg
                else:
                    for _alias in _setting['alias']:
                        aliases[_alias] = _arg
            
        for key in args:
            if key in aliases:
                args.setdefault(aliases[key], args[key])
                del args[key]

        # perform value type validation and regex substitution

        totype = {}
        for key in args:
            if key in self.kwarg_settings:
                if 'type' in self.kwarg_settings[key]:
                    type_ = self.kwarg_settings[key]['type']
                    if not isinstance(args[key], type_):
                        totype[key] = type_

        for key in args:
            if 'translations' in self.kwarg_settings.get(key,{}):
                if isinstance(args[key], str):
                    for (regex, translation) in self.kwarg_settings[key]['translations']:
                        args[key] = re.sub(regex, translation, args[key])        

        # apply value type conversion

        param = BiothingsQueryParamInterpreter(args.get('jsoninput',''))
        for key, type_ in totype.items():
            if isinstance(args[key], str) and type_ == bool:
                args[key] = param.str_to_bool(args[key])
            elif isinstance(args[key], str) and type_ == list:
                args[key] = param.str_to_list(args[key])
            else:
                args[key] = param.obj_to_type(args[key], type_)

        for key in args:
            if isinstance(args[key], list):
                args[key] = args[key][:self.kwarg_settings[key].get('max',
                        getattr(self.web_settings, 'LIST_SIZE_CAP', 1000))]

        args = self._sanitize_params(args)
        return args

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
        self.write(self.web_settings.HTML_OUT_TEMPLATE.format(data=json.dumps(data), 
            img_src=self.web_settings.HTML_OUT_HEADER_IMG, link=_link, link_decode=unquote_plus(_link),
            title_html=self.web_settings.HTML_OUT_TITLE, docs_link=_docs))
        return

    def return_yaml(self, data, status_code=200):

        if not SUPPORT_YAML:
            self.set_status(500)
            self.write({"success":False,"error":"Extra requirements for biothings.web needed."})
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
        elif  _format == 'yaml':
            self.return_yaml(data=data, status_code=status_code)
            return
        elif SUPPORT_MSGPACK and self.web_settings.ENABLE_MSGPACK and _format == 'msgpack':
            self.return_json(data=data, encode=encode, indent=indent, status_code=status_code, is_msgpack=True)
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
            _json_data = json.dumps(data, cls=DateTimeJSONEncoder, indent=indent) if encode else data
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
        self.set_header("Cache-Control", "max-age={}, public".format(self.web_settings.CACHE_MAX_AGE))
        if etag:
            self.set_header('Etag', etag)

    def support_cors(self, *args, **kwargs):
        '''Provide server side support for CORS request.'''
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Methods", "{}".format(self.web_settings.ACCESS_CONTROL_ALLOW_METHODS))
        self.set_header("Access-Control-Allow-Headers",
                        "{}".format(self.web_settings.ACCESS_CONTROL_ALLOW_HEADERS))
        self.set_header("Access-Control-Allow-Credentials", "false")
        self.set_header("Access-Control-Max-Age", "60")

    def options(self, *args, **kwargs):
        self.support_cors()
