import json
import datetime
import tornado.web
import re
from biothings.utils.web.analytics import GAMixIn
from biothings.utils.web.tracking import StandaloneTrackingMixin
from biothings.utils.common import is_str, is_seq
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
import logging

try:
    import msgpack

    def msgpack_encode_datetime(obj):
        if isinstance(obj, datetime.datetime):
            return {'__datetime__': True, 'as_str': obj.strftime("%Y%m%dT%H:%M:%S.%f")}
        return obj
    SUPPORT_MSGPACK = True
except ImportError:
    SUPPORT_MSGPACK = False

class DateTimeJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        else:
            return super(DateTimeJSONEncoder, self).default(obj)

class BiothingParameterTypeError(Exception):
    pass

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
        """ Logs the current exception in tornado logs and in hipchat room if available.
            This must be called in an exception handler """
        _msg = self._format_log_exception_message(exception_msg)
        if self.web_settings._hipchat_logger:
            self.web_settings._hipchat_logger.exception(_msg)
        else:
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
        self.use_msgpack = args.pop('msgpack', False) if SUPPORT_MSGPACK else False
        return args

    def _typify(self, arg, argval, json_list_input=False):
        ''' Try to get the parameter's type from settings '''
        # first see if this parameter has an alias
        # do value translations, if they exist
        if 'type' not in self.kwarg_settings[arg]:
            return argval

        if self.kwarg_settings[arg]['type'] == list:
            ret = []
            if json_list_input:
                try:
                    ret = json.loads(argval)
                    if not isinstance(ret, list):
                        raise ValueError
                except Exception:
                    ret = []
                    #raise BiothingParameterTypeError('Could not listify "{}" in parameter "{}" with "jsoninput" True'.format(argval, arg))
            if not ret:
                ret = [x for x in re.split(getattr(self.web_settings, 'LIST_SPLIT_REGEX', '[\s\r\n+|,]+'), argval) if x]
            ret = ret[:self.kwarg_settings[arg].get('max', getattr(self.web_settings, 'LIST_SIZE_CAP', 1000))]
        elif self.kwarg_settings[arg]['type'] == int:
            try:
                ret = int(argval)
            except ValueError:
                raise BiothingParameterTypeError("Expected '{0}' parameter to have integer type.  Couldn't convert '{1}' to integer".format(arg, argval))
        elif self.kwarg_settings[arg]['type'] == float:
            try:
                ret = float(argval)
            except ValueError:
                raise BiothingParameterTypeError("Expected '{0}' parameter to have float type.  Couldn't convert '{1}' to float".format(arg, argval))
        elif self.kwarg_settings[arg]['type'] == bool:
            ret = self._boolify(argval)
        else:
            ret = argval
        return ret

    def _boolify(self, val):
        return val.lower() in ['1', 'true', 'y', 't']

    def _translate_input_arg_value(self, arg, argval):
        if 'translations' in self.kwarg_settings[arg]:
            for (regex, translation) in self.kwarg_settings[arg]['translations']:
                argval = re.sub(regex, translation, argval)
        return argval 

    def _translate_and_typify_arg_values(self, args, json_list_input=False):
        for _arg in list(args.keys()):
            if _arg in self.kwarg_settings:
                args[_arg] = self._translate_input_arg_value(_arg, args[_arg])
                args[_arg] = self._typify(_arg, args[_arg], json_list_input=json_list_input)
        return args
    
    def _alias_input_args(self, args):
        alias_dict = dict([(_arg, _setting['alias']) for (_arg, _setting) in self.kwarg_settings.items() 
                            if 'alias' in _setting])
        for (target, src) in alias_dict.items():
            if is_str(src) and src in args:
                args.setdefault(target, args[src])
            elif is_seq(src):
                for param in src:
                    if param in args:
                        args.setdefault(target, args[param])
                        break
        return args

    def get_query_params(self):
        '''Extract, typify, and sanitize the parameters from the URL query string. '''
        _args = dict([(k, self.get_argument(k)) for k in self.request.arguments])
        _args = self._alias_input_args(_args)
        _args = self._translate_and_typify_arg_values(_args, json_list_input=self._boolify(_args.get('jsoninput','')))
        _args = self._sanitize_params(_args)
        return _args

    def return_html(self, data, status_code=200):
        self.set_status(status_code)
        if not self.web_settings.DISABLE_CACHING:
            #get etag if data is a dictionary and has "etag" attribute.
            etag = data.get('etag', None) if isinstance(data, dict) else None
            self.set_cacheable(etag=etag)
        self.support_cors()
        self.set_header("Content-Type", "text/html; charset=utf-8")
        self.write(self.web_settings.HTML_OUT_TEMPLATE.format(data=json.dumps(data), img_src=self.web_settings.HTML_OUT_HEADER_IMG, link=self.request.full_url()))
        return
        
    def return_json(self, data, encode=True, indent=None, status_code=200, _format='json'):
        '''Return passed data object as JSON response.
        If **jsonp** parameter is set in the  request, return a valid 
        `JSONP <https://en.wikipedia.org/wiki/JSONP>`_ response.
            
        :param data: object to return as JSON
        :param encode: if encode is False, assumes input data is already a JSON encoded string.
        :param indent: number of indents per level in JSON string
        :param status_code: HTTP status code for response
        :param _format: output format - currently either "html" or "json"
        '''    
        if _format == 'html':
            self.return_html(data, status_code)
            return
        indent = indent or 2   # tmp settings
        self.set_status(status_code)
        if SUPPORT_MSGPACK and self.web_settings.ENABLE_MSGPACK and getattr(self, 'use_msgpack', False):
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
        self.set_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.set_header("Access-Control-Allow-Headers",
                        "Content-Type, Depth, User-Agent, X-File-Size, X-Requested-With, If-Modified-Since, X-File-Name, Cache-Control")
        self.set_header("Access-Control-Allow-Credentials", "false")
        self.set_header("Access-Control-Max-Age", "60")

    def options(self, *args, **kwargs):
        self.support_cors()
