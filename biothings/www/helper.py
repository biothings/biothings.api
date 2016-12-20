import json
import datetime
import tornado.web
from biothings.utils.analytics import GAMixIn
from importlib import import_module
from itertools import product

SUPPORT_MSGPACK = True
if SUPPORT_MSGPACK:
    import msgpack

    def msgpack_encode_datetime(obj):
        if isinstance(obj, datetime.datetime):
            return {'__datetime__': True, 'as_str': obj.strftime("%Y%m%dT%H:%M:%S.%f")}
        return obj

class DateTimeJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        else:
            return super(DateTimeJSONEncoder, self).default(obj)

class BaseESResponseTransformer(object):
    # class contains (at least) 4 functions to process the results of an ES query:
    #
    # clean_annotation_GET_response, clean_annotation_POST_response, clean_query_GET_response, clean_query_POST

class BaseHandler(tornado.web.RequestHandler, GAMixIn):
    ''' Parent class of all biothings handlers.

            Methods:

    '''
    # Override to change JSONP callback
    jsonp_parameter = 'callback'

    # Override to change caching behavior
    cache_max_age = 604800  # 7days
    disable_caching = False

    # Override to add boolean parameters
    boolean_parameters = set()

    def initialize(self, web_settings):
        """ Overridden (what a weird word...) to add settings for this biothing API. """
        self.web_settings = web_settings
        self.ga_event_object_ret = {'category': self.web_settings.ga_event_category}
        self._extra_initializations()

    def ga_event_object(self, data={}):
        ''' Create the data object for google analytics tracking. '''
        # Most of the structure of this object is formed during self.initialize
        if data:
            self.ga_event_object_ret['label'] = list(data.keys()).pop()
            self.ga_event_object_ret['value'] = list(data.values()).pop()
        return self.ga_event_object_ret

    def _extra_initializations(self):
        pass

    def _sanitize_boolean_params(self, kwargs):
        '''Normalize the value of boolean parameters.
           if 1 or true, set to True, otherwise False.
        '''
        for k in kwargs:
            if k in self.boolean_parameters:
                kwargs[k] = kwargs[k].lower() in ['1', 'true']
        return kwargs

    def _sanitize_extra_params(self, kwargs):
        ''' Subclass to sanitize extra params. '''
        return kwargs
    
    def get_query_params(self):
        _args = {}
        for k in self.request.arguments:
            v = self.get_arguments(k)
            if len(v) == 1:
                _args[k] = v[0]
            else:
                _args[k] = v
        _args.pop(self.jsonp_parameter, None)   # exclude jsonp parameter if passed.
        if SUPPORT_MSGPACK:
            _args.pop('msgpack', None)

        # sanitize the query inputs
        self._sanitize_boolean_params(_args)
        self._sanitize_extra_params(_args)
        return _args

    def return_json(self, data, encode=True, indent=None):
        '''return passed data object as JSON response.
           if <jsonp_parameter> is passed, return a valid JSONP response.
           if encode is False, assumes input data is already a JSON encoded
           string.
        '''    
        # call the recursive function to sort the data by keys and add the json-ld information
        indent = indent or 2   # tmp settings
        jsoncallback = self.get_argument(self.jsonp_parameter, '')  # return as JSONP
        if SUPPORT_MSGPACK:
            use_msgpack = self.get_argument('msgpack', '')
        if SUPPORT_MSGPACK and use_msgpack:
            _json_data = msgpack.packb(data, use_bin_type=True, default=msgpack_encode_datetime)
            self.set_header("Content-Type", "application/x-msgpack")
        else:
            _json_data = json.dumps(data, cls=DateTimeJSONEncoder, indent=indent) if encode else data
            self.set_header("Content-Type", "application/json; charset=UTF-8")
        if not self.disable_caching:
            #get etag if data is a dictionary and has "etag" attribute.
            etag = data.get('etag', None) if isinstance(data, dict) else None
            self.set_cacheable(etag=etag)
        self.support_cors()
        if jsoncallback:
            self.write('%s(%s)' % (jsoncallback, _json_data))
        else:
            self.write(_json_data)

    def set_cacheable(self, etag=None):
        '''set proper header to make the response cacheable.
           set etag if provided.
        '''
        self.set_header("Cache-Control", "max-age={}, public".format(self.cache_max_age))
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
