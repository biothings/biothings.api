import datetime
import json
import logging
import re
from collections import OrderedDict, defaultdict
from itertools import product
from pprint import pformat
from urllib.parse import (parse_qs, unquote_plus, urlencode, urlparse,
                          urlunparse)

from tornado.escape import json_decode
from tornado.web import HTTPError

from biothings.utils.common import DateTimeJSONEncoder, dotdict, split_ids
from biothings.utils.web.analytics import GAMixIn
from biothings.utils.web.tracking import StandaloneTrackingMixin
from biothings.web.api.helper import BadRequest, EndRequest
from biothings.web.api.options import OptionArgsParser
from biothings.web.handler import BaseHandler

try:
    import msgpack

    def msgpack_encode_datetime(obj):
        if isinstance(obj, datetime.datetime):
            return {'__datetime__': True,
                    'as_str': obj.strftime("%Y%m%dT%H:%M:%S.%f")}
        return obj
except ImportError:
    SUPPORT_MSGPACK = False
else:
    SUPPORT_MSGPACK = True


try:
    import yaml
except ImportError:
    SUPPORT_YAML = False
else:
    SUPPORT_YAML = True


class BaseAPIHandler(BaseHandler, GAMixIn, StandaloneTrackingMixin):
    """
        Contains the common functions in the biothings handler universe:

            * return data in json, html, yaml and msgpack
            * set CORS and Cache Control HTTP headers
            * typify the URL and body keyword arguments
            * optionally send tracking data to google analytics
    """
    name = ''
    kwarg_types = ()
    kwarg_methods = ()

    out_format = 'json'

    @classmethod
    def setup(cls, web_settings):
        '''
        Override me to extend class level setup. Called in generate API.
        Populate relevent kwarg settings in _kwarg_settings.
        Access with attribute kwarg_settings.
        '''
        cls.web_settings = web_settings

        if not cls.name:
            return

        cls._kwarg_settings = defaultdict(dict)
        for method, kwarg_type in product(cls.kwarg_methods, cls.kwarg_types):
            key = '_'.join((cls.name, method, kwarg_type, 'kwargs')).upper()
            if hasattr(web_settings, key):
                setting = cls._kwarg_settings[method.upper()]
                setting[kwarg_type] = getattr(web_settings, key)

    @property
    def kwarg_settings(self):
        '''
        Return the appropriate kwarg settings basing on the request method.
        '''
        if hasattr(self, '_kwarg_settings'):
            if self.request.method in self._kwarg_settings:
                return self._kwarg_settings[self.request.method]
        return {}

    def initialize(self):

        self.kwargs = dotdict()
        self.json_arguments = {}
        self.ga_event_object_ret = {
            'category': '{}_api'.format(self.web_settings.API_VERSION)
            # 'action': 'query_get', 'gene_post', 'fetch_all', etc.
            # 'label': 'total', 'qsize', etc.
            # 'value': 0, corresponds to label ...
        }

    def prepare(self):
        '''
        Extract body and url query parameters into functional groups.
        Typify predefined user inputs patterns here. Rules:

            * Inputs are combined and then separated into functional catagories.
            * Duplicated query or body arguments will overwrite the previous value.
            * JSON body input will not overwrite query arguments in URL.
            * Path arguments can overwirte all other existing values.

        Extend to add more customizations.
        '''
        if self.request.headers.get('Content-Type') == 'application/json':
            try:
                self.json_arguments = json_decode(self.request.body)
            except Exception:
                raise HTTPError(400, reason='Invalid JSON body.')

        args = dict(self.json_arguments)
        args.update({key: self.get_argument(key) for key in self.request.arguments})

        logging.debug("Kwarg settings:\n%s", pformat(self.kwarg_settings, width=150))
        logging.debug("Kwargs received:\n%s", pformat(args, width=150))

        for catagory, settings in self.kwarg_settings.items():
            self.kwargs[catagory] = options = {}
            for keyword, setting in settings.items():

                parser = OptionArgsParser(keyword, setting)
                parser.list_size_cap = self.web_settings.LIST_SIZE_CAP
                parser.string_as_json = args.get('jsoninput')
                value = parser.parse(args, self.path_args, self.path_kwargs)

                if value is not None:
                    options[keyword] = value

        logging.debug("Processed kwargs:\n%s", pformat(self.kwargs, width=150))

    def write(self, chunk):
        """
        Override to write output basing on the specified format.
        """
        if isinstance(chunk, dict) and self.out_format not in ('json', ''):
            if self.out_format == 'yaml' and SUPPORT_YAML:
                self.set_header("Content-Type", "text/x-yaml; charset=UTF-8")
                chunk = self._format_yaml(chunk)

            elif self.out_format == 'msgpack' and SUPPORT_MSGPACK:
                self.set_header("Content-Type", "application/x-msgpack")
                chunk = self._format_msgpack(chunk)

            elif self.out_format == 'html':
                self.set_header("Content-Type", "text/html; charset=utf-8")
                chunk = self._format_html(chunk)
            else:
                self.set_status(400)
                chunk = {"success": False, "code": 400,
                         "error": f"Server not configured to output {self.out_format}."}

        elif isinstance(chunk, (dict, list)):
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            chunk = json.dumps(chunk, cls=DateTimeJSONEncoder)

        super().write(chunk)

    def _format_yaml(self, data):

        def ordered_dump(data, stream=None, Dumper=yaml.Dumper, **kwds):
            class OrderedDumper(Dumper):
                pass

            def _dict_representer(dumper, data):
                return dumper.represent_mapping(
                    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
                    data.items())
            OrderedDumper.add_representer(OrderedDict, _dict_representer)
            return yaml.dump(data, stream, OrderedDumper, **kwds)

        return ordered_dump(
            data=data, Dumper=yaml.SafeDumper, default_flow_style=False)

    def _format_msgpack(self, data):

        return msgpack.packb(
            data, use_bin_type=True, default=msgpack_encode_datetime)

    def _format_html(self, data):

        _link = self.request.full_url()
        d = urlparse(_link)
        q = parse_qs(d.query)
        q.pop('format', None)
        d = d._replace(query=urlencode(q, True))
        _link = urlunparse(d)

        return self.web_settings.HTML_OUT_TEMPLATE.format(
            data=json.dumps(data),
            img_src=self.web_settings.HTML_OUT_HEADER_IMG,
            link=_link,
            link_decode=unquote_plus(_link),
            title_html=self.web_settings.HTML_OUT_TITLE,
            docs_link=getattr(self.web_settings, self.name.upper()+'_DOCS_URL', '')
        )

    def on_finish(self):
        """
        This is a tornado lifecycle hook.
        Override to provide tracking features.
        """
        logging.debug("Track: %s", self.ga_event_object_ret)
        self.ga_track(event=self.ga_event_object_ret)
        self.self_track(data=self.ga_event_object_ret)

    def ga_event_object(self, data=None):
        ''' Create the data object for google analytics tracking. '''
        # Most of the structure of this object is formed during self.initialize
        if data and isinstance(data, dict):
            self.ga_event_object_ret['label'] = list(data.keys()).pop()
            self.ga_event_object_ret['value'] = list(data.values()).pop()
        return self.ga_event_object_ret

    def options(self, *args, **kwargs):

        self.set_status(204)
        self.finish()

    def set_default_headers(self):

        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Methods",
                        self.web_settings.ACCESS_CONTROL_ALLOW_METHODS)
        self.set_header("Access-Control-Allow-Headers",
                        self.web_settings.ACCESS_CONTROL_ALLOW_HEADERS)
        self.set_header("Access-Control-Allow-Credentials", "false")
        self.set_header("Access-Control-Max-Age", "60")

        if not self.web_settings.DISABLE_CACHING:
            seconds = self.web_settings.CACHE_MAX_AGE
            self.set_header("Cache-Control", f"max-age={seconds}, public")
