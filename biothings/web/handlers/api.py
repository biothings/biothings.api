"""

Biothings Web API Handlers

    Supports: (all features in parent classes and ...)
    - payload type 'application/json' (through self.json_arguments)
    - parsing keyword argument options (type, default, alias, ...)
    - multi-type dictionary output (json, yaml, html, msgpack)
    - standardized error response (exception -> error template)
    - analytics and usage tracking (Google Analytics and AWS)
    - default common http headers (CORS and Cache Control)

    Subclasses:
    - discovery.web.api.APIBaseHandler
"""

import datetime
import json
from collections import OrderedDict
from pprint import pformat
from urllib.parse import (parse_qs, unquote_plus, urlencode, urlparse,
                          urlunparse)

from tornado.escape import json_decode
from tornado.web import HTTPError

from biothings.utils.common import DateTimeJSONEncoder
from biothings.utils.web.analytics import GAMixIn
from biothings.utils.web.tracking import StandaloneTrackingMixin
from biothings.web.options import OptionError, ReqArgs

from . import BaseHandler
from .exceptions import BadRequest

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

__all__ = [
    'BaseAPIHandler',
    'APISpecificationHandler'
]

class BaseAPIHandler(BaseHandler, GAMixIn, StandaloneTrackingMixin):

    name = ''
    kwargs = None  # dict
    kwarg_groups = ()
    kwarg_methods = ()
    format = 'json'

    def initialize(self):

        self.args = {}  # processed args will be available here
        self.args_query = {}  # query parameters in the URL
        self.args_form = {}  # form-data and x-www-form-urlencoded
        self.args_json = {}  # applicatoin/json type body
        self.event = {
            'category': '{}_api'.format(self.web_settings.API_VERSION),
            'action': self.request.method,  # 'query_get', 'fetch_all', etc.
            # 'label': 'total', 'qsize', etc.
            # 'value': 0, corresponds to label ...
        }

    def prepare(self):
        """
        Extract body and url query parameters into functional groups.
        Typify predefined user inputs patterns here. Rules:

            * Inputs are combined and then separated into functional catagories.
            * Duplicated query or body arguments will overwrite the previous value.
            * JSON body input will not overwrite query arguments in URL.
            * Path arguments can overwirte all other existing values.

        Extend to add more customizations.
        """
        if self.request.headers.get('Content-Type', '').startswith('application/json'):
            if not self.request.body:
                raise HTTPError(400, reason=(
                    'Empty body is not a valid JSON. '
                    'Remove the content-type header, or '
                    'provide an empty object in the body.'))
            try:
                # pylint: disable=attribute-defined-outside-init
                self.args_json = json_decode(self.request.body)
            except json.JSONDecodeError:
                raise HTTPError(400, reason='Invalid JSON body.')

        # pylint: disable=attribute-defined-outside-init
        self.args_query = {
            key: self.get_query_argument(key)
            for key in self.request.query_arguments}
        # pylint: disable=attribute-defined-outside-init
        self.args_form = {
            key: self.get_body_argument(key)
            for key in self.request.body_arguments}

        # self.logger.debug("Incoming Requests:\n%s\n%s", # TODO: one request, one log
        #                   self.request.uri, pformat(args, width=150))

        if self.name:
            optionset = self.web_settings.optionsets.get(self.name)
            try:
                # pylint: disable=attribute-defined-outside-init
                self.args = optionset.parse(
                    self.request.method, ReqArgs(
                        ReqArgs.Path(
                            args=self.path_args,
                            kwargs=self.path_kwargs),
                        query=self.args_query,
                        form=self.args_form,
                        json=self.args_json
                    ))
            except OptionError as err:
                raise BadRequest(**err.info)

        self.logger.debug("Processed Arguments:\n%s",
                          pformat(self.args, width=150))

    def write(self, chunk):
        """
        Override to write output basing on the specified format.
        """
        if isinstance(chunk, dict) and self.format not in ('json', ''):
            if self.format == 'yaml' and SUPPORT_YAML:
                self.set_header("Content-Type", "text/x-yaml; charset=UTF-8")
                chunk = self._format_yaml(chunk)

            elif self.format == 'msgpack' and SUPPORT_MSGPACK:
                self.set_header("Content-Type", "application/x-msgpack")
                chunk = self._format_msgpack(chunk)

            elif self.format == 'html':
                self.set_header("Content-Type", "text/html; charset=utf-8")
                chunk = self._format_html(chunk)
            else:
                self.set_status(400)
                chunk = {"success": False, "code": 400,
                         "error": f"Server not configured to output {self.format}."}

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

        return self.render_string(
            template_name="api.html",
            data=json.dumps(data),
            img_src=self.web_settings.HTML_OUT_HEADER_IMG,
            link=_link,  # url to get regular format
            link_decode=unquote_plus(_link),
            title_html=self.web_settings.HTML_OUT_TITLE,
            docs_link=getattr(self.web_settings, self.name.upper()+'_DOCS_URL', '')
        )

    def on_finish(self):
        """
        This is a tornado lifecycle hook.
        Override to provide tracking features.
        """
        self.logger.debug("Event: %s", self.event)
        self.ga_track(self.event)
        self.self_track(self.event)

    def write_error(self, status_code, **kwargs):

        reason = kwargs.pop('reason', self._reason)
        assert '\n' not in reason

        message = {
            "code": status_code,
            "success": False,
            "error": reason
        }
        if 'exc_info' in kwargs:
            exception = kwargs['exc_info'][1]
            message.update(self.parse_exception(exception))

        self.finish(message)

    def parse_exception(self, exception):
        """
        Return customized error message basing on exception types.
        """
        if isinstance(exception, BadRequest):
            if exception.kwargs:
                return exception.kwargs
        return {}

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

class APISpecificationHandler(BaseAPIHandler):

    def get(self):
        self.finish(self.web_settings.optionsets.log())
