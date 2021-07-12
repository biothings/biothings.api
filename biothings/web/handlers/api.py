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
from collections import OrderedDict, UserDict
from pprint import pformat
from urllib.parse import (parse_qs, unquote_plus, urlencode, urlparse,
                          urlunparse)

import yaml
from biothings.utils.common import DateTimeJSONEncoder
from biothings.web.analytics.notifiers import AnalyticsMixin
from biothings.web.analytics.events import Event
from biothings.web.options import OptionError, ReqArgs
from biothings.web.options.openapi import OpenAPIDocumentBuilder
from tornado.escape import json_decode
from tornado.web import HTTPError

from . import BaseHandler
from .exceptions import BadRequest, EndRequest

try:
    import msgpack

    def msgpack_encode_datetime(obj):
        if isinstance(obj, datetime.datetime):
            return {
                '__datetime__': True,
                'as_str': obj.strftime("%Y%m%dT%H:%M:%S.%f")
            }
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

class BaseAPIHandler(BaseHandler, AnalyticsMixin):

    name = ''
    kwargs = None  # dict
    format = 'json'
    cache = 604800  # 7 days

    def initialize(self):

        self.args = {}  # processed args will be available here
        self.args_query = {}  # query parameters in the URL
        self.args_form = {}  # form-data and x-www-form-urlencoded
        self.args_json = {}  # applicatoin/json type body
        self.args_yaml = {}  # applicatoin/yaml type body
        self.event = Event()

    def prepare(self):

        content_type = self.request.headers.get('Content-Type', '')
        if content_type.startswith('application/json'):
            self.args_json = self._parse_json()
        elif content_type.startswith('application/yaml'):
            self.args_yaml = self._parse_yaml()

        self.args_query = {
            key: self.get_query_argument(key)
            for key in self.request.query_arguments}
        self.args_form = {
            key: self.get_body_argument(key)
            for key in self.request.body_arguments}

        reqargs = ReqArgs(
            ReqArgs.Path(
                args=self.path_args,
                kwargs=self.path_kwargs),
            query=self.args_query,
            form=self.args_form,
            json_=self.args_json
        )
        # standardized request arguments
        self.args = self._parse_args(reqargs)

    def _parse_json(self):
        if not self.request.body:
            raise HTTPError(400, reason=(
                'Empty body is not a valid JSON. '
                'Remove the content-type header, or '
                'provide an empty object in the body.'))
        try:
            return json_decode(self.request.body)
        except json.JSONDecodeError:
            raise HTTPError(400, reason='Invalid JSON body.')

    def _parse_yaml(self):
        try:
            return yaml.load(self.request.body, Loader=yaml.SafeLoader)
        except (yaml.scanner.ScannerError, yaml.parser.ParserError):
            raise HTTPError(400, reason='Invalid YAML body.')

    def _parse_args(self, reqargs):

        if not self.name:  # feature disabled
            return {}  # default value

        optionsets = self.biothings.optionsets
        optionset = optionsets.get(self.name)

        try:  # uses biothings.web.options to standardize args
            args = optionset.parse(self.request.method, reqargs)

        except OptionError as err:
            args = err  # for logging
            raise BadRequest(**err.info)

        else:  # set on self.args
            return args

        finally:  # one log message regardless of success
            self.logger.debug(
                "%s %s\n%s\n%s",
                self.request.method,
                self.request.uri,
                reqargs, args)

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

        elif isinstance(chunk, UserDict):
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            chunk = json.dumps(dict(chunk), cls=DateTimeJSONEncoder)

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
            img_src=self.biothings.config.HTML_OUT_HEADER_IMG,
            link=_link,  # url to get regular format
            link_decode=unquote_plus(_link),
            title_html=self.biothings.config.HTML_OUT_TITLE,
            docs_link=getattr(self.web_settings, self.name.upper()+'_DOCS_URL', '')  # TODO Evaluate if this belongs in the base handler
        )

    def on_finish(self):
        """
        This is a tornado lifecycle hook.
        Override to provide tracking features.
        """
        self.logger.debug(self.event)

    def write_error(self, status_code, **kwargs):

        reason = kwargs.pop('reason', self._reason)
        # "reason" is a reserved tornado keyword
        # see RequestHandler.send_error
        assert isinstance(reason, str)
        assert '\n' not in reason

        message = {
            "code": status_code,
            "success": False,
            "error": reason
        }
        # add exception info
        if 'exc_info' in kwargs:
            exception = kwargs['exc_info'][1]
            exc_info = self.parse_exception(exception)
            if isinstance(exc_info, dict):
                message.update(exc_info)
            else:  # cannot merge it with the template
                message = exc_info

        self.finish(message)

    def parse_exception(self, exception):
        """
        Return customized error message basing on exception types.
        """
        if isinstance(exception, EndRequest):
            if exception.kwargs:
                return exception.kwargs

        # override like this
        # if isinstance(exception, OtherExceptions):
        #     return exception.data

        return {}

    def options(self, *args, **kwargs):

        self.set_status(204)
        self.finish()

    def set_default_headers(self):

        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Methods", "*")
        self.set_header("Access-Control-Allow-Headers", "*")
        self.set_header("Access-Control-Allow-Credentials", "false")
        self.set_header("Access-Control-Max-Age", "60")

        if self.cache and isinstance(self.cache, int):
            # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control
            self.set_header("Cache-Control", f"max-age={self.cache}, public")

        # to disable caching for a handler, set cls.cache to 0 or
        # run self.clear_header('Cache-Control') in an HTTP method


class APISpecificationHandler(BaseAPIHandler):

    def get(self):
        # openapi = OpenAPIDocumentBuilder()
        # openapi.info(title='Biothings API', version='v1')
        # self.finish(openapi.document)
        self.finish(self.biothings.optionsets.log())
