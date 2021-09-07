"""
Biothings Web Handlers

biothings.web.handlers.BaseHandler

    Supports:
    - access to biothings namespace
    - monitor exceptions with Sentry

biothings.web.handlers.BaseAPIHandler

    Additionally supports:
    - JSON and YAML payload in the request body
    - request arguments standardization
    - multi-type output (json, yaml, html, msgpack)
    - standardized error response (exception -> error template)
    - analytics and usage tracking (Google Analytics and AWS)
    - default common http headers (CORS and Cache Control)

"""
import json
import logging

import yaml
from biothings.utils import serializer
from biothings.web.analytics.events import Event
from biothings.web.analytics.notifiers import AnalyticsMixin
from biothings.web.options import OptionError, ReqArgs
from tornado.escape import json_decode
from tornado.web import HTTPError, RequestHandler

try:
    from raven.contrib.tornado import SentryMixin
except ImportError:
    class SentryMixin():
        """dummy mixin"""

logger = logging.getLogger(__name__)

class BaseHandler(SentryMixin, RequestHandler):

    @property
    def biothings(self):
        return self.application.biothings

    def get_sentry_client(self):
        # Override and retrieve from tornado settings instead.
        client = self.settings.get('sentry_client')
        if not client:  # need to set config.SENTRY_CLIENT_KEY
            raise ValueError("Sentry Not Configured.")
        return client


class BaseAPIHandler(BaseHandler, AnalyticsMixin):

    name = '__base__'
    kwargs = {
        '*': {
            'format': {
                'type': str,
                'default': 'json',
                'enum': ('json', 'yaml', 'html', 'msgpack'),
            }
        }
    }
    format = 'json'
    cache = 604800  # 7 days

    def initialize(self):

        self.args = {}  # processed args will be available here
        self.args_query = {}  # query parameters in the URL
        self.args_form = {}  # form-data and x-www-form-urlencoded
        self.args_json = {}  # applicatoin/json type body
        self.args_yaml = {}  # applicatoin/yaml type body
        self.event = Event()

        # do not assume the data types of some the variables
        # defined above. self.args can be a dotdict after
        # processing. json/yaml can be any serializable objs.
        # self.event may be replaced with its sub-classes.

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
        self.format = self.args.format

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
            args = err  # for logging in "finally" clause
            raise HTTPError(400, None, err.info)

        else:  # set on self.args
            return args

        finally:  # one log message regardless of success
            logger.debug(
                "%s %s\n%s\n%s",
                self.request.method,
                self.request.uri,
                reqargs, args)

    def write(self, chunk):
        try:

            if self.format == "json":
                chunk = serializer.to_json(chunk)
                self.set_header("Content-Type", "application/json; charset=UTF-8")

            elif self.format == "yaml":
                chunk = serializer.to_yaml(chunk)
                self.set_header("Content-Type", "text/x-yaml; charset=UTF-8")

            elif self.format == "msgpack":
                chunk = serializer.to_msgpack(chunk)
                self.set_header("Content-Type", "application/x-msgpack")

            elif self.format == "html":
                chunk = self.render_string("api.html", data=json.dumps(chunk))
                self.set_header("Content-Type", "text/html; charset=utf-8")

        except Exception as exc:
            # this is a low-level method, used in many places,
            # error handling should happen in the upper layers,
            logger.warning(exc)

        super().write(chunk)

    def get_template_path(self):
        # APIs should not normally need to use templating
        # set the path to where we can find the api.html
        import biothings.web.templates
        return next(iter(biothings.web.templates.__path__))

    def on_finish(self):
        """
        This is a tornado lifecycle hook.
        Override to provide tracking features.
        """
        logger.debug(self.event)
        super().on_finish()

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
        try:  # merge exception info
            exception = kwargs['exc_info'][1]
            message.update(exception.args[0])
        except:
            pass

        self.finish(message)

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
