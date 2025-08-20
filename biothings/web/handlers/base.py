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

import logging

import orjson
import yaml
from tornado.web import HTTPError, RequestHandler

from biothings.utils import serializer
from biothings.web.analytics.events import Event
from biothings.web.analytics.notifiers import AnalyticsMixin
from biothings.web.options import OptionError, ReqArgs

logger = logging.getLogger(__name__)


class BaseHandler(RequestHandler):
    @property
    def biothings(self):
        return self.application.biothings


class BaseAPIHandler(BaseHandler, AnalyticsMixin):
    name = "__base__"
    kwargs = {
        "*": {
            "format": {
                "type": str,
                "default": "json",
                "enum": ("json", "yaml", "html", "msgpack"),
            }
        }
    }
    format = "json"
    cache = None
    cache_control_template = "max-age={cache}, public"

    def initialize(self, cache=None):
        cache_value = self.biothings.config.DEFAULT_CACHE_MAX_AGE
        if self.cache is not None:
            cache_value = self.cache
        if cache is not None:
            cache_value = cache
        # self._header has already set when call set_default_headers func before
        # so we need to overwrite it to make custom cache age works
        self.set_cache_header(cache_value)

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
        content_type = self.request.headers.get("Content-Type", "")
        if content_type.startswith("application/json"):
            self.args_json = self._parse_json()
        elif content_type.startswith("application/yaml"):
            self.args_yaml = self._parse_yaml()

        self.args_query = {key: self.get_query_argument(key) for key in self.request.query_arguments}
        self.args_form = {key: self.get_body_argument(key) for key in self.request.body_arguments}

        reqargs = ReqArgs(
            ReqArgs.Path(args=self.path_args, kwargs=self.path_kwargs),
            query=self.args_query,
            form=self.args_form,
            json_=self.args_json,
        )
        # standardized request arguments
        self.args = self._parse_args(reqargs)

        # Handle cases where args is a plain dict (e.g., when name is missing or None)
        if hasattr(self.args, 'format'):
            self.format = self.args.format
        else:
            # Use the default format when args doesn't have format attribute
            self.format = "json"

    def _parse_json(self):
        if not self.request.body:
            return {}
        try:
            return orjson.loads(self.request.body)
        except orjson.JSONDecodeError:
            raise HTTPError(400, reason="Invalid JSON body.")

    def _parse_yaml(self):
        try:
            return yaml.load(self.request.body, Loader=yaml.SafeLoader)
        except (yaml.scanner.ScannerError, yaml.parser.ParserError):
            raise HTTPError(400, reason="Invalid YAML body.")

    def _parse_args(self, reqargs):
        if not self.name:  # feature disabled
            return {}  # default value

        # Check if handler defines kwargs but not a unique name - this is an error
        if self.name == "__base__" and self.__class__ != BaseAPIHandler:
            # Check if this handler defines its own kwargs (not inherited from BaseAPIHandler)
            handler_has_kwargs = (
                hasattr(self.__class__, 'kwargs') and
                self.__class__.kwargs is not BaseAPIHandler.kwargs
            )

            if handler_has_kwargs:
                # Handler defines kwargs but uses default name - this will cause conflicts
                raise ValueError(
                    f"Handler {self.__class__.__name__} defines 'kwargs' but doesn't define "
                    f"a unique 'name' attribute. This causes parameter validation conflicts. "
                    f"Please add: name = 'your_handler_name' to the class definition."
                )
            else:
                # Handler doesn't define kwargs and doesn't define name - this is ok
                # Return empty args to disable parameter validation
                return {}

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
            logger.debug("%s %s\n%s\n%s", self.request.method, self.request.uri, reqargs, args)

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
                chunk = self.render_string("api.html", data=serializer.to_json(chunk))
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
        """
        from tornado.web import Finish, HTTPError

        raise HTTPError(404)
        raise HTTPError(404, reason="document not found")
        raise HTTPError(404, None, {"id": "-1"}, reason="document not found") ->
        {
          "code": 404,
          "success": False,
          "error": "document not found"
          "id": "-1"
        }
        """

        reason = kwargs.pop("reason", self._reason)
        # "reason" is a reserved tornado keyword
        # see RequestHandler.send_error
        assert isinstance(reason, str)
        assert "\n" not in reason

        message = {"code": status_code, "success": False, "error": reason}
        try:  # merge exception info
            logger.debug("", exc_info=kwargs["exc_info"])  # log the full traceback in debug mode
            exception = kwargs["exc_info"][1]
            if isinstance(exception.args[0], dict):
                message.update(exception.args[0])
            elif isinstance(exception.args[0], str):
                message["details"] = exception.args[0]
        except Exception:
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

    def set_cache_header(self, cache_value):
        if isinstance(cache_value, int):
            # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control
            # to disable caching for a handler, set cls.cache to 0 or
            # run self.clear_header('Cache-Control') in an HTTP method
            # or set cache value on the config file:
            # r"/api/query/?", "biothings.web.handlers.QueryHandler", {"biothing_type": "schema", "cache": 0}),
            self.set_header("Cache-Control", self.cache_control_template.format(cache=cache_value))
