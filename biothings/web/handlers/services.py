from biothings.web.handlers import BaseHandler, BaseAPIHandler
from biothings.web.options.openapi import OpenAPIDocumentBuilder


class StatusHandler(BaseHandler):
    """ Web service health check """

    # if calling set_status instead of raising exceptions
    # when failure happens, no error will be propogated
    # to sentry monitoring. choose the desired one basing
    # on overall health check architecture.

    def head(self):
        return self._check()

    async def get(self):

        dev = self.get_argument('dev', None)
        res = await self._check(dev is not None)
        self.finish(res)

    async def _check(self, dev=False):

        try:  # some db connections support async operations
            response = await self.biothings.health.async_check(dev)
        except (AttributeError, NotImplementedError):
            response = self.biothings.health.check()
        return response

class FrontPageHandler(BaseHandler):

    def get(self):
        self.render(
            template_name="home.html",
            alert='Front Page Not Configured.',
            title='Biothings API',
            contents=self.biothings.handlers.keys(),
            support=self.biothings.metadata.types,
            url='http://biothings.io/'
        )

    def get_template_path(self):
        import biothings.web.templates
        return next(iter(biothings.web.templates.__path__))

class APISpecificationHandler(BaseAPIHandler):

    # Proof of concept
    # Not documented for public access

    # There are multiple **correctness** issues
    # For internal use only. Use with caution.

    @staticmethod
    def _type_to_schema(_type):  # for query strings
        _mapping = {
            "list": "array",
            "bool": "boolean",
            "int": "integer",
            "str": "string",
            "float": "number"
        }
        _type = _mapping.get(_type, "object")
        if _type == "array":
            return {
                "type": "array",
                "items": {"type": "string"}
            }
        else:
            return {
                "type": _type
            }

    @staticmethod
    def _binds(context, param, option):
        # https://swagger.io/specification/#parameter-object
        location = option.get("location", ("query", ))
        _type = option.get("type", str).__name__

        if "query" in location:
            _param = context.parameter(
                param, "query",
                option.get("required", False)
            )
            _schema = APISpecificationHandler._type_to_schema(_type)
            if option.get("default"):
                _schema["default"] = option["default"]
            _param.schema(_schema)

    def get(self):
        openapi = OpenAPIDocumentBuilder()
        openapi.info(title='Biothings API', version='0.0.0')
        for path, handler in self.biothings.handlers.items():
            if not issubclass(handler, BaseAPIHandler):
                continue
            if path != "/":
                path = path.rstrip("/?")
            PATH_PARAM = r"(?:/([^/]+))"
            PATH_TOKEN = r"/{id}"
            if PATH_PARAM in path:
                # this is pretty much hard-coded.
                # the corresponding path param is
                # also likely not handled correctly.
                path = path.replace(PATH_PARAM, PATH_TOKEN)
            _path = openapi.path(path)
            optionset = self.biothings.optionsets[handler.name]
            for param, option in optionset.get("*", {}).items():
                self._binds(_path, param, option)
            for method in ("get", "post", "put", "delete"):
                if getattr(handler, method) is type(self)._unimplemented_method:
                    continue
                _method = getattr(_path, method)()
                for param, option in optionset.get(method.upper(), {}).items():
                    self._binds(_method, param, option)
                if PATH_TOKEN in path:
                    for param in _method.document["parameters"]:
                        if param["name"] == "id":  # hard-coded...
                            param["in"] = "path"
                if method == "post":
                    # might be simplified by using "$ref" syntax
                    _method.document["requestBody"] = {
                        "content": {
                            "application/json": {"schema": {
                                "type": "object",
                                "properties": {
                                    key: self._type_to_schema(val.get("type", str).__name__)
                                    for key, val in optionset.get(method.upper(), {}).items()
                                }
                            }},
                            "application/yaml": {},
                            "application/x-www-form-urlencoded": {},
                            "multipart/form-data": {}
                        }
                    }
        self.finish(openapi.document)

        # internal parameter parsing data structure
        # self.finish(self.biothings.optionsets.log())
