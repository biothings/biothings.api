
class WebAPIValidator():

    def validate(self, config):

        assert config.API_VERSION or config.API_PREFIX
        assert isinstance(config.LIST_SIZE_CAP, int)
        assert isinstance(config.ACCESS_CONTROL_ALLOW_METHODS, str)
        assert isinstance(config.ACCESS_CONTROL_ALLOW_HEADERS, str)


class DBParamValidator():

    def validate(self, config):

        assert isinstance(config.ES_INDICES, dict)

        # compatibility settings
        if getattr(config, "ES_INDEX", None) and \
           getattr(config, "ES_DOC_TYPE", None):
            from biothings.web.settings import default
            if config.ES_INDICES is default.ES_INDICES:
                config.ES_INDICES = {}
            else:  # combine with the user provided value
                config.ES_INDICES = dict(config.ES_INDICES)
            config.ES_INDICES[config.ES_DOC_TYPE] = config.ES_INDEX

        # assert '*' not in config.ES_DOC_TYPE TODO

        ERROR = "UNSUPPORTED SETTINGS."
        # encountering the following attributes indicate
        # the application is built for a previous version.
        # to ensure delivering intended behaviors,
        # upgrade the config module of the application.
        assert not hasattr(config, 'ES_SNIFF'), ERROR
        assert not hasattr(config, 'ES_CLIENT_TIMEOUT'), ERROR

class SubmoduleValidator():

    def __init__(self):
        self._prefixes = set()

    def validate(self, config):
        assert config.API_PREFIX, "API_PREFIX must be defined for a submodule."
        assert config.API_PREFIX not in self._prefixes, "API_PREFIX conflicts."
        self._prefixes.add(config.API_PREFIX)

class MongoParamValidaor():

    def validate(self, config):
        pass
