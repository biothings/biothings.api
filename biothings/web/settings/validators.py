class WebAPIValidator:
    def validate(self, config):
        # Compatibility

        if getattr(config, "API_PREFIX", None) is not None:
            config.APP_PREFIX = config.API_PREFIX
        if getattr(config, "API_VERSION", None) is not None:
            config.APP_VERSION = config.API_VERSION

        assert config.APP_VERSION or config.APP_PREFIX, (
            "Require at least one of the follwing settings:"
            "(APP_VERSION, APP_PREFIX) to create a layer of"
            "separation for the default biothings routes."
        )


class DBParamValidator:
    def validate(self, config):
        assert isinstance(config.ES_INDICES, dict)

        # compatibility settings to convert ES_INDEX/ES_DOC_TYPE settings to ES_INDICES
        if getattr(config, "ES_INDEX", None):
            from biothings.web.settings import default

            if config.ES_INDICES is default.ES_INDICES:
                config.ES_INDICES = {}
            else:  # combine with the user provided value
                config.ES_INDICES = dict(config.ES_INDICES)
            # _doc_type can be None if not provided, in this case, ES_INDEX value will be
            # set to "None" key in ES_INDICES as the default index used in the handlers
            _doc_type = getattr(config, "ES_DOC_TYPE", None) or None
            config.ES_INDICES[_doc_type] = config.ES_INDEX

        ERROR = "UNSUPPORTED SETTINGS."
        # encountering the following attributes indicate
        # the application is built for a previous version.
        # to ensure delivering intended behaviors,
        # upgrade the config module of the application.
        assert not hasattr(config, "ES_SNIFF"), ERROR
        assert not hasattr(config, "ES_CLIENT_TIMEOUT"), ERROR


class SubmoduleValidator:
    def __init__(self):
        self._prefixes = set()

    def validate(self, config):
        assert config.APP_PREFIX, "APP_PREFIX must be defined for a submodule."
        assert config.APP_PREFIX not in self._prefixes, "APP_PREFIX conflicts."
        self._prefixes.add(config.APP_PREFIX)


class MongoParamValidaor:
    def validate(self, config):
        pass
