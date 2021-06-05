import inspect
import logging
from pydoc import locate
from types import SimpleNamespace

import tornado.web
from biothings.utils.info import DevInfo, FieldNote
from biothings.web import connections
from biothings.web.options import OptionsManager as OptionSets
from biothings.web.query.builder import *
from biothings.web.query.engine import *
from biothings.web.query.formatter import *
from biothings.web.query.pipeline import *
from biothings.web.services.metadata import *
from biothings.web.services.health import *

try:
    from raven.contrib.tornado import AsyncSentryClient
except ImportError:
    __SENTRY_INSTALLED__ = False
else:
    __SENTRY_INSTALLED__ = True

logger = logging.getLogger(__name__)

def load_class(kls):
    if inspect.isclass(kls):
        return kls
    if isinstance(kls, str):
        return locate(kls)
    raise ValueError()


def _requires(config_key):
    def requires(f):
        def _(self):
            if not getattr(self.config, config_key, None):
                return  # skip this database context
            return f(self)
        return _
    return requires

class BiothingsDBProxy():
    """
        Provide database-agnostic access to 
        common biothings service components,
        for single database application.
    """

    def __init__(self):
        self.pipeline = None
        self.metadata = None
        self.health = None

    def configure(self, db):
        self.pipeline = db.pipeline
        self.metadata = db.metadata
        self.health = db.health

class BiothingsNamespace():

    # toolbox to use when writing handlers
    # contains common biothings services

    def __init__(self, config):
        self.config = config

        self.fieldnote = FieldNote(config.AVAILABLE_FIELDS_NOTES_PATH)
        self.devinfo = DevInfo(config.APP_GIT_REPOSITORY)

        self.optionsets = OptionSets()
        self.handlers = {}

        self.db = BiothingsDBProxy()
        self._configure_elasticsearch()
        self._configure_mongodb()
        self._configure_sql()

        # shortcuts
        self.pipeline = self.db.pipeline
        self.metadata = self.db.metadata
        self.health = self.db.health

    @_requires('ES_HOST')
    def _configure_elasticsearch(self):
        self.elasticsearch = SimpleNamespace()

        self.elasticsearch.client = connections.es.get_client(
            self.config.ES_HOST, **self.config.ES_ARGS)
        self.elasticsearch.async_client = connections.es.get_async_client(
            self.config.ES_HOST, **self.config.ES_ARGS)

        self.elasticsearch.userquery = ESUserQuery(self.config.USERQUERY_DIR)
        self.elasticsearch.metadata = BiothingsESMetadata(
            self.config.ES_INDICES,
            self.elasticsearch.async_client
        )
        self.elasticsearch.pipeline = load_class(self.config.ES_QUERY_PIPELINE)(
            load_class(self.config.ES_QUERY_BUILDER)(
                self.elasticsearch.userquery,
                self.config.ANNOTATION_ID_REGEX_LIST,  # TODO
                self.config.ANNOTATION_DEFAULT_SCOPES,  # TODO
                self.config.ALLOW_RANDOM_QUERY,
                self.config.ALLOW_NESTED_AGGS
            ),
            load_class(self.config.ES_QUERY_BACKEND)(
                self.elasticsearch.async_client,
                self.config.ES_INDICES,
                self.config.ES_SCROLL_TIME,
                self.config.ES_SCROLL_SIZE
            ),
            load_class(self.config.ES_RESULT_TRANSFORM)(
                self.elasticsearch.metadata.biothing_licenses,
                self.config.LICENSE_TRANSFORM,
                self.fieldnote.get_field_notes(),  # TODO
                self.config.AVAILABLE_FIELDS_EXCLUDED
            ),
        )
        self.elasticsearch.health = ESHealth(
            self.elasticsearch.async_client,
            self.config.STATUS_CHECK
        )
        self.db.configure(self.elasticsearch)

    @_requires('MONGO_URI')
    def _configure_mongodb(self):
        self.mongo = SimpleNamespace()

        self.mongo.client = connections.mongo.get_client(
            self.config.MONGO_URI, **self.config.MONGO_ARGS)
        self.mongo.metadata = BiothingsMongoMetadata(
            self.config.MONGO_COLS,
            self.mongo.client
        )
        self.mongo.pipeline = MongoQueryPipeline(
            MongoQueryBuilder(),
            MongoQueryBackend(
                self.mongo.client,
                self.config.MONGO_COLS),
            MongoResultFormatter()
        )
        self.mongo.health = MongoHealth(
            self.mongo.client
        )
        self.db.configure(self.mongo)

    @_requires('SQL_URI')
    def _configure_sql(self):
        self.sql = SimpleNamespace()

        self.sql.client = connections.sql.get_client(
            self.config.SQL_URI, **self.config.SQL_ARGS)
        self.sql.metadata = BiothingsSQLMetadata(
            self.config.SQL_TBLS,
            self.sql.client
        )
        self.sql.pipeline = SQLQueryPipeline(
            SQLQueryBuilder(self.config.SQL_TBLS),
            SQLQueryBackend(self.sql.client),
            SQLResultFormatter()
        )
        self.sql.health = SQLHealth(
            self.sql.client
        )
        self.db.configure(self.sql)
