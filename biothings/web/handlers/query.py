"""
Elasticsearch Handlers

biothings.web.handlers.BaseESRequestHandler

    Supports: (all features above and)
    - access to biothing_type attribute
    - access to ES query pipeline stages
    - pretty print elasticsearch exceptions
    - common control option out_format

    Subclasses:
    - biothings.web.handlers.MetadataSourceHandler
    - biothings.web.handlers.MetadataFieldHandler
    - myvariant.web.beacon.BeaconHandler

biothings.web.handlers.ESRequestHandler

    Supports: (all features above and)
    - common control options (raw, rawquery)
    - common transform options (dotfield, always_list...)
    - query pipeline customization hooks
    - single query through GET
    - multiple quers through POST

    Subclasses:
    - biothings.web.handlers.BiothingHandler
    - biothings.web.handlers.QueryHandler

"""

import json
import logging
from collections import Counter
from types import CoroutineType

from biothings.utils import serializer
from biothings.web.analytics.events import GAEvent
from biothings.web.handlers.base import BaseAPIHandler
from biothings.web.query.pipeline import (QueryPipelineException,
                                          QueryPipelineInterrupt)
from tornado.web import Finish

from .exceptions import EndRequest

__all__ = [
    'BaseQueryHandler',
    'MetadataSourceHandler',
    'MetadataFieldHandler',
    'BiothingHandler',
    'QueryHandler'
]

logger = logging.getLogger(__name__)


class BaseQueryHandler(BaseAPIHandler):

    def initialize(self, biothing_type=None):

        super().initialize()
        self.biothing_type = biothing_type
        self.pipeline = self.biothings.pipeline
        self.metadata = self.biothings.metadata

    def prepare(self):
        super().prepare()

        # provide convenient access to next stages
        self.args.biothing_type = self.biothing_type

        self.event = GAEvent({
            '__secondary__': [],  # secondary analytical objective: field tracking
            'category': '{}_api'.format(self.biothings.config.APP_VERSION),  # eg.'v1_api'
            'action': '_'.join((self.name, self.request.method.lower())),  # eg.'query_get'
            # 'label': 'fetch_all', etc.
            # 'value': 100, # number of queries
        })

        if self.args._source:
            fields = [str(field) for field in self.args._source]  # in case input is not str
            fields = [field.split('.', 1)[0] for field in fields]  # only consider root keys
            for field, cnt in Counter(fields).items():
                self.event['__secondary__'].append(GAEvent({
                    'category': 'parameter_tracking',
                    'action': 'field_filter',
                    'label': field,
                    'value': cnt
                }))
        else:
            self.event['__secondary__'].append(GAEvent({
                'category': 'parameter_tracking',
                'action': 'field_filter',
                'label': 'all'
            }))

    def write(self, chunk):
        # add an additional header to the JSON formatter
        # with a header image and a title-like section
        # further more, show a link to documentation

        # relevant settings in config:
        # HTML_OUT_TITLE
        # HTML_OUT_HEADER_IMG
        # HTML_OUT_<ENDPOINT>_DOCS

        DEFAULT_TITLE = "<p>Biothings API</p>"
        DEFAULT_IMG = "https://biothings.io/static/favicon.ico"

        try:
            if self.format == "html":
                config = self.biothings.config
                chunk = self.render_string(
                    template_name="api.html", data=json.dumps(chunk),
                    link=serializer.URL(self.request.full_url()).remove('format'),
                    title_div=getattr(config, "HTML_OUT_TITLE", "") or DEFAULT_TITLE,
                    header_img=getattr(config, "HTML_OUT_HEADER_IMG", "") or DEFAULT_IMG,
                    learn_more=getattr(config, f"HTML_OUT_{self.name.upper()}_DOCS", "")
                )
                self.set_header("Content-Type", "text/html; charset=utf-8")
                return super(BaseAPIHandler, self).write(chunk)

        except Exception as exc:
            logger.warning(exc)

        super().write(chunk)

class MetadataSourceHandler(BaseQueryHandler):
    """
    GET /metadata
    """
    name = 'metadata'
    kwargs = dict(BaseQueryHandler.kwargs)
    kwargs['GET'] = {
        'dev': {'type': bool, 'default': False},
        'raw': {'type': bool, 'default': False}
    }

    async def get(self):
        info = await self.metadata.refresh(self.biothing_type)
        meta = self.metadata.get_metadata(self.biothing_type)

        if self.args.raw:
            raise Finish(info)

        elif self.args.dev:
            meta['software'] = self.biothings.devinfo.get()

        else:  # remove debug info
            for field in list(meta):
                if field.startswith('_'):
                    meta.pop(field, None)

        meta = self.extras(meta)  # override here
        self.finish(dict(sorted(meta.items())))

    def extras(self, _meta):
        """
        Override to add app specific metadata.
        """
        return _meta


class MetadataFieldHandler(BaseQueryHandler):
    """
    GET /metadata/fields
    """
    name = 'fields'
    kwargs = dict(BaseQueryHandler.kwargs)
    kwargs['GET'] = {
        'raw': {'type': bool, 'default': False},
        'search': {'type': str, 'default': None},
        'prefix': {'type': str, 'default': None}
    }

    async def get(self):
        await self.metadata.refresh(self.biothing_type)
        mapping = self.metadata.get_mappings(self.biothing_type)

        if self.args.raw:
            raise Finish(mapping)

        result = self.pipeline.formatter.transform_mapping(
            mapping, self.args.prefix, self.args.search)

        self.finish(result)


async def ensure_awaitable(obj):
    if isinstance(obj, CoroutineType):
        return await obj
    return obj

def capture_exceptions(coro):
    async def _method(*args, **kwargs):
        try:
            return await coro(*args, **kwargs)
        except QueryPipelineInterrupt as itr:
            raise EndRequest(**itr.details)
        except QueryPipelineException as exc:
            kwargs = exc.details if isinstance(exc.details, dict) else {}
            raise EndRequest(exc.code, reason=exc.summary, **kwargs)
    return _method

class BiothingHandler(BaseQueryHandler):
    """
    Biothings Annotation Endpoint

    URL pattern examples:

        /{pre}/{ver}/{typ}/?
        /{pre}/{ver}/{typ}/([^\/]+)/?

        queries a term against a pre-determined field that
        represents the id of a document, like _id and dbsnp.rsid

        GET -> {...} or [{...}, ...]
        POST -> [{...}, ...]
    """
    name = 'annotation'

    @capture_exceptions
    async def post(self, *args, **kwargs):
        self.event['value'] = len(self.args['id'])

        result = await ensure_awaitable(
            self.pipeline.fetch(**self.args))
        self.finish(result)

    @capture_exceptions
    async def get(self, *args, **kwargs):
        self.event['value'] = 1

        result = await ensure_awaitable(
            self.pipeline.fetch(**self.args))
        self.finish(result)


class QueryHandler(BaseQueryHandler):
    '''
    Biothings Query Endpoint

    URL pattern examples:

        /{pre}/{ver}/{typ}/query/?
        /{pre}/{ver}//query/?

        GET -> {...}
        POST -> [{...}, ...]
    '''
    name = 'query'

    @capture_exceptions
    async def post(self, *args, **kwargs):
        self.event['value'] = len(self.args['q'])

        result = await ensure_awaitable(
            self.pipeline.search(**self.args))
        self.finish(result)

    @capture_exceptions
    async def get(self, *args, **kwargs):
        self.event['value'] = 1
        if self.args.get('fetch_all'):
            self.event['label'] = 'fetch_all'

        if self.args.get('fetch_all') or \
                self.args.get('scroll_id') or \
                self.args.get('q') == '__any__':
            self.clear_header('Cache-Control')

        response = await ensure_awaitable(
            self.pipeline.search(**self.args))
        self.finish(response)
