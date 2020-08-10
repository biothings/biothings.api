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

from tornado.web import Finish

from biothings.utils.version import get_software_info
from biothings.utils.web.es import get_es_versions

from .api import BaseAPIHandler
from .exceptions import BadRequest, EndRequest

__all__ = [
    'BaseESRequestHandler',
    'MetadataSourceHandler',
    'MetadataFieldHandler',
    'ESRequestHandler',
    'BiothingHandler',
    'QueryHandler'
]


class BaseESRequestHandler(BaseAPIHandler):

    kwargs = {
        '*': {
            'out_format': {
                'type': str,
                'default': 'json',
                'enum': ('json', 'yaml', 'html', 'msgpack'),
                'alias': ['format']
            },
            'jsoninput': {
                'type': bool
            }  # deprecated
        }
    }

    def initialize(self, biothing_type=None):

        super(BaseESRequestHandler, self).initialize()
        self.biothing_type = biothing_type or self.web_settings.ES_DOC_TYPE
        self.pipeline = self.web_settings.pipeline

    def prepare(self):

        super().prepare()

        # supported across es requests
        self.format = self.args.out_format or 'json'

        # look up GA action tab in web settings
        # defined by request method and endpoint name
        action_key = f'GA_ACTION_{self.name.upper()}_{self.request.method}'
        if hasattr(self.web_settings, action_key):
            self.event['action'] = getattr(self.web_settings, action_key)

    def parse_exception(self, exception):

        message = super().parse_exception(exception)

        if '_es_error' in message:
            _es_error = message.pop('_es_error')
            message['error'] = _es_error.error
            try:
                root_cause = _es_error.info['error']['root_cause'][0]['reason']
                root_cause = root_cause.replace('\"', '\'').split('\n')
                for index, cause in enumerate(root_cause):
                    message['root_cuase_line_'+f'{index:02}'] = cause
            except Exception:
                self.logger.exception('Error parsing es exception.')

        return message


class MetadataSourceHandler(BaseESRequestHandler):
    """
    GET /metadata
    """
    name = 'metadata'
    kwargs = dict(BaseESRequestHandler.kwargs)
    kwargs['GET'] = {
        'dev': {'type': bool, 'default': False},
        'raw': {'type': bool, 'default': False}
    }

    async def get(self):

        _raw = await self.web_settings.metadata.refresh(self.biothing_type)
        _meta = self.web_settings.metadata.biothing_metadata[self.biothing_type]

        if self.args.raw:
            raise Finish(_raw)
        elif self.args.dev:
            _meta['software'] = get_software_info(
                app_dir=self.web_settings.devinfo.get_git_repo_path())
            _meta['cluster'] = await get_es_versions(
                client=self.web_settings.connections.async_client)
            _meta['hosts'] = self.web_settings.connections.async_client.transport.info
        else:  # remove correlation info
            _meta.pop('_biothing', None)
            _meta.pop('_indices', None)

        _meta = self.extras(_meta)  # override here

        self.finish(dict(sorted(_meta.items())))

    def extras(self, _meta):
        """
        Override to add app specific metadata
        """
        return _meta


class MetadataFieldHandler(BaseESRequestHandler):
    """
    GET /metadata/fields
    """
    name = 'fields'
    kwargs = dict(BaseESRequestHandler.kwargs)
    kwargs['GET'] = {
        'raw': {'type': bool, 'default': False},
        'search': {'type': str, 'default': None},
        'prefix': {'type': str, 'default': None}
    }

    async def get(self):

        await self.web_settings.metadata.refresh(self.biothing_type)
        mapping = self.web_settings.metadata.biothing_mappings[self.biothing_type]

        if self.args.raw:
            raise Finish(mapping)

        # reformat
        result = self.pipeline.transform_mapping(
            mapping, self.args.prefix, self.args.search)

        self.finish(result)


class ESRequestHandler(BaseESRequestHandler):
    '''
    Default Implementation of ES Query Pipelines
    '''
    # kwargs defined in biothing.web.settings.default
    kwarg_groups = ('control', 'esqb', 'es', 'transform')
    kwarg_methods = ('get', 'post')

    async def get(self, *args, **kwargs):
        return await self.execute_pipeline(*args, **kwargs)

    async def post(self, *args, **kwargs):
        return await self.execute_pipeline(*args, **kwargs)

    async def execute_pipeline(self, *args, **kwargs):

        options = self.pre_query_builder_hook(self.args)

        ###################################################
        #                   Build query
        ###################################################

        self._query = self.pipeline.build(options.esqb.q, options.esqb)
        self._query = self.pre_query_hook(options, self._query)

        ###################################################
        #                   Execute query
        ###################################################

        self._res = await self.pipeline.execute(self._query, options.es)
        self._res = self.pre_transform_hook(options, self._res)

        ###################################################
        #                 Transform result
        ###################################################

        res = self.pipeline.transform(self._res, options.transform)
        res = self.pre_finish_hook(options, res)

        self.finish(res)

    def pre_query_builder_hook(self, options):
        """
        Override this in subclasses.
        At this stage, we have the cleaned user input available.
        Might be a good place to implement input based tracking.
        """
        options.es.biothing_type = self.biothing_type
        options.transform.biothing_type = self.biothing_type

        # define multi-query response format
        if self.request.method == 'POST':
            queries = options.esqb.q
            options.transform.templates = (dict(query=q) for q in queries)
            options.transform.template_miss = dict(notfound=True)
            options.transform.template_hit = dict()

        return options

    def pre_query_hook(self, options, query):
        """
        Override this in subclasses.
        By default, return raw query, if requested.
        Might want to persist this behavior by calling super().
        """
        if options.control.rawquery:
            raise Finish(query.to_dict())
        return query

    def pre_transform_hook(self, options, res):
        """
        Override this in subclasses.
        By default, return query response, if requested.
        Might want to persist this behavior by calling super().
        """
        if options.control.raw:
            raise Finish(res)
        return res

    def pre_finish_hook(self, options, res):
        '''
        Override this in subclasses.
        Could implement additional result translation.
        '''
        return res


class BiothingHandler(ESRequestHandler):
    """
    Biothings Annotation Endpoint

    URL pattern examples:

        /{pre}/{ver}/{typ}/?
        /{pre}/{ver}/{typ}/([^\/]+)/?

        queries a term against a pre-determined field that
        represents the id of a document, like _id and dbsnp.rsid

        GET -> {...}
        POST -> [{...}, ...]
    """
    name = 'annotation'

    def pre_query_builder_hook(self, options):
        '''
        Annotation query has default scopes.
        Annotation query include _version field.
        '''
        if self.request.method == 'POST':
            self.event['qsize'] = len(options.ids)  # GA
            options.esqb.q = options.ids
        elif self.request.method == 'GET':
            options.esqb.q = options.id
        options.esqb.regexs = self.web_settings.ANNOTATION_ID_REGEX_LIST
        options.esqb.scopes = self.web_settings.ANNOTATION_DEFAULT_SCOPES
        options.esqb.version = True
        options = super().pre_query_builder_hook(options)
        return options

    def pre_finish_hook(self, options, res):
        """
        Return single result for GET.
        Empty results trigger 404 error.

        Keep _version field,
        Discard _score field.
        """
        if isinstance(res, dict):
            if not res.get('hits'):
                template = self.web_settings.ID_NOT_FOUND_TEMPLATE
                reason = template.format(bid=options.esqb.q)
                raise EndRequest(404, reason=reason)
            if len(res['hits']) > 1:
                raise EndRequest(404, reason='not a unique id.')
            res = res['hits'][0]
            res.pop('_score', None)

        elif isinstance(res, list):
            for hit in res:
                hit.pop('_score', None)

        return res


class QueryHandler(ESRequestHandler):
    '''
    Biothings Query Endpoint

    URL pattern examples:

        /{pre}/{ver}/{typ}/query/?
        /{pre}/{ver}//query/?

        GET -> {...}
        POST -> [{...}, ...]
    '''
    name = 'query'

    def pre_query_builder_hook(self, options):

        options = super().pre_query_builder_hook(options)

        # GA
        if self.request.method == 'GET':
            self.event['total'] = 0
        elif self.request.method == 'POST':
            self.event['qsize'] = len(options.esqb.q)

        # by default scroll ignores sort and size
        if options.es.fetch_all:
            options.esqb.pop('sort', None)
            options.esqb.pop('size', None)

        return options

    def pre_finish_hook(self, options, res):

        # GA
        if self.request.method == 'GET':
            if options.es.fetch_all:
                self.event['action'] = 'fetch_all'
                self.event['total'] = res.get('total', 0)

        # Headers
        if options.es.fetch_all or options.es.scroll_id:
            self.clear_header('Cache-Control')

        return res
