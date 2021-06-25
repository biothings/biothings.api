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

import logging
from collections import UserDict
from types import CoroutineType

import elasticsearch
from biothings.web.query.builder import RawQueryInterrupt
from biothings.web.query.engine import EndScrollInterrupt, RawResultInterrupt
from biothings.web.query.pipeline import QueryPipelineException
from tornado.web import Finish, HTTPError

from .api import BaseAPIHandler
from .exceptions import BadRequest, EndRequest

__all__ = [
    'BaseESRequestHandler',
    'MetadataSourceHandler',
    'MetadataFieldHandler',
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
            }
        }
    }

    def initialize(self, biothing_type=None):

        super(BaseESRequestHandler, self).initialize()
        self.biothing_type = biothing_type
        self.pipeline = self.biothings.pipeline
        self.metadata = self.biothings.metadata

    def prepare(self):

        super().prepare()

        # provide convenient access to next stages
        self.args.biothing_type = self.biothing_type

        # supported across es requests
        self.format = self.args.out_format or 'json'

        # look up GA action tab in web settings
        # defined by request method and endpoint name
        action_key = f'GA_ACTION_{self.name.upper()}_{self.request.method}'
        if hasattr(self.biothings.config, action_key):
            self.event['action'] = getattr(self.biothings.config, action_key)

    def parse_exception(self, exception):  # TODO merge this into  ESQueryPipelineHandler

        message = super().parse_exception(exception)

        if '_es_error' in message:
            _es_error = message.pop('_es_error')
            message['error'] = _es_error.error
            try:
                root_cause = _es_error.info.get('error', _es_error.info)
                root_cause = root_cause['root_cause'][0]['reason']
                root_cause = root_cause.replace('\"', '\'').split('\n')
                for index, cause in enumerate(root_cause):
                    message['root_cuase_line_'+f'{index:02}'] = cause
            except IndexError:
                pass  # no root cause
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
        await self.metadata.refresh(self.biothing_type)
        mapping = self.metadata.get_mappings(self.biothing_type)

        if self.args.raw:
            raise Finish(mapping)

        result = self.pipeline.formatter.transform_mapping(
            mapping, self.args.prefix, self.args.search)

        self.finish(result)


class InterruptResult(UserDict):
    pass

def ESQueryPipelineHandler(handler_class):
    def handle_exceptions(method):
        async def _method(*args, **kwargs):
            try:
                return await method(*args, **kwargs)
            except (
                RawQueryInterrupt,  # correspond to 'rawquery' option
                RawResultInterrupt,  # correspond to 'raw' option
                EndScrollInterrupt
            ) as exc:
                _exc = EndRequest()
                _exc.kwargs = InterruptResult(exc.data) \
                    if isinstance(exc.data, dict) else exc.data
                # use a non-dict wrapper so that the data
                # is not merged with the error template.
                raise _exc

            except AssertionError as exc:
                # in our application, AssertionError should be internal
                # the individual components raising the error should instead
                # rasie exceptions like ValueError and TypeError for bad input
                logging.error("FIXME: Unexpected Assertion Error.")
                raise HTTPError(reason=str(exc))

            except (ValueError, TypeError) as exc:
                raise BadRequest(reason=type(exc).__name__, details=str(exc))

            except QueryPipelineException as exc:
                raise HTTPError(exc.code, reason=exc.data)

            except elasticsearch.ConnectionError:  # like timeouts..
                raise HTTPError(503)

            except elasticsearch.RequestError as exc:  # 400s
                raise BadRequest(_es_error=exc)

            except elasticsearch.TransportError as exc:  # >400
                if exc.error == 'search_phase_execution_exception':
                    reason = exc.info.get("caused_by", {}).get("reason", "")
                    if "rejected execution" in reason:
                        raise EndRequest(503, reason="server busy")
                    else:  # unexpected, provide additional information
                        raise EndRequest(500, _es_error=exc, **exc.info)
                elif exc.error == 'index_not_found_exception':
                    raise HTTPError(500, reason=exc.error)
                elif exc.status_code == 'N/A':
                    raise HTTPError(503)
                else:  # unexpected
                    raise

        return _method
    handler_class.get = handle_exceptions(handler_class.get)
    handler_class.post = handle_exceptions(handler_class.post)
    return handler_class


async def ensure_awaitable(obj):
    if isinstance(obj, CoroutineType):
        return await obj
    return obj

@ESQueryPipelineHandler
class BiothingHandler(BaseESRequestHandler):
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

    async def post(self, *args, **kwargs):
        self.event['qsize'] = len(self.args['id'])

        result = await ensure_awaitable(
            self.pipeline.fetch(**self.args))
        self.finish(result)

    async def get(self, *args, **kwargs):
        result = await ensure_awaitable(
            self.pipeline.fetch(**self.args))
        self.finish(result)


@ESQueryPipelineHandler
class QueryHandler(BaseESRequestHandler):
    '''
    Biothings Query Endpoint

    URL pattern examples:

        /{pre}/{ver}/{typ}/query/?
        /{pre}/{ver}//query/?

        GET -> {...}
        POST -> [{...}, ...]
    '''
    name = 'query'

    async def post(self, *args, **kwargs):
        self.event['qsize'] = len(self.args['q'])

        result = await ensure_awaitable(
            self.pipeline.search(**self.args))
        self.finish(result)

    async def get(self, *args, **kwargs):
        if self.args.get('fetch_all'):
            self.event['action'] = 'fetch_all'

        if self.args.get('fetch_all') or \
                self.args.get('scroll_id') or \
                self.args.get('q') == '__any__':
            self.clear_header('Cache-Control')

        response = await ensure_awaitable(
            self.pipeline.search(**self.args))
        self.finish(response)

        self.event['total'] = response.get('total', 0)
