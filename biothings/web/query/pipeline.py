
import asyncio
import logging
from collections import UserDict
from dataclasses import dataclass
from datetime import date

import elasticsearch
from biothings.web.query.builder import RawQueryInterrupt
from biothings.web.query.engine import EndScrollInterrupt, RawResultInterrupt

# here this module defines two types of operations supported in
# each query pipeline class, one called "search" which corresponds to
# a key-value pair search, with the sometimes optional parameter "scopes"
# being a list of keys to look for the value, named as parameter "q".
# the other type of query is called "fetch", that looks up documents
# basing on pre-defined fields that function as their mostly unique
# identifiers, it is a special type of "search" that ususally returns
# only one document. in this type of query, "scopes" are absolutely NOT
# provided, and the query term is called "id" in the parameter list.

# this module is called pipeline because
# a) it is a combination of individual query processing stages
# b) during async operations, it functions like the CPU pipeline,
#    more than 1 stage can be busy at a single point in time.

logger = logging.getLogger(__name__)

@dataclass
class QueryPipelineException(Exception):
    code: str = 500
    summary: str = ""
    details: object = None
    # use code here to indicate error types instead of
    # exception types, this reduces the number of potential
    # exception types this module needs to create.
    # furthermore, consider using a superset of HTTP codes,
    # so that error translation from the upper layers
    # is also convenient and straightforward.

class QueryPipelineInterrupt(QueryPipelineException):
    def __init__(self, data):
        super().__init__(200, None, data)

class QueryPipeline():
    def __init__(self, builder, backend, formatter):
        self.builder = builder
        self.backend = backend
        self.formatter = formatter

    def search(self, q, **options):
        query = self.builder.build(q, **options)
        result = self.backend.execute(query, **options)
        return self.formatter.transform(result, **options)

    def fetch(self, id, **options):
        assert options.get('scopes') is None
        result = self.search(id, **options)
        return result


def _simplify_ES_exception(exc, debug=False):
    result = {}
    try:
        root_cause = exc.info.get('error', exc.info)
        root_cause = root_cause['root_cause'][0]['reason']
        root_cause = root_cause.replace('\"', '\'').split('\n')
        for index, cause in enumerate(root_cause):
            result['root_cuase_line_'+f'{index:02}'] = cause
    except IndexError:
        pass  # no root cause
    except Exception:
        logger.exception('Error parsing es exception.')  # TODO

    if debug:  # raw ES error response
        result["debug"] = exc.info

    return exc.error, result

def capturesESExceptions(func):
    async def _(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except(
            RawQueryInterrupt,  # correspond to 'rawquery' option
            RawResultInterrupt,  # correspond to 'raw' option
            EndScrollInterrupt
        ) as exc:
            raise QueryPipelineInterrupt(exc.data)

        except AssertionError as exc:
            # in our application, AssertionError should be internal
            # the individual components raising the error should instead
            # rasie exceptions like ValueError and TypeError for bad input
            logging.error("FIXME: Unexpected Assertion Error.")
            raise QueryPipelineException(data=str(exc))

        except (ValueError, TypeError) as exc:
            raise QueryPipelineException(400, type(exc).__name__, str(exc))

        except elasticsearch.ConnectionError:  # like timeouts..
            raise QueryPipelineException(503)

        except elasticsearch.RequestError as exc:  # 400s
            raise QueryPipelineException(400, *_simplify_ES_exception(exc))

        # it seems like the managed Elasticsearch service by AWS
        # may provide slightly different exceptions when the server
        # is overloaded comparing to that of self-managed ES.

        # this case and most of the handling below can be further studied.
        # most of the exception handlings from this point on are based on
        # experience. further documentation in details will be helpful.

        except elasticsearch.TransportError as exc:  # >400
            if exc.error == 'search_phase_execution_exception':
                reason = exc.info.get("caused_by", {}).get("reason", "")

                if "rejected execution" in reason:
                    raise QueryPipelineException(503)
                else:  # unexpected, provide additional information for debug
                    raise QueryPipelineException(500, *_simplify_ES_exception(exc, True))

            elif exc.error == 'index_not_found_exception':
                raise QueryPipelineException(500, exc.error)

            elif exc.status_code in (429, 'N/A'):
                raise QueryPipelineException(503)
            else:  # unexpected
                raise
    return _

class AsyncESQueryPipeline(QueryPipeline):

    @capturesESExceptions
    async def search(self, q, **options):

        if isinstance(q, list):  # multisearch
            options['templates'] = (dict(query=_q) for _q in q)
            options['template_miss'] = dict(notfound=True)
            options['template_hit'] = dict()

        query = self.builder.build(q, **options)
        response = await self.backend.execute(query, **options)
        result = self.formatter.transform(response, **options)
        return result

    @capturesESExceptions
    async def fetch(self, id, **options):
        if options.get('scopes'):
            raise ValueError("Scopes Not Allowed.")

        # for builder
        options['autoscope'] = True
        # for formatter
        options['version'] = True
        options['score'] = False
        options['one'] = True

        # annotation endpoint should work on fields with reasonable
        # uniqueness of values, like id and symbol fields. because
        # we do not provide pagination for this endpoint, as it is
        # largely unncessary, in the case of matching too many docs
        # for one request, raise an exception instead of providing
        # the user incomplete matches. usually, when this happends,
        # it indicates bad choices of values as the default fields.

        MAX_MATCH = 1000
        options['size'] = MAX_MATCH + 1

        result = await self.search(id, **options)
        if result is None:
            raise QueryPipelineException(404, "Not Found.")

        if isinstance(result, list) and len(result) > MAX_MATCH:
            raise QueryPipelineException(500, "Too Many Matches.")

        return result

class ESQueryPipeline(QueryPipeline):  # over async client

    # These implementations may not be performance optimized
    # It is used as a proof of concept and helps the design
    # of upper layer constructs, it also simplifies testing
    # by providing ioloop management, enabling sync access.

    # >>> from biothings.web.query.pipeline import ESQueryPipeline
    # >>> pipeline = ESQueryPipeline()
    #
    # >>> pipeline.fetch("1017", _source=["symbol"])
    # {'_id': '1017', '_version': 1, 'symbol': 'CDK2'}
    #
    # >>> pipeline.search("1017", _source=["symbol"])
    # {
    #   'took': 11,
    #   'total': 1,
    #   'max_score': 4.0133753,
    #   'hits': [
    #       {
    #           '_id': '1017',
    #           '_score': 4.0133753,
    #           'symbol': 'CDK2'
    #       }
    #   ]
    # }

    def __init__(
        self, builder=None, backend=None, formatter=None, *args, **kwargs
    ):
        if not builder:
            from biothings.web.query.builder import ESQueryBuilder
            builder = ESQueryBuilder()

        if not backend:
            from biothings.web.connections import get_es_client
            from biothings.web.query.engine import ESQueryBackend
            client = get_es_client(async_=True)
            backend = ESQueryBackend(client)

        if not formatter:
            from biothings.web.query.formatter import ESResultFormatter
            formatter = ESResultFormatter()

        super().__init__(builder, backend, formatter, *args, **kwargs)

    def _run_coroutine(self, coro, *args, **kwargs):
        loop = asyncio.get_event_loop()
        pipeline = AsyncESQueryPipeline(
            self.builder, self.backend, self.formatter)
        return loop.run_until_complete(
            coro(pipeline, *args, **kwargs))

    def search(self, q, **options):
        return self._run_coroutine(
            AsyncESQueryPipeline.search, q, **options)

    def fetch(self, id, **options):
        return self._run_coroutine(
            AsyncESQueryPipeline.fetch, id, **options)

class MongoQueryPipeline(QueryPipeline):
    pass

class SQLQueryPipeline(QueryPipeline):
    pass
