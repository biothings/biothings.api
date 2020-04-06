"""
    Biothings Annotation Endpoint

    URL pattern examples:

        /{pre}/{ver}/{typ}/?
        /{pre}/{ver}/{typ}/([^\/]+)/?

    GET /v3/gene/1017
        {
            "HGNC": "1771",
            "MIM": "116953",
            "_id": "1017",
            "_version": 1
            ...
        }

    GET /v3/gene/0
        {
            "success": false,
            "error": "ID '0' not found"
        }

    POST /v3/gene
    {
        "ids": ["1017"]
    }
    >>> [
            {
                "query": "1017",
                "HGNC": "1771",
                "MIM": "116953",
                "_id": "1017",
                ...
            }
        ]

    POST /v3/gene
    {
        "ids": ["0"]
    }
    >>> [
            {
                "query": "0",
                "notfound": true
            }
        ]

"""

from biothings.web.api.es.handlers.base_handler import ESRequestHandler
from biothings.web.api.handler import EndRequest

class BiothingHandler(ESRequestHandler):
    """
    An Annotation Request

    1.  queries a term against a pre-determined field that
        represents the id of a document, like _id and dbsnp.rsid

    2.  should find either exactly 1 or 0 result and returns
        either 200 for a document found or 404 for not found.

    3.  formats the response like the _source field in elasticsearch.
        Result does not contain _score field, but contains _version.

    """
    name = 'annotation'

    def pre_query_builder_hook(self, options):
        '''
        Use default scopes.
        Set GA tracking object.
        '''
        options.esqb.scopes = []
        if self.request.method == 'POST':
            self.ga_event_object({'qsize': len(options.esqb.q)})
        return options

    def pre_query_hook(self, options, query):
        '''
        Request _version field.
        '''
        options.es.version = True
        return super().pre_query_hook(options, query)

    def pre_finish_hook(self, options, res):
        '''
        Return single result for GET.
        Also does not return empty results.
        '''
        if isinstance(res, dict):
            if not res.get('hits'):
                template = self.web_settings.ID_NOT_FOUND_TEMPLATE
                reason = template.format(bid=options.esqb.q)
                raise EndRequest(404, reason=reason)
            res = res['hits'][0]
            res.pop('_score', None)

        elif isinstance(res, list):
            for hit in res:
                hit.pop('_score', None)

        return res
