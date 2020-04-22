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
        Annotation query has default scopes.
        Annotation query include _version field.
        Set GA tracking object.
        '''
        options = super().pre_query_builder_hook(options)
        if self.request.method == 'POST':
            self.ga_event_object({'qsize': len(options.esqb.ids)})
            options.esqb.q = options.esqb.ids
        elif self.request.method == 'GET':
            options.esqb.q = options.esqb.id
        options.esqb.regexs = self.web_settings.ANNOTATION_ID_REGEX_LIST
        options.esqb.scopes = self.web_settings.ANNOTATION_DEFAULT_SCOPES
        options.esqb.version = True
        return options

    def pre_finish_hook(self, options, res):
        """
        Return single result for GET.
        Also does not return empty results.

        GET /v3/gene/0
        {
            "success": false,
            "error": "ID '0' not found"
        }
        """
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
