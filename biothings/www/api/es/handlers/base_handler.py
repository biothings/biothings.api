from biothings.www.helper import BaseHandler
import re

class BaseESRequestHandler(BaseHandler):
    # override these in child class
    control_kwargs = {}
    es_kwargs = {}
    esqb_kwargs = {}
    transform_kwargs = {}

    @property
    def _allowed_kwargs(self):
        ''' Return the kwargs that are allowed for this request '''
        try:
            return self._all_kwargs
        except:
            pass
        self._all_kwargs = set(list(self.control_kwargs.keys()) + list(self.es_kwargs.keys()) +
                               list(self.esqb_kwargs.keys()) + list(self.transform_kwargs.keys()))
        return self._all_kwargs

    def initialize(self, web_settings):
        ''' Initializations common to all ES Request Handlers here '''
        super(BaseESRequestHandler, self).initialize(web_settings)

    def _get_query_index(self, options):
        ''' Subclass to change query index for this request. '''
        return self.web_settings.ES_INDEX

    def _get_doc_type(self, options):
        ''' Subclass to change doc_type for this request. '''
        return self.web_settings.ES_DOC_TYPE
    
    def _translate_datasource(self, q, trim_from="", unescape=False):
        ''' translate string q using app specific regex '''
        for src in self.web_settings.DATASOURCE_TRANSLATIONS.keys():
            regex = DATASOURCE_TRANSLATIONS[src]
            if trim_from:
                regex = re.sub(trim_from + ".*", "", regex)
                src = re.sub(trim_from + ".*", "", src)
            if unescape:
                regex = regex.replace("\\", "")
                src = src.replace("\\", "")
            q = re.sub(src, regex, q, flags=re.I)
        return q

    def _should_sanitize(self, param, kwargs):
        return param in kwargs and param in self._allowed_kwargs

    def _sanitize_ids_param(self, kwargs):
        if self._should_sanitize('ids', kwargs):
            kwargs['ids'] = re.split('[\s\r\n+|,]+', kwargs['ids'])
        return kwargs

    def _sanitize_q_param(self, kwargs):
        # only translate datasources for query GET only
        if self._should_sanitize('q', kwargs):
            if self.request.method == 'GET':
                kwargs['q'] = self._translate_datasource(kwargs['q'])
            elif self.request.method == 'POST':
                pass
        return kwargs

    # class for handlers that make ES requests
    def _sanitize_facets_param(self, kwargs):
        '''Normalize facets params'''
        # Keep "facets" as part of API but translate to "aggregations" for ES2 compatibility
        if self._should_sanitize('facets', kwargs):
            kwargs['aggs'] = kwargs['facets']
            del kwargs['facets']
        return kwargs

    def _sanitize_fields_param(self, kwargs):
        '''support "filter" as an alias of "fields" parameter for back-compatability.'''
        if 'filter' in kwargs and 'fields' not in kwargs:
            #support filter as an alias of "fields" parameter (back compatibility)
            kwargs['fields'] = kwargs['filter']
            del kwargs['filter']
        if self._should_sanitize('fields', kwargs):
            kwargs['fields'] = self._translate_datasource(kwargs['fields'])
        return kwargs

    def _sanitize_scopes_param(self, kwargs):
        if self._should_sanitize('scopes', kwargs):
            kwargs['scopes'] = self._translate_datasource(kwargs['scopes'])
        return kwargs

    def _sanitize_size_param(self, kwargs):
        # cap size
        if self._should_sanitize('size', kwargs):
            cap = self.web_settings.SIZE_CAP
            kwargs['size'] = kwargs['size'] if (kwargs['size'] < cap) else cap
        return kwargs

    def _sanitize_paging_param(self, kwargs):
        '''support paging parameters, limit and skip as the aliases of size and from.'''
        if 'limit' in kwargs and 'size' not in kwargs:
            kwargs['size'] = kwargs['limit']
            del kwargs['limit']
        if 'skip' in kwargs and 'from' not in kwargs:
            kwargs['from'] = kwargs['skip']
            del kwargs['skip']
        if 'from' in kwargs:
            kwargs['from_'] = kwargs['from']   # elasticsearch python module using from_ for from parameter
            del kwargs['from']
        return kwargs

    def get_cleaned_options(self, kwargs):
        ''' Get options for handlers using ES requests '''
        options = dotdict()

        # split kwargs into one (or more) of 4 categories:
        #   * control_kwargs:  kwargs that control aspects of the handler's pipeline (e.g. raw, rawquery)
        #   * es_kwargs: kwargs that go directly to the ES query (e.g. fields, size, ...)
        #   * esqb_kwargs: kwargs that go directly to the ESQueryBuilder instance
        #   * transform_kwargs: kwargs that go directly to the response transformer (e.g. jsonld, dotfield)

        for kwarg_category in ["control_kwargs", "es_kwargs", "esqb_kwargs", "transform_kwargs"]:
            options.setdefault(kwarg_category, dotdict())
            for option, default in self.get(kwarg_category):
                options.get(kwarg_category).setdefault(option, kwargs.get(option, default))
        # store the host in kwargs for jsonld (not sure this is still required...)
        options.control_kwargs.host = self.request.host
        return options

    def _sanitize_params(self, kwargs):
        super(BaseESRequestHandler, self)._sanitize_params(kwargs)
        self._sanitize_fields_param(kwargs)
        self._sanitize_facets_param(kwargs)
        self._sanitize_paging_param(kwargs)
        self._sanitize_ids_param(kwargs)
        self._sanitize_size_param(kwargs)
        self._sanitize_q_param(kwargs)
        self._sanitize_scopes_param(kwargs)
