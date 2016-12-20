from biothings.www.helper import BaseHandler

class BaseESRequestHandler(BaseHandler):
    # override these in child class
    control_kwargs = {}
    es_kwargs = {}
    esqb_kwargs = {}

    def _clean_annotation_GET_response(self, res, options):
        pass

    def _clean_annotation_POST_response(self, res, options):
        pass

    def _clean_query_GET_response(self, res, options):
        pass

    def _clean_query_POST_response(self, res, options):
        pass

    def _sanitize_ids_param(self, kwargs):
        ids = kwargs.pop('ids', '')
        if ids:
            ids = re.split('[\s\r\n+|,]+', ids)
            kwargs['ids'] = ids
        return kwargs

    # class for handlers that make ES requests
    def _sanitize_facets_param(self, kwargs):
        '''Normalize facets params'''
        # Keep "facets" as part of API but translate to "aggregations" for ES2 compatibility
        if 'facets' in kwargs:
            kwargs['aggs'] = kwargs['facets']
            del kwargs['facets']
        return kwargs

    def _sanitize_fields_param(self, kwargs):
        '''support "filter" as an alias of "fields" parameter for back-compatability.'''
        if 'filter' in kwargs and 'fields' not in kwargs:
            #support filter as an alias of "fields" parameter (back compatibility)
            kwargs['fields'] = kwargs['filter']
            del kwargs['filter']
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
        # cap size
        if 'size' in kwargs:
            cap = self.web_settings.size_cap
            try:
                kwargs['size'] = int(kwargs['size']) > cap and cap or kwargs['size']
            except ValueError:
                # int conversion failure is delegated to later process
                pass
        return kwargs

    def _extra_cleaned_options(self, options, kwargs):
        ''' Subclass to add endpoint-specific options'''
        pass

    def _sanitize_extra_ES_params(self, kwargs):
        ''' Subclass to add checks for ES specific parameters. '''
        pass

    def _get_cleaned_options(self, kwargs):
        ''' Get options for handlers using ES requests '''
        options = dotdict()

        # do any kwargs transformations
        kwargs = self._transform_kwargs(kwargs)

        # split kwargs into one (or more) of 3 categories:
        #   * control_kwargs:  kwargs that control aspects of the handler's pipeline (e.g. dotfield, jsonld)
        #   * es_kwargs: kwargs that go directly to the ES query (e.g. fields, size, ...)
        #   * esqb_kwargs: kwargs that go directly to the ESQueryBuilder instance

        for kwarg_category in ["control_kwargs", "es_kwargs", "esqb_kwargs"]:
            options.setdefault(kwarg_category, dotdict())
            for option, default in self.get(kwarg_category):
                options.get(kwarg_category).setdefault(option, kwargs.get(option, default))
        # store the host in kwargs for jsonld (not sure this is still required...)
        options.control_kwargs.host = self.request.host
        return options

    def get_cleaned_options(self, kwargs):
        options = self._get_cleaned_options(kwargs)
        options = self._extra_cleaned_options(options, kwargs)
        return options

    def _sanitize_extra_params(self, kwargs):
        ''' sanitize query parameters specific to ES '''
        self._sanitize_fields_param(kwargs)
        self._sanitize_paging_param(kwargs)
        self._sanitize_extra_ES_params(kwargs)
        return kwargs
