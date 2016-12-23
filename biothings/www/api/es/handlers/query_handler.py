from tornado.web import HTTPError
from biothings.www.api_es.handlers.base_handler import BaseESRequestHandler
from biothings.www.helper import BiothingParameterTypeError

class QueryHandler(BaseESRequestHandler):
    def initialize(self, web_settings):
        super(QueryHandler, self).initialize(web_settings)
        self.ga_event_object_ret['action'] = self.request.method
        if self.request.method == 'GET'
            self.ga_event_object_ret['action'] = self.web_settings.QUERY_GET_GA_ACTION
            self.control_kwargs = self.web_settings.QUERY_GET_CONTROL_KWARGS
            self.es_kwargs = self.web_settings.QUERY_GET_ES_KWARGS
            self.esqb_kwargs = self.web_settings.QUERY_GET_ESQB_KWARGS
            self.transform_kwargs = self.web_settings.QUERY_GET_TRANSFORM_KWARGS
        elif self.request.method == 'POST':
            self.ga_event_object_ret['action'] = self.web_settings.QUERY_POST_GA_ACTION
            self.control_kwargs = self.web_settings.QUERY_POST_CONTROL_KWARGS
            self.es_kwargs = self.web_settings.QUERY_POST_ES_KWARGS
            self.esqb_kwargs = self.web_settings.QUERY_POST_ESQB_KWARGS
            self.transform_kwargs = self.web_settings.QUERY_POST_TRANSFORM_KWARGS
        else:
            # handle other verbs?
            pass
        self.logger.debug("QueryHandler - {}".format(self.request.method))
        self.logger.debug("Boolean parameters: {}".format(self.boolean_parameters))
        self.logger.debug("Google Analytics Base object: {}".format(self.ga_event_object_ret))
    
    def get(self):
        '''
            QUERY GET HANDLER
        '''
        try:
            kwargs = self.get_query_params()
        except BiothingParameterTypeError as e:
            self.return_json({'success': False, 'error': "{0}".format(e)})
            self.ga_track(event=self._ga_event_object({'total': 0}))
            return

        options = self.get_cleaned_options(kwargs)

        self.logger.debug("Request kwargs: {}".format(kwargs))
        self.logger.debug("Request options: {}".format(options))

        if not options.control_kwargs.q and not options.control_kwargs.scroll_id:
            self.return_json({'success': False, 'error': "Missing required parameters."})
            self.ga_track(event=self._ga_event_object({'total': 0}))
            return

        _backend = self.web_settings.ES_QUERY(index=self._get_query_index(options), 
            doc_type=self._get_doc_type(options), client=self.es_client, 
            logger_lvl=self.web_settings._LOGGER_LEVEL)
        _response_transformer = self.web_settings.RESPONSE_TRANSFORMER(options=options.transform_kwargs,
            logger_lvl=self.web_settings._LOGGER_LEVEL)

        if options.control_kwargs.scroll_id:
            # Do scroll
            res = _backend.scroll(options.control_kwargs.scroll_id)

            self.logger.debug("Raw scroll query result: {}".format(res))
            
            if options.control_kwargs.raw:
                self.return_json(res)
                self.ga_track(event=self.ga_event_object({'total': res.get('total', 0)}))
                return

            res = _response_transformer.clean_scroll_result(res)
        else:
            _query_builder = self.web_settings.ES_QUERY_BUILDER(options=options.esqb_kwargs,
                datasource_translation=self.web_settings.DATASOURCE_TRANSLATION, 
                logger_lvl=self.web_settings._LOGGER_LEVEL)
            _query = _query_builder.query_GET_query(q=options.control_kwargs.q)

            if options.control_kwargs.rawquery:
                self.return_json(_query)
                self.ga_track(event=self.ga_event_object({'total': 0}))
                return

            res = _backend.query(query=_query, options=options)

            self.logger.debug("Raw query result: {}".format(res))

            # return raw result if requested
            if options.control_kwargs.raw:
                self.return_json(res)
                self.ga_track(event=self.ga_event_object({'total': res.get('total', 0)}))
                return

            # clean result
            res = _response_transformer.clean_query_GET_response(res)

        # return and track
        self.return_json(res)
        if options.control_kwargs.fetch_all:
            self.ga_event_object_ret['action'] = 'fetch_all'
        self.ga_track(event=self.ga_event_object({'total': res.get('total', 0)}))
    
    def post(self):
        '''
            QUERY POST HANDLER
        '''
        """kwargs = self.get_query_params()
        self._examine_kwargs('POST', kwargs)
        q = kwargs.pop('q', None)
        jsoninput = kwargs.pop('jsoninput', None) in ('1', 'true')
        if q:
            # ids = re.split('[\s\r\n+|,]+', q)
            try:
                ids = json.loads(q) if jsoninput else split_ids(q)
                if not isinstance(ids, list):
                    raise ValueError
            except ValueError:
                ids = None
                res = {'success': False, 'error': 'Invalid input for "q" parameter.'}
            if ids:
                scopes = kwargs.pop('scopes', None)
                fields = kwargs.pop('fields', None)
                res = self.esq.mquery_biothings(ids, fields=fields, scopes=scopes, **kwargs)
        else:
            res = {'success': False, 'error': "Missing required parameters."}

        encode = not isinstance(res, str)    # when res is a string, e.g. when rawquery is true, do not encode it as json
        self.return_json(res, encode=encode)
        self.ga_track(event=self._ga_event_object('POST', {'qsize': len(q) if q else 0}))"""
        pass
