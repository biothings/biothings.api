from tornado.web import HTTPError
from biothings.www.api_es.handlers.base_handler import BaseESRequestHandler

class BiothingHandler(BaseESRequestHandler):
    def initialize(self, web_settings):
        super(BiothingHandler, self).initialize(web_settings)
        self.ga_event_object_ret['action'] = self.request.method
        if self.request.method == 'GET'
            self.ga_event_object_ret['action'] = self.web_settings.ANNOTATION_GET_GA_ACTION
            self.control_kwargs = self.web_settings.ANNOTATION_GET_CONTROL_KWARGS
            self.es_kwargs = self.web_settings.ANNOTATION_GET_ES_KWARGS
            self.esqb_kwargs = self.web_settings.ANNOTATION_GET_ESQB_KWARGS
            self.transform_kwargs = self.web_settings.ANNOTATION_GET_TRANSFORM_KWARGS
        elif self.request.method == 'POST':
            self.ga_event_object_ret['action'] = self.web_settings.ANNOTATION_POST_GA_ACTION
            self.control_kwargs = self.web_settings.ANNOTATION_POST_CONTROL_KWARGS
            self.es_kwargs = self.web_settings.ANNOTATION_POST_ES_KWARGS
            self.esqb_kwargs = self.web_settings.ANNOTATION_POST_ESQB_KWARGS
            self.transform_kwargs = self.web_settings.ANNOTATION_POST_TRANSFORM_KWARGS
        else:
            # handle other verbs?
            pass
        self.logger.debug("BiothingHandler - {}".format(self.request.method))
        self.logger.debug("Boolean parameters: {}".format(self.boolean_parameters))
        self.logger.debug("Google Analytics Base object: {}".format(self.ga_event_object_ret))

    def _regex_redirect(self, bid):
        ''' subclass to redirect based on a regex pattern (or whatever)...'''
        pass

    def get(self, bid=None):
        '''
            ANNOTATION GET HANDLER
        '''
        if not bid:
            raise HTTPError(404)
            #return
            
        # redirect this id
        self._regex_redirect(bid)

        # get kwargs from query and sanitize them
        kwargs = self.get_query_params()

        # split kwargs into options
        options = self.get_cleaned_options(kwargs)

        self.logger.debug("Request kwargs: {}".format(kwargs))
        self.logger.debug("Request options: {}".format(options))

        # get the query for annotation GET handler
        _query_builder = self.web_settings.ES_QUERY_BUILDER(
            regex_list=self.web_settings.ANNOTATION_ID_REGEX_LIST, options=options.esqb_kwargs,
            logger_lvl=self.web_settings._LOGGER_LEVEL)
        _query = _query_builder.annotation_GET_query(bid)

        self.logger.debug("Request query: {}".format(_query))

        # return raw query, if requested
        if options.control_kwargs.rawquery:
            self.return_json(_query)
            self.ga_track(event=self.ga_event_object())
            return

        _backend = self.web_settings.ES_QUERY(index=self._get_query_index(options),
            doc_type=self._get_doc_type(options), client=self.es_client, 
            logger_lvl=self.web_settings._LOGGER_LEVEL)

        # do query
        if _query:
            res = _backend.query(query=_query, **options.es_kwargs)
        else:
            # try to get by _id, if no regex pattern matched
            res = _backend.get_biothing(bid, **options.es_kwargs)
        
        self.logger.debug("Raw query result: {}".format(res))

        # return raw result if requested
        if options.control_kwargs.raw:
            self.return_json(res)
            self.ga_track(event=self.ga_event_object())
            return

        # clean result
        _response_transformer = self.web_settings.RESPONSE_TRANSFORMER(
            options=options.control_kwargs, logger_lvl=self.web_settings._LOGGER_LEVEL)
        res = _response_transformer.clean_annotation_GET_response(res)

        # return result
        if not res:
            raise HTTPError(404)
            #return

        self.return_json(res)
        self.ga_track(event=self.ga_event_object())

    def post(self, ids=None):
        '''
            ANNOTATION POST HANDLER
        '''
        # get kwargs from query
        kwargs = self.get_query_params()
        
        # split kwargs into options
        options = self.get_cleaned_options(kwargs)
        
        self.logger.debug("Request kwargs: {}".format(kwargs))
        self.logger.debug("Request options: {}".format(options))
        
        if not options.control_kwargs.ids:
            self.return_json({'success': False, 'error': "Missing required parameters."})
            self.ga_track(event=self._ga_event_object({'qsize': 0}))
            return

        _query_builder = self.web_settings.ES_QUERY_BUILDER(options=options.esqb_kwargs,
            regex_list=self.web_settings.ANNOTATION_ID_REGEX_LIST, 
            logger_lvl=self.web_settings._LOGGER_LVL)
        _query = _query_builder.annotation_POST_query(options.control_kwargs.ids)
        
        self.logger.debug("Request query: {}".format(_query))

        if options.control_kwargs.rawquery:
            self.return_json(_query)
            self.ga_track(event=self.ga_event_object({'qsize': len(options.control_kwargs.ids)}))
            return
        
        _backend = self.web_settings.ES_QUERY(index=self._get_query_index(options),
            doc_type=self._get_doc_type(options), client=self.es_client, 
            logger_lvl=self.web_settings._LOGGER_LEVEL)

        res = _backend.mget_biothings(ids, **options.es_kwargs)
        
        self.logger.debug("Raw query result: {}".format(res))

        # return raw result if requested
        if options.control_kwargs.raw:
            self.return_json(res)
            self.ga_track(event=self.ga_event_object())
            return

        # clean result
        _response_transformer = self.web_settings.RESPONSE_TRANSFORMER(
            options=options.transform_kwargs, logger_lvl=self.web_settings._LOGGER_LEVEL)
        res = _response_transformer.clean_annotation_POST_response(res)
        
        # return and track
        self.return_json(res)
        self.ga_track(event=self._ga_event_object({'qsize': len(options.control_kwargs.ids)}))
