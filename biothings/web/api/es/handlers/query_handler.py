from tornado.web import HTTPError
from biothings.web.api.es.handlers.base_handler import BaseESRequestHandler
from biothings.web.api.es.transform import ScrollIterationDone
from biothings.web.api.es.query import BiothingScrollError, BiothingSearchError
from biothings.web.api.helper import BiothingParameterTypeError
from biothings.utils.web import sum_arg_dicts
import logging

class QueryHandler(BaseESRequestHandler):
    ''' Request handlers for requests to the query endpoint '''

    def initialize(self, web_settings):
        ''' Tornado handler `.initialize() <http://www.tornadoweb.org/en/stable/web.html#tornado.web.RequestHandler.initialize>`_ function for all requests to the query endpoint.
        Here, the allowed arguments are set (depending on the request method) for each kwarg category.'''
        super(QueryHandler, self).initialize(web_settings)
        self.ga_event_object_ret['action'] = self.request.method
        if self.request.method == 'GET':
            self.ga_event_object_ret['action'] = self.web_settings.GA_ACTION_QUERY_GET
            self.control_kwargs = self.web_settings.QUERY_GET_CONTROL_KWARGS
            self.es_kwargs = self.web_settings.QUERY_GET_ES_KWARGS
            self.esqb_kwargs = self.web_settings.QUERY_GET_ESQB_KWARGS
            self.transform_kwargs = self.web_settings.QUERY_GET_TRANSFORM_KWARGS
        elif self.request.method == 'POST':
            self.ga_event_object_ret['action'] = self.web_settings.GA_ACTION_QUERY_POST
            self.control_kwargs = self.web_settings.QUERY_POST_CONTROL_KWARGS
            self.es_kwargs = self.web_settings.QUERY_POST_ES_KWARGS
            self.esqb_kwargs = self.web_settings.QUERY_POST_ESQB_KWARGS
            self.transform_kwargs = self.web_settings.QUERY_POST_TRANSFORM_KWARGS
        else:
            # handle other verbs?
            pass
        self.kwarg_settings = sum_arg_dicts(self.control_kwargs, self.es_kwargs,
                                            self.esqb_kwargs, self.transform_kwargs)
        logging.debug("QueryHandler - {}".format(self.request.method))
        logging.debug("Google Analytics Base object: {}".format(self.ga_event_object_ret))
        logging.debug("Kwarg Settings: {}".format(self.kwarg_settings))

    def _pre_scroll_transform_GET_hook(self, options, res):
        ''' Override me. '''
        return res

    async def get(self):
        ''' Handle a GET to the query endpoint. '''
        ###################################################
        #          Get/type/alias query parameters
        ###################################################

        try:
            kwargs = self.get_query_params()
        except BiothingParameterTypeError as e:
            self._return_data_and_track(
                {'success': False, 'error': "{0}".format(e)},
                ga_event_data={'total': 0},
                status_code=400)
            return
        #except Exception as e:
        #    self.log_exceptions("Error in get_query_params")
        #    self._return_data_and_track({'success': False, 'error': "Error parsing input parameter, check input types"}, ga_event_data={'total': 0})
        #    return

        ###################################################
        #      Split query parameters into categories
        ###################################################

        options = self.get_cleaned_options(kwargs)

        logging.debug("Request kwargs: {}".format(kwargs))
        logging.debug("Request options: {}".format(options))

        if not options.control_kwargs.q and not options.control_kwargs.scroll_id:
            self._return_data_and_track(
                {'success': False, 'error': "Missing required parameters."},
                ga_event_data={'total': 0},
                status_code=400, _format=options.control_kwargs.out_format)
            return

        options = self._pre_query_builder_GET_hook(options)

        ###################################################
        #          Instantiate pipeline classes
        ###################################################

        # Instantiate query builder, query, and transform classes
        _query_builder = self.web_settings.ES_QUERY_BUILDER(
            options=options.esqb_kwargs,
            index=self._get_es_index(options),
            doc_type=self._get_es_doc_type(options),
            es_options=options.es_kwargs,
            userquery_dir=self.web_settings.USERQUERY_DIR,
            scroll_options={
                'scroll': self.web_settings.ES_SCROLL_TIME,
                'size': self.web_settings.ES_SCROLL_SIZE},
            default_scopes=self.web_settings.DEFAULT_SCOPES,
            allow_random_query=self.web_settings.ALLOW_RANDOM_QUERY)
        _backend = self.web_settings.ES_QUERY(
            client=self.web_settings.async_es_client,
            options=options.es_kwargs)
        _result_transformer = self.web_settings.ES_RESULT_TRANSFORMER(
            options=options.transform_kwargs, host=self.request.host,
            doc_url_function=self.web_settings.doc_url,
            output_aliases=self.web_settings.OUTPUT_KEY_ALIASES,
            source_metadata=self.web_settings.source_metadata(),
            licenses=self.web_settings.LICENSE_TRANSFORM)

        ###################################################
        #           Scroll request pipeline
        ###################################################

        if options.control_kwargs.scroll_id:
            ###################################################
            #             Build scroll query
            ###################################################

            _query = _query_builder.scroll(options.control_kwargs.scroll_id)
            #try:
            #    _query = _query_builder.scroll(options.control_kwargs.scroll_id)
            #except Exception as e:
            #    self.log_exceptions("Error building scroll query")
            #    self._return_data_and_track({'success': False, 'error': 'Error building scroll query for scroll_id "{}"'.format(options.control_kwargs.scroll_id)}, ga_event_data={'total': 0})
            #    return

            ###################################################
            #             Get scroll results
            ###################################################

            try:
                res = await _backend.scroll(_query)
            except BiothingScrollError as e:
                self._return_data_and_track(
                    {'success': False, 'error': '{}'.format(e)},
                    ga_event_data={'total': 0},
                    status_code=400, _format=options.control_kwargs.out_format)
                return
            #except Exception as e:
            #    self.log_exceptions("Error getting scroll batch")
            #    self._return_data_and_track({'success': False, 'error': 'Error retrieving scroll results for scroll_id "{}"'.format(options.control_kwargs.scroll_id)}, ga_event_data={'total': 0})
            #    return

            #logging.debug("Raw scroll query result: {}".format(res))

            if options.control_kwargs.raw:
                self._return_data_and_track(
                    res, ga_event_data={'total': res.get('total', 0)},
                    _format=options.control_kwargs.out_format)
                return

            res = self._pre_scroll_transform_GET_hook(options, res)

            ###################################################
            #             Transform scroll result
            ###################################################

            try:
                res = _result_transformer.clean_scroll_response(res)
            except ScrollIterationDone as e:
                self._return_data_and_track(
                    {'success': False, 'error': '{}'.format(e)},
                    ga_event_data={'total': res.get('total', 0)},
                    status_code=200, _format=options.control_kwargs.out_format)
                return
            #except Exception as e:
            #    self.log_exceptions("Error transforming scroll batch")
            #    self._return_data_and_track({'success': False, 'error': 'Error transforming scroll results for scroll_id "{}"'.format(options.control_kwargs.scroll_id)})
            #    return
        else:
            #####  Non-scroll query GET pipeline #############
            ###################################################
            #             Build query
            ###################################################

            _query = _query_builder.query_GET_query(q=options.control_kwargs.q)
            #try:
            #    _query = _query_builder.query_GET_query(q=options.control_kwargs.q)
            #except Exception as e:
            #    self.log_exceptions("Error building query")
            #    self._return_data_and_track({'success': False, 'error': 'Error building query from q="{}"'.format(options.control_kwargs.q)}, ga_event_object={'total': 0})
            #    return

            if options.control_kwargs.rawquery:
                self._return_data_and_track(
                    _query, ga_event_data={'total': 0},
                    rawquery=True, _format=options.control_kwargs.out_format)
                return

            _query = self._pre_query_GET_hook(options, _query)

            ###################################################
            #             Get query results
            ###################################################

            try:
                res = await _backend.query_GET_query(_query)
            except BiothingSearchError as e:
                self._return_data_and_track(
                    {'success': False, 'error': '{0}'.format(e)},
                    ga_event_data={'total': 0},
                    status_code=400, _format=options.control_kwargs.out_format)
                return
            #except Exception as e:
            #    self.log_exceptions("Error executing query")
            #    self._return_data_and_track({'success': False, 'error': 'Error executing query'},
            #                                    ga_event_data={'total': 0})
            #    return

            #logging.debug("Raw query result")
            #logging.debug("Raw query result: {}".format(res))

            # return raw result if requested
            if options.control_kwargs.raw:
                self._return_data_and_track(
                    res, ga_event_data={'total': res.get('total', 0)},
                    _format=options.control_kwargs.out_format)
                return

            res = self._pre_transform_GET_hook(options, res)

            ###################################################
            #            Transform query results
            ###################################################
            # clean result
            res = _result_transformer.clean_query_GET_response(res)
            #try:
            #    res = _result_transformer.clean_query_GET_response(res)
            #except Exception as e:
            #    self.log_exceptions("Error transforming query")
            #    logging.debug("Return query GET")
            #    self._return_data_and_track({'success': False, 'error': 'Error transforming query result'},
            #                                ga_event_data={'total': res.get('total', 0)})
            #    return

        res = self._pre_finish_GET_hook(options, res)

        # return and track
        self.return_object(res, _format=options.control_kwargs.out_format)
        if options.control_kwargs.fetch_all:
            self.ga_event_object_ret['action'] = 'fetch_all'
        self.ga_track(event=self.ga_event_object({'total': res.get('total', 0)}))
        self.self_track(data=self.ga_event_object_ret)
        return

    ###########################################################################

    async def post(self):
        ''' Handle a POST to the query endpoint.'''
        ###################################################
        #          Get/type/alias query parameters
        ###################################################

        try:
            kwargs = self.get_query_params()
        except BiothingParameterTypeError as e:
            self._return_data_and_track(
                {'success': False, 'error': "{0}".format(e)},
                ga_event_data={'qsize': 0},
                status_code=400)
            return
        #except Exception as e:
        #    self.log_exceptions("Error in get_query_params")
        #    self._return_data_and_track({'success': False, 'error': "Error parsing input parameter, check input types"}, ga_event_data={'qsize': 0})
        #    return

        options = self.get_cleaned_options(kwargs)

        logging.debug("Request kwargs: {}".format(kwargs))
        logging.debug("Request options: {}".format(options))

        if not options.control_kwargs.q:
            self._return_data_and_track(
                {'success': False, 'error': "Missing required parameters."},
                ga_event_data={'qsize': 0},
                status_code=400, _format=options.control_kwargs.out_format)
            return

        options = self._pre_query_builder_POST_hook(options)

        ###################################################
        #          Instantiate pipeline classes
        ###################################################

        # Instantiate query builder, query, and transform classes
        _query_builder = self.web_settings.ES_QUERY_BUILDER(
            options=options.esqb_kwargs,
            index=self._get_es_index(options),
            doc_type=self._get_es_doc_type(options),
            es_options=options.es_kwargs,
            userquery_dir=self.web_settings.USERQUERY_DIR,
            default_scopes=self.web_settings.DEFAULT_SCOPES)
        _backend = self.web_settings.ES_QUERY(
            client=self.web_settings.async_es_client,
            options=options.es_kwargs)
        _result_transformer = self.web_settings.ES_RESULT_TRANSFORMER(
            options=options.transform_kwargs, host=self.request.host,
            doc_url_function=self.web_settings.doc_url,
            output_aliases=self.web_settings.OUTPUT_KEY_ALIASES,
            source_metadata=self.web_settings.source_metadata(),
            licenses=self.web_settings.LICENSE_TRANSFORM)

        ###################################################
        #                  Build query
        ###################################################

        _query = _query_builder.query_POST_query(
            qs=options.control_kwargs.q,
            scopes=options.esqb_kwargs.scopes)
        #try:
        #    _query = _query_builder.query_POST_query(qs=options.control_kwargs.q, scopes=options.esqb_kwargs.scopes)
        #except Exception as e:
        #    self.log_exceptions("Error building POST query")
        #    logging.debug("Returning query POST")
        #    self._return_data_and_track({'success': False, 'error': 'Error building query'}, ga_event_data={'qsize': len(options.control_kwargs.q)})
        #    return

        if options.control_kwargs.rawquery:
            self._return_data_and_track(
                _query,
                ga_event_data={
                    'qsize': len(
                        options.control_kwargs.q)},
                rawquery=True,
                _format=options.control_kwargs.out_format)
            return

        _query = self._pre_query_POST_hook(options, _query)

        ###################################################
        #                 Execute query
        ###################################################

        try:
            res = await _backend.query_POST_query(_query)
        except BiothingSearchError as e:
            self._return_data_and_track(
                {'success': False, 'error': '{0}'.format(e)},
                ga_event_data={'qsize': len(options.control_kwargs.q)},
                status_code=400, _format=options.control_kwargs.out_format)
            return
        #except Exception as e:
        #    self.log_exceptions("Error executing POST query")
        #    self._return_data_and_track({'success': False, 'error': 'Error executing query'}, ga_event_data={'qsize': len(options.control_kwargs.q)})
        #    return

        logging.debug("Raw query result: {}".format(res))

        # return raw result if requested
        if options.control_kwargs.raw:
            self._return_data_and_track(
                res, ga_event_data={'qsize': len(options.control_kwargs.q)},
                _format=options.control_kwargs.out_format)
            return

        res = self._pre_transform_POST_hook(options, res)

        ###################################################
        #          Transform query results
        ###################################################

        # clean result
        res = _result_transformer.clean_query_POST_response(qlist=options.control_kwargs.q, res=res)
        #try:
        #    res = _result_transformer.clean_query_POST_response(qlist=options.control_kwargs.q, res=res)
        #except Exception as e:
        #    self.log_exceptions("Error transforming POST query")
        #    self._return_data_and_track({'success': False, 'error': 'Error transforming query result'},
        #                        ga_event_data={'qsize': len(options.control_kwargs.q)})
        #    return

        res = self._pre_finish_POST_hook(options, res)

        # return and track
        self._return_data_and_track(
            res, ga_event_data={'qsize': len(options.control_kwargs.q)},
            _format=options.control_kwargs.out_format)
