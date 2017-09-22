from tornado.web import HTTPError
from biothings.web.api.es.handlers.base_handler import BaseESRequestHandler
from biothings.utils.web import sum_arg_dicts
from biothings.web.api.helper import BiothingParameterTypeError
import logging
import traceback

class BiothingHandler(BaseESRequestHandler):
    ''' Request handlers for requests to the annotation lookup endpoint '''
    def initialize(self, web_settings):
        ''' Tornado handler `.initialize() <http://www.tornadoweb.org/en/stable/web.html#tornado.web.RequestHandler.initialize>`_ function for all requests to the annotation lookup endpoint.
        Here, the allowed arguments are set (depending on the request method) for each kwarg category.'''
        super(BiothingHandler, self).initialize(web_settings)
        self.ga_event_object_ret['action'] = self.request.method
        if self.request.method == 'GET':
            self.ga_event_object_ret['action'] = self.web_settings.GA_ACTION_ANNOTATION_GET
            self.control_kwargs = self.web_settings.ANNOTATION_GET_CONTROL_KWARGS
            self.es_kwargs = self.web_settings.ANNOTATION_GET_ES_KWARGS
            self.esqb_kwargs = self.web_settings.ANNOTATION_GET_ESQB_KWARGS
            self.transform_kwargs = self.web_settings.ANNOTATION_GET_TRANSFORM_KWARGS
        elif self.request.method == 'POST':
            self.ga_event_object_ret['action'] = self.web_settings.GA_ACTION_ANNOTATION_POST
            self.control_kwargs = self.web_settings.ANNOTATION_POST_CONTROL_KWARGS
            self.es_kwargs = self.web_settings.ANNOTATION_POST_ES_KWARGS
            self.esqb_kwargs = self.web_settings.ANNOTATION_POST_ESQB_KWARGS
            self.transform_kwargs = self.web_settings.ANNOTATION_POST_TRANSFORM_KWARGS
        else:
            # handle other verbs?
            pass
        self.kwarg_settings = sum_arg_dicts(self.control_kwargs, self.es_kwargs, 
                                        self.esqb_kwargs, self.transform_kwargs)
        logging.debug("BiothingHandler - {}".format(self.request.method))
        logging.debug("Google Analytics Base object: {}".format(self.ga_event_object_ret))
        logging.debug("Kwarg settings: {}".format(self.kwarg_settings))

    def _regex_redirect(self, bid):
        ''' subclass to redirect based on a regex pattern (or whatever)...'''
        pass

    def get(self, bid=None):
        ''' Handle a GET to the annotation lookup endpoint.'''
        if not bid:
            self.return_json({'success': False, 'error': self.web_settings.ID_REQUIRED_MESSAGE}, status_code=404)
            return
            
        # redirect this id
        self._regex_redirect(bid)

        ###################################################
        #              Get query parameters    
        ###################################################

        # get kwargs from query and sanitize them
        kwargs = self.get_query_params()
        
        ###################################################
        #           Split kwargs into categories    
        ###################################################

        # split kwargs into options
        options = self.get_cleaned_options(kwargs)

        logging.debug("Request kwargs: {}".format(kwargs))
        logging.debug("Request options: {}".format(options))

        options = self._pre_query_builder_GET_hook(options)
        
        ###################################################
        #           Instantiate pipeline classes    
        ###################################################

        # Instantiate query builder, query and transform classes
        _query_builder = self.web_settings.ES_QUERY_BUILDER(options=options.esqb_kwargs,
                regex_list=self.web_settings.ANNOTATION_ID_REGEX_LIST, index=self._get_es_index(options),
                doc_type=self._get_es_doc_type(options), es_options=options.es_kwargs, 
                default_scopes=self.web_settings.DEFAULT_SCOPES)
        _backend = self.web_settings.ES_QUERY(client=self.web_settings.es_client, options=options.es_kwargs)
        _result_transformer = self.web_settings.ES_RESULT_TRANSFORMER(options=options.transform_kwargs, 
            host=self.request.host, doc_url_function=self.web_settings.doc_url,
            output_aliases=self.web_settings.OUTPUT_KEY_ALIASES, jsonld_context=self.web_settings._jsonld_context, source_metadata=self.web_settings.source_metadata())
        
        ###################################################
        #                Build query    
        ###################################################
        
        # get the query for annotation GET handler
        _query = _query_builder.annotation_GET_query(bid)

        logging.debug("Request query kwargs: {}".format(_query))

        # return raw query, if requested
        if options.control_kwargs.rawquery:
            self._return_data_and_track(_query.get('body', {'GET': bid}), rawquery=True)
            return

        _query = self._pre_query_GET_hook(options, _query)
        
        ###################################################
        #               Execute query    
        ###################################################

        try:
            res = _backend.annotation_GET_query(_query)
        except Exception:
            self.log_exceptions("Error executing query")
            self.return_json({'success': False, 'error': self.web_settings.ID_NOT_FOUND_TEMPLATE.format(bid=bid)}, status_code=404)
            #raise HTTPError(404)
            return
        
        #logging.debug("Raw query result: {}".format(res))

        # return raw result if requested
        if options.control_kwargs.raw:
            self._return_data_and_track(res)
            return

        res = self._pre_transform_GET_hook(options, res)
        
        ###################################################
        #           Transforming query result    
        ###################################################

        # clean result
        try:
            res = _result_transformer.clean_annotation_GET_response(res)
        except Exception:
            self.log_exceptions("Error transforming result")
            self.return_json({'success': False, 'error': self.web_settings.ID_NOT_FOUND_TEMPLATE.format(bid=bid)}, status_code=404)
            #raise HTTPError(404)
            return

        # return result
        if not res:
            self.return_json({'success': False, 'error': self.web_settings.ID_NOT_FOUND_TEMPLATE.format(bid=bid)}, status_code=404)
            #raise HTTPError(404)
            return

        res = self._pre_finish_GET_hook(options, res)

        self._return_data_and_track(res)

    ###########################################################################

    def post(self, ids=None):
        ''' Handle a POST to the annotation lookup endpoint '''
        
        ###################################################
        #           Get query parameters    
        ###################################################
        
        try:
            kwargs = self.get_query_params()
        except BiothingParameterTypeError as e:
            self._return_data_and_track({'success': False, 'error': "{0}".format(e)}, ga_event_data={'qsize': 0}, status_code=400)
            return
        #except Exception as e:
        #    self.log_exceptions("Error in get_query_params")
        #    self._return_data_and_track({'success': False, 'error': "Error parsing input parameter, check input types"}, ga_event_data={'qsize': 0})
        #    return 

        # split kwargs into options
        options = self.get_cleaned_options(kwargs)
        
        logging.debug("Request kwargs: {}".format(kwargs))
        logging.debug("Request options: {}".format(options))
        
        if not options.control_kwargs.ids:
            self._return_data_and_track({'success': False, 'error': "Missing required parameters."}, 
                                        ga_event_data={'qsize': 0}, status_code=400)
            return
        
        options = self._pre_query_builder_POST_hook(options)
        
        ###################################################
        #           Instantiate pipeline classes    
        ###################################################

        _query_builder = self.web_settings.ES_QUERY_BUILDER(options=options.esqb_kwargs,
            regex_list=self.web_settings.ANNOTATION_ID_REGEX_LIST, index=self._get_es_index(options),
            doc_type=self._get_es_doc_type(options), es_options=options.es_kwargs, default_scopes=self.web_settings.DEFAULT_SCOPES)
        _backend = self.web_settings.ES_QUERY(client=self.web_settings.es_client, options=options.es_kwargs)
        _result_transformer = self.web_settings.ES_RESULT_TRANSFORMER(options=options.transform_kwargs, 
            host=self.request.host, doc_url_function=self.web_settings.doc_url,
            jsonld_context=self.web_settings._jsonld_context, output_aliases=self.web_settings.OUTPUT_KEY_ALIASES, source_metadata=self.web_settings.source_metadata())
        
        ###################################################
        #           Build query    
        ###################################################

        _query = _query_builder.annotation_POST_query(options.control_kwargs.ids)
        #try:
        #    _query = _query_builder.annotation_POST_query(options.control_kwargs.ids)
        #except Exception as e:
        #    self.log_exceptions("Error building annotation POST query")
        #    self._return_data_and_track({'success': False, 'error': 'Error building query'}, ga_event_data={'qsize': len(options.control_kwargs.ids)})
        #    return

        logging.debug("Request query: {}".format(_query))

        if options.control_kwargs.rawquery:
            self._return_data_and_track(_query, ga_event_data={'qsize': len(options.control_kwargs.ids)}, rawquery=True)
            return

        _query = self._pre_query_POST_hook(options, _query)
        
        ###################################################
        #           Execute query    
        ###################################################

        try:
            res = _backend.annotation_POST_query(_query)
        except TypeError as e:
            self.log_exceptions("Error executing annotation POST query")
            self._return_data_and_track({'success': False, 'error': 'Error executing query'},
                            ga_event_data={'qsize': len(options.control_kwargs.ids)}, status_code=400)
            return

        #logging.debug("Raw query result: {}".format(res))

        # return raw result if requested
        if options.control_kwargs.raw:
            self._return_data_and_track(res, ga_event_data={'qsize': len(options.control_kwargs.ids)})
            return

        res = self._pre_transform_POST_hook(options, res)

        ###################################################
        #           Transform query results    
        ###################################################

        # clean result
        res = _result_transformer.clean_annotation_POST_response(bid_list=options.control_kwargs.ids, res=res)
        #try:
        #    res = _result_transformer.clean_annotation_POST_response(bid_list=options.control_kwargs.ids, res=res)
        #except Exception as e:
        #    self.log_exceptions("Error transforming annotation POST results")
        #    self._return_data_and_track({'success': False, 'error': 'Error transforming results'},
        #                    ga_event_data={'qsize': len(options.control_kwargs.ids)})       
 
        res = self._pre_finish_POST_hook(options, res)

        # return and track
        self._return_data_and_track(res, ga_event_data={'qsize': len(options.control_kwargs.ids)})
