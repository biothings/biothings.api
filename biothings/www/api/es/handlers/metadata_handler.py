from tornado.web import HTTPError
from biothings.www.api.es.handlers.base_handler import BaseESRequestHandler
from biothings.utils.common import sum_arg_dicts
import logging

class MetadataHandler(BaseESRequestHandler):
    def initialize(self, web_settings):
        super(MetadataHandler, self).initialize(web_settings)
        if self.request.method == 'GET':
            self.control_kwargs = self.web_settings.METADATA_GET_CONTROL_KWARGS
            self.es_kwargs = self.web_settings.METADATA_GET_ES_KWARGS
            self.esqb_kwargs = self.web_settings.METADATA_GET_ESQB_KWARGS
            self.transform_kwargs = self.web_settings.METADATA_GET_TRANSFORM_KWARGS
            self.kwarg_settings = sum_arg_dicts(self.control_kwargs, self.es_kwargs,
                                    self.esqb_kwargs, self.transform_kwargs)
        logging.debug("MetadataHandler - {}".format(self.request.method))
        logging.debug("Kwarg settings: {}".format(self.kwarg_settings))

    def get(self):
        logging.debug("Request object: {}".format(self.request))
        logging.debug("Request path: {}".format(self.request.path))
        kwargs = self.get_query_params()

        if kwargs is None:
            return

        options = self.get_cleaned_options(kwargs)

        logging.debug("Request kwargs: {}".format(kwargs))
        logging.debug("Request options: {}".format(options))

        options = self._pre_query_builder_GET_hook(options)

        # Instantiate query builder, query and transform classes
        _query_builder = self.web_settings.ES_QUERY_BUILDER(options=options.esqb_kwargs,
            index=self._get_es_index(options), doc_type=self._get_es_doc_type(options), es_options=options.es_kwargs)
        _backend = self.web_settings.ES_QUERY(client=self.web_settings.es_client, options=options.es_kwargs)
        _result_transformer = self.web_settings.ES_RESULT_TRANSFORMER(options=options.transform_kwargs, 
                    host=self.request.host, app_dir=self.web_settings.APP_GIT_REPOSITORY)

        # get the query for annotation GET handler
        try:
            _query = _query_builder.metadata_query()
        except Exception as e:
            logging.exception("Error building metadata query")
            self.return_json({'success': False, 'error': 'Error building query'})
            return

        logging.debug("Request query kwargs: {}".format(_query))

        # return raw query, if requested
        if options.control_kwargs.rawquery:
            self.return_json({}, rawquery=True)
            return

        _query = self._pre_query_GET_hook(options, _query)

        try:
            res = _backend.metadata_query(_query)
        except Exception:
            logging.exception("Error running query")
            self.return_json({'success': False, 'error': 'Error executing query'})
            return

        #logging.debug("Raw query result: {}".format(res))

        # return raw result if requested
        if options.control_kwargs.raw:
            self.return_json(res)
            return

        res = self._pre_transform_GET_hook(options, res)

        # clean result
        try:
            res = _result_transformer.clean_metadata_response(res, fields=self.request.path.endswith('fields'))
        except Exception:
            logging.exception("Error transforming result")
            self.return_json({'success': False, 'error': 'Error transforming query result'})
            return

        res = self._pre_finish_GET_hook(options, res)

        self.return_json(res)

        #_meta = self.esq.get_mapping_meta(**kwargs)
        #self._fill_software_info(_meta)
        #self.return_json(_meta)

