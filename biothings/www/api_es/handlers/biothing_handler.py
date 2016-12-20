import re
import json
from tornado.web import HTTPError
from biothings.www.helper import BaseHandler
from biothings.utils.common import split_ids
from biothings.utils.version import get_software_info
from collections import OrderedDict

class BiothingHandler(BaseESRequestHandler):
    def _extra_initializations(self):
        self.boolean_parameters = set(self.web_settings.annotation_boolean_params)
        self.ga_event_object_ret['action'] = self.request.method
        if self.request.method == 'GET'
            self.ga_event_object_ret['action'] = '_'.join([self.web_settings.annotation_endpoint,
                                                        self.web_settings.ga_event_for_get_action])
            self.control_kwargs = self.web_settings.annotation_GET_control_kwargs
            self.es_kwargs = self.web_settings.annotation_GET_es_kwargs
            self.esqb_kwargs = self.web_settings.annotation_GET_esqb_kwargs
        elif self.request.method == 'POST':
            self.ga_event_object_ret['action'] = '_'.join([self.web_settings.annotation_endpoint,
                                                        self.web_settings.ga_event_for_post_action])
            self.control_kwargs = self.web_settings.annotation_GET_control_kwargs
            self.es_kwargs = self.web_settings.annotation_GET_es_kwargs
            self.esqb_kwargs = self.web_settings.annotation_GET_esqb_kwargs
        else:
            # handle other verbs?
            pass

    def _regex_redirect(self, bid):
        ''' subclass to redirect based on a regex pattern (or whatever)...'''
        pass

    def get(self, bid=None):
        '''
        '''
        if bid:
            # redirect this id
            self._regex_redirect(bid)

            # get kwargs from query and sanitize them
            kwargs = self.get_query_params()

            # split kwargs into options
            options = self.get_cleaned_options(kwargs)

            # get the query for annotation GET handler
            _query_builder = self.web_settings.es_query_builder(
                regex_list=self.web_settings.annotation_id_regex_list, **options.esqb_kwargs)
            _query = _query_builder.annotation_GET_query(bid)

            # return raw query, if requested
            if options.control_kwargs.rawquery:
                self.return_json(_query)
                self.ga_track(event=self.ga_event_object())
                return

            # get backend for query
            _backend = self.web_settings.es_query(client=self.web_settings.es_client)

            # do query
            if _query:
                res = _backend.annotation_GET_query(query=_query, **options.es_kwargs)
            else:
                # try to get by _id, if no regex pattern matched
                res = _backend.get_biothing(bid, **options.es_kwargs)

            # return raw result if requested
            if options.control_kwargs.raw:
                self.return_json(res)
                self.ga_track(event=self.ga_event_object())
                return

            # clean result
            _response_transformer = web_settings.response_transformer(options.control_kwargs)
            res = _response_transformer.clean_annotation_GET_response(res)

            # return result
            if res:
                self.return_json(res)
                self.ga_track(event=self.ga_event_object())
                return
            else:
                raise HTTPError(404)
        else:
            raise HTTPError(404)

    def post(self, ids=None):
        '''
           This is essentially the same as post request in QueryHandler, with different defaults.

           parameters:
            ids
            fields
            email
            jsonld
        '''
        # get kwargs from query
        kwargs = self.get_query_params()
        # split kwargs into options
        options = self.get_cleaned_options(kwargs)
        if options.control_kwargs.ids:
            _query_builder = self.web_settings.es_query_builder(regex_list=self.web_settings.annotation_id_regex_list,
                                    **options.esqb_kwargs)
            _query = _query_builder.annotation_POST_query(options.control_kwargs.ids)

            if options.control_kwargs.rawquery:
                self.return_json(_query)
                self.ga_track(event=self.ga_event_object({'qsize': len(options.control_kwargs.ids)
                return

            _backend = self.web_settings.es_query(client=self.web_settings.es_client)

            res = _backend.mget_biothings(ids, **options.es_kwargs)
        else:
            res = {'success': False, 'error': "Missing required parameters."}
        encode = not isinstance(res, str)    # when res is a string, e.g. when rawquery is true, do not encode it as json
        self.return_json(res, encode=encode)
        self.ga_track(event=self._ga_event_object('POST', {'qsize': len(ids) if ids else 0}))
