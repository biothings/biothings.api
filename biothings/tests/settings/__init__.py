# -*- coding: utf-8 -*-
import os
from importlib import import_module
from biothings.settings import BiothingSettings

bs = BiothingSettings()

# Import the biothing settings module
nosetest_config = None
try:
    nosetest_config = import_module(bs.nosetest_settings)
except ImportError:
    pass

default = import_module('biothings.tests.settings.default')

class NosetestSettings(object):
    config_vars = {}
    if nosetest_config:
        config_vars = vars(nosetest_config)
    default_vars = vars(default)

    def _return_var(self, key):
        # return variable named key
        try:
            return self.config_vars[key]
        except KeyError:
            return self.default_vars[key]

    @property
    def jsonld_context_path(self):
        return bs.jsonld_context_path

    @property
    def nosetest_envar(self):
        return self._return_var('HOST_ENVAR_NAME')

    @property
    def nosetest_default_url(self):
        return self._return_var('NOSETEST_DEFAULT_URL')

    @property
    def api_version(self):
        return bs._api_version

    @property
    def query_endpoint(self):
        return bs._query_endpoint

    @property
    def annotation_endpoint(self):
        return bs._annotation_endpoint

    @property
    def annotation_attribute_query(self):
        return self._return_var('ANNOTATION_OBJECT_ID')
    
    @property
    def annotation_attribute_list(self):
        return self._return_var('ANNOTATION_OBJECT_EXPECTED_ATTRIBUTE_LIST')

    @property
    def annotation_GET(self):
        return self._return_var('ANNOTATION_GET_IDS')
    
    @property
    def annotation_POST(self):
        return self._return_var('ANNOTATION_POST_DATA')

    @property
    def query_GET(self):
        return self._return_var('QUERY_GETS')
    
    @property
    def query_POST(self):
        return self._return_var('QUERY_POST_DATA')

    @property
    def test_na_annotation(self):
        return self._return_var('ANNOTATION_NON_ASCII_ID')
    
    @property
    def test_na_query(self):
        return self._return_var('QUERY_NON_ASCII')
    
    @property
    def callback_query(self):
        return self._return_var('QUERY_CALLBACK_TEST')

    @property
    def test_query_size(self):
        return self._return_var('QUERY_SIZE_TEST')

    @property
    def minimum_acceptable_fields(self):
        return self._return_var('MINIMUM_NUMBER_OF_ACCEPTABLE_FIELDS')

    @property
    def test_fields_get_fields_endpoint(self):
        return self._return_var('TEST_FIELDS_GET_FIELDS_ENDPOINT')
    
    @property
    def additional_fields_for_check_fields_subset(self):
        return self._return_var('CHECK_FIELDS_SUBSET_ADDITIONAL_FIELDS')
    
    @property
    def additional_fields_for_check_fields_subset(self):
        return self._return_var('UNICODE_TEST_STRING')
