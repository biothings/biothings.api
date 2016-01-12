# -*- coding: utf-8 -*-
from biothings.www.api.handlers import MetaDataHandler, BiothingHandler, QueryHandler, StatusHandler, FieldsHandler
from www.api.es import ESQuery
import config

class ${annotation_handler_name}(BiothingHandler):
    ''' This class is for the /${annotation_endpoint} endpoint. '''
    _settings = config.${settings_class}()
    esq = ESQuery(_settings)

class ${query_handler_name}(QueryHandler):
    ''' This class is for the /${query_endpoint} endpoint. '''
    _settings = config.${settings_class}()
    esq = ESQuery(_settings)

class StatusHandler(StatusHandler):
    ''' This class is for the /status endpoint. '''
    _settings = config.${settings_class}()
    esq = ESQuery(_settings)

class FieldsHandler(FieldsHandler):
    ''' This class is for the /metadata/fields endpoint. '''
    _settings = config.${settings_class}()
    esq = ESQuery(_settings)

class MetaDataHandler(MetaDataHandler):
    ''' This class is for the /metadata endpoint. '''
    _settings = config.${settings_class}()
    esq = ESQuery(_settings)


def return_applist():
    _settings = config.${settings_class}()
    ret = [
        (r"/status", StatusHandler),
        (r"/metadata", MetaDataHandler),
        (r"/metadata/fields", FieldsHandler),
    ]
    if _settings._api_version:
        ret += [
            (r"/" + _settings._api_version + "/metadata", MetaDataHandler),
            (r"/" + _settings._api_version + "/metadata/fields", FieldsHandler),
            (r"/" + _settings._api_version + "/${annotation_endpoint}/(.+)/?", ${annotation_handler_name}),
            (r"/" + _settings._api_version + "/${annotation_endpoint}/?$", ${annotation_handler_name}),
            (r"/" + _settings._api_version + "/${query_endpoint}/?", ${query_handler_name}),
        ]
    else:
        ret += [
            (r"/${annotation_endpoint}/(.+)/?", ${annotation_handler_name}),
            (r"/${annotation_endpoint}/?$", ${annotation_handler_name}),
            (r"/${query_endpoint}/?", ${query_handler_name}),
        ]
    return ret