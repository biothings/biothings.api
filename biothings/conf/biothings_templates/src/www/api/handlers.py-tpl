# -*- coding: utf-8 -*-
from biothings.www.api.handlers import MetaDataHandler, BiothingHandler, QueryHandler, StatusHandler, FieldsHandler
from biothings.settings import BiothingSettings
from www.api.es import ESQuery
import config

biothing_settings = BiothingSettings()

class ${annotation_handler_name}(BiothingHandler):
    ''' This class is for the /${annotation_endpoint} endpoint. '''
    esq = ESQuery()

class ${query_handler_name}(QueryHandler):
    ''' This class is for the /${query_endpoint} endpoint. '''
    esq = ESQuery()

class StatusHandler(StatusHandler):
    ''' This class is for the /status endpoint. '''
    esq = ESQuery()

class FieldsHandler(FieldsHandler):
    ''' This class is for the /metadata/fields endpoint. '''
    esq = ESQuery()

class MetaDataHandler(MetaDataHandler):
    ''' This class is for the /metadata endpoint. '''
    esq = ESQuery()


def return_applist():
    ret = [
        (r"/status", StatusHandler),
        (r"/metadata", MetaDataHandler),
        (r"/metadata/fields", FieldsHandler),
    ]
    if biothing_settings._api_version:
        ret += [
            (r"/" + biothing_settings._api_version + "/metadata", MetaDataHandler),
            (r"/" + biothing_settings._api_version + "/metadata/fields", FieldsHandler),
            (r"/" + biothing_settings._api_version + "/${annotation_endpoint}/(.+)/?", ${annotation_handler_name}),
            (r"/" + biothing_settings._api_version + "/${annotation_endpoint}/?$$", ${annotation_handler_name}),
            (r"/" + biothing_settings._api_version + "/${query_endpoint}/?", ${query_handler_name}),
        ]
    else:
        ret += [
            (r"/${annotation_endpoint}/(.+)/?", ${annotation_handler_name}),
            (r"/${annotation_endpoint}/?$$", ${annotation_handler_name}),
            (r"/${query_endpoint}/?", ${query_handler_name}),
        ]
    return ret