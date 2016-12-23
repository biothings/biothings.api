from biothings.www.logging import get_logger

class ESResponseTransformer(object):
    def __init__(self, options, logger_lvl=None):
        self.options = options
        if logger_lvl:
            self.logger = get_logger(mod_name=__name__, lvl=logger_lvl)
        else:
            self.logger = get_logger(mod_name=__name__)

    # class contains (at least) 4 functions to process the results of an ES query:
    #
    # clean_annotation_GET_response, clean_annotation_POST_response, clean_query_GET_response, clean_query_POST
