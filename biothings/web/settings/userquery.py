
import json
import logging
import os
from copy import deepcopy

from elasticsearch_dsl import Q


class ESUserQuery():

    def __init__(self, path):

        self._queries = {}
        self._filters = {}
        try:
            for (dirpath, dirnames, filenames) in os.walk(path):
                if dirnames:
                    logging.info("Detected user query folders: %s.", dirnames)
                    continue
                for filename in filenames:
                    with open(os.path.join(dirpath, filename)) as text_file:
                        if 'query' in filename:
                            ## alternative implementation
                            # self._queries[os.path.basename(dirpath)] = text_file.read()
                            ##
                            self._queries[os.path.basename(dirpath)] = json.load(text_file)
                        elif 'filter' in filename:
                            self._filters[os.path.basename(dirpath)] = json.load(text_file)
        except Exception:
            pass

    def has_query(self, named_query):

        return named_query in self._queries

    def has_filter(self, named_query):

        return named_query in self._filters

    def get_query(self, named_query, **kwargs):

        def in_place_sub(dic, kwargs):
            for key in dic:
                if isinstance(dic[key], dict):
                    in_place_sub(dic[key], kwargs)
                elif isinstance(dic[key], list):
                    for item in dic[key]:
                        in_place_sub(item, kwargs)
                elif isinstance(dic[key], str):
                    dic[key] = dic[key].format(**kwargs).format(**kwargs)  # {{q}}

        dic = deepcopy(self._queries.get(named_query))
        in_place_sub(dic, kwargs)
        key, val = dic.popitem()
        return Q(key, **val)

        ## alternative implementation
        # string = self._queries.get(named_query)
        # string1 = re.sub(r"\}", "}}", string)
        # string2 = re.sub(r"\{", "{{", string1)
        # string3 = re.sub(r'\{\{\{\{(?P<var>.*?)\}\}\}\}', r'{\g<var>}', string2)
        # return string3
        ##

    def get_filter(self, named_query):

        dic = self._filters.get(named_query)
        key, val = dic.popitem()
        return Q(key, **val)
