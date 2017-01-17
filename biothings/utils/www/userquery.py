import os.path
import re
#import logging

query_cache = {}
filter_cache = {}

def get_userquery(query_folder, query_name):
    try:
        return query_cache[query_name]
    except KeyError:
        pass

    # get the query from file, assumed that query_folder/query_name/query.txt exists
    query_dir = os.path.join(query_folder, query_name)
    with open(os.path.join(query_dir, 'query.txt'), 'r') as query_handle:
        query_cache[query_name] = re.sub(r'\{\{\{\{(?P<var>.*?)\}\}\}\}', '{\g<var>}', 
                                  re.sub(r"\{", "{{", re.sub(r"\}", "}}", query_handle.read())))
    #logging.debug("query_cache[{}]: {}".format(query_name, query_cache[query_name]))

    return query_cache[query_name]

def get_userfilter(query_folder, query_name):
    try:
        return filter_cache[query_name]
    except KeyError:
        pass

    # get the filter from file, assumed that query_folder/query_name/filter.txt exists
    query_dir = os.path.join(query_folder, query_name)
    with open(os.path.join(query_dir, 'filter.txt'), 'r') as filter_handle:
        filter_cache[query_name] = filter_handle.read()
    #logging.debug("filter_cache[{}]: {}".format(query_name, filter_cache[query_name]))

    return filter_cache[query_name]
