import os.path
import logging

query_cache = {}
filter_cache = {}

def get_userquery(query_folder, query_name):
    try:
        return query_cache[query_name]
    except KeyError:
        pass

    # get the query from file
    query_dir = os.path.join(query_folder, query_name)
    if os.path.exists(query_dir) and os.path.isdir(query_dir) 
        and os.path.exists(os.path.join(query_dir, 'query.txt')):
        with open(os.path.join(query_dir, 'query.txt'), 'r') as query_handle:
            query_cache[query_name] = query_handle.read()
    else:
        return '{{}}'
    
    #logging.debug("query_cache[{}]: {}".format(query_name, query_cache[query_name]))

    return query_cache[query_name]

def get_userfilter(query_folder, query_name):
    try:
        return filter_cache[query_name]
    except KeyError:
        pass

    query_dir = os.path.join(query_folder, query_name)
    if os.path.exists(query_dir) and os.path.isdir(query_dir) 
        and os.path.exists(os.path.join(query_dir, 'query.txt')):
        with open(os.path.join(query_dir, 'filter.txt'), 'r') as filter_handle:
            filter_cache[query_name] = filter_handle.read()
    else:
        return '{{}}'
    
    #logging.debug("filter_cache[{}]: {}".format(query_name, filter_cache[query_name]))

    return filter_cache[query_name]
