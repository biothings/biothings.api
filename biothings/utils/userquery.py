import os.path
import logging

cache = {}

def get_userquery(query_folder, query_name):
    try:
        return cache[query_name]
    except KeyError:
        pass

    # get the query from file
    query_dir = os.path.join(query_folder, query_name)
    assert os.path.exists(query_dir) and os.path.isdir(query_dir), "query directory not found"
    
    query_file = os.path.join(query_dir, 'query')
    assert os.path.exists(query_file), "query file not found"

    with open(query_file, 'r') as query_handle:
        cache[query_name] = query_handle.read()

    logging.error("cache[{}]: {}".format(query_name, cache[query_name]))

    return cache[query_name]
