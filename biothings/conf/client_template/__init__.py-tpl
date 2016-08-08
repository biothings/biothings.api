'''
Python Client for {% full_url %} services
'''
from __future__ import print_function
import sys
import os
import time
from itertools import islice
from collections import Iterable

import requests
try:
    from pandas import DataFrame
    from pandas.io.json import json_normalize
    df_avail = True
except:
    df_avail = False

try:
    import requests_cache
    caching_avail = True
except:
    caching_avail = False

__version__ = '0.1.0'

if sys.version_info[0] == 3:
    str_types = str
else:
    str_types = (str, unicode)


class ScanError(Exception):
    # for errors in scan search type
    pass


def alwayslist(value):
    '''If input value if not a list/tuple type, return it as a single value list.
    Example:
    >>> x = 'abc'
    >>> for xx in alwayslist(x):
    ...     print xx
    >>> x = ['abc', 'def']
    >>> for xx in alwayslist(x):
    ...     print xx
    '''
    if isinstance(value, (list, tuple)):
        return value
    else:
        return [value]


def safe_str(s, encoding='utf-8'):
    '''if input is an unicode string, do proper encoding.'''
    try:
        _s = str(s)
    except UnicodeEncodeError:
        _s = s.encode(encoding)
    return _s


def list_itemcnt(list):
    '''Return number of occurrence for each type of item in the list.'''
    x = {}
    for item in list:
        if item in x:
            x[item] += 1
        else:
            x[item] = 1
    return [(i, x[i]) for i in x]


def iter_n(iterable, n, with_cnt=False):
    '''
    Iterate an iterator by chunks (of n)
    if with_cnt is True, return (chunk, cnt) each time
    '''
    it = iter(iterable)
    if with_cnt:
        cnt = 0
    while True:
        chunk = tuple(islice(it, n))
        if not chunk:
            return
        if with_cnt:
            cnt += len(chunk)
            yield (chunk, cnt)
        else:
            yield chunk

class {% client_class_name %}:
    '''This is the client for {% full_url %} web services.
    Example:
        >>> client ={% client_class_name %}()
    '''
    def __init__(self, url='{% default_server_url %}'):
        self.url = url
        if self.url[-1] == '/':
            self.url = self.url[:-1]
        self.max_query = 1000
        # delay and step attributes are for batch queries.
        self.delay = 1
        self.step = 1000
        self.scroll_size = 1000
        # raise requests.exceptions.HTTPError for status_code > 400
        #   but not for 404 on get{% annotation_type %}
        #   set to False to surpress the exceptions.
        self.raise_for_status = True
        self._cached = False

    def _dataframe(self, obj, dataframe, df_index=True):
        """
        converts object to DataFrame (pandas)
        """
        if not df_avail:
            print("Error: pandas module must be installed for as_dataframe option.")
            return
        # if dataframe not in ["by_source", "normal"]:
        if dataframe not in [1, 2]:
            raise ValueError("dataframe must be either 1 (using json_normalize) or 2 (using DataFrame.from_dict")
        if 'hits' in obj:
            if dataframe == 1:
                df = json_normalize(obj['hits'])
            else:
                df = DataFrame.from_dict(obj['hits'])
        else:
            if dataframe == 1:
                df = json_normalize(obj)
            else:
                df = DataFrame.from_dict(obj)
        if df_index:
            df = df.set_index('query')
        return df

    def _get(self, url, params={}, none_on_404=False, verbose=True):
        debug = params.pop('debug', False)
        return_raw = params.pop('return_raw', False)
        headers = {'user-agent': "Python-requests_{% client_user_agent_header %}/%s (gzip)" % requests.__version__}
        res = requests.get(url, params=params, headers=headers)
        from_cache = getattr(res, 'from_cache', False)
        if debug:
            return from_cache, res
        if none_on_404 and res.status_code == 404:
            return from_cache, None
        if self.raise_for_status:
            # raise requests.exceptions.HTTPError if not 200
            res.raise_for_status()
        if return_raw:
            return from_cache, res.text
        ret = res.json()
        return from_cache, ret

    def _post(self, url, params, verbose=True):
        return_raw = params.pop('return_raw', False)
        headers = {'content-type': 'application/x-www-form-urlencoded',
                   'user-agent': "Python-requests_{% client_user_agent_header %}/%s (gzip)" % requests.__version__}
        res = requests.post(url, data=params, headers=headers)
        from_cache = getattr(res, 'from_cache', False)
        if self.raise_for_status:
            # raise requests.exceptions.HTTPError if not 200
            res.raise_for_status()
        if return_raw:
            return from_cache, res
        ret = res.json()
        return from_cache, ret

    def _format_list(self, a_list, sep=','):
        if isinstance(a_list, (list, tuple)):
            _out = sep.join([safe_str(x) for x in a_list])
        else:
            _out = a_list     # a_list is already a comma separated string
        return _out

    def _repeated_query_old(self, query_fn, query_li, verbose=True, **fn_kwargs):
        '''This is deprecated, query_li can only be a list'''
        step = min(self.step, self.max_query)
        if len(query_li) <= step:
            # No need to do series of batch queries, turn off verbose output
            verbose = False
        for i in range(0, len(query_li), step):
            is_last_loop = i+step >= len(query_li)
            if verbose:
                print("querying {0}-{1}...".format(i+1, min(i+step, len(query_li))), end="")
            query_result = query_fn(query_li[i:i+step], **fn_kwargs)
            
            yield query_result
            
            if verbose:
                print("done.")
            if not is_last_loop and self.delay:
                time.sleep(self.delay)

    def _repeated_query(self, query_fn, query_li, verbose=True, **fn_kwargs):
        '''run query_fn for input query_li in a batch (self.step).
           return a generator of query_result in each batch.
           input query_li can be a list/tuple/iterable
        '''
        step = min(self.step, self.max_query)
        i = 0
        for batch, cnt in iter_n(query_li, step, with_cnt=True):
            if verbose:
                print("querying {0}-{1}...".format(i+1, cnt), end="")
            i = cnt
            from_cache, query_result = query_fn(batch, **fn_kwargs)
            yield query_result
            if verbose:
                cache_str = " {0}".format(self._from_cache_notification) if from_cache else ""
                print("done.{0}".format(cache_str))
            if self.delay:
                time.sleep(self.delay)

    @property
    def _from_cache_notification(self):
        ''' Notification to alert user that a cached result is being returned.'''
        return "[ from cache ]"

    def metadata(self, verbose=True, **kwargs):
        '''Return a dictionary of {% full_url %} metadata.
        Example:
        >>> metadata = client.metadata()
        '''
        _url = self.url+'/metadata'
        from_cache, ret = self._get(_url, params=kwargs, verbose=verbose)
        if verbose and from_cache:
            print(self._from_cache_notification)
        return ret

    def set_caching(self, cache_db='{% default_cache_name %}', verbose=True, **kwargs):
        ''' Installs a local cache for all requests.
            **cache_db** is the path to the local sqlite cache database.'''
        if caching_avail:
            requests_cache.install_cache(cache_name=cache_db, allowable_methods=('GET', 'POST'), **kwargs)
            self._cached = True
            if verbose:
                print('[ Future queries will be cached in "{0}" ]'.format(os.path.abspath(cache_db + '.sqlite')))
        else:
            print("Error: The requests_cache python module is required to use request caching.")
            print("See - https://requests-cache.readthedocs.io/en/latest/user_guide.html#installation")
        return

    def stop_caching(self):
        ''' Stop caching.'''
        if self._cached and caching_avail:
            requests_cache.uninstall_cache()
            self._cached = False
        return

    def clear_cache(self):
        ''' Clear the globally installed cache. '''
        try:
            requests_cache.clear()
        except:
            pass

    def get_fields(self, search_term=None, verbose=True):
        ''' Wrapper for {% default_server_url %}/metadata/fields
            **search_term** is a case insensitive string to search for in available field names.
            If not provided, all available fields will be returned.
        Example:
        >>> client.get_fields()
        >>> client.get_fields("")
        >>> client.get_fields("")
        .. Hint:: This is useful to find out the field names you need to pass to **fields** parameter of other methods.
        '''
        _url = self.url + '/metadata/fields'
        if search_term:
            params = {'search': search_term}
        else:
            params = {}
        from_cache, ret = self._get(_url, params=params, verbose=verbose)
        for (k, v) in ret.items():
            # Get rid of the notes column information
            if "notes" in v:
                del v['notes']
        if verbose and from_cache:
            print(self._from_cache_notification)
        return ret

    def get{% annotation_type %}(self, oid, fields=None, **kwargs):
        '''Return the {% annotation_type %} object for the given id.
        This is a wrapper for GET query of "/{% annotation_endpoint %}/<id>" service.
        :param id: 
        :param fields: fields to return, a list or a comma-separated string.
                       If not provided or **fields="all"**, all available fields
                       are returned. Use get_fields() to see all available fields.
        :return: a {% annotation_type %} object as a dictionary, or None if id is not found.
        Example:
        >>> mv.get{% annotation_type %}('')
        >>> mv.get{% annotation_type %}('', fields='')
        >>> mv.get{% annotation_type %}('', fields=['', ''])
        >>> mv.get{% annotation_type %}('', fields='all')
        .. Hint:: The supported field names passed to **fields** parameter can be found from
                  any full {% annotation_type %} object (without **fields**, or **fields="all"**). Note that field name supports dot
                  notation for nested data structure as well.
        '''
        verbose = kwargs.pop('verbose', True)
        if fields:
            kwargs['fields'] = self._format_list(fields)
        _url = self.url + '/{% annotation_endpoint %}/' + str(oid)
        from_cache, ret = self._get(_url, kwargs, none_on_404=True, verbose=verbose)
        if verbose and from_cache:
            print(self._from_cache_notification)
        return ret

    def _get{% annotation_type %}s_inner(self, oids, verbose=True, **kwargs):
        _kwargs = {'ids': self._format_list(oids)}
        _kwargs.update(kwargs)
        _url = self.url + '/{% annotation_endpoint %}/'
        return self._post(_url, _kwargs, verbose=verbose)

    def _{% annotation_type %}s_generator(self, query_fn, oids, verbose=True, **kwargs):
        ''' Convenience function to yield a batch of hits one at a yime. '''
        for hits in self._repeated_query(query_fn, oids, verbose=verbose):
            for hit in hits:
                yield hit

    def get{% annotation_type %}s(self, oids, fields=None, **kwargs):
        '''Return the list of {% annotation_type %} annotation objects for the given list of ids.
        This is a wrapper for POST query of "/{% annotation_endpoint %}" service.
        :param oids: a list/tuple/iterable or a string of comma-separated ids.
        :param fields: fields to return, a list or a comma-separated string.
                       If not provided or **fields="all"**, all available fields
                       are returned. Use get_fields() for all available fields.
        :param generator:  if True, will yield the results in a generator.
        :param as_dataframe: if True or 1 or 2, return object as DataFrame (requires Pandas).
                                  True or 1: using json_normalize
                                  2        : using DataFrame.from_dict
                                  otherwise: return original json
        :param df_index: if True (default), index returned DataFrame by 'query',
                         otherwise, index by number. Only applicable if as_dataframe=True.
        :return: a list of {% annotation_type %} objects or a pandas DataFrame object (when **as_dataframe** is True)
        Example:
        >>> vars = ['',
        ...         '',
        ...         '',
        ...         '',
        ...         '',
        ...         '',
        ...         '',
        ...         '',
        ...         '']
        >>> mv.get{% annotation_type %}s(vars, fields="")
        >>> mv.get{% annotation_type %}s(',', fields="all")
        >>> mv.get{% annotation_type %}s(['', ''], as_dataframe=True)
        .. Hint:: A large list of more than 1000 input ids will be sent to the backend
                  web service in batches (1000 at a time), and then the results will be
                  concatenated together. So, from the user-end, it's exactly the same as
                  passing a shorter list. You don't need to worry about saturating our
                  backend servers.
        .. Hint:: If you need to pass a very large list of input ids, you can pass a generator
                  instead of a full list, which is more memory efficient.
        '''
        if isinstance(oids, str_types):
            oids = oids.split(',') if oids else []
        if (not (isinstance(oids, (list, tuple, Iterable)))):
            raise ValueError('input "oids" must be a list, tuple or iterable.')
        if fields:
            kwargs['fields'] = self._format_list(fields)
        verbose = kwargs.pop('verbose', True)
        dataframe = kwargs.pop('as_dataframe', None)
        df_index = kwargs.pop('df_index', True)
        generator = kwargs.pop('generator', False)
        if dataframe in [True, 1]:
            dataframe = 1
        elif dataframe != 2:
            dataframe = None
        return_raw = kwargs.get('return_raw', False)
        if return_raw:
            dataframe = None

        query_fn = lambda oids: self._get{% annotation_type %}s_inner(oids, verbose=verbose, **kwargs)
        if generator:
            return self._{% annotation_type %}s_generator(query_fn, oids, verbose=verbose, **kwargs)
        out = []
        for hits in self._repeated_query(query_fn, oids, verbose=verbose):
            if return_raw:
                out.append(hits)   # hits is the raw response text
            else:
                out.extend(hits)
        if return_raw and len(out) == 1:
            out = out[0]
        if dataframe:
            out = self._dataframe(out, dataframe, df_index=df_index)
        return out

    def query(self, q, **kwargs):
        '''Return  the query result.
        This is a wrapper for GET query of "/query?q=<query>" service.
        :param q: a query string
        :param fields: fields to return, a list or a comma-separated string.
                       If not provided or **fields="all"**, all available fields
                       are returned. See get_fields() for all available fields.
        :param size:   the maximum number of results to return (with a cap
                       of 1000 at the moment). Default: 10.
        :param skip:   the number of results to skip. Default: 0.
        :param sort:   Prefix with "-" for descending order, otherwise in ascending order.
                       Default: sort by matching scores in decending order.
        :param as_dataframe: if True or 1 or 2, return object as DataFrame (requires Pandas).
                                  True or 1: using json_normalize
                                  2        : using DataFrame.from_dict
                                  otherwise: return original json
        :param fetch_all: if True, return a generator to all query results (unsorted).  This can provide a very fast
                          return of all hits from a large query.
                          Server requests are done in blocks of 1000 and yielded individually.  Each 1000 block of
                          results must be yielded within 1 minute, otherwise the request will expire at server side.
        :return: a dictionary with returned {% annotation_type %} hits or a pandas DataFrame object (when **as_dataframe** is True)
                 or a generator of all hits (when **fetch_all** is True)
        Example:
        >>> mv.query('')
        >>> mv.query('')
        >>> mv.query('')
        >>> mv.query('', size=5)
        >>> mv.query('', fetch_all=True)
        >>> mv.query('')
        .. Hint:: By default, **query** method returns the first 10 hits if the matched hits are >10. If the total number
                  of hits are less than 1000, you can increase the value for **size** parameter. For a query returns
                  more than 1000 hits, you can pass "fetch_all=True" to return a `generator <http://www.learnpython.org/en/Generators>`_
                  of all matching hits (internally, those hits are requested from the server-side in blocks of 1000).
        '''
        verbose = kwargs.pop('verbose', True)
        kwargs.update({'q': q})
        fetch_all = kwargs.get('fetch_all')
        if fetch_all in [True, 1]:
            return self._fetch_all(verbose=verbose, **kwargs)
        dataframe = kwargs.pop('as_dataframe', None)
        if dataframe in [True, 1]:
            dataframe = 1
        elif dataframe != 2:
            dataframe = None
        _url = self.url + '/query'
        from_cache, out = self._get(_url, kwargs, verbose=verbose)
        if verbose and from_cache:
            print(self._from_cache_notification)
        if dataframe:
            out = self._dataframe(out, dataframe, df_index=False)
        return out

    def _fetch_all(self, verbose=True, **kwargs):
        ''' Function that returns a generator to results.  Assumes that 'q' is in kwargs.'''
        # get the total number of hits and start the scroll_id
        _url = self.url + '/query'
        # function to get the next batch of results, automatically disables cache if we are caching
        def _batch():
            if caching_avail and self._cached:
                self._cached = False
                with requests_cache.disabled():
                    from_cache, ret = self._get(_url, params=kwargs, verbose=verbose)
                self._cached = True
            else:
                from_cache, ret = self._get(_url, params=kwargs, verbose=verbose)
            return ret
        batch = _batch()
        if verbose:
            print("Fetching {} {% annotation_type %}(s) . . .".format(batch['total']))
        for key in ['q', 'fetch_all']:
            kwargs.pop(key)
        while not batch.get('error', '').startswith('No results to return.'):
            if 'error' in batch:
                print(batch['error'])
                break
            if '_warning' in batch and verbose:
                print(batch['_warning'])
            for hit in batch['hits']:
                yield hit
            kwargs.update({'scroll_id': batch['_scroll_id']})
            batch = _batch()

    def _querymany_inner(self, qterms, verbose=True, **kwargs):
        _kwargs = {'q': self._format_list(qterms)}
        _kwargs.update(kwargs)
        _url = self.url + '/query'
        return self._post(_url, params=_kwargs, verbose=verbose)

    def querymany(self, qterms, scopes=None, **kwargs):
        '''Return the batch query result.
        This is a wrapper for POST query of "/query" service.
        :param qterms: a list/tuple/iterable of query terms, or a string of comma-separated query terms.
        :param scopes: specify the type (or types) of identifiers passed to **qterms**, either a list or a comma-separated fields to specify type of
                       input qterms
                       See get_fields() for a full list of supported fields.
        :param fields: fields to return, a list or a comma-separated string.
                       If not provided or **fields="all"**, all available fields
                       are returned. See get_fields() for all available fields.
        :param returnall:   if True, return a dict of all related data, including dup. and missing qterms
        :param verbose:     if True (default), print out information about dup and missing qterms
        :param as_dataframe: if True or 1 or 2, return object as DataFrame (requires Pandas).
                                  True or 1: using json_normalize
                                  2        : using DataFrame.from_dict
                                  otherwise: return original json
        :param df_index: if True (default), index returned DataFrame by 'query',
                         otherwise, index by number. Only applicable if as_dataframe=True.
        :return: a list of matching {% annotation_type %} objects or a pandas DataFrame object.
        Example:
        >>> mv.querymany(['', ''], scopes='')
        >>> mv.querymany(['', '', ''], scopes='')
        >>> mv.querymany(['', '', ''], scopes='', fields='')
        >>> mv.querymany(['', '', ''], scopes='',
        ...              fields='', as_dataframe=True)
        .. Hint:: :py:meth:`querymany` is perfect for query {% annotation_type %}s based different ids, e.g. rsid, clinvar ids, etc.
        .. Hint:: Just like :py:meth:`get{% annotation_type %}s`, passing a large list of ids (>1000) to :py:meth:`querymany` is perfectly fine.
        .. Hint:: If you need to pass a very large list of input qterms, you can pass a generator
                  instead of a full list, which is more memory efficient.
        '''
        if isinstance(qterms, str_types):
            qterms = qterms.split(',') if qterms else []
        if (not (isinstance(qterms, (list, tuple, Iterable)))):
            raise ValueError('input "qterms" must be a list, tuple or iterable.')

        if scopes:
            kwargs['scopes'] = self._format_list(scopes)
        if 'fields' in kwargs:
            kwargs['fields'] = self._format_list(kwargs['fields'])
        returnall = kwargs.pop('returnall', False)
        verbose = kwargs.pop('verbose', True)
        dataframe = kwargs.pop('as_dataframe', None)
        if dataframe in [True, 1]:
            dataframe = 1
        elif dataframe != 2:
            dataframe = None
        df_index = kwargs.pop('df_index', True)
        return_raw = kwargs.get('return_raw', False)
        if return_raw:
            dataframe = None

        out = []
        li_missing = []
        li_dup = []
        li_query = []
        query_fn = lambda qterms: self._querymany_inner(qterms, verbose=verbose, **kwargs)
        for hits in self._repeated_query(query_fn, qterms, verbose=verbose):
            if return_raw:
                out.append(hits)   # hits is the raw response text
            else:
                out.extend(hits)
                for hit in hits:
                    if hit.get('notfound', False):
                        li_missing.append(hit['query'])
                    else:
                        li_query.append(hit['query'])

        if verbose:
            print("Finished.")
        if return_raw:
            if len(out) == 1:
                out = out[0]
            return out
        if dataframe:
            out = self._dataframe(out, dataframe, df_index=df_index)

        # check dup hits
        if li_query:
            li_dup = [(query, cnt) for query, cnt in list_itemcnt(li_query) if cnt > 1]
        del li_query

        if verbose:
            if li_dup:
                print("{0} input query terms found dup hits:".format(len(li_dup)))
                print("\t"+str(li_dup)[:100])
            if li_missing:
                print("{0} input query terms found no hit:".format(len(li_missing)))
                print("\t"+str(li_missing)[:100])
        if returnall:
            return {'out': out, 'dup': li_dup, 'missing': li_missing}
        else:
            if verbose and (li_dup or li_missing):
                print('Pass "returnall=True" to return complete lists of duplicate or missing query terms.')
            return out
