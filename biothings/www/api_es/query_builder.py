class ESQueryBuilder(object):
    def __init__(self, **query_options):
        self._query_options = query_options
        self._options = self._query_options.pop('options', {})

    def build_id_query(self, bid, scopes=None):
        _default_scopes = '_id'
        scopes = scopes or _default_scopes
        if is_str(scopes):
            _query = {
                "match": {
                    scopes: {
                        "query": "{}".format(bid),
                        "operator": "and"
                    }
                }
            }
        elif is_seq(scopes):
            _query = {
                "multi_match": {
                    "query": "{}".format(bid),
                    "fields": scopes,
                    "operator": "and"
                }
            }
        else:
            raise ValueError('"scopes" cannot be "%s" type'.format(type(scopes)))
        _q = {"query": _query}
        self._query_options.pop("query", None)    # avoid "query" be overwritten by self.query_options
        _q.update(self._query_options)
        return _q

    def build_multiple_id_query(self, bid_list, scopes=None):
        """make a query body for msearch query."""
        _q = []
        for id in bid_list:
            _q.extend(['{}', json.dumps(self.build_id_query(id, scopes))])
        _q.append('')
        return '\n'.join(_q)

    def default_query(self, q):
        return {
            "query": {
                "query_string": {
                    "query": q.lstrip('*?')
                }
            }
        }

    def user_query(self, q):
        args = [q]
        args.extend(self._options.userquery_args)
        return json.loads(get_userquery(biothing_settings.userquery_dir, self._options.userquery).format(*args))

    def get_query_filters(self):
        '''Subclass to add specific filters'''
        return []

    def add_query_filters(self, _query):
        '''filters added here will be applied in a filtered query,
           thus will affect the facet counts.
        '''
        filters = self.get_query_filters()
        if not filters:
            return _query

        #add filters as filtered query
        #this will apply to facet counts
        _query = {
            'filtered': {
                'query': _query,
                'filter': filters
            }
        }

        return _query

    def generate_query(self, q):
        '''
        Return query dict according to passed arg "q". Can be:
            - match query
            - wildcard query
            - raw_string query
            - "match all" query
        Also add query filters
        '''
        # Check if fielded/boolean query, excluding special goid query
        # raw_string_query should be checked ahead of wildcard query, as
        # raw_string may contain wildcard as well # e.g., a query
        # "symbol:CDK?", should be treated as raw_string_query.
        if self._is_user_query():
            _query = self.user_query(q)
        elif q == '__all__':
            _query = {"match_all": {}}
        elif self._is_raw_string_query(q):
            #logging.debug("this is raw string query")
            _query = self.raw_string_query(q)
        elif self._is_wildcard_query(q):
            #logging.debug("this is wildcard query")
            _query = self.wildcard_query(q)
        else:
            #logging.debug("this is dis max query")
            _query = self.dis_max_query(q)

        _query = self.add_query_filters(_query)

        return _query

    def _is_user_query(self):
        ''' Return True if query is a userquery '''
        if self._options.userquery:
            return True
        return False

    def _is_wildcard_query(self, query):
        '''Return True if input query is a wildcard query.'''
        return query.find('*') != -1 or query.find('?') != -1

    def _is_raw_string_query(self, query):
        '''Return True if input query is a wildchar/fielded/boolean query.'''
        for v in [':', '~', ' AND ', ' OR ', 'NOT ']:
            if query.find(v) != -1:
                return True
        if query.startswith('"') and query.endswith('"'):
            return True
        return False

    def raw_string_query(self, q):
        _query = {
            "query_string": {
                "query": "%(q)s",
                # "analyzer": "string_lowercase",
                "default_operator": "AND",
                "auto_generate_phrase_queries": True
            }
        }
        _query = json.dumps(_query)
        try:
            _query = json.loads(_query % {'q': q.replace('"', '\\"')})
        except ValueError:
            raise QueryError("invalid query term.")
        return _query

    def wildcard_query(self, q):
        raise NotImplemented("Wildcard queries not supported (or implement in subclass)")

    def dis_max_query(self, q):
        raise NotImplemented("Dis max queries not supported (or implement in subclass)")
