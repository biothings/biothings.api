import json
from biothings.utils.common import dotdict, is_str, is_seq, find_doc
from biothings.utils.es import get_es
from elasticsearch import NotFoundError, RequestError
from biothings.settings import BiothingSettings

biothing_settings = BiothingSettings()

class QueryError(Exception):
    pass


class ScrollSetupError(Exception):
    pass


class ESQuery():
    def __init__(self):
        self._es = get_es(biothing_settings.es_host)
        self._index = biothing_settings.es_index
        self._doc_type = biothing_settings.es_doc_type
        self._allowed_options = biothing_settings.allowed_options
        self._scroll_time = biothing_settings.scroll_time
        self._total_scroll_size = biothing_settings.scroll_size   # Total number of hits to return per scroll batch
        try:
            self._context = json.load(open(biothing_settings.jsonld_context_path, 'r'))
        except FileNotFoundError:
            self._context = {}
        if self._total_scroll_size % self.get_number_of_shards() == 0:
            # Total hits per shard per scroll batch
            self._scroll_size = int(self._total_scroll_size / self.get_number_of_shards())
        else:
            raise ScrollSetupError("_total_scroll_size of {} can't be ".format(self._total_scroll_size) +
                                     "divided evenly among {} shards.".format(self.get_number_of_shards()))

    def _get_biothingdoc(self, hit, options=None):
        doc = hit.get('_source', hit.get('fields', {}))
        doc.setdefault('_id', hit['_id'])
        for attr in ['_score', '_version']:
            if attr in hit:
                doc.setdefault(attr, hit[attr])

        if hit.get('found', None) is False:
            # if found is false, pass that to the doc
            doc['found'] = hit['found']
        if options.jsonld:
            doc = self._insert_jsonld(doc)
        # add other keys to object, if necessary
        doc = self._modify_biothingdoc(doc=doc, options=options)
        return doc

    def _modify_biothingdoc(self, doc, options=None):
        # function for overriding in subclass
        return doc

    def _cleaned_res(self, res, empty=[], error={'error': True}, single_hit=False, options=None):
        '''res is the dictionary returned from a query.
           do some reformating of raw ES results before returning.

           This method is used for self.mget_biothings and self.get_biothing method.
        '''
        if 'error' in res:
            return error

        hits = res['hits']
        total = hits['total']
        if total == 0:
            return empty
        elif total == 1 and single_hit:
            return self._get_biothingdoc(hit=hits['hits'][0], options=options)
        else:
            return [self._get_biothingdoc(hit=hit, options=options) for hit in hits['hits']]

    def _cleaned_res2(self, res, options=None):
        '''res is the dictionary returned from a query.
           do some reformating of raw ES results before returning.

           This method is used for self.query method.
        '''
        if 'aggregations' in res:
            # need to normalize back to what "facets" used to return
            # (mostly key renaming + total computation)
            res["facets"] = res.pop("aggregations")
            for facet in res["facets"]:
                # restuls always coming from terms aggregations
                res["facets"][facet]["_type"] = "terms"
                res["facets"][facet]["terms"] = res["facets"][facet].pop("buckets")
                res["facets"][facet]["other"] = res["facets"][facet].pop("sum_other_doc_count")
                res["facets"][facet]["missing"] = res["facets"][facet].pop("doc_count_error_upper_bound")
                count = 0
                for term in res["facets"][facet]["terms"]:
                    # modif in-place
                    term["count"] = term.pop("doc_count")
                    term["term"] = term.pop("key")
                    count += term["count"]
                res["facets"][facet]["total"] = count

        _res = res['hits']
        for attr in ['took', 'facets', '_scroll_id']:
            if attr in res:
                _res[attr] = res[attr]
        _res['hits'] = [self._get_biothingdoc(hit=hit, options=options) for hit in _res['hits']]
        return _res

    def _cleaned_scopes(self, scopes):
        '''return a cleaned scopes parameter.
            should be either a string or a list of scope fields.
        '''
        if scopes:
            if is_str(scopes):
                scopes = [x.strip() for x in scopes.split(',')]
            if is_seq(scopes):
                scopes = [x for x in scopes if x]
                if len(scopes) == 1:
                    scopes = scopes[0]
            else:
                scopes = None
        else:
            scopes = None
        return scopes

    def _cleaned_fields(self, fields):
        '''return a cleaned fields parameter.
            should be either None (return all fields) or a list fields.
        '''
        if fields:
            if is_str(fields):
                if fields.lower() == 'all':
                    fields = None     # all fields will be returned.
                else:
                    fields = [x.strip() for x in fields.split(',')]
        else:
            fields = self._default_fields
        return fields

    def _parse_sort_option(self, options):
        sort = options.get('sort', None)
        if sort:
            _sort_array = []
            for field in sort.split(','):
                field = field.strip()
                # if field == 'name' or field[1:] == 'name':
                #     # sorting on "name" field is ignored, as it is a multi-text field.
                #     continue
                if field.startswith('-'):
                    _f = "%s:desc" % field[1:]
                else:
                    _f = "%s:asc" % field
                _sort_array.append(_f)
            options["sort"] = ','.join(_sort_array)
        return options

    def _parse_facets_option(self, kwargs):
        aggs = kwargs.pop('aggs', None)
        if aggs:
            _aggs = {}
            for field in aggs.split(','):
                _aggs[field] = {"terms": {"field": field}}
            return _aggs

    def _get_options(self, options, kwargs):
        ''' Function to override to add more options to the get_cleaned_query_options function below .'''
        return options

    def _get_cleaned_query_options(self, kwargs):
        """common helper for processing fields, kwargs and other options passed to ESQueryBuilder."""
        options = dotdict()
        options.raw = kwargs.pop('raw', False)
        options.rawquery = kwargs.pop('rawquery', False)
        options.fetch_all = kwargs.pop('fetch_all', False)
        options.host = kwargs.pop('host', biothing_settings.ga_tracker_url)
        options.jsonld = kwargs.pop('jsonld', False)
        options = self._get_options(options, kwargs)
        scopes = kwargs.pop('scopes', None)
        if scopes:
            options.scopes = self._cleaned_scopes(scopes)
        fields = kwargs.pop('fields', None)
        if fields:
            fields = self._cleaned_fields(fields)
            if fields:
                kwargs["_source"] = fields
        kwargs = self._parse_sort_option(kwargs)
        for key in set(kwargs) - set(self._allowed_options):
            del kwargs[key]
        options.kwargs = kwargs
        return options

    def get_number_of_shards(self):
        r = self._es.indices.get_settings(self._index)
        n_shards = r[list(r.keys())[0]]['settings']['index']['number_of_shards']
        n_shards = int(n_shards)
        return n_shards

    def exists(self, bid):
        """return True/False if a biothing id exists or not."""
        try:
            doc = self.get_biothing(bid, fields=None)
            return doc['found']
        except NotFoundError:
            return False

    def get_biothing(self, bid, **kwargs):
        '''unknown vid return None'''
        options = self._get_cleaned_query_options(kwargs)
        kwargs = {"_source": options.kwargs["_source"]} if "_source" in options.kwargs else {}
        try:
            res = self._es.get(index=self._index, id=bid, doc_type=self._doc_type, **kwargs)
        except NotFoundError:
            return

        if options.raw:
            return res

        res = self._get_biothingdoc(res, options=options)
        return res

    def mget_biothings(self, bid_list, **kwargs):
        '''for /query post request'''
        options = self._get_cleaned_query_options(kwargs)
        qbdr = ESQueryBuilder(**options.kwargs)
        try:
            _q = qbdr.build_multiple_id_query(bid_list, scopes=options.scopes)
        except QueryError as err:
            return {'success': False,
                    'error': err.message}
        if options.rawquery:
            return _q
        res = self._es.msearch(body=_q, index=self._index, doc_type=self._doc_type)['responses']
        if options.raw:
            return res

        assert len(res) == len(bid_list)
        _res = []

        for i in range(len(res)):
            hits = res[i]
            qterm = bid_list[i]
            hits = self._cleaned_res(hits, empty=[], single_hit=False, options=options)
            if len(hits) == 0:
                _res.append({u'query': qterm,
                             u'notfound': True})
            elif 'error' in hits:
                _res.append({u'query': qterm,
                             u'error': True})
            else:
                for hit in hits:
                    hit[u'query'] = qterm
                    _res.append(hit)
        return _res

    def _build_query(self, q, kwargs):
        # can override this function if more query types are to be added
        esqb = ESQueryBuilder()
        return esqb.default_query(q)

    def query(self, q, **kwargs):
        aggs = self._parse_facets_option(kwargs)
        options = self._get_cleaned_query_options(kwargs)
        scroll_options = {}
        if options.fetch_all:
            scroll_options.update({'search_type': 'scan', 'size': self._scroll_size, 'scroll': self._scroll_time})
        options['kwargs'].update(scroll_options)
        _query = self._build_query(q, kwargs)
        if aggs:
            _query['aggs'] = aggs 
        try:
            #import logging
            #logging.error("q: %s, o: %s" % (_query,options))
            res = self._es.search(index=self._index, doc_type=self._doc_type, body=_query, **options.kwargs)
        except RequestError:
            return {"error": "invalid query term.", "success": False}

        # if options.fetch_all:
        #     return res

        if not options.raw:
            res = self._cleaned_res2(res, options=options)
        return res

    def scroll(self, scroll_id, **kwargs):
        '''return the results from a scroll ID, recognizes options.raw'''
        options = self._get_cleaned_query_options(kwargs)
        r = self._es.scroll(scroll_id, scroll=self._scroll_time)
        scroll_id = r.get('_scroll_id')
        if scroll_id is None or not r['hits']['hits']:
            return {'success': False, 'error': 'No results to return.'}
        else:
            if not options.raw:
                res = self._cleaned_res2(r, options=options)
            # res.update({'_scroll_id': scroll_id})
            if r['_shards']['failed']:
                res.update({'_warning': 'Scroll request has failed on {} shards out of {}.'.format(r['_shards']['failed'], r['_shards']['total'])})
        return res

    def _insert_jsonld(self, k):
        ''' Insert the jsonld links into this document.  Called by _get_variantdoc. '''
        # get the context
        context = self._context

        # set the root
        k.update(context['root'])

        for key in context:
            if key != 'root':
                keys = key.split('/')
                try:
                    doc = find_doc(k, keys)
                    if type(doc) == list:
                        for _d in doc:
                            _d.update(context[key])
                    elif type(doc) == dict:
                        doc.update(context[key])
                    else:
                        continue
                        #print('error')
                except:
                    continue
                    #print('keyerror')
        return k

    def get_mapping_meta(self):
        """ return the current _meta field."""
        m = self._es.indices.get_mapping(index=self._index, doc_type=self._doc_type)
        m = m[list(m.keys())[0]]['mappings'][self._doc_type]
        return m.get('_meta', {})

    def query_fields(self, **kwargs):
        # query the metadata to get the available fields for a biothing object
        r = self._es.indices.get(index=self._index)
        return r[list(r.keys())[0]]['mappings'][self._doc_type]['properties']

    def status_check(self, bid):
        r = self.get_biothing(bid)
        return r


class ESQueryBuilder:
    def __init__(self, **query_options):
        self._query_options = query_options

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
