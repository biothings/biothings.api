from biothings.utils.common import is_str, is_seq
from biothings.utils.version import get_software_info
from biothings.utils.www import flatten_doc
from collections import OrderedDict
import logging

class ScrollIterationDone(Exception):
    pass

class ESResultTransformer(object):
    def __init__(self, options, host, jsonld_context={}, data_sources={}, output_aliases={}, app_dir=''):
        self.options = options
        self.host = host
        self.jsonld_context = jsonld_context
        self.data_sources = data_sources
        self.output_aliases = output_aliases
        self.app_dir = app_dir

    def _flatten_doc(self, doc, outfield_sep='.', context_sep='/'):
        def _recursion_helper(d, ret, path, out):
            if isinstance(d, dict):
                for key in d:
                    new_path_key = key if not path else context_sep.join([path, key])
                    new_out_key = self._alias_output_keys(new_path_key, key) if not out else outfield_sep.join(
                                                            [out, self._alias_output_keys(new_path_key, key)])
                    _recursion_helper(d[key], ret, new_path_key, new_out_key)
            elif is_seq(d):
                for obj in d:
                    _recursion_helper(obj, ret, path, out)
            else:
                if out in ret:
                    if isinstance(ret[out], list):
                        ret[out].append(d)
                    else:
                        ret[out] = [ret[out], d]
                else:
                    ret[out] = d
        ret = {}
        _recursion_helper(doc, ret, '', '')
        return OrderedDict([(k,v) for (k,v) in sorted(ret.items(), key=lambda x: x[0])])
    
    def _sort_and_annotate_doc(self, doc, sort=True, jsonld=False, data_src=False, field_sep='/'):
        def _recursion_helper(doc, context_key):
            if is_seq(doc):
                return [_recursion_helper(_doc, context_key) for _doc in doc]
            elif isinstance(doc, dict):
                if jsonld and context_key in self.jsonld_context:
                    doc['@context'] = self.jsonld_context[context_key]['@context']
                if data_src and context_key in self.data_sources:
                    doc['@sources'] = self.data_sources[context_key]['@sources']
                if sort:
                    _doc = sorted(doc)
                else:
                    _doc = doc.keys()
                this_list = []
                for key in _doc:
                    new_context = key if context_key == 'root' else field_sep.join([context_key, key])
                    this_list.append((self._alias_output_keys(new_context, key), _recursion_helper(doc[key], new_context)))
                if sort:
                    return OrderedDict(this_list)
                else:
                    return dict(this_list)
            else:
                return doc

        return _recursion_helper(doc, 'root') 

    def _form_doc(self, doc, score=True):
        _doc = doc.get('_source', doc.get('fields', {}))
        for attr in ['_id', '_score', '_version']:
            if attr in doc:
                _doc.setdefault(attr, doc[attr])
        
        if not score:
            _doc.pop('_score', None)

        if doc.get('found', None) is False:
            _doc['found'] = doc['found']

        self._modify_doc(_doc)
           
        if self.options.dotfield:
            return self._flatten_doc(_doc)
        else:
            return self._sort_and_annotate_doc(_doc, jsonld=self.options.jsonld, 
                            sort=self.options._sorted, data_src=self.options.datasource)

    def _modify_doc(self, doc):
        ''' Override to add custom fields to doc before flattening/sorting '''
        pass

    def _alias_output_keys(self, context, key):
        if context in self.output_aliases:
            return self.output_aliases[context]
        return key
    
    def _clean_common_POST_response(self, _list, res, single_hit=True, score=True):
        res = res['responses']

        assert len(res) == len(_list)
        _res = []
        for (qterm, result) in zip(_list, res):
            if 'error' in result:
                _res.append({u'query': qterm, 'error': True})

            hits = result['hits']
            total = hits['total']

            if total == 0:
                _res.append({u'query': qterm, u'notfound': True})
            elif total == 1 and single_hit:
                _ret = OrderedDict({u'query': qterm})
                _ret.update(self._form_doc(doc=hits['hits'][0], score=score))
                _res.append(_ret)
            else:
                for hit in hits['hits']:
                    _ret = OrderedDict({u'query': qterm})
                    _ret.update(self._form_doc(doc=hit, score=score))
                    _res.append(_ret)
        return _res

    def _clean_annotation_GET_response(self, res):
        # if the search was from an es.get
        if 'hits' not in res:
            return self._form_doc(res, score=False)
        # if the search was from an es.search
        _res = [self._form_doc(hit, score=False) for hit in res['hits']['hits']]
        if len(_res) == 1:
            return _res[0]
        return _res

    def _clean_annotation_POST_response(self, bid_list, res, single_hit=False):
        return self._clean_common_POST_response(_list=bid_list, res=res, single_hit=single_hit, score=False)

    def _clean_query_GET_response(self, res):
        if 'aggregations' in res:
            res['facets'] = res.pop('aggregations')
            for facet in res['facets']:
                res['facets'][facet]['_type'] = 'terms'
                res['facets'][facet]['terms'] = res['facets'][facets].pop('buckets')
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
        _res['hits'] = [self._form_doc(doc=doc) for doc in _res['hits']]
        _resf = OrderedDict([(k, v) for (k, v) in sorted(_res.items(), key=lambda i: i[0]) 
                                if k != 'hits'])
        _resf['hits'] = _res['hits'] 
        return _resf

    def _clean_query_POST_response(self, qlist, res, single_hit=False):
        return self._clean_common_POST_response(_list=qlist, res=res, single_hit=single_hit)

    def _clean_metadata_response(self, res, fields=False):
        # assumes only one doc_type in the index... maybe a bad assumption
        _index = next(iter(res))
        _doc_type = next(iter(res[_index]['mappings']))
        if fields:
            # this is an available fields request
            _properties = res[_index]['mappings'][_doc_type]['properties']
            _properties = OrderedDict([(k.replace('.properties', ''), v) for (k,v) in flatten_doc(_properties).items()])
            _fields = OrderedDict()
            for (k,v) in _properties.items():
                _k = '.'.join(k.split('.')[:-1])
                if ((not self.options.prefix and not self.options.search) or 
                    (self.options.prefix and _k.startswith(self.options.prefix)) or 
                    (self.options.search and self.options.search in _k)): 
                    _fields.setdefault(_k, OrderedDict())
                    _fields[_k][k.split('.')[-1]] = v
            return _fields

        # normal metadata request
        _meta = res[_index]['mappings'][_doc_type].get('_meta', {})
        if self.options.dev:
            _meta['software'] = self._get_software_info()
        return self._sort_and_annotate_doc(_meta)

    def _clean_scroll_response(self, res):
        scroll_id = res.get('_scroll_id', None)
        if not scroll_id or not res['hits']['hits']:
            raise ScrollIterationDone("No results to return")

        _ret = self._clean_query_GET_response(res)

        if res['_shards']['failed']:
            _ret.update({'_warning': 'Scroll request has failed on {} shards out of {}.'.format(res['_shards']['failed'], res['_shards']['total'])})
        return _ret

    def _get_software_info(self):
        ''' Override me '''
        return get_software_info(app_dir=self.app_dir)
 
    def clean_annotation_GET_response(self, res):
        return self._clean_annotation_GET_response(res)

    def clean_annotation_POST_response(self, bid_list, res, single_hit=True):
        return self._clean_annotation_POST_response(bid_list, res, single_hit)

    def clean_query_GET_response(self, res):
        return self._clean_query_GET_response(res)

    def clean_query_POST_response(self, qlist, res, single_hit=True):
        return self._clean_query_POST_response(qlist=qlist, res=res, single_hit=single_hit)

    def clean_metadata_response(self, res, fields=False):
        return self._clean_metadata_response(res, fields=fields)

    def clean_scroll_response(self, res):
        return self._clean_scroll_response(res)
