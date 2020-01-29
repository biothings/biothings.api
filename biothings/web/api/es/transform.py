from biothings.utils.version import get_software_info
from biothings.utils.common import is_seq
from biothings.utils.web.es import exists_or_null
from biothings.utils.doc_traversal import breadth_first_traversal
from collections import OrderedDict
import logging


class ScrollIterationDone(Exception):
    ''' Thrown when no more results are left in request scroll batch. '''
    pass


class ESResultTransformer(object):
    ''' Class to transform the results of the Elasticsearch query generated prior in the pipeline.
    This contains the functions to extract the final document from the elasticsearch query result
    in `Elasticsearch Query`_.  This also contains the code to flatten a document (if **dotfield** is True), or
    to add JSON-LD context to the document (if **jsonld** is True).

    :param options: Options from the URL string controlling result transformer
    :param host: Host name (extracted from request), used for JSON-LD address generation
    :param doc_url_function: a function that takes one argument (a biothing id) and returns a URL to that biothing
    :param data_sources: unused currently (optional)
    :param output_aliases: list of output key names to alias, unused currently (optional)
    :param app_dir: Application directory for this app (used for getting app information in /metadata)
    :param source_metadata: Metadata object containing source information for _license keys
    :param excluded_keys: A list of keys to exclude from the available keys output
    :param field_notes: A dictionary of notes to add to the available keys output'''

    def __init__(self, options, host,
                 doc_url_function=lambda x: x,
                 data_sources={},
                 output_aliases={},
                 app_dir='',
                 source_metadata={},
                 excluded_keys=[],
                 field_notes={},
                 licenses={}):
        self.options = options
        self.host = host
        self.doc_url_function = doc_url_function
        self.data_sources = data_sources
        self.output_aliases = output_aliases
        self.app_dir = app_dir
        self.source_metadata = source_metadata
        self.excluded_keys = excluded_keys
        self.field_notes = field_notes
        self.licenses = licenses

    def _flatten_doc(self, doc, outfield_sep='.', context_sep='.'):
        def _recursion_helper(d, ret, path, out):
            if isinstance(d, dict):
                for key in d:
                    new_path_key = key if not path else context_sep.join([path, key])
                    new_out_key = self._alias_output_keys(
                        new_path_key, key) if not out else outfield_sep.join(
                        [out, self._alias_output_keys(new_path_key, key)])
                    _recursion_helper(d[key], ret, new_path_key, new_out_key)
            elif is_seq(d):
                for obj in d:
                    _recursion_helper(obj, ret, path, out)
            else:
                if self.options.always_list and out in self.options.always_list:
                    ret[out] = []
                if out in ret:
                    if isinstance(ret[out], list):
                        ret[out].append(d)
                    else:
                        ret[out] = [ret[out], d]
                else:
                    ret[out] = d
        ret = {}
        _recursion_helper(doc, ret, '', '')
        return OrderedDict([(k, v) for (k, v) in sorted(ret.items(), key=lambda x: x[0])])

    def _sort_and_annotate_doc(self, doc, sort=True, data_src=False, field_sep='.'):
        def _recursion_helper(doc, path, parent_type):
            if is_seq(doc):
                return [_recursion_helper(_doc, path, type(doc)) for _doc in doc]
            elif isinstance(doc, dict):
                if data_src and path in self.data_sources:
                    doc['@sources'] = self.data_sources[path]['@sources']
                if sort:
                    _doc = sorted(doc)
                else:
                    _doc = doc.keys()
                this_list = []
                for key in _doc:
                    new_path = key if not path else field_sep.join([path, key])
                    this_list.append(
                        (self._alias_output_keys(
                            new_path, key), _recursion_helper(
                            doc[key], new_path, type(doc))))

                if parent_type != list and parent_type != tuple and self.options.always_list and path in self.options.always_list:
                    if sort:
                        return [OrderedDict(this_list)]
                    else:
                        return [dict(this_list)]
                else:
                    if sort:
                        return OrderedDict(this_list)
                    else:
                        return dict(this_list)
            elif parent_type != list and parent_type != tuple and self.options.always_list and path in self.options.always_list:
                return [doc]
            else:
                return doc

        return _recursion_helper(doc, '', type(doc))

    def _get_doc(self, doc):
        return doc.get('_source', doc.get('fields', {}))

    def _form_doc(self, doc, score=True):
        _doc = self._get_doc(doc)
        for attr in ['_id', '_score', '_version']:
            if attr in doc:
                _doc.setdefault(attr, doc[attr])

        if not score:
            _doc.pop('_score', None)

        if doc.get('found', None) is False:
            _doc['found'] = doc['found']

        self._append_licenses(_doc)
        self._modify_doc(_doc)

        _doc = self._sort_and_annotate_doc(
            _doc, sort=self.options._sorted, data_src=self.options.datasource)
        for _field in self.options.allow_null:
            _doc = exists_or_null(_doc, _field)
        if self.options.dotfield:
            _doc = self._flatten_doc(_doc)
        return _doc

    def _append_licenses(self, doc):
        '''
            Add "_license" URL to corresponding fields.
            URLs are retrieved from ES index metadata.
            Specify field source conversion in settings.
            Support dot field representation.
            May override default behavior.
        '''

        get_url = lambda val: val.get('license_url_short', val.get('license_url'))
        sources = self.source_metadata.get(self.options.assembly, {}) \
            if self.options.assembly else self.source_metadata
        licenses = {source: get_url(val) for source, val in sources.items() if get_url(val)}

        def flatten_key(dic):
            '''
            {
                "gnomad_exome": {
                    "af": {
                        "af": 0.0000119429,
                        "af_afr": 0.000123077
                    }
                },
                "exac_nontcga": {
                    "af": 0.00001883
                }
            }
            will be translated to a generator of
            [
                "gnomad_exome",
                "gnomad_exome.af",
                "exac_nontcga"
            ]
            '''
            for key in dic:
                if isinstance(dic[key], dict):
                    yield key, dic[key]
                    for sub_key, val in flatten_key(dic[key]):
                        yield '.'.join((key, sub_key)), val
                elif isinstance(dic[key], list):
                    yield key, dic[key]

        def set_license(obj, url):
            '''
            If we have the following settings in web_config.py

            LICENSE_TRANSFORM = {
                "exac_nontcga": "exac",
                "snpeff.ann": "snpeff"
            },

            Then GET /v1/variant/chr6:g.38906659G>A would look like:
            {
                "exac": {
                    "_license": "http://bit.ly/2H9c4hg",
                    "af": 0.00002471
                },
                "exac_nontcga": {
                    "_license": "http://bit.ly/2H9c4hg",         <---
                    "af": 0.00001883
                }, ...
            }

            And GET /v1/variant/chr14:g.35731936G>C could look like:
            {
                "snpeff": {
                    "_license": "http://bit.ly/2suyRKt",
                    "ann": [
                        {
                            "_license": "http://bit.ly/2suyRKt", <---
                            "effect": "intron_variant",
                            "feature_id": "NM_014672.3", ...
                        },
                        {
                            "_license": "http://bit.ly/2suyRKt", <---
                            "effect": "intron_variant",
                            "feature_id": "NM_001256678.1", ...
                        }, ...
                    ]
                }, ...
            }

            The arrow marked fields would not exist without the setting lines.
            '''
            if isinstance(obj, dict):
                obj['_license'] = url
            elif isinstance(obj, list):
                for item in obj:
                    if isinstance(item, dict):
                        item['_license'] = url

        if licenses:
            for fkey, val in flatten_key(doc):
                if fkey in self.licenses:
                    set_license(val, licenses[self.licenses[fkey]])
                elif '.' not in fkey and fkey in licenses:
                    set_license(val, licenses[fkey])

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

    def _clean_annotation_GET_response(self, res, score=False):
        # if the search was from an es.get
        if 'hits' not in res:
            return self._form_doc(res, score=score)
        # if the search was from an es.search
        _res = [self._form_doc(hit, score=score) for hit in res['hits']['hits']]
        if len(_res) == 1:
            return _res[0]
        return _res

    def _clean_annotation_POST_response(self, bid_list, res, single_hit=False):
        return self._clean_common_POST_response(
            _list=bid_list, res=res, single_hit=single_hit, score=False)

    def _clean_aggregations_response(self, res):
        for facet in res:
            res[facet]['_type'] = 'terms'
            res[facet]['terms'] = res[facet].pop('buckets')
            res[facet]['other'] = res[facet].pop('sum_other_doc_count')
            res[facet]['missing'] = res[facet].pop('doc_count_error_upper_bound')
            count = 0
            for term in res[facet]['terms']:
                term['count'] = term.pop('doc_count')
                count += term['count']
                term['term'] = term.pop('key')
                for agg_k in list(term.keys()):
                    if agg_k not in ['count', 'term']:
                        term.update(self._clean_aggregations_response({agg_k: term[agg_k]}))
            res[facet]['total'] = count
        return res

    def _clean_query_GET_response(self, res):
        if 'aggregations' in res:
            res['facets'] = res.pop('aggregations')
            res['facets'] = self._clean_aggregations_response(res['facets'])

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
        def _form_key(t, sep='.'):
            ''' takes a tuple, returns the key name as a string '''
            return sep.join(t).replace('.properties', '')

        # assumes only one doc_type in the index... maybe a bad assumption
        _index = next(iter(res))
        _doc_type = next(iter(res[_index]['mappings']))
        if fields:
            # this is an available fields request
            _properties = res[_index]['mappings'][_doc_type]['properties']
            _fields = OrderedDict()
            for (k, v) in breadth_first_traversal(_properties):
                if isinstance(v, dict):
                    _k = _form_key(k)
                    _arr = []
                    if (('properties' in v) or ('type' in v and isinstance(
                            v['type'], str) and v['type'].lower() == 'object')):
                        # object datatype
                        _arr.append(('type', 'object'))
                    elif 'type' in v and isinstance(v['type'], str):
                        # other type
                        _arr.append(('type', v['type'].lower()))
                    if _arr:
                        if 'index' in v and isinstance(v['index'], str) and v['index'].lower() in [
                                'no', 'false']:
                            _arr.append(('index', False))
                        else:
                            _arr.append(('index', True))
                        if 'analyzer' in v and isinstance(v['analyzer'], str):
                            _arr.append(('analyzer', v['analyzer'].lower()))
                        if 'copy_to' in v and isinstance(
                                v['copy_to'],
                                list) and 'all' in v['copy_to']:
                            _arr.append(('searched_by_default', True))
                        if _k in self.field_notes:
                            _arr.append(('notes', self.field_notes[_k]))
                    _v = OrderedDict(_arr)
                    if ((_k.lower() not in self.excluded_keys) and (_v and ((not self.options.prefix and not self.options.search)
                                                                            or (self.options.prefix and _k.startswith(self.options.prefix))
                                                                            or (self.options.search and self.options.search in _k)))):
                        _fields.setdefault(_k, _v)
            return OrderedDict(sorted(_fields.items(), key=lambda x: x[0]))

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
            _ret.update({'_warning': 'Scroll request has failed on {} shards out of {}.'.format(
                res['_shards']['failed'], res['_shards']['total'])})
        return _ret

    def _get_software_info(self):
        ''' Override me '''
        return get_software_info(app_dir=self.app_dir)

    def clean_annotation_GET_response(self, res):
        ''' Transform the results of a GET to the annotation lookup endpoint.

        :param res: Results from `Elasticsearch Query`_. '''
        return self._clean_annotation_GET_response(res)

    def clean_annotation_POST_response(self, bid_list, res, single_hit=True):
        ''' Transform the results of a POST to the annotation lookup endpoint.

        :param bid_list: List of biothing id inputs
        :param res: Results from `Elasticsearch Query`_
        :param single_hit: If ``True``, render queries with 1 result as a dictionary, else as a 1-element list containing a dictionary '''
        return self._clean_annotation_POST_response(bid_list, res, single_hit)

    def clean_query_GET_response(self, res):
        ''' Transform the results of a GET to the query endpoint.

        :param res: Results from `Elasticsearch Query`_. '''
        return self._clean_query_GET_response(res)

    def clean_query_POST_response(self, qlist, res, single_hit=True):
        ''' Transform the results of a POST to the query endpoint.

        :param qlist: List of query inputs
        :param res: Results from `Elasticsearch Query`_
        :param single_hit: If ``True``, render queries with 1 result as a dictionary, else as a 1-element list containing a dictionary '''
        return self._clean_query_POST_response(qlist=qlist, res=res, single_hit=single_hit)

    def clean_metadata_response(self, res, fields=False):
        ''' Transform the results of a GET to the metadata endpoint.

        :param res: Results from `Elasticsearch Query`_. '''
        return self._clean_metadata_response(res, fields=fields)

    def clean_scroll_response(self, res):
        ''' Transform the results of a GET to the scroll endpoint

        :param res: Results from `Elasticsearch Query`_. '''
        return self._clean_scroll_response(res)
