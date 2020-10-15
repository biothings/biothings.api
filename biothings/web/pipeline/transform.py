"""
    Elasticsearch Query Result Transform
"""
from collections import defaultdict

from biothings.utils.common import dotdict


class ResultTransformException(Exception):
    pass

class ESResultTransform(object):
    ''' Class to transform the results of the Elasticsearch query generated prior in the pipeline.
    This contains the functions to extract the final document from the elasticsearch query result
    in `Elasticsearch Query`_.  This also contains the code to flatten a document etc.
    '''

    def __init__(self, web_settings):

        # license appending settings
        self.source_licenses = web_settings.metadata.biothing_licenses
        self.license_transform = web_settings.LICENSE_TRANSFORM

        # mapping transform
        self.field_notes = web_settings.fieldnote.get_field_notes()
        self.excluded_keys = web_settings.AVAILABLE_FIELDS_EXCLUDED

    @classmethod
    def traverse(cls, obj, leaf_node=False):
        """
        Output path-dictionary pairs. For example, input:
        {
            'exac_nontcga': {'af': 0.00001883},
            'gnomad_exome': {'af': {'af': 0.0000119429, 'af_afr': 0.000123077}},
            'snpeff': {'ann': [{'effect': 'intron_variant',
                                'feature_id': 'NM_014672.3'},
                               {'effect': 'intron_variant',
                                'feature_id': 'NM_001256678.1'}]}
        }
        will be translated to a generator:
        (
            ("exac_nontcga", {"af": 0.00001883}),
            ("gnomad_exome.af", {"af": 0.0000119429, "af_afr": 0.000123077}),
            ("gnomad_exome", {"af": {"af": 0.0000119429, "af_afr": 0.000123077}}),
            ("snpeff.ann", {"effect": "intron_variant", "feature_id": "NM_014672.3"}),
            ("snpeff.ann", {"effect": "intron_variant", "feature_id": "NM_001256678.1"}),
            ("snpeff.ann", [{ ... },{ ... }]),
            ("snpeff", {"ann": [{ ... },{ ... }]}),
            ('', {'exac_nontcga': {...}, 'gnomad_exome': {...}, 'snpeff': {...}})
        )
        or when traversing leaf nodes:
        (
            ('exac_nontcga.af', 0.00001883),
            ('gnomad_exome.af.af', 0.0000119429),
            ('gnomad_exome.af.af_afr', 0.000123077),
            ('snpeff.ann.effect', 'intron_variant'),
            ('snpeff.ann.feature_id', 'NM_014672.3'),
            ('snpeff.ann.effect', 'intron_variant'),
            ('snpeff.ann.feature_id', 'NM_001256678.1')
        )
        """
        if isinstance(obj, dict):  # path level increases
            for key in obj:
                for sub_path, val in cls.traverse(obj[key], leaf_node):
                    yield '.'.join((key, sub_path)).strip('.'), val
            if not leaf_node:
                yield '', obj
        elif isinstance(obj, list):  # does not affect path
            for item in obj:
                for sub_path, val in cls.traverse(item, leaf_node):
                    yield sub_path, val
            if not leaf_node or not obj:  # [] count as a leaf node
                yield '', obj
        elif leaf_node:  # including str, int, float, and *None*.
            yield '', obj

    def transform(self, response, options):
        """
        Transform the query response to a user-friendly structure.

        Options:
            dotfield: flatten a dictionary using dotfield notation
            _sorted: sort keys alaphabetically in ascending order
            always_list: ensure the fields specified are lists or wrapped in a list
            allow_null: ensure the fields specified are present in the result,
                        the fields may be provided as type None or [].
            # only related to multiqueries
            template: base dict for every result, for example: {"success": true}
            templates: a different base for every result, replaces the setting above
            template_hit: a dict to update every positive hit result, default: {"found": true}
            template_miss: a dict to update every query with no hit, default: {"found": false}

        """
        if not isinstance(options, dotdict):
            options = dotdict(options)
        if isinstance(response, list):
            responses_ = []
            template = options.pop('template', {})
            templates = options.pop('templates', [template]*len(response))
            template_hit = options.pop('template_hit', dict(found=True))
            template_miss = options.pop('template_miss', dict(found=False))
            responses = [self.transform(res, options) for res in response]
            for res_, res in zip(templates, responses):
                if not res.get('hits'):
                    res_.update(template_miss)
                    responses_.append(res_)
                else:
                    for hit in res['hits']:
                        hit_ = dict(res_)
                        hit_.update(template_hit)
                        hit_.update(hit)
                        responses_.append(hit_)
            return list(filter(None, responses_))
        if isinstance(response, dict):
            response.update(response.pop('hits', {}))  # collapse one level
            response.pop('_shards')
            response.pop('timed_out')
            if 'hits' in response:
                for hit in response['hits']:
                    hit.update(hit.pop('_source', {}))  # collapse one level
                    for path, obj in self.traverse(hit):
                        self.transform_hit(path, obj, options)
                        if options.allow_null:
                            self.option_allow_null(path, obj, options.allow_null)
                        if options.always_list:
                            self.option_always_list(path, obj, options.always_list)
                        if options._sorted:
                            self.option_sorted(path, obj)
                    if options.dotfield:
                        self.option_dotfield(hit, options)
            if 'aggregations' in response:
                self.transform_aggs(response['aggregations'])
                response['facets'] = response.pop('aggregations')
                response['hits'] = response.pop('hits')  # order
            return response
        return {}

    @staticmethod
    def option_allow_null(path, obj, fields):
        """
        The specified fields should be set to None if it does not exist.
        When flattened, the field could be converted to an empty list.
        """
        if isinstance(obj, dict):
            for field in fields or []:
                if field.startswith(path):
                    key = field[len(path):].lstrip('.')
                    if '.' not in key and key not in obj:
                        obj[key] = None

    @staticmethod
    def option_always_list(path, obj, fields):
        """
        The specified fields, if exist, should be set to a list type.
        None converts to an emtpy list [] instead of [None].
        """
        if isinstance(obj, dict):
            for field in fields:
                if field.startswith(path):
                    key = field[len(path):].lstrip('.')
                    if key in obj and not isinstance(obj[key], list):
                        obj[key] = [obj[key]] if obj[key] is not None else []

    @staticmethod
    def option_sorted(_, obj):
        """
        Sort a container in-place.
        """
        try:
            if isinstance(obj, dict):
                sorted_items = sorted(obj.items())
                obj.clear()
                obj.update(sorted_items)
        except Exception:
            pass  # TODO logging

    @classmethod
    def option_dotfield(cls, dic, options):
        """
        Flatten a dictionary.
        #TODO examples
        """
        hit_ = defaultdict(list)
        for path, value in cls.traverse(dic, leaf_node=True):
            hit_[path].append(value)
        for key, lst in hit_.items():
            if len(lst) == 1 and key not in (options.always_list or []):
                hit_[key] = lst[0]
            else:  # multi-element list
                hit_[key] = list(filter(None, lst))
                if options._sorted:
                    cls.option_sorted(key, hit_[key])
        dic.clear()
        dic.update(hit_)

    def transform_hit(self, path, doc, options):
        """
        By default add licenses

        If a source has a license url in its metadata,
        Add "_license" key to the corresponding fields.
        Support dot field representation field alias.

        If we have the following settings in web_config.py

        LICENSE_TRANSFORM = {
            "exac_nontcga": "exac",
            "snpeff.ann": "snpeff"
        },

        Then GET /v1/variant/chr6:g.38906659G>A should look like:
        {
            "exac": {
                "_license": "http://bit.ly/2H9c4hg",
                "af": 0.00002471},
            "exac_nontcga": {
                "_license": "http://bit.ly/2H9c4hg",         <---
                "af": 0.00001883}, ...
        }
        And GET /v1/variant/chr14:g.35731936G>C could look like:
        {
            "snpeff": {
                "_license": "http://bit.ly/2suyRKt",
                "ann": [{"_license": "http://bit.ly/2suyRKt", <---
                        "effect": "intron_variant",
                        "feature_id": "NM_014672.3", ...},
                        {"_license": "http://bit.ly/2suyRKt", <---
                        "effect": "intron_variant",
                        "feature_id": "NM_001256678.1", ...}, ...]
            }, ...
        }

        The arrow marked fields would not exist without the setting lines.
        """
        if path == '':
            doc.pop('_index')
            doc.pop('_type', None)    # not available by default on es7
            doc.pop('sort', None)     # added when using sort
            doc.pop('_node', None)    # added when using explain
            doc.pop('_shard', None)   # added when using explain

        licenses = self.source_licenses[options.biothing_type]
        if path in self.license_transform:
            path = self.license_transform[path]
        if path in licenses and isinstance(doc, dict):
            doc['_license'] = licenses[path]

    def transform_aggs(self, res):
        """
        Transform the aggregations field and make it more presentable.
        For example, these are the fields of a two level nested aggregations:

            aggregations.<term>.doc_count_error_upper_bound
            aggregations.<term>.sum_other_doc_count
            aggregations.<term>.buckets.key
            aggregations.<term>.buckets.key_as_string
            aggregations.<term>.buckets.doc_count
            aggregations.<term>.buckets.<nested_term>.* (recursive)

        After the transformation, we'll have:

            facets.<term>._type
            facets.<term>.total
            facets.<term>.missing
            facets.<term>.other
            facets.<term>.terms.count
            facets.<term>.terms.term
            facets.<term>.terms.<nested_term>.* (recursive)

        Note the first level key change doesn't happen here.
        """

        for facet in res:

            res[facet]['_type'] = 'terms'  # a type of ES Bucket Aggs
            res[facet]['terms'] = res[facet].pop('buckets')
            res[facet]['other'] = res[facet].pop('sum_other_doc_count')
            res[facet]['missing'] = res[facet].pop('doc_count_error_upper_bound')

            count = 0

            for bucket in res[facet]['terms']:
                bucket['count'] = bucket.pop('doc_count')
                bucket['term'] = bucket.pop('key')
                if 'key_as_string' in bucket:
                    bucket['term'] = bucket.pop('key_as_string')
                count += bucket['count']

                # nested aggs
                for agg_k in list(bucket.keys()):
                    if isinstance(bucket[agg_k], dict):
                        bucket.update(self.transform_aggs(dict({agg_k: bucket[agg_k]})))

            res[facet]['total'] = count

        return res

    def transform_mapping(self, mapping, prefix, search):
        """
        Transform Elasticsearch mapping definition to
        user-friendly field definitions metadata result
        """
        assert isinstance(mapping, dict)
        assert isinstance(prefix, str) or prefix is None
        assert isinstance(search, str) or search is None

        result = {}
        todo = list(mapping.items())
        todo.reverse()

        while todo:
            key, dic = todo.pop()
            dic = dict(dic)
            dic.pop('dynamic', None)
            dic.pop('normalizer', None)

            if key in self.field_notes:
                result['notes'] = self.field_notes[key]

            if 'copy_to' in dic:
                if 'all' in dic['copy_to']:
                    dic['searched_by_default'] = True
                del dic['copy_to']

            if 'index' not in dic:
                if 'enabled' in dic:
                    dic['index'] = dic.pop('enabled')
                else:  # true by default
                    dic['index'] = True

            if 'properties' in dic:
                dic['type'] = 'object'
                subs = (('.'.join((key, k)), v) for k, v in dic['properties'].items())
                todo.extend(reversed(list(subs)))
                del dic['properties']

            if all((not self.excluded_keys or key not in self.excluded_keys,
                    not prefix or key.startswith(prefix),
                    not search or search in key)):
                result[key] = dict(sorted(dic.items()))

        return result
