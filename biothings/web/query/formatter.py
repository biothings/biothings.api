"""
    Search Result Formatter

    Transform the raw query result into consumption-friendly
    structures by possibly removing from, adding to, and/or
    flattening the raw response from the database engine for
    one or more individual queries.

"""
from collections import UserDict, defaultdict

from elastic_transport import ObjectApiResponse

from biothings.utils.common import dotdict, traverse
from biothings.utils.jmespath import options as jmp_options

import logging
logger = logging.getLogger(__name__)

class FormatterDict(UserDict):
    def collapse(self, key):
        self.update(self.pop(key))

    def exclude(self, keys):
        for key in keys:
            self.pop(key, None)

    def include(self, keys):
        for key in list(self.keys()):
            if key in keys:
                self.pop(key)

    def wrap(self, key, kls):
        if isinstance(self.get(key), list):
            self[key] = [kls(x) for x in self[key]]
        else:
            self[key] = kls(self[key])


class Hits(FormatterDict):
    """
    {
        "total": ... ,
        "hits": [
            { ... },
            { ... },
            ...
        ]
    }
    """


class Doc(FormatterDict):
    """
    {
        "_id": ... ,
        "_score": ... ,
        ...
    }
    """


class ResultFormatterException(Exception):
    pass


class ResultFormatter:
    def transform(self, response):
        return response

    def transform_mapping(self, mapping, prefix=None, search=None):
        return mapping


class ESResultFormatter(ResultFormatter):
    """Class to transform the results of the Elasticsearch query generated prior in the pipeline.
    This contains the functions to extract the final document from the elasticsearch query result
    in `Elasticsearch Query`_.  This also contains the code to flatten a document etc.
    """

    class _Hits(Hits):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            # make sure the document is coming from
            # elasticsearch at initialization time
            assert "hits" in self.data
            assert "total" in self.data["hits"]
            assert "hits" in self.data["hits"]
            for hit in self.data["hits"]["hits"]:
                assert "_source" in hit

    class _Doc(Doc):
        pass

    def __init__(self, licenses=None, license_transform=None, field_notes=None, excluded_keys=()):
        # do not directly use the "or" syntax
        # to turn a None object to a {}, that
        # would also replace external empty {}
        # that will later get populated with
        # contents when data is available.

        if licenses is None:
            licenses = {}
        if license_transform is None:
            license_transform = {}
        if field_notes is None:
            field_notes = {}

        # license settings
        # ---------------------
        # this feature is turned on by default,
        # it appends license fields to the documents
        # by looking at its first level field names
        # or the transformed field name equivalences.

        self.licenses = licenses
        # example:
        # {
        #     "exac": "http://example.com/licenseA",
        #     "snpeff": "http://example.com/licenseB"
        # }
        self.license_transform = license_transform
        # example:
        # {
        #     "exac_nontcga": "exac",
        #     "snpeff.ann": "snpeff"
        # }

        # mapping dispaly settings
        # -------------------------
        self.field_notes = field_notes
        self.excluded_keys = excluded_keys

    # for compatibility
    traverse = staticmethod(traverse)

    def transform(self, response, **options):
        """
        Transform the query response to a user-friendly structure.
        Mainly deconstruct the elasticsearch response structure and
        hand over to transform_doc to apply the options below.

        Options:
            # generic transformations for dictionaries
            # ------------------------------------------
            dotfield: flatten a dictionary using dotfield notation
            _sorted: sort keys alaphabetically in ascending order
            always_list: ensure the fields specified are lists or wrapped in a list
            allow_null: ensure the fields specified are present in the result,
                        the fields may be provided as type None or [].

            # additional multisearch result transformations
            # ------------------------------------------------
            template: base dict for every result, for example: {"success": true}
            templates: a different base for every result, replaces the setting above
            template_hit: a dict to update every positive hit result, default: {"found": true}
            template_miss: a dict to update every query with no hit, default: {"found": false}

            # document format and content management
            # ---------------------------------------
            biothing_type: result document type to apply customized transformation.
                        for example, add license field basing on document type's metadata.
            one: return the individual document if there's only one hit. ignore this setting
                if there are multiple hits. return None if there is no hit. this option is
                not effective when aggregation results are also returned in the same query.
            native: bool, if the returned result is in python primitive types.
            version: bool, if _version field is kept.
            score: bool, if _score field is kept.
            with_total: bool, if True, the response will include max_total documents,
                and a message to tell how many query terms return greater than the max_size of hits.
                The default is False.
                An example when with_total is True:
                {
                    'max_total': 100,
                    'msg': '12 query terms return > 1000 hits, using from=1000 to retrieve the remaining hits',
                    'hits': [...]
                }
            jmespath: passed as "<target_field>|<jmes_query>" to transform any target field in hit using
                jmespath query syntax.
            jmespath_exclude_empty: bool, if True, exclude the hit from the hits list if the transformed value
                is empty (e.g. [] or None). Default is False.
        """
        options = dotdict(options)
        if isinstance(response, ObjectApiResponse):
            response = response.body

        if isinstance(response, list):
            max_total = 0
            count_query_exceed_max_size = 0
            # If options.set isn't set, the default number of hits returned by the ES is 10.
            # Ref: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-multi-search.html#search-multi-search-api-request-body  # noqa: F501
            max_size = options.size or 10

            responses_ = []
            options.pop("one", None)  # ignore
            template = options.pop("template", {})
            templates = options.pop("templates", [template] * len(response))
            template_hit = options.pop("template_hit", dict(found=True))
            template_miss = options.pop("template_miss", dict(found=False))
            responses = [self.transform(res, **options) for res in response]
            for tpl, res in zip(templates, responses):
                if options.with_total:
                    total = res.get("total", {}).get("value") or 0
                    if total > max_total:
                        max_total = total
                    if total > max_size:
                        count_query_exceed_max_size += 1

                for _res in res if isinstance(res, list) else [res]:
                    assert isinstance(_res, dict)
                    if _res and "hits" not in _res:
                        hit_ = dict(tpl)
                        hit_.update(template_hit)
                        hit_.update(_res)
                        responses_.append(hit_)
                        continue
                    if not _res or not _res["hits"]:
                        tpl.update(template_miss)
                        responses_.append(tpl)
                        continue
                    for hit in _res["hits"]:
                        hit_ = dict(tpl)
                        hit_.update(template_hit)
                        hit_.update(hit)
                        responses_.append(hit_)
            response_ = list(filter(None, responses_))

            if options.with_total:
                response_ = {
                    "max_total": max_total,
                    "hits": response_,
                }
                if count_query_exceed_max_size > 0:
                    _from = (options.get("from") or 0) + max_size
                    response_["msg"] = (
                        f"{count_query_exceed_max_size} query terms return > {max_size} hits, "
                        f"using from={_from} to retrieve the remaining hits"
                    )

            return response_


        if isinstance(response, dict):
            response = self._Hits(response)
            response.collapse("hits")
            response.exclude(("_shards", "_node", "timed_out"))
            response.wrap("hits", self._Doc)

            for hit in response["hits"]:
                hit.collapse("_source")
                # 'sort' is introduced when sorting
                hit.exclude(("_index", "_type", "sort"))
                self._transform_hit(hit, options)

            if options.get("native", True):
                response["hits"] = [hit.data for hit in response["hits"] if not (hit.__doc__ or "").startswith("__exclude__")]
                response = response.data

            if "aggregations" in response:
                self.transform_aggs(response["aggregations"])
                response["facets"] = response.pop("aggregations")
                hits = response.pop("hits")  # move key order
                if hits:  # hide "hits" field when size=0
                    response["hits"] = hits

            elif options.get("one"):
                # prefer one-level presentation
                # or structures as simple as possible
                if len(response["hits"]) == 1:
                    response = response["hits"][0]
                elif len(response["hits"]) == 0:
                    response = None
                else:  # show a list of docs
                    response = response["hits"]

            return response

        raise TypeError(f"Invalid response type of {type(response)}.")

    def _transform_hit(self, doc, options):
        """
        In-place apply a variety of transformations to a document like:
        {
            "_id": ... ,
            "_index": ... ,
            ...
        }
        """
        if not options.get("version", False):
            doc.pop("_version", None)
        if not options.get("score", True):
            doc.pop("_score", None)

        for path, obj in self.traverse(doc):
            self.transform_hit(path, obj, doc, options)
            if options.allow_null:
                self._allow_null(path, obj, options.allow_null)
            if options.always_list:
                self._always_list(path, obj, options.always_list)
            if options._sorted:
                self._sorted(path, obj)
        if options.dotfield:
            self._dotfield(doc, options)

    @staticmethod
    def _allow_null(path, obj, fields):
        """
        The specified fields should be set to None if it does not exist.
        When flattened, the field could be converted to an empty list.
        """
        if isinstance(obj, (dict, Doc)):
            for field in fields or []:
                if field.startswith(path):
                    key = field[len(path) :].lstrip(".")  # noqa: E203
                    if "." not in key and key not in obj:
                        obj[key] = None

    @staticmethod
    def _always_list(path, obj, fields):
        """
        The specified fields, if exist, should be set to a list type.
        None converts to an emtpy list [] instead of [None].
        """
        if isinstance(obj, (dict, Doc)):
            for field in fields:
                if field.startswith(path):
                    key = field[len(path) :].lstrip(".")  # noqa: E203
                    if key in obj and not isinstance(obj[key], list):
                        obj[key] = [obj[key]] if obj[key] is not None else []

    @staticmethod
    def _sorted(_, obj):
        """
        Sort a container in-place.
        """
        try:
            if isinstance(obj, (dict, Doc)):
                sorted_items = sorted(obj.items())
                obj.clear()
                obj.update(sorted_items)
        except Exception:
            pass

    @classmethod
    def _dotfield(cls, dic, options):
        """
        Flatten a dictionary.
        See biothings.utils.common.traverse for examples.
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
                    cls._sorted(key, hit_[key])
        dic.clear()
        dic.update(hit_)

    def transform_hit(self, path, obj, doc, options):
        """
        Transform an individual search hit result.
        By default add licenses for the configured fields.

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

        licenses = self.licenses.get(options.biothing_type, {})
        if path in self.license_transform:
            path = self.license_transform[path]
        if path in licenses and isinstance(obj, dict):
            obj["_license"] = licenses[path]

        if options.jmespath:
            self.trasform_jmespath(path, obj, doc, options)

    def trasform_jmespath(self, path: str, obj, doc, options) -> None:
        """Transform any target field in obj using jmespath query syntax.
        The jmespath query parameter value should have the pattern of "<target_list_fieldname>|<jmespath_query_expression>"
        <target_list_fieldname> can be any sub-field of the input obj using dot notation, e.g. "aaa.bbb".
          If empty or ".", it will be the root field.
        The flexible jmespath syntax allows to filter/transform any nested objects in the input obj on the fly.
        The output of the jmespath transformation will then be used to replace the original target field value.
        Examples:
             * filtering an array sub-field
                 jmespath=tags|[?name==`Metadata`]  # filter tags array by name field
                 jmespath=aaa.bbb|[?(sub_a==`val_a`||sub_a==`val_aa`)%26%26sub_b==`val_b`]    # use %26%26 for &&

        obj: the object to be transformed, which corresponding to the current path
        doc: the whole document we are traversing in the upstream _transform_hit method
             passed here in case we need to make changes to the whole document.
        """
        # options.jmespath is already validated and processed as a tuple
        # see biothings.web.options.manager.Coverter.translate
        parent_path, target_field, jmes_query = options.jmespath
        # mark the hit to be removed from the hits list, in transform method,
        # if transformed value is empty
        jmespath_exclude_empty = options.get("jmespath_exclude_empty", False)

        if path == parent_path:
            # we handle jmespath transformation at its parent field level,
            # so that we can set a transformed value
            if not isinstance(obj, (dict, UserDict)):
                raise ResultFormatterException(
                    f"\"parent_path\" in jmespath transformation cannot be non-dict type: \"{parent_path}\"({type(obj).__name__})"
                )
            target_field_value = obj.get(target_field) if target_field else obj
            if target_field_value:
                # pass jmp_options to include our own custom jmespath functions
                transformed_field_value = jmes_query.search(target_field_value, options=jmp_options)
                if not transformed_field_value and jmespath_exclude_empty:
                    # if transformed value is empty, mark the hit to be removed from the hits list
                    doc.__doc__ = "__exclude__"
                if target_field:
                    obj[target_field] = transformed_field_value
                else:
                    # if the target field is the root field, we need to replace the whole obj
                    # note that we cannot use `obj = transformed_field_value` here, because
                    # it will break the reference to the original obj object
                    obj.clear()
                    obj.update(transformed_field_value)

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
            res[facet]["_type"] = "terms"  # a type of ES Bucket Aggs
            res[facet]["terms"] = res[facet].pop("buckets")
            res[facet]["other"] = res[facet].pop("sum_other_doc_count", 0)
            res[facet]["missing"] = res[facet].pop("doc_count_error_upper_bound", 0)

            count = 0

            for bucket in res[facet]["terms"]:
                bucket["count"] = bucket.pop("doc_count")
                bucket["term"] = bucket.pop("key")
                if "key_as_string" in bucket:
                    bucket["term"] = bucket.pop("key_as_string")
                count += bucket["count"]

                # nested aggs
                for agg_k in list(bucket.keys()):
                    if isinstance(bucket[agg_k], dict):
                        bucket.update(self.transform_aggs(dict({agg_k: bucket[agg_k]})))

            res[facet]["total"] = count

        return res

    def transform_mapping(self, mapping, prefix=None, search=None):
        """
        Transform Elasticsearch mapping definition to
        user-friendly field definitions metadata results.
        """
        assert isinstance(mapping, dict)
        assert isinstance(prefix, str) or prefix is None
        assert isinstance(search, str) or search is None

        result = {}
        items = list(mapping.items())
        items.reverse()

        while items:
            key, dic = items.pop()
            dic = dict(dic)
            dic.pop("dynamic", None)
            dic.pop("normalizer", None)

            if key in self.field_notes:
                result["notes"] = self.field_notes[key]

            if "copy_to" in dic:
                if "all" in dic["copy_to"]:
                    dic["searched_by_default"] = True
                del dic["copy_to"]

            if "index" not in dic:
                if "enabled" in dic:
                    dic["index"] = dic.pop("enabled")
                else:  # true by default
                    dic["index"] = True

            if "properties" in dic:
                dic["type"] = "object"
                subs = ((".".join((key, k)), v) for k, v in dic["properties"].items())
                items.extend(reversed(list(subs)))
                del dic["properties"]

            if all(
                (
                    not self.excluded_keys or key not in self.excluded_keys,
                    not prefix or key.startswith(prefix),
                    not search or search in key,
                )
            ):
                result[key] = dict(sorted(dic.items()))

        return result


class MongoResultFormatter(ResultFormatter):
    def transform(self, result, **options):
        return {"total": {"value": len(result), "relation": "gte"}, "hits": result}


class SQLResultFormatter(ResultFormatter):
    def transform(self, result, **options):
        header, content = result
        return {
            "total": {"value": len(content), "relation": "gte"},
            "hits": [dict(zip(header, row)) for row in content],
        }
