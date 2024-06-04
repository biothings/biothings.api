"""
    Test Query Endpoint

    GET /query
    POST /query

"""
from biothings.tests.web import BiothingsWebAppTest
from setup import setup_es  # pylint: disable=unused-import  # noqa: F401


class TestQueryKeywords(BiothingsWebAppTest):
    def test_00_facet(self):
        """GET /v1/query?q=__all__&aggs=type_of_gene
        {
            "facets": {
                "type_of_gene": {
                "_type": "terms",
                "terms": [
                    {
                        "count": 82,
                        "term": "protein-coding"
                    }
                ],
                "other": 0,
                "missing": 0,
                "total": 82
                }
            },
            "max_score": 1,
            "took": 7,
            "total": 100,
            ...
        }
        """
        res = self.request("/v1/query?q=__all__&aggs=type_of_gene").json()
        term = res["facets"]["type_of_gene"]["terms"][0]
        assert term["count"] == 82
        assert term["term"] == "protein-coding"

    def test_01_facet_size(self):
        """GET /v1/query?q=__all__&aggs=uniprot.TrEMBL&facet_size=2
        {
            "facets": {
                "uniprot.TrEMBL": {
                    "_type": "terms",
                    "terms": [ ... ],
                    "other": 79,
                    "missing": 3,
                    "total": 2
                }
            },
            ...
        }
        """
        res = self.request("/v1/query?q=__all__&aggs=uniprot.TrEMBL&facet_size=2").json()
        terms = res["facets"]["uniprot.TrEMBL"]["terms"]
        assert len(terms) == 2

    def test_02_facet_size_default(self):
        """GET /v1/query?q=__all__&aggs=uniprot.TrEMBL
        {
            "facets": {
                "uniprot.TrEMBL": {
                    "_type": "terms",
                    "terms": [ ... ],
                    "other": 6,
                    "missing": 0,
                    "total": 3          # default is 3
                }
            },
            ...
        }
        """
        res = self.request("/v1/query?q=__all__&aggs=uniprot.TrEMBL").json()
        terms = res["facets"]["uniprot.TrEMBL"]["terms"]
        assert len(terms) == 3

    def test_03_facet_size_max(self):
        """GET /v1/query?q=__all__&aggs=uniprot.TrEMBL&facet_size=10
        {
            "success": false,
            "status": 400,
            "error": "Bad Request",
            "keyword": "facet_size",
            "num": 9,
            "max": 5
        }
        """
        res = self.request(
            "/v1/query?q=__all__&aggs=uniprot.TrEMBL&facet_size=10",
            expect=400,
        ).json()
        assert res["success"] is False

    def test_04_facet_nested(self):
        """GET /v1/query?q=__all__&facets=symbol(alias)
        {
            "took": 32,
            "total": 100,
            "max_score": 0.0,
            "hits": [ ... ],
            "facets": {
                "symbol": {
                    "_type": "terms",
                    "terms": [{
                        "alias": {
                            "_type": "terms",
                            "terms": [
                                {"count": 1,
                                 "term": "a630093n05rik"},
                                {"count": 1,
                                 "term": "cdkn2"},
                                {"count": 1,
                                 "term": "p33(cdk2)"}
                            ],
                            ...
                        },
                        "count": 100,
                        "term": "cdk2"
                    }],
                    ...
                }
            }
        }
        """
        res = self.request("/v1/query?q=__all__&facets=symbol(alias)").json()
        assert res["facets"]["symbol"]["terms"]
        assert res["facets"]["symbol"]["terms"][0]["alias"]

    def test_10_from(self):
        """GET /v1/query?q=__all__&from=99
        {
            "max_score": 1,
            "took": 6,
            "total": 100,
            "hits": [
                { ... }
            ]
        }
        """
        res = self.request("/v1/query?q=__all__&from=99").json()
        assert len(res["hits"]) == 1

    def test_11_from_oob(self):
        """GET /v1/query?q=__all__&from=10001
        {
            "success": false,
            "status": 400,
            "error": "Bad Request",
            "keyword": "from",
            "num": 10001,
            "max": 10000
        }
        """
        res = self.request("/v1/query?q=__all__&from=10001", expect=400).json()
        assert res["success"] is False

    def test_12_size(self):
        """GET /v1/query?q=__all__&size=3
        {
            "max_score": 1,
            "took": 1,
            "total": 100,
            "hits": [
                { ... },
                { ... },
                { ... }
            ]
        }
        """
        res = self.request("/v1/query?q=__all__&size=3").json()
        assert len(res["hits"]) == 3

    def test_13_size_oob(self):
        """GET /v1/query?q=__all__&size=1001
        {
            "success": false,
            "status": 400,
            "error": "Bad Request",
            "keyword": "size",
            "num": 1001,
            "max": 1000
        }
        """
        res = self.request("/v1/query?q=__all__&size=1001", expect=400).json()
        assert res["success"] is False

    def test_14_explain(self):
        """GET /v1/query?q=__any__&explain
        {
            "took": 5,
            "total": 100,
            "max_score": 0.99260014,
            "hits": [
                {
                    "_explanation": {
                        "description": "sum of:",
                        "details": [ ... ],
                        "value": 0.99260014
                    },
                    "_id": "109287657",
                    "_score": 0.99260014,
                    ...
                }
                ...
            ]
        }
        """
        res = self.request("/v1/query?q=__any__&explain").json()
        assert "_explanation" in res["hits"][0]

    def test_15_sort(self):
        """GET /v1/query?q=__all__&sort=taxid
        {
            "max_score": null,
            "took": 21,
            "total": 100,
            "hits": [
                { ... , "taxid": 7091 },
                { ... , "taxid": 7425 },
                { .. ,  "taxid": 7764 },
                ...
            ]
        }
        """
        res = self.request("/v1/query?q=__all__&sort=taxid").json()
        assert res["hits"][0]["taxid"] == 7091
        assert res["hits"][1]["taxid"] == 7425
        assert res["hits"][2]["taxid"] == 7764

    def test_16_sort_desc(self):
        """GET /v1/query?q=__all__&sort=-taxid
        {
            "max_score": null,
            "took": 21,
            "total": 100,
            "hits": [
                { ... , "taxid": 2587831 },
                { ... , "taxid": 1868482 },
                { .. ,  "taxid": 1841481 },
                ...
            ]
        }
        """
        res = self.request("/v1/query?q=__all__&sort=-taxid").json()
        assert res["hits"][0]["taxid"] == 2587831
        assert res["hits"][1]["taxid"] == 1868482
        assert res["hits"][2]["taxid"] == 1841481

    def test_17_sorted_false_dotfield(self):
        """GET /v1/query?q=1017&dotfield&_sorted=false
        {
            "took": 16,
            "total": 1,
            "max_score": 3.0910425,
            "hits": [
                {
                    "_id": "1017",
                    "_score": 3.0910425,
                    "accession.translation.protein": [
                        "BAA32794.1",
                        "NP_001789.2",
                        "AAA35667.1",
                        "BAF84630.1",
                        "NP_001277159.1",
                        "XP_011536034.1",
                        "AAH03065.1",
                        "BAG56780.1",
                        "CAA43807.1",
                        "CAA43985.1",
                        "AAP35467.1",
                        "NP_439892.2"
                    ],  // this field is flattened
                    ...
                },
            ]
        }
        """
        res = self.request("/v1/query?q=1017&dotfield&_sorted=false").json()
        protein = res["hits"][0]["accession.translation.protein"]
        assert protein[0] == "BAA32794.1"

    def test_20_always_list(self):
        """GET /v1/query?q=1017&always_list=symbol
        {
            "took": 23,
            "total": 1,
            "max_score": 3.0910425,
            "hits": [
                {
                    "_id": "1017",
                    "_score": 3.0910425,
                    "symbol": "CDK2" -> ["CDK2"],
                    ...
                }
            ]
        }
        """
        res = self.request("/v1/query?q=1017&always_list=symbol").json()
        hit = res["hits"][0]
        assert "symbol" in hit
        assert hit["symbol"] == ["CDK2"]

    def test_21_always_list_noop(self):
        """GET /v1/query?q=1017&always_list=alias,accession.genomic
        {
            "took": 23,
            "total": 1,
            "max_score": 3.0910425,
            "hits": [
                {
                    "_id": "1017",
                    "_score": 3.0910425,
                    "alias": [
                        "CDKN2",
                        "p33(CDK2)"
                    ],                      // no-op
                    "accession": {
                        "genomic": [
                            "AC025162.48",
                            "AC034102.32",
                            ...
                        ],                  // no-op
                        ...
                    },
                    ...
                }
            ]
        }
        """
        res = self.request("/v1/query?q=1017&always_list=alias,accession.genomic").json()
        hit = res["hits"][0]
        assert isinstance(hit["alias"], list)
        assert isinstance(hit["accession"]["genomic"], list)

    def test_22_always_list_multilist(self):
        """GET /v1/query?q=1017&always_list=exons.position,exons.chr
        {
            "took": 27,
            "total": 1,
            "max_score": 3.0910425,
            "hits": [
                {
                    "_id": "1017",
                    "_score": 3.0910425,
                    "exons": [
                        {
                            ...
                            "chr": ["12"],          // str -> [str]
                            "position": [           // [[]...] no-op
                                [
                                    55966829,
                                    55967124
                                ],
                                ...
                            ],
                            ...
                        },
                        {
                            ...
                            "chr": ["12"],          // str -> [str]
                            "position": [ ... ],    // no-op
                            ...
                        },
                        ...
                    ]
                }
            ]
        }
        """
        res = self.request("/v1/query?q=1017&always_list=exons.position,exons.chr").json()
        exons = res["hits"][0]["exons"]
        for item in exons:
            assert isinstance(item["chr"], list)
            assert isinstance(item["position"], list)

    def test_23_always_list_obj(self):
        """GET /v1/query?q=1017&always_list=genomic_pos_hg19
        {
            "took": 11,
            "total": 1,
            "max_score": 3.0910425,
            "hits": [
                {
                    "_id": "1017",
                    "_score": 3.0910425,
                    "genomic_pos_hg19": [       // obj -> [obj]
                        {
                            "chr": "12",
                            "end": 56366568,
                            "start": 56360553,
                            "strand": 1
                        }
                    ]
                    ...
                }
            ]
        }
        """
        res = self.request("/v1/query?q=1017&always_list=genomic_pos_hg19").json()
        genomic = res["hits"][0]["genomic_pos_hg19"]
        assert isinstance(genomic, list)

    def test_24_allow_null(self):
        """GET /v1/query?q=1017&allow_null=__test__
        {
            "took": 8,
            "total": 1,
            "max_score": 3.0910425,
            "hits": [
                {
                    "__test__": null,
                    "_id": "1017",
                    "_score": 3.0910425,
                    "symbol": "CDK2",
                    ...
                },
                ...
            ]
        }
        """
        res = self.request("/v1/query?q=1017&allow_null=__test__").json()
        hit = res["hits"][0]
        assert hit["__test__"] is None

    def test_25_allow_null_list(self):
        """GET /v1/query?q=1017&allow_null=accession.translation.__test__
        {
            "took": 11,
            "total": 1,
            "max_score": 3.0910425,
            "hits": [
                {
                    "_id": "1017",
                    "_score": 3.0910425,
                    "accession": {
                        "translation": [
                            {
                                "__test__": null,
                                "protein": "BAA32794.1",
                                "rna": "AB012305.1"
                            },
                            {
                                "__test__": null,
                                "protein": "NP_001789.2",
                                "rna": "NM_001798.5"
                            },
                            ...
                        ]
                    }
                }
            ]
        }
        """
        res = self.request("/v1/query?q=1017&allow_null=accession.translation.__test__").json()
        translations = res["hits"][0]["accession"]["translation"]
        for item in translations:
            assert item["__test__"] is None

    def test_26_allow_null_list_always(self):
        """GET /v1/query?q=1017&allow_null=accession.translation.__test__
                                &always_list=accession.translation.__test__
        {
            "took": 9,
            "total": 1,
            "max_score": 3.0910425,
            "hits": [
                {
                    "_id": "1017",
                    "_score": 3.0910425,
                    "accession": {
                        "translation": [
                            {
                                "__test__": [],
                                "protein": "BAA32794.1",
                                "rna": "AB012305.1"
                            },
                            {
                                "__test__": [],
                                "protein": "NP_001789.2",
                                "rna": "NM_001798.5"
                            },
                            ...
                        ],
                        ...
                    }
                }
            ]
        }
        """
        res = self.request(
            "/v1/query?q=1017"
            "&allow_null=accession.translation.__test__"
            "&always_list=accession.translation.__test__"
        ).json()
        translations = res["hits"][0]["accession"]["translation"]
        for item in translations:
            assert item["__test__"] == []

    def test_27_allow_null_dotfield(self):
        """GET /v1/query?q=1017&allow_null=genomic_pos.__test__&dotfield
        {
            ...
            "hits": [
                {
                    "_id": "1017",
                    "_score": 3.0910425,
                    "genomic_pos.__test__": null,
                    "genomic_pos.chr": "12",
                    "genomic_pos.end": 55972789,
                    "genomic_pos.ensemblgene": "ENSG00000123374",
                    "genomic_pos.start": 55966781,
                    "genomic_pos.strand": 1
                    ...
                }
                ...
            ]
        }
        """
        res = self.request("/v1/query?q=1017&allow_null=genomic_pos.__test__&dotfield").json()
        hit = res["hits"][0]
        assert hit["genomic_pos.__test__"] is None

    def test_28_allow_null_dotfield_list(self):
        """GET /v1/query?q=1017&allow_null=accession.translation.__test__&dotfield
        {
            "took": 8,
            "total": 1,
            "max_score": 3.0910425,
            "hits": [
                {
                    "_id": "1017",
                    "_score": 3.0910425,
                    "accession.translation.__test__": [],   // this field is flattened
                    "accession.translation.protein": [ ... ],
                    "accession.translation.rna": [ ... ]
                }
            ]
        }
        """
        res = self.request(
            "/v1/query?q=1017&dotfield" "&allow_null=accession.translation.__test__",
        ).json()
        hit = res["hits"][0]
        assert hit["accession.translation.__test__"] == []

    def test_29_allow_null_dotfield_list_always(self):
        """GET /v1/query?q=1017&allow_null=accession.translation.__test__
                                &always_list=accession.translation.__test__
                                &dotfield
        {
            "took": 8,
            "total": 1,
            "max_score": 3.0910425,
            "hits": [
                {
                    "_id": "1017",
                    "_score": 3.0910425,
                    "accession.translation.__test__": [],   // this field is flattened
                    "accession.translation.protein": [ ... ],
                    "accession.translation.rna": [ ... ]
                }
            ]
        }
        """
        res = self.request(
            "/v1/query?q=1017&dotfield"
            "&allow_null=accession.translation.__test__"
            "&always_list=accession.translation.__test__"
        ).json()
        hit = res["hits"][0]
        assert hit["accession.translation.__test__"] == []

    def test_30_scroll(self):
        """GET /v1/query?q=__all__&fetch_all
        {
            "_scroll_id": ...,
            ...
        }
        """
        res = self.request("/v1/query?q=__all__&fetch_all").json()
        assert len(res["hits"]) == 60
        scroll_id = res["_scroll_id"]

        res = self.request("/v1/query?scroll_id=" + scroll_id).json()
        assert len(res["hits"]) == 40
        scroll_id = res["_scroll_id"]

        res = self.request("/v1/query?scroll_id=" + scroll_id).json()
        assert res["success"] is False

    def test_31_scroll_stale(self):
        """GET /v1/query?scroll_id=<invalid>
        {
            "success": false,
            "status": 400,
            "error": "Invalid or stale scroll_id."
        }
        """
        res = self.request("/v1/query?scroll_id=<invalid>", expect=400).json()
        assert res["success"] is False

    def test_32_filter(self):
        """
        apply filter changes the facet result:
        {
            'facets': {
                'type_of_gene': {
                    '_type': 'terms',
                    'missing': 0,
                    'other': 0,
                    'terms': [{'count': 1, 'term': 'protein-coding'}],
                    'total': 1
                }
            },
            'hits': [...],
            'max_score': 0.4116186,
            'took': 7,
            'total': 1
        }
        """

        res = self.request(
            "/v1/query?q={q}&aggs={aggs}&filter={_filter}".format(
                q="cyclin dependent kinase 2",
                aggs="type_of_gene",
                _filter="taxid:216574",
            )
        ).json()

        assert res["total"] == 1
        term = res["facets"]["type_of_gene"]["terms"][0]
        assert term["count"] == 1
        assert term["term"] == "protein-coding"

    def test_33_post_filter(self):
        """
        apply post_filter won't change the facet result:
        {
            'facets': {
                'type_of_gene': {
                    '_type': 'terms',
                    'missing': 0,
                    'other': 0,
                    'terms': [{'count': 79, 'term': 'protein-coding'}],
                    'total': 79
                }
            },
            'hits': [...],
            'max_score': 0.4116186,
            'took': 7,
            'total': 1
        }
        """

        res = self.request(
            "/v1/query?q={q}&aggs={aggs}&post_filter={post_filter}".format(
                q="cyclin dependent kinase 2",
                aggs="type_of_gene",
                post_filter="taxid:216574",
            )
        ).json()

        assert res["total"] == 1
        term = res["facets"]["type_of_gene"]["terms"][0]
        assert term["count"] == 79
        assert term["term"] == "protein-coding"

    def test_34_jmespath(self):
        """GET /v1/query?q=_id:1017&fields=accession.rna&jmespath=accession.rna|[?contains(@, `NM_`) || contains(@, `XM_`)]
        {
            "hits": [
                {
                    "_id": "1017",
                    "_score": 17.804987,
                    "accession": {
                        "rna": [
                            "NM_001290230.2",
                            "NM_001798.5",
                            "NM_052827.4",
                            "XM_011537732.2",
                            "XM_054370731.1"
                        ]
                    }
                }
            ]
        }
        """
        # get the original len of accession.rna field
        res = self.request("/v1/query?q=_id:1017&fields=accession.rna").json()
        len_0 = len(res["hits"][0]["accession"]["rna"])
        assert len_0 > 0
        # apply jmespath transformation to filter rna list to those contain either NM_ or XM_
        res = self.request(
            "/v1/query?q=symbol:cdk2&species=human&fields=accession.rna&jmespath=accession.rna|[?contains(@, 'NM_') || contains(@, 'XM_')]"
        ).json()
        transformed_rna = res["hits"][0]["accession"]["rna"]
        assert len(transformed_rna) > 0
        assert len(transformed_rna) < len_0
        assert [x for x in transformed_rna if not (x.startswith("NM_") or x.startswith("XM_"))] == []

    def test_35_jmespath_root(self):
        """GET /v1/query?q=_id:1017&fields=exons&jmespath=.|{exon_count: length(exons)}
        {
            "hits": [
                {
                    "exon_count": 3,
                }
            ]
        }
        """
        # set target field to `.` to apply jmespath to the root object
        res = self.request("/v1/query?q=_id:1017&fields=exons&jmespath=.|{exon_count: length(exons)}").json()
        assert res["hits"][0]["exon_count"] == 3

        # empty target field works the same as `.` to apply jmespath to the root object
        res = self.request("/v1/query?q=_id:1017&fields=exons&jmespath=|{exon_count: length(exons)}").json()
        assert res["hits"][0]["exon_count"] == 3

    def test_36_jmespath_post(self):
        """Test jmespath works for POST query as well"""
        res = self.request(
            "/v1/query",
            method="POST",
            json={
                "q": [1017, 406715],
                "scopes": "entrezgene",
                "fields": "pathway.reactome",
                # filter reactome pathways that contain DNA in their name
                "jmespath": "pathway.reactome|[?contains(name, 'DNA')]",
            },
        ).json()
        assert len(res) == 2
        for hit in res:
            assert len(hit["pathway"]["reactome"]) > 0
            assert all([x for x in hit["pathway"]["reactome"] if "DNA" in x["name"]])

        # jmespath should work the same if passed as a query parameter
        res2 = self.request(
            "/v1/query?jmespath=pathway.reactome|[?contains(name, 'DNA')]",
            method="POST",
            json={
                "q": [1017, 406715],
                "scopes": "entrezgene",
                "fields": "pathway.reactome",
            },
        ).json()
        assert res2 == res

    def test_37_jmespath_nested(self):
        res_0 = self.request(
            "/v1/query?q=_id:1017&fields=exons.position"
        ).json()
        a_pos = res_0["hits"][0]["exons"][1]["position"][4][1]   #should be 55969576
        res_1 = self.request(
            f"/v1/query?q=_id:1017&fields=exons.position&jmespath=exons.position|[?[1]==`{a_pos}`]"
        ).json()
        pos_1 = [x["position"] for x in res_1["hits"][0]["exons"]]
        assert pos_1 == [[], [[55969474, 55969576]], []]
        res_2 = self.request(
            f"/v1/query?q=_id:1017&fields=exons.position&jmespath=exons.position|[?[1]==`{a_pos}`]&jmespath_exclude_empty=1"
        ).json()
        pos_2 = [x["position"] for x in res_2["hits"][0]["exons"]]
        assert pos_2 == [[[55969474, 55969576]]]
        res_3 = self.request(
            "/v1/query?q=_id:1017&fields=exons.position&jmespath=exons.position|[?[1]==`0`]&jmespath_exclude_empty=1"
        ).json()
        hits_3 = res_3["hits"]
        assert hits_3 == []

    def test_38_jmespath_exclude_empty(self):
        res = self.request("/v1/query?q=_exists_:accession&fields=accession&jmespath=accession.translation|[?rna=='NM_052827.4']").json()
        assert len(res["hits"]) == 10
        res2 = self.request("/v1/query?q=_exists_:accession&fields=accession&jmespath_exclude_empty=1&jmespath=accession.translation|[?rna=='NM_052827.4']").json()
        assert len(res2["hits"]) == 1

    def test_39_jmespath_invalid(self):
        # invalid jmespath query should return 400
        res = self.request("/v1/query?q=_exists_:accession&fields=accession&jmespath=accession.translation.rna|()", expect=400).json()
        assert res["success"] is False

        # unknown target_field should leave the response untouched
        res_0 = self.request("/v1/query?q=_exists_:accession&fields=accession").json()
        res_1 = self.request("/v1/query?q=_exists_:accession&fields=accession&jmespath=accession.xxx|@").json()
        assert res_0["hits"] == res_1["hits"]

class TestQueryString(BiothingsWebAppTest):
    def test_00_all(self):
        """GET /query?q=__all__
        {
            "max_score": 1,
            "took": ... ,
            "total": 100,
            "hits": [ ... ]
        }
        """
        res = self.query(q="__all__")
        assert res["max_score"] == 1
        assert res["total"] == 100

    def test_01_any(self):
        """GET /query?q=__any__
        {
            "max_score": 0.9865444,
            "took": 8 ,
            "total": 100,
            "hits": [ ... ]
        }
        """
        res1 = self.query(q="__any__")
        res2 = self.query(q="__any__")
        assert res1["hits"][0]["_id"] != res2["hits"][0]["_id"]

    def test_02_none(self):
        """GET /query?q=
        {
            ...
            "total": 0,
            "hits" : [],
        }
        """
        self.query(q="", hits=False)

    def test_03_querystring(self):
        """GET /query?q=taxid:9606
        {
            "max_score": 1,
            "took": 4,
            "total": 1,
            "hits": [
                {
                    "_id": "1017",
                    ...
                    "taxid": 9606,
                    ...
                }
            ]
        }
        """
        res = self.query(q="taxid:9606")
        assert res["hits"][0]["_id"] == "1017"

    def test_10_userquery_query(self):
        """GET /v1/query?q=gene&userquery=prefix
        {
            "took": 18,
            "total": 5,
            "max_score": 1,
            "hits": [ ... ] // 5 items
        }
        """
        res = self.request("/v1/query?q=gene&userquery=prefix").json()
        assert len(res["hits"]) == 5

    def test_11_userquery_query_rawquery(self):
        """GET /v1/query?q=cdk2&userquery=prefix&rawquery
        {
            "query": {
                "bool": {
                    "should": [
                        { "prefix": { "name": "cdk2" } },
                        { "prefix": { "symbol": {
                                        "value": "cdk2",
                                        "boost": 10.0 } } },
                        { "prefix": { "all": "cdk2" } },
                        { "prefix": { "ensembl.gene": "cdk2" } },
                        { "prefix": { "other_names": "cdk2" } }
                    ]   // substituted query.txt
                }
            }
        }
        """
        res = self.request("/v1/query?q=cdk2&userquery=prefix&rawquery").json()
        assert res["query"]["bool"]["should"][0]["prefix"]["name"] == "cdk2"

    def test_12_userquery_filter_rawquery(self):
        """GET /v1/query?q=cdk2&userquery=exrna&rawquery
        {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "type_of_gene": "ncRNA"
                            }
                        }   // content in filter.txt
                    ],
                    "must": [
                        {
                            "query_string": {
                                "query": "cdk2"
                            }
                        }
                    ]
                }
            }
        }
        """
        res = self.request("/v1/query?q=cdk2&userquery=exrna&rawquery").json()
        assert res["query"]["bool"]["filter"][0]["term"]["type_of_gene"] == "ncRNA"

    ### Invalid Values ###  # noqa: E266

    def test_22_invalid(self):
        """Unmatched Quotes"""
        # Sentry
        # Issue 529121368
        # Event 922fc99638cb4987bccbfd30c914ff03
        _q = '/v1/query?q=c("ZNF398", "U2AF...'
        self.request(_q, expect=400)


class TestQueryMatch(BiothingsWebAppTest):
    # nested match
    # https://github.com/biothings/biothings.api/issues/49

    def test_01(self):
        self.query(method="POST", json={"q": "1017"})

    def test_02(self):
        self.query(method="POST", json={"q": 1017})

    def test_03(self):
        self.query(method="POST", json={"q": ["1017"]})

    def test_04(self):
        self.query(method="POST", json={"q": [1017]})

    def test_05(self):
        self.query(method="POST", json={"q": ["1017"], "scopes": []})  # default es *.*

    def test_06(self):
        payload = {
            "q": "cdk2",
            "scopes": ["symbol", "entrezgene"],
            "fields": ["symbol", "name", "taxid", "entrezgene", "ensemblgene"],
        }
        ans = self.query(method="POST", json=payload)
        assert len(ans) == 10

    def test_07(self):
        """
        [
            {
                "query": "cdk2",
                "_id": "100689039",
                "_score": 1.2667861,
                ...
            },
            ...
        ]
        """
        self.request("query", method="POST", json={"q": [1017], "scopes": "*"})

    def test_08(self):
        # related to sentry issue 2230892501
        self.request("query", method="POST", data={"q": "", "scopes": "symbol"}, expect=400)

    def test_09(self):
        self.request("query", method="POST", json={"q": [], "scopes": "taxid"}, expect=400)

    def test_10(self):
        # for biothings client 0.2.3 compatibility
        self.query(method="POST", json={"q": "8167, 8036", "scopes": "taxid"})

    def test_20_nested(self):
        """
        [
            {
                "query": [
                    "cdk2",
                    "9555"
                ],
                "_id": "101025892",
                "_score": 1.0840356,
                "entrezgene": "101025892",
                "name": "cyclin dependent kinase 2",
                "symbol": "CDK2",
                "taxid": 9555
            }
        ]
        """
        payload = {
            "q": [["cdk2", "9555"]],
            "scopes": ["symbol", "taxid"],
            "fields": ["symbol", "name", "taxid", "entrezgene", "ensemblgene"],
        }
        ans = self.query(method="POST", json=payload)
        assert len(ans) == 1

    def test_21_nested(self):
        """
        [
            {
                "query": [
                    "101025892",
                    "9555"
                ],
                "_id": "101025892",
                "_score": 3.8134108,
                "entrezgene": "101025892",
                "name": "cyclin dependent kinase 2",
                "symbol": "CDK2",
                "taxid": 9555
            }
        ]
        """
        payload = {
            "q": [["101025892", "9555"]],
            "scopes": [["symbol", "entrezgene"], "taxid"],
            "fields": ["symbol", "name", "taxid", "entrezgene", "ensemblgene"],
        }
        ans = self.query(method="POST", json=payload)
        assert len(ans) == 1

    def test_22_nested_invalid(self):
        """
        {
            "code": 400,
            "success": false,
            ...
        }
        """
        payload = {
            "q": [["101025892", "9555"]],  # 2 values
            "scopes": [["symbol", "entrezgene"], "taxid", "taxi"],  # 3 values
            "fields": ["symbol", "name", "taxid", "entrezgene", "ensemblgene"],
        }
        self.request("query", method="POST", json=payload, expect=400)

    def test_23_with_total_has_msg(self):
        """
        {
            'hits': [...],
            'max_total': 100,
            'msg': '1 query terms return > 10 hits, using from=10 to retrieve the remaining hits'
        }
        """
        payload = {
            "q": ["cyclin dependent kinase 2"],
            "scopes": ["name"],
            "size": 10,
            "with_total": True,
        }

        res = self.request("query", method="POST", json=payload, expect=200).json()

        assert isinstance(res, dict)
        assert res["max_total"] > 10
        assert "1 query terms return > 10 hits, using from=10 to retrieve the remaining hits" == res["msg"]
        assert len(res["hits"]) == 10

    def test_24_with_total_hasnot_msg(self):
        """
        {
            'hits': [...],
            'max_total': 100
        }
        """
        payload = {
            "q": ["cyclin dependent kinase 2"],
            "scopes": ["name"],
            "size": 1000,
            "with_total": True,
        }

        res = self.request("query", method="POST", json=payload, expect=200).json()

        assert isinstance(res, dict)
        assert "msg" not in res
        assert len(res["hits"]) == res["max_total"]

    def test_25_analyzer(self):
        """
        {
            'hits': [...],
            'max_total': 100
        }
        """
        payload = {
            "q": ["CDKN2 p33(CDK2)"],
            "scopes": ["alias"],
            "analyzer": "whitespace",
            "with_total": True,
            "size": 100,
        }

        res = self.request("query", method="POST", json=payload, expect=200).json()

        assert isinstance(res, dict)
        assert len(res["hits"]) == 1
