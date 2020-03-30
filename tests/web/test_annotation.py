'''
    Test Annotation Endpoint

    GET  /<biothing_type>/<id>
    POST /<biothing_type>

'''

from helper import BiothingsTestCase
from setup import setup_es  # pylint: disable=unused-import


class TestAnnotationGET(BiothingsTestCase):

    ### Query Builder Keywords ###

    def test_00_hit(self):
        """ GET /v1/gene/101952537
        {
            "HGNC": "1771",
            "MIM": "116953",
            "_id": "1017",
            ...
        }
        """
        res = self.request('/v1/gene/1017').json()
        assert res['HGNC'] == '1771'

    def test_01_miss(self):
        """ GET /v1/gene/0
        {
            "success": false,
            "error": "ID '0' not found"
        }
        """
        res = self.request('/v1/gene/0', expect_status=404).json()
        assert 'not found' in res['error']

    ### Query Backend Keywords ###

    def test_10_fields(self):
        """ GET /v1/gene/101952537?fields=symbol
        {
            "_id": "1017",
            "symbol": "CDK2"
        }
        """
        res = self.request('/v1/gene/1017?fields=symbol').json()
        assert len(res) == 2
        assert '_id' in res
        assert 'symbol' in res

    ### Workflow Control Keywords ###

    def test_20_raw(self):
        """ GET /v1/gene/1017?raw
        {
            "took": 8,
            "timed_out": false,
            "_shards": { ... },
            "hits": {
                "total": 1,
                "max_score": 1.0,
                "hits": [
                    {
                        "_index": ... ,
                        "_type": "gene",
                        "_id": "1017",
                        "_score": 1.0,
                        "_source": { ... }
                    }
                ]
            }
        }
        """
        res = self.request('/v1/gene/1017?raw').json()
        assert '_shards' in res
        assert res['hits']['hits'][0]['_id'] == "1017"

    def test_21_rawquery(self):
        """ GET /v1/gene/1017?rawquery
        {
            "query": {
                "multi_match": {
                    "query": "1017",
                    "fields": [
                        "_id"
                    ],
                    "operator": "and"
                }
            }
        }
        """
        res = self.request('/v1/gene/1017?rawquery').json()
        assert res['query']['multi_match']['query'] == "1017"

    def test_22_format_yaml(self):
        """ GET /v1/gene/102812112?format=yaml
        _id: '102812112'
        accession:
            genomic:
            - AMDV01086628.1
            - NW_006408551.1
        ...
        """
        res = self.request('/v1/gene/102812112?format=yaml').text
        assert res.startswith("_id: '102812112'")

    def test_23_format_html(self):
        """ GET /v1/gene/102812112?format=html
        _id: '101952537'
        accession:
            genomic:
            - AMDV01086628.1
            - NW_006408551.1
        ...
        """
        res = self.request('/v1/gene/102812112?format=html').text
        assert res.strip().startswith('<html>')
        assert 'AMDV01086628.1' in res

    ### Result Transform Keywords ###

    def test_30_dotfield(self):
        """ GET /v1/gene/102812112?dotfield
        {
            "_id": "102812112",
            "accession.genomic": [
                "AMDV01086628.1",
                "NW_006408551.1"
            ],
            "accession.protein": [
                "XP_006859466.1",
                "XP_006859467.1"
            ],
            ...
        }
        """
        res = self.request('/v1/gene/102812112?dotfield').json()
        assert "accession.genomic" in res
        assert "accession.protein" in res

    def test_31_sorted(self):
        """ GET /v1/gene/102812112?_sorted=false
        {
            "_id": "102812112",
            "taxid": 185453,
            "symbol": "CDK2",
            "name": "cyclin dependent kinase 2",
            "type_of_gene": "protein-coding",
            "other_names": "cyclin-dependent kinase 2",
            "entrezgene": "102812112",
            ...
        }
        """
        keys = iter(self.request('/v1/gene/102812112?_sorted=false').json().keys())
        assert next(keys) == '_id'
        assert next(keys) == 'taxid'
        assert next(keys) == 'symbol'

    def test_32_always_list(self):
        """ GET /v1/gene/102812112?always_list=symbol,taxid
        {
            "_id": "102812112",
            "taxid": [
                185453
            ],
            "symbol": [
                "CDK2"
            ],
            ...
        }
        """
        res = self.request('/v1/gene/102812112?always_list=symbol,taxid').json()
        assert isinstance(res['symbol'], list)
        assert isinstance(res['taxid'], list)

    def test_33_allow_null(self):
        """ GET /v1/gene/102812112?allow_null=_uid,_index
        {
            "_id": "102812112",
            "_index": null,
            "_uid": null,
            "accession": { ... },
            "entrezgene": "102812112",
            "name": "cyclin dependent kinase 2",
            "other_names": "cyclin-dependent kinase 2",
            ...
        }
        """
        res = self.request('/v1/gene/102812112?allow_null=_uid,_index').json()
        assert res['_uid'] is None
        assert res['_index'] is None


class TestAnnotationPOST(BiothingsTestCase):

    def request(self, *args, **kwargs):
        method = kwargs.pop('method', 'POST')
        return super().request(method=method, *args, **kwargs)

    def test_00_id_not_provided(self):
        """ POST /v1/gene
        {
            "success": false,
            "error": "Bad Request",
            "missing": "ids",
            "received": {}
        }
        """
        res = self.request('/v1/gene', expect_status=400).json()
        assert 'error' in res

    def test_01_id_miss(self):
        """ POST /v1/gene
        {
            "ids": ["11"]
        }
        [
            {
                "query": "11",
                "notfound": true
            }
        ]
        """
        res = self.request('/v1/gene', json={"ids": ["11"]}).json()
        assert res[0]['query'] == '11'
        assert res[0]['notfound']

    def test_02_id_hit(self):
        """ POST /v1/gene
        {
            "ids": ["1017"]
        }
        [
            {
                "query": "1017",
                "HGNC": "1771",
                "MIM": "116953",
                "_id": "1017",
                ...
                "taxid": 9606,
                ...
            }
        ]
        """
        res = self.request('/v1/gene', json={"ids": ["1017"]}).json()
        assert res[0]['query'] == '1017'
        assert res[0]['taxid'] == 9606

    def test_03_ids(self):
        """ POST /v1/gene
        {
            "ids": ["1017", "11"]
        }
        [
            {
                "query": "1017",
                "HGNC": "1771",
                "MIM": "116953",
                "_id": "1017",
                ...
                "taxid": 9606,
                ...
            },
            {
                "query": "11",
                "notfound": true
            }
        ]
        """
        res = self.request('/v1/gene', json={"ids": ["1017", "11"]}).json()
        assert res[0]['query'] == '1017'
        assert res[0]['taxid'] == 9606
        assert res[1]['query'] == '11'
        assert res[1]['notfound']

    def test_10_form_encoded(self):
        """ POST /v1/gene
        ids=1017%2C11
        [
            {
                "query": "1017",
                "HGNC": "1771",
                "MIM": "116953",
                "_id": "1017",
                ...
                "taxid": 9606,
                ...
            },
            {
                "query": "11",
                "notfound": true
            }
        ]
        """
        res = self.request('/v1/gene', data={"ids": "1017,11"}).json()
        assert res[0]['query'] == '1017'
        assert res[0]['taxid'] == 9606
        assert res[1]['query'] == '11'
        assert res[1]['notfound']

    def test_11_json_type_str(self):
        """ POST /v1/gene
        {
            "ids": "1017" # instead of a list
        }
        [
            {
                "query": "1017",
                "HGNC": "1771",
                "MIM": "116953",
                "_id": "1017",
                ...
            }
        ]
        """
        res = self.request('/v1/gene', json={"ids": "1017"}).json()
        assert res[0]['query'] == '1017'
        assert res[0]['taxid'] == 9606

    def test_12_json_type_int(self):
        """ POST /v1/gene
        {
            "ids": 2    # number instead of string
        }
        [
            {
                "query": "2",
                "notfound": true
            }
        ]
        """
        res = self.request('/v1/gene', json={"ids": 2}).json()
        assert res[0]['query'] == '2'
        assert res[0]['notfound']

    def test_13_json_invalid(self):
        """ POST /v1/gene
        {
            "ids": { "ISCW006791" } # malformat
        }
        {
            "success": false,
            "error": "Invalid JSON body."
        }
        """
        res = self.request(
            path='/v1/gene', data='{"ids":{"ISCW006791"}}'.encode(),
            headers={"Content-Type": "application/json"}, expect_status=400).json()
        assert not res['success']
        assert res['error'] == "Invalid JSON body."
