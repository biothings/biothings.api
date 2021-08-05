'''
    Test Metadata Endpoint

    GET /v1/metadata
    GET /v1/metadata/fields

'''

from time import sleep

from biothings.tests.web import BiothingsWebAppTest

from setup import setup_es  # pylint: disable=unused-import


class TestMetadata(BiothingsWebAppTest):

    def test_00_meta(self):
        """ GET /v1/metadata
        {
            "biothing_type": "gene",
            "build_date": "2020-01-19T02:00:00.027534",
            "build_version": "20200119",
            "src": { ... },
            "stats": { ... }
        }
        """
        res = self.request('/v1/metadata').json()
        assert res['biothing_type'] == 'gene'

    def test_01_meta_dev(self):
        """ GET /v1/metadata?dev
        {
            "biothing_type": "gene",
            "build_date": "2020-01-19T02:00:00.027534",
            "build_version": "20200119",
            "software": {
                "biothings": {
                    "commit-hash": "705a19d62c62529826fc1316e4956acede9d3673",
                    "master-commits": "1894",
                    "repository-url": "https://github.com/biothings/biothings.api.git",
                    "version": "0.6.dev"
                },
                "codebase": { ... },
                "python-info": { ... },
                "python-package-info": [ ... ]
            },
            ...
        }
        """
        res = self.request('/v1/metadata?dev').json()
        assert 'software' in res

    def test_10_field(self):
        """ GET /v1/metadata/fields
        {
            ...
            "refseq": { ... }
            "refseq.genomic": {
                "type": "text",
                "index": false
            },
            ...
        }
        """
        res = self.request('/v1/metadata/fields').json()
        assert not res['refseq.genomic']['index']

    def test_11_field_search(self):
        """ GET /v1/metadata/fields?search=HGNC
        {
            "HGNC": { ... },
            "pantherdb.HGNC": { ... },
            "pantherdb.ortholog.HGNC": { ... }
        }
        """
        res = self.request('/v1/metadata/fields?search=HGNC').json()
        assert res
        for key in res:
            assert 'HGNC' in key

    def test_12_field_prefix(self):
        """ GET /v1/metadata/fields?prefix=accession
        {
            "accession": { ... },
            "accession.genomic": { ... },
            "accession.protein": { ... },
            "accession.rna": { ... },
            "accession.translation": { ... },
            "accession_agg": { ... }
        }
        """
        res = self.request('/v1/metadata/fields?prefix=accession').json()
        assert res
        for key in res:
            assert key.startswith('accession')

class TestMetadataLicense(BiothingsWebAppTest):

    def test_20_license(self):
        """ GET /v1/gene/12566?fields=pantherdb.uniprot_kb
        {
            "_id": "12566",
            "_version": 1,
            "pantherdb": {
                "_license": "http://pantherdb.org/tou.jsp",
                "uniprot_kb": "P97377"
                ...
            }
            ...
        }
        """
        sleep(3)  # wait for metadata service internal refresh
        res = self.request('/v1/gene/12566?fields=pantherdb.uniprot_kb').json()
        assert res['pantherdb']['_license'] == "http://pantherdb.org/tou.jsp"

    def test_21_license_transform(self):
        """ GET /v1/gene/12566?fields=interpro
        {
            "_id": "12566",
            "_version": 1,
            "interpro": [
                {
                    "_license": "http://pantherdb.org/tou.jsp",
                    "desc": "Protein kinase domain",
                    "id": "IPR000719",
                    "short_desc": "Prot_kinase_dom"
                },
                ...
            ]
        }
        """
        sleep(3)  # wait for metadata service internal refresh
        res = self.request('/v1/gene/12566?fields=interpro').json()
        for dic in res['interpro']:
            assert dic['_license'] == "http://pantherdb.org/tou.jsp"

    def test_22_license_transform(self):
        """ GET /v1/gene/12566?fields=pantherdb.ortholog
        {
            "_id": "12566",
            "_version": 1,
            "pantherdb": {
                "_license": "http://pantherdb.org/tou.jsp",
                "ortholog": [
                    {
                        "RGD": "70486",
                        "_license": "http://pantherdb.org/tou.jsp",
                        ...
                    },
                    ...
                ],
            }
        """
        sleep(3)  # wait for metadata service internal refresh
        res = self.request('/v1/gene/12566?fields=pantherdb.ortholog').json()
        for dic in res['pantherdb']['ortholog']:
            assert dic['_license'] == "http://pantherdb.org/tou.jsp"


class TestStatus(BiothingsWebAppTest):

    def test_01_get(self):
        """
        GET /status
        {
            "success": true,
            "status": "yellow"
        }
        GET /status?dev
        {
            ...
            "status": "yellow",
            "payload": {
                "id": "1017",
                "index": "bts_test",
                "doc_type": "_all"
            },
            "document": {
                "_index": "bts_test",
                "_type": "gene",
                "_id": "1017",
                "_version": 1,
                "found": true,
                "_source": { ... }
            }
        }
        """
        res = self.request('/status').json()
        assert res['success']
        res = self.request('/status?dev').json()
        assert res['document']['found']

    def test_02_head(self):

        self.request('/status', method='HEAD')
