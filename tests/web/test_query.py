'''
    Biothings Query Component Common Tests
'''

import elasticsearch
import pytest

from helper import BiothingsTestCase

TEST_INDEX = 'bts_test'
TEST_DOC_TYPE = 'doc'

@pytest.fixture(scope="module")
def setup_es():
    """
    Populate a document in ES for testing.
    """
    kwargs = {
        "index": TEST_INDEX,
        "doc_type": TEST_DOC_TYPE,
        "body": {
            "1101": "test-299",
            "1102": "test-300"
        },
        "id": 1
    }
    if elasticsearch.__version__[0] > 6:
        kwargs.pop('doc_type')

    client = elasticsearch.Elasticsearch()
    client.index(**kwargs)
    client.indices.refresh()
    yield
    client.indices.delete(index=TEST_INDEX)

@pytest.mark.usefixtures("setup_es")
class TestHandlers(BiothingsTestCase):

    @classmethod
    def setup_class(cls):
        cls.settings.ES_INDEX = TEST_INDEX

    ### Base Handler ###

    def test_31(self):

        res = self.request('query', json=dict(q=299)).json()
        assert res['hits'][0]['_id'] == '1'

    ### Metadata Handler ###

    def test_41(self):

        res = self.request('metadata/fields').json()
        assert "1101" in res
        assert "1102.fields.keyword" in res

    ### Status Handler ###

    def test_51(self):
        pass


@pytest.mark.usefixtures("setup_es")
class TestQuery(BiothingsTestCase):

    @classmethod
    def setup_class(cls):
        cls.settings.ES_INDEX = TEST_INDEX

    ### Control Keywords ###

    def test_01_format_default(self):

        self.query(q='__all__', size='1')

    def test_02_format_yaml(self):

        res = self.request('query?q=__all__&size=1&format=yaml').text
        assert res.startswith('max_score:')

    def test_03_format_html(self):

        res = self.request('query?q=__all__&size=1&format=html').text
        assert '<html>' in res

    ### Annotation GET ###

    def test_11(self):

        res = self.request('doc/1')
        assert res.json()['_id'] == '1'

    ### Query GET ###

    def test_21(self):

        self.query(q=299)

    def test_22(self):
        ''' Unmatched Quotes'''
        # Sentry
        # Issue 529121368
        # Event 922fc99638cb4987bccbfd30c914ff03
        _q = 'query?q=c("ZNF398", "U2AF...'
        self.request(_q, expect_status=400)
