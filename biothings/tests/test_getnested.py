import pytest
from biothings.tests.web import BiothingsWebAppTest


class TestBiothingsWebAppTestGetNested:
    def test_non_list(self):
        d = {
            'k1': {
                'k2': 1
            }
        }
        assert BiothingsWebAppTest.get_all_nested(d, 'k1.k2') == [1]

    def test_with_list(self):
        d = {
            'k1': [
                {'k2': 1},
                {'k2': 2},
            ]
        }
        assert 1 in BiothingsWebAppTest.get_all_nested(d, 'k1.k2')

    def test_list_l2(self):
        d = {'k1': {'k2': [1, 2, 3]}}
        assert BiothingsWebAppTest.get_all_nested(d, 'k1.k2') == [1, 2, 3]

    def test_with_none(self):
        d = {'k1': {'k2': None}}
        assert BiothingsWebAppTest.get_all_nested(d, 'k1.k2') == [None]

    def test_raises_1(self):
        d = {}
        with pytest.raises(KeyError):
            BiothingsWebAppTest.get_all_nested(d, 'k1.k2')
