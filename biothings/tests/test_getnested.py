from biothings.tests.web import BiothingsDataTest


class TestBiothingsWebAppTestGetNested(BiothingsDataTest):
    def test_non_list(self):
        d = {
            'k1': {
                'k2': 1
            }
        }
        assert self.value_in_result(1, d, 'k1.k2')

    def test_with_list(self):
        d = {
            'k1': [
                {'k2': 1},
                {'k2': 2},
            ]
        }
        assert self.value_in_result(1, d, 'k1.k2')
        assert self.value_in_result(2, d, 'k1.k2')

    def test_list_l2(self):
        d = {'k1': {'k2': [1, 2, 3]}}
        assert self.value_in_result(1, d, 'k1.k2')
        assert self.value_in_result(2, d, 'k1.k2')
        assert self.value_in_result(3, d, 'k1.k2')

    def test_list_input(self):
        d = [
            {'k1': {'k2': 1}},
            {'k1': {'k2': 2}},
        ]
        assert self.value_in_result(1, d, 'k1.k2')
        assert self.value_in_result(2, d, 'k1.k2')

    def test_with_none(self):
        d = {'k1': {'k2': None}}
        assert self.value_in_result(None, d, 'k1.k2')

    def test_no_result(self):
        d = {}
        assert not self.value_in_result(None, d, 'k1.k2')
