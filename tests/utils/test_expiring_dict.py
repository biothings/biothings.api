import pytest
import time
from biothings.utils.web.analytics import ExpiringDict


class TestExpiringDict:
    def test_init(self):
        d = ExpiringDict()

    def test_set_get(self):
        d = ExpiringDict()
        d['k'] = 'v'
        assert d['k'] == 'v'

    def test_contains_item(self):
        d = ExpiringDict()
        d['k'] = 'v'
        assert 'k' in d

    def test_not_contain(self):
        d = ExpiringDict()
        assert 'k' not in d

    def test_expired_item(self):
        d = ExpiringDict(ttl=0)
        d['k'] = 'v'
        with pytest.raises(KeyError):
            assert d['k'] == 'v'

    def test_item_expire_not_contain(self):
        d = ExpiringDict(ttl=0)
        d['k'] = 'v'
        assert 'k' not in d

    def test_get_refresh_item(self):
        d = ExpiringDict(ttl=2)
        d['k1'] = 'v1'
        d['k2'] = 'v2'
        time.sleep(1)
        t = d['k2']
        time.sleep(1)
        assert 'k1' not in d
        assert 'k2' in d


    def test_auto_clean_old_item(self):
        d = ExpiringDict(ttl=1)
        d['k1'] = 'v'
        d['k2'] = 'v'
        time.sleep(1)
        assert 'k2' not in d
        assert 'k1' not in d.od  # autocleaned

    def test_max_size(self):
        d = ExpiringDict(max_size=1)
        d['k1'] = 'v1'
        d['k2'] = 'v2'
        assert len(d.od) <= 1

    def test_zero_size(self):
        d = ExpiringDict(max_size=0)
        d['k1'] = 'v1'
        d['k2'] = 'v2'
        assert len(d) == 0

    def test_max_size_same_key(self):
        d = ExpiringDict(max_size=1)
        d['k1'] = 'v1'
        d['k1'] = 'v2'
        assert len(d.od) <= 1
        assert d['k1'] == 'v2'
