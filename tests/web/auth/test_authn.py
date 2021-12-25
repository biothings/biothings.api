import pytest
import random


from biothings.tests.web import BiothingsWebAppTest


class TestAuthn(BiothingsWebAppTest):
    def test_401(self):
        resp = self.request('/user1', expect=401)
        assert resp.headers['WWW-Authenticate'] == 'Bearer realm=dummy_bearer'

    def test_403(self):
        # the second endpoint only has 403 because it can't have www-authenticate
        self.request('/user2', expect=403)

    def test_user_cookie(self):
        uid = random.randint(0, 2 << 10)
        resp = self.request('/user1', cookies={'USER_ID': str(uid)})
        assert resp.json() == {'user_id': uid}

    def test_user_header(self):
        uid = random.randint(0, 2 << 10)
        resp = self.request(
            '/user1',
            headers={'Authorization': f'Bearer BioThingsUser{uid}'}
        )
        assert resp.json() == {'user_id': uid}

    def test_user2_header_not_work(self):
        uid = random.randint(0, 2 << 10)
        self.request(
            '/user2',
            expect=403,
            headers={'Authorization': f'Bearer BioThingsUser{uid}'}
        )

    def test_user2_cookie(self):
        uid = random.randint(0, 2 << 10)
        resp = self.request(
            '/user2',
            cookies={'USR': f'{uid}'}
        )
        assert resp.json()['user_id'] == uid
