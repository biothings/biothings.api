"""
Tests for the optional login to the Hub API (biothings.hub.api.handlers.auth): with users defined
in HUB_API_USERS, requests running commands or changing data require a login, requests reading
data stay open. Without users, the API stays open.
"""

import asyncio
import json
import os
import shutil
import tempfile
import time
from types import SimpleNamespace
from unittest import mock

import pytest
from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application, HTTPError

from biothings.hub.api import EndpointDefinition, generate_api_routes
from biothings.hub.api.handlers import auth
from biothings.hub.api.handlers.base import LoginHandler, RootHandler
from biothings.hub.api.handlers.log import HubLogDirHandler
from biothings.hub.api.handlers.shell import ShellHandler
from biothings.hub.api.handlers.upload import UploadHandler
from biothings.utils.configuration import ConfigurationError
from biothings.utils.passwords import hash_password

USERS = {
    "alice": hash_password("alice-password", iterations=1000),
    "bob": hash_password("bob-password", iterations=1000),
}


def hub_config(users=USERS):
    return SimpleNamespace(HUB_API_USERS=users, HUB_API_LOGIN_EXPIRY=3600)


@pytest.fixture(autouse=True)
def no_failed_logins():
    auth._failed_logins.clear()
    yield
    auth._failed_logins.clear()


def login(username, password):
    with mock.patch.object(auth, "config", hub_config()):
        return asyncio.run(auth.login(username, password))


def login_error(username, password):
    with pytest.raises(HTTPError) as error:
        login(username, password)
    return error.value.status_code


# --- tokens ---------------------------------------------------------------------------------


def test_token():
    token = auth.create_token("alice", USERS["alice"], int(time.time()) + 60)
    assert auth.get_token_user(token, USERS) == "alice"


def test_token_is_a_jwt():
    header, claims, _ = auth.create_token("alice", USERS["alice"], 2000000000).split(".")
    assert json.loads(auth._b64decode(header)) == {"alg": "HS256", "typ": "JWT"}
    assert json.loads(auth._b64decode(claims))["sub"] == "alice"
    assert json.loads(auth._b64decode(claims))["exp"] == 2000000000


def test_expired_token():
    token = auth.create_token("alice", USERS["alice"], int(time.time()) - 1)
    assert auth.get_token_user(token, USERS) is None


def test_token_invalid_after_password_change_or_user_removal():
    token = auth.create_token("alice", USERS["alice"], int(time.time()) + 60)
    assert auth.get_token_user(token, {**USERS, "alice": hash_password("new-password", iterations=1000)}) is None
    assert auth.get_token_user(token, {"bob": USERS["bob"]}) is None


def test_token_changed_to_another_user():
    header, claims, signature = auth.create_token("alice", USERS["alice"], int(time.time()) + 60).split(".")
    payload = json.loads(auth._b64decode(claims))
    payload["sub"] = "bob"
    forged = ".".join([header, auth._b64encode(json.dumps(payload).encode()), signature])
    assert auth.get_token_user(forged, USERS) is None


def signed_token(claims):
    signed = "eyJhbGciOiAiSFMyNTYifQ." + auth._b64encode(json.dumps(claims).encode())
    return signed + "." + auth._b64encode(auth._signature(signed, USERS["alice"]))


@pytest.mark.parametrize(
    "token",
    [
        None,
        "",
        "token",
        "a.b.c",
        "a.b.c.d",
        "é.é.é",
        signed_token(["alice"]),
        signed_token({"sub": ["alice"], "exp": 2000000000}),
        signed_token({"sub": "alice"}),
        signed_token({"sub": "alice", "exp": "tomorrow"}),
    ],
)
def test_invalid_tokens(token):
    assert auth.get_token_user(token, USERS) is None


# --- users and logins -----------------------------------------------------------------------


def test_check_users():
    with mock.patch.object(auth, "config", hub_config()):
        assert auth.check_users() == ["alice", "bob"]
    with mock.patch.object(auth, "config", hub_config(users={})):
        assert auth.check_users() == []


@pytest.mark.parametrize("users", [{"alice": "alice-password"}, {"alice": "9RKfd8gDuNf0Q"}, ["alice"]])
def test_check_users_with_passwords_not_hashed(users):
    with mock.patch.object(auth, "config", hub_config(users=users)):
        with pytest.raises(ConfigurationError):
            auth.check_users()


def test_login():
    result = login("alice", "alice-password")
    assert result["username"] == "alice"
    assert auth.get_token_user(result["token"], USERS) == "alice"
    assert 3590 < result["expires"].timestamp() - time.time() <= 3600


def test_login_with_wrong_password_or_unknown_user():
    assert login_error("alice", "bob-password") == 401
    assert login_error("mallory", "alice-password") == 401
    # only the users who exist are tracked
    assert list(auth._failed_logins) == ["alice"]


def test_failed_logins_slow_down_password_guessing():
    for _ in range(auth.MAX_FAILED_LOGINS):
        assert login_error("alice", "wrong") == 401
    # even with the right password, until the delay has passed
    assert login_error("alice", "alice-password") == 429
    assert login("bob", "bob-password")["username"] == "bob"
    with mock.patch.object(auth, "LOGIN_RETRY_DELAY", 0):
        assert login("alice", "alice-password")["username"] == "alice"
    # starting over after a successful login
    assert login_error("alice", "wrong") == 401


# --- Hub API --------------------------------------------------------------------------------


class FakeShell:
    """The parts of the hub's shell used by /shell and the command endpoints"""

    def __init__(self):
        self.commands = {"status": self.status, "dump": self.dump}
        self.extra_ns = {}
        self.ran = []

    def status(self):
        return "running"

    def dump(self, src):
        self.ran.append("dump %s" % src)
        return "dumping %s" % src

    def eval(self, cmd, secure=False):
        self.ran.append(cmd)
        return [""]

    def register_command(self, cmd, result):
        return result


class HubApiTestCase(AsyncHTTPTestCase):
    users = USERS

    def get_app(self):
        self.shell = FakeShell()
        self.folder = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.folder)
        endpoints = {
            "status": EndpointDefinition(name="status", method="get"),
            "source": [EndpointDefinition(name="dump", method="put", suffix="dump")],
        }
        return Application(
            generate_api_routes(self.shell, endpoints)
            + [
                ("/shell", ShellHandler, {"shell": self.shell, "shellog": mock.Mock()}),
                (r"/dataupload/([\w\.-]+)?", UploadHandler, {"upload_root": self.folder}),
                ("/logs/(.*)", HubLogDirHandler, {"path": self.folder}),
                ("/login", LoginHandler),
                ("/readonly", RootHandler, {"features": ["dump", "readonly"]}),
                ("/", RootHandler, {"features": ["dump", "terminal"]}),
            ]
        )

    def setUp(self):
        super().setUp()
        patcher = mock.patch.object(auth, "config", hub_config(users=self.users))
        patcher.start()
        self.addCleanup(patcher.stop)

    def request(self, path, method="GET", body=None, token=None, headers=None):
        headers = dict(headers or {})
        if token:
            headers[auth.TOKEN_HEADER] = token
        return self.fetch(path, method=method, body=body, headers=headers)

    def login(self, username="alice", password="alice-password"):
        body = json.dumps({"username": username, "password": password})
        return self.request("/login", method="POST", body=body)

    def run_shell(self, token=None):
        return self.request("/shell", method="PUT", body=json.dumps({"cmd": "dump('mygene')"}), token=token)

    def upload(self, token=None):
        body = (
            b"--BOUNDARY\r\n"
            b'Content-Disposition: form-data; name="file"; filename="data.txt"\r\n'
            b"Content-Type: application/octet-stream\r\n\r\n"
            b"some data\r\n--BOUNDARY--\r\n"
        )
        headers = {"Content-Type": "multipart/form-data; boundary=BOUNDARY"}
        return self.request("/dataupload/mysource", method="POST", body=body, token=token, headers=headers)

    def result(self, response):
        return json.loads(response.body)["result"]

    def error(self, response):
        return json.loads(response.body)["error"]


class TestHubApiWithUsers(HubApiTestCase):
    def token(self):
        return self.result(self.login())["token"]

    def test_shell_requires_login(self):
        response = self.run_shell()
        assert response.code == 401
        assert self.error(response) == "HTTP 401: Login required"
        assert self.shell.ran == []

    def test_shell_with_login(self):
        assert self.run_shell(token=self.token()).code == 200
        assert self.shell.ran == ["dump('mygene')"]

    def test_shell_with_invalid_token(self):
        response = self.run_shell(token="not-a-token")
        assert response.code == 401
        assert "log in again" in self.error(response)
        assert self.shell.ran == []

    def test_commands_require_login(self):
        assert self.request("/source/mygene/dump", method="PUT", body="{}").code == 401
        assert self.shell.ran == []
        response = self.request("/source/mygene/dump", method="PUT", body="{}", token=self.token())
        assert response.code == 200
        assert self.result(response) == "dumping mygene"

    def test_upload_requires_login(self):
        assert self.upload().code == 401
        assert not os.path.exists(os.path.join(self.folder, "mysource"))
        assert self.upload(token=self.token()).code == 200
        assert os.path.exists(os.path.join(self.folder, "mysource", "data.txt"))

    def test_reading_stays_open(self):
        assert self.result(self.request("/status")) == "running"
        assert self.request("/logs/").code == 200
        assert self.request("/shell", method="OPTIONS").code == 200  # CORS pre-flight

    def test_root_tells_login_is_required(self):
        assert self.result(self.request("/"))["login_required"] is True
        assert self.result(self.request("/readonly"))["login_required"] is False

    def test_login(self):
        response = self.login()
        assert response.code == 200
        assert self.result(response)["username"] == "alice"
        assert self.login(password="wrong").code == 401
        assert self.login(password="").code == 401
        assert self.login(username="mallory").code == 401

    def test_login_with_invalid_body(self):
        for body in ["", "not json", "[]", '{"username": "alice"}', '{"username": "alice", "password": 1}']:
            assert self.request("/login", method="POST", body=body).code == 400


class TestHubApiWithoutUsers(HubApiTestCase):
    users = {}

    def test_everything_open(self):
        assert self.run_shell().code == 200
        assert self.request("/source/mygene/dump", method="PUT", body="{}").code == 200
        assert self.upload().code == 200
        assert self.shell.ran == ["dump('mygene')", "dump mygene"]

    def test_root_tells_no_login_required(self):
        assert self.result(self.request("/"))["login_required"] is False

    def test_no_login(self):
        assert self.login().code == 404
