"""
Tests for the logins to the hub's SSH console (HubSSHServer.validate_password), with the users
and password hashes of HUB_PASSWD
"""

import asyncio
import warnings
from unittest import mock

import pytest

from biothings.hub import HubSSHServer, default_config
from biothings.hub.api.handlers import auth
from biothings.utils.passwords import hash_password

with warnings.catch_warnings():
    warnings.simplefilter("ignore", DeprecationWarning)
    try:
        import crypt
    except ImportError:  # removed in Python 3.13
        crypt = None

PASSWORDS = {"alice": hash_password("alice-password", iterations=1000)}


@pytest.fixture(autouse=True)
def no_failed_logins():
    auth._failed_logins.clear()
    yield
    auth._failed_logins.clear()


def ssh_login(username, password, passwords=PASSWORDS):
    with mock.patch.object(HubSSHServer, "PASSWORDS", passwords):
        return asyncio.run(HubSSHServer().validate_password(username, password))


def test_password_hash():
    assert ssh_login("alice", "alice-password")
    assert not ssh_login("alice", "wrong")
    assert not ssh_login("bob", "alice-password")


def test_failed_logins_slow_down_password_guessing():
    for _ in range(auth.MAX_FAILED_LOGINS):
        assert not ssh_login("alice", "wrong")
    assert not ssh_login("alice", "alice-password")


@pytest.mark.skipif(crypt is None, reason="Python 3.13 removed the crypt module")
def test_default_guest_user_without_password():
    assert ssh_login("guest", "", passwords=default_config.HUB_PASSWD)
    assert not ssh_login("guest", "guest", passwords=default_config.HUB_PASSWD)


@pytest.mark.skipif(crypt is None, reason="Python 3.13 removed the crypt module")
def test_crypt_hash():
    passwords = {**PASSWORDS, "bob": crypt.crypt("bob-password", "ab")}
    assert ssh_login("bob", "bob-password", passwords=passwords)
    assert not ssh_login("bob", "wrong", passwords=passwords)
