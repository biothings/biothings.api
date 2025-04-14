"""
Fixture and configuration setup for testing the
web authentication
"""

from typing import Optional
import sys

from tornado.web import HTTPError
import pytest


from biothings.utils.common import DummyConfig
from biothings.web.auth.authn import BioThingsAuthnMixin, BioThingsAuthenticationProviderInterface
from biothings.web.handlers.base import BaseAPIHandler


class DummyCookieAuthProvider(BioThingsAuthenticationProviderInterface):
    WWW_AUTHENTICATE_HEADER = None

    def __init__(self, handler, cookie_name="USER_ID"):
        super().__init__(handler)
        self.cookie_name = cookie_name

    def get_current_user(self) -> Optional[dict]:
        uid = self.handler.get_cookie(self.cookie_name, None)
        if uid:
            return {"user_id": int(uid)}
        return None


class DummyBearerAuthProvider(BioThingsAuthenticationProviderInterface):
    WWW_AUTHENTICATE_HEADER = "Bearer realm=dummy_bearer"

    def get_current_user(self) -> Optional[dict]:
        token: str = self.handler.request.headers.get("Authorization", None)
        if token is None:
            return None
        parts = token.split()
        if len(parts) != 2 or parts[0] != "Bearer":
            return None
        if parts[1].startswith("BioThingsUser"):
            uid = int(parts[1][13:])
            return {"user_id": uid}
        return None


class BaseUserIdHandler(BioThingsAuthnMixin, BaseAPIHandler):
    def get(self):
        if self.current_user:
            self.write(self.current_user)
        else:
            # return 401 or 403
            header = self.get_www_authenticate_header()
            if header:
                self.clear()
                self.set_header("WWW-Authenticate", header)
                self.set_status(401, "Unauthorized")
                self.finish()
            else:
                raise HTTPError(403)


class SpecialCookieUserIdHandler(BaseUserIdHandler):
    AUTHN_PROVIDERS = [(DummyCookieAuthProvider, {"cookie_name": "USR"})]


@pytest.fixture(scope="session", autouse=True)
def handler_configuration():
    config_mod = DummyConfig(name="config")

    config_mod.APP_LIST = [
        (r"/user1", BaseUserIdHandler),
        (r"/user2", SpecialCookieUserIdHandler),
    ]

    config_mod.AUTHN_PROVIDERS = [
        (DummyCookieAuthProvider, {}),
        (DummyBearerAuthProvider, {}),
    ]

    prior_config = sys.modules.get("config", None)
    prior_biothings_config = sys.modules.get("biothings.config", None)

    sys.modules["config"] = config_mod
    sys.modules["biothings.config"] = config_mod
    yield config_mod
    sys.modules["config"] = prior_config
    sys.modules["biothings.config"] = prior_biothings_config
