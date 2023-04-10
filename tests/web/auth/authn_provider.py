from typing import Optional

from biothings.web.auth.authn import BioThingsAuthenticationProviderInterface


class DummyCookieAuthProvider(BioThingsAuthenticationProviderInterface):
    WWW_AUTHENTICATE_HEADER = None

    def __init__(self, handler, cookie_name="USER_ID"):
        super(DummyCookieAuthProvider, self).__init__(handler)
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
