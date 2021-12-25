from tornado.web import HTTPError

from biothings.web.handlers.base import BaseAPIHandler
from biothings.web.auth.authn import BioThingsAuthnMixin

from authn_provider import DummyCookieAuthProvider


class BaseUserIdHandler(BioThingsAuthnMixin, BaseAPIHandler):
    def get(self):
        if self.current_user:
            self.write(self.current_user)
        else:
            # return 401 or 403
            header = self.get_www_authenticate_header()
            if header:
                self.clear()
                self.set_header('WWW-Authenticate', header)
                self.set_status(401, "Unauthorized")
                self.finish()
            else:
                raise HTTPError(403)


class SpecialCookieUserIdHandler(BaseUserIdHandler):
    AUTHN_PROVIDERS = [(DummyCookieAuthProvider, {'cookie_name': 'USR'})]
