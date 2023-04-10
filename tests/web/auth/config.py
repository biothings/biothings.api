from authn_provider import DummyBearerAuthProvider, DummyCookieAuthProvider
from handlers import BaseUserIdHandler, SpecialCookieUserIdHandler

APP_LIST = [
    (r"/user1", BaseUserIdHandler),
    (r"/user2", SpecialCookieUserIdHandler),
]

AUTHN_PROVIDERS = [
    (DummyCookieAuthProvider, {}),
    (DummyBearerAuthProvider, {}),
]
