from handlers import *
from authn_provider import *

APP_LIST = [
    (r'/user1', BaseUserIdHandler),
    (r'/user2', SpecialCookieUserIdHandler),
]

AUTHN_PROVIDERS = [
    (DummyCookieAuthProvider, {}),
    (DummyBearerAuthProvider, {}),
]
