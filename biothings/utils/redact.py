"""
Hide secrets (passwords, access keys, tokens...) in data shown to users, eg. by the Hub API
"""

import re
from collections import UserList
from collections.abc import Mapping
from functools import lru_cache

REDACTED = "********"
# words of a key telling its value is a secret, eg. AWS_SECRET, HUB_PASSWD, SLACK_WEBHOOK, http_auth
SECRET_WORDS = {"password", "passwd", "secret", "token", "credential", "credentials", "webhook", "auth"}
# or a pair of words, eg. access_key (AWS_ACCESS_KEY_ID), api_key (UMLS_API_KEY, apiKey)
SECRET_PAIRS = ("access_key", "secret_key", "private_key", "api_key", "aws_key")
# credentials in a URL, eg. mongodb://user:password@host
URL_CREDENTIALS = re.compile(r"(\w+://)[^/\s@]+@")


@lru_cache(maxsize=4096)  # (the same keys, again and again, in big results)
def is_secret_key(key):
    """True if a key (eg. of a config parameter) names a secret"""
    words = re.findall(r"[a-z0-9]+", re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(key)).lower())
    name = "_".join(words)
    return bool(SECRET_WORDS.intersection(words)) or any(pair in name for pair in SECRET_PAIRS)


def redact_secrets(value, secret=False):
    """
    Copy of value (dicts, lists... eg. a command's result) where the values of the keys naming a
    secret (see is_secret_key()) are replaced by "********", as well as the credentials in URLs
    """
    if isinstance(value, Mapping):
        return {key: redact_secrets(item, secret or is_secret_key(key)) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, UserList)):
        return [redact_secrets(item, secret) for item in value]
    if secret and value is not None and not isinstance(value, bool) and value != "":
        return REDACTED
    if isinstance(value, str) and "@" in value and "://" in value:
        return URL_CREDENTIALS.sub(r"\1%s@" % REDACTED, value)
    return value
