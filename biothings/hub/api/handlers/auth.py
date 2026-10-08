"""
Optional login to the Hub API

When users are defined in the hub's config (HUB_API_USERS: usernames and password hashes),
requests running commands or changing data require a login: any request with another method
than GET, HEAD and OPTIONS, eg. PUT /shell, POST /dataupload/<source> or PUT /source/<name>/dump.
Requests reading data stay open. Without users defined, the whole API is open.

Clients log in with POST /login, and send the token they get in the X-Biothings-Access-Token
header. Tokens are JWTs signed with a key derived from the user's password hash, so changing
a user's password, or removing the user, invalidates the tokens already given out.
"""

import asyncio
import base64
import datetime
import functools
import hashlib
import hmac
import json
import logging
import secrets
import time
from concurrent.futures import ThreadPoolExecutor

from tornado.web import HTTPError

from biothings import config
from biothings.utils.configuration import ConfigurationError
from biothings.utils.passwords import hash_password, is_password_hash, verify_password

TOKEN_HEADER = "X-Biothings-Access-Token"
# methods only reading data, which never require a login (OPTIONS: CORS pre-flight requests)
READ_METHODS = ("GET", "HEAD", "OPTIONS")
# after this number of failed logins in a row, a user can only try again once per LOGIN_RETRY_DELAY
MAX_FAILED_LOGINS = 5
LOGIN_RETRY_DELAY = 60  # seconds

# checking a password takes a fraction of a second of CPU: in their own threads, password checks
# don't block the hub, and don't wait behind the jobs running in the hub's threads
_password_checks = ThreadPoolExecutor(max_workers=2, thread_name_prefix="hub_login")
# username => (number of failed logins in a row, time of the last attempt)
_failed_logins = {}


def get_users():
    """
    Users who can log in to the Hub API, as {username: password hash}
    """
    return getattr(config, "HUB_API_USERS", None) or {}


def check_users():
    """
    Return the usernames defined in HUB_API_USERS, raising a ConfigurationError
    if their passwords aren't hashes made with "python -m biothings.utils.passwords"
    """
    users = get_users()
    if not isinstance(users, dict):
        raise ConfigurationError("HUB_API_USERS must be a dict of {username: password hash}")
    invalid = [repr(username) for username, hashed in users.items() if not is_password_hash(hashed)]
    if invalid:
        raise ConfigurationError(
            "HUB_API_USERS: the password of %s isn't a password hash, "
            "get one with 'python -m biothings.utils.passwords'" % ", ".join(invalid)
        )
    return list(users)


def require_login(handler):
    """
    Raise a 401 HTTPError if the request requires a login and doesn't have a valid login
    token: when users are defined (HUB_API_USERS), for requests not only reading data
    """
    if handler.request.method in READ_METHODS:
        return
    users = get_users()
    if not users:
        return
    token = handler.request.headers.get(TOKEN_HEADER)
    username = get_token_user(token, users) if token else None
    if username is None:
        raise HTTPError(401, reason="Invalid or expired login, log in again" if token else "Login required")
    handler.current_user = username
    logging.info("Hub API: %s %s by user %r", handler.request.method, handler.request.path, username)


class TooManyFailedLogins(Exception):
    """
    The user must wait before trying to log in again, after too many failed logins in a row
    """

    def __init__(self, wait):
        super().__init__("Too many failed logins, try again in %d seconds" % (int(wait) + 1))


async def check_password(username, password, users):
    """
    Return True if password is the password of username in users ({username: password hash}).
    Raise TooManyFailedLogins if the user must wait before trying again, after too many failed
    logins in a row
    """
    hashed = users.get(username)
    if hashed:
        failures, last_attempt = _failed_logins.get(username, (0, 0))
        wait = last_attempt + LOGIN_RETRY_DELAY - time.time()
        if failures >= MAX_FAILED_LOGINS and wait > 0:
            raise TooManyFailedLogins(wait)
        # counted as failed until the password is checked, so concurrent attempts are limited too
        _failed_logins[username] = (failures + 1, time.time())
    loop = asyncio.get_running_loop()
    valid = await loop.run_in_executor(_password_checks, _check_password, password, hashed)
    if valid:
        _failed_logins.pop(username, None)
    return valid


async def login(username, password, remote_ip=None):
    """
    Check the password of a user of the Hub API, and return a login token for this user with
    its expiry date. Raise a 401 HTTPError if the username or password is wrong, or a 429 one
    if the user must wait before trying again, after too many failed logins in a row
    """
    users = get_users()
    try:
        valid = await check_password(username, password, users)
    except TooManyFailedLogins as error:
        raise HTTPError(429, reason=str(error))
    if not valid:
        logging.warning("Hub API: failed login for user %.100r from %s", username, remote_ip)
        raise HTTPError(401, reason="Invalid username or password")
    logging.info("Hub API: user %r logged in from %s", username, remote_ip)
    expires = int(time.time()) + int(config.HUB_API_LOGIN_EXPIRY)
    return {
        "username": username,
        "token": create_token(username, users[username], expires),
        "expires": datetime.datetime.fromtimestamp(expires).astimezone(),
    }


def create_token(username, hashed, expires):
    """
    Return a login token for the user whose password hash is hashed, valid until expires (a timestamp)
    """
    header = _b64encode(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    claims = _b64encode(json.dumps({"sub": username, "iat": int(time.time()), "exp": expires}).encode())
    signed = header + "." + claims
    return signed + "." + _b64encode(_signature(signed, hashed))


def get_token_user(token, users):
    """
    Return the username of a login token, or None if the token is invalid or expired
    """
    try:
        header, claims, signature = token.split(".")
        payload = json.loads(_b64decode(claims))
        hashed = users.get(payload["sub"])
        if hashed is None or not hmac.compare_digest(_signature(header + "." + claims, hashed), _b64decode(signature)):
            return None
        return payload["sub"] if payload["exp"] > time.time() else None
    except (AttributeError, KeyError, TypeError, ValueError):
        return None


def _check_password(password, hashed):
    # unknown users: check the password anyway, to answer as slowly as for known users
    return verify_password(password, hashed or _unknown_user_hash()) and hashed is not None


@functools.cache
def _unknown_user_hash():
    return hash_password(secrets.token_urlsafe())


def _signature(signed, hashed):
    # each user's tokens are signed with a key derived from their password hash
    key = hmac.new(hashed.encode(), b"biothings hub api login", hashlib.sha256).digest()
    return hmac.new(key, signed.encode("ascii"), hashlib.sha256).digest()


def _b64encode(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64decode(text):
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))
