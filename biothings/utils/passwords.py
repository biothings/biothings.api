"""
Password hashes for the users who can log in to a BioThings Hub's API (HUB_API_USERS in
the hub's config)

Passwords are hashed with PBKDF2-SHA256 and a random salt, in the format Django uses:
"pbkdf2_sha256$<iterations>$<salt>$<base64 hash>". To get the hash of a password:

    python -m biothings.utils.passwords
"""

import base64
import getpass
import hashlib
import hmac
import secrets
import sys

ALGORITHM = "pbkdf2_sha256"
ITERATIONS = 600_000  # OWASP's recommendation for PBKDF2-SHA256


def hash_password(password, iterations=ITERATIONS):
    salt = secrets.token_urlsafe(16)
    return "%s$%d$%s$%s" % (ALGORITHM, iterations, salt, _pbkdf2(password, salt, iterations))


def is_password_hash(value):
    """
    Return True if value has the format of a hash made by hash_password()
    """
    try:
        algorithm, iterations, salt, digest = value.split("$")
        return algorithm == ALGORITHM and int(iterations) > 0 and bool(salt) and bool(digest)
    except (AttributeError, ValueError):
        return False


def verify_password(password, hashed):
    """
    Return True if password matches the hash made by hash_password()
    """
    if not is_password_hash(hashed):
        return False
    _, iterations, salt, digest = hashed.split("$")
    return hmac.compare_digest(_pbkdf2(password, salt, int(iterations)).encode(), digest.encode())


def _pbkdf2(password, salt, iterations):
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), iterations)
    return base64.b64encode(digest).decode("ascii")


def main():
    password = getpass.getpass("Password: ")
    if not password:
        sys.exit("The password can't be empty")
    if password != getpass.getpass("Password (again): "):
        sys.exit("The passwords don't match")
    print(hash_password(password))


if __name__ == "__main__":
    main()
