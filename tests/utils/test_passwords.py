import pytest

from biothings.utils.passwords import ITERATIONS, hash_password, is_password_hash, verify_password


def test_hash_and_verify():
    hashed = hash_password("s3cret", iterations=1000)
    assert hashed.startswith("pbkdf2_sha256$1000$")
    assert is_password_hash(hashed)
    assert verify_password("s3cret", hashed)
    assert not verify_password("S3cret", hashed)
    assert not verify_password("", hashed)


def test_hashes_are_salted():
    assert hash_password("s3cret", iterations=1000) != hash_password("s3cret", iterations=1000)


def test_default_iterations():
    assert hash_password("s3cret").split("$")[1] == str(ITERATIONS)


def test_non_ascii_password():
    hashed = hash_password("pässwörd", iterations=1000)
    assert verify_password("pässwörd", hashed)
    assert not verify_password("passwrd", hashed)


@pytest.mark.parametrize(
    "value",
    [
        None,
        "",
        "s3cret",  # a password instead of its hash
        "9RKfd8gDuNf0Q",  # a crypt hash, like in HUB_PASSWD
        "pbkdf2_sha256$1000$salt",
        "pbkdf2_sha256$many$salt$digest",
        "pbkdf2_sha256$0$salt$digest",
        "pbkdf2_sha1$1000$salt$digest",
        "pbkdf2_sha256$1000$$digest",
        "pbkdf2_sha256$1000$salt$digest$more",
    ],
)
def test_invalid_hashes(value):
    assert not is_password_hash(value)
    assert not verify_password("s3cret", value)
