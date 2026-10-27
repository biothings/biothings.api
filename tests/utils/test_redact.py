import pytest

from biothings.utils.redact import REDACTED, is_secret_key, redact_secrets


@pytest.mark.parametrize(
    "key",
    [
        "AWS_SECRET",
        "AWS_KEY",
        "aws_access_key_id",
        "secret_key",
        "HUB_PASSWD",
        "password",
        "SLACK_WEBHOOK",
        "http_auth",
        "UMLS_API_KEY",
        "apiKey",
        "github_token",
        "credentials",
        "HUB_API_USERS",
    ],
)
def test_secret_keys(key):
    assert is_secret_key(key)


@pytest.mark.parametrize(
    "key", ["author", "tokenizer", "HUB_API_PORT", "keys", "monkey", "region", "bucket", "authority", 1, None]
)
def test_other_keys(key):
    assert not is_secret_key(key)


def test_redact_secrets():
    build = {
        "_id": "mygene_20240101_abcdefgh",
        "snapshot": {
            "mygene_20240101_abcdefgh": {
                "conf": {
                    "cloud": {"type": "aws", "access_key": "AKIA123", "secret_key": "s3cr3t", "region": "us-west-2"},
                    "repository": {"name": "repo", "settings": {"bucket": "snapshots"}},
                    "indexer": {"args": {"http_auth": ("user", "pass")}},
                },
            }
        },
        "uri": "mongodb://user:pass@su09:27017/hubdb",
        "doc_url": "https://example.com/data@2024",
        "AWS_SECRET": None,  # nothing to hide
        "SLACK_WEBHOOK": "",
        "auth": {"enabled": True, "token": 1234},
    }
    redacted = redact_secrets(build)
    conf = redacted["snapshot"]["mygene_20240101_abcdefgh"]["conf"]
    assert conf["cloud"] == {"type": "aws", "access_key": REDACTED, "secret_key": REDACTED, "region": "us-west-2"}
    assert conf["repository"] == {"name": "repo", "settings": {"bucket": "snapshots"}}
    assert conf["indexer"]["args"]["http_auth"] == [REDACTED, REDACTED]
    assert redacted["uri"] == "mongodb://%s@su09:27017/hubdb" % REDACTED
    assert redacted["doc_url"] == "https://example.com/data@2024"  # not credentials
    assert redacted["AWS_SECRET"] is None and redacted["SLACK_WEBHOOK"] == ""
    assert redacted["auth"] == {"enabled": True, "token": REDACTED}  # booleans are kept: whether it's set
    # the original is left untouched
    assert build["snapshot"]["mygene_20240101_abcdefgh"]["conf"]["cloud"]["secret_key"] == "s3cr3t"
    assert redact_secrets(["text", 1, None]) == ["text", 1, None]


def test_password_hashes_are_hidden():
    hashed = "pbkdf2_sha256$600000$salt$digest"
    assert redact_secrets({"HUB_API_USERS": {"alice": hashed}}) == {"HUB_API_USERS": {"alice": REDACTED}}
    assert redact_secrets({"value": {"bob": hashed}, "other": "pbkdf2_sha256 isn't a hash"}) == {
        "value": {"bob": REDACTED},  # whatever its key
        "other": "pbkdf2_sha256 isn't a hash",
    }


def test_structures_under_secret_keys_are_kept():
    """eg. a mapping's "token" field: its definition isn't a secret, the keys inside are checked"""
    mapping = {"properties": {"token": {"type": "keyword"}, "api_key": {"type": "text"}}}
    assert redact_secrets(mapping) == mapping
    assert redact_secrets({"credentials": {"user": "me", "password": "pw"}}) == {
        "credentials": {"user": "me", "password": REDACTED}
    }
