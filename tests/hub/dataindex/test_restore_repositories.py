import copy
import io
import json
import threading
from collections import Counter
from fnmatch import fnmatchcase
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from unittest.mock import Mock

import boto3
import mongomock
import pytest
import requests
from botocore.response import StreamingBody
from botocore.stub import Stubber
from elasticsearch import AuthorizationException, NotFoundError

from biothings.hub.dataindex import snapshot_restore
from biothings.hub.dataindex.snapshooter import RepositoryConfig, SnapshotEnv


def missing():
    return NotFoundError("missing", Mock(status=404), {"error": {"type": "repository_missing_exception"}})


@pytest.fixture
def target(monkeypatch):
    builds = mongomock.MongoClient().db.builds
    monkeypatch.setattr(snapshot_restore, "get_src_build", lambda: builds)
    monkeypatch.setattr(snapshot_restore.config, "VERSION_URLS", [], raising=False)
    monkeypatch.setattr(snapshot_restore.config, "STANDALONE_AWS_CREDENTIALS", {}, raising=False)
    state = SimpleNamespace(repositories={}, snapshots={}, builds=builds)
    env = SnapshotEnv.__new__(SnapshotEnv)
    env.name = "target"
    env.idxenv = "destination"
    env.repcfg = RepositoryConfig({"name": "drug_repository-$(Y)", "settings": {"base_path": "%(build_config.name)s"}})
    env.client = Mock()
    env.client.indices.get.return_value = {}

    def repositories(name):
        found = {repo: info for repo, info in state.repositories.items() if fnmatchcase(repo, name)}
        if not found:
            raise missing()
        return copy.deepcopy(found)

    def snapshots(repository, snapshot, **kwargs):
        if repository not in state.repositories:
            raise missing()
        return {
            "snapshots": [
                copy.deepcopy(state.snapshots[repository][name])
                for name in snapshot
                if name in state.snapshots[repository]
            ]
        }

    env.client.snapshot.get_repository.side_effect = repositories
    env.client.snapshot.get.side_effect = snapshots

    def add(repository, name="snapshot_alpha", **options):
        state.repositories[repository] = {"type": "s3", "settings": {"bucket": "snapshots", "readonly": True}}
        state.snapshots.setdefault(repository, {})[name] = {
            "snapshot": name,
            "uuid": f"{repository}-{name}",
            "state": "SUCCESS",
            "shards": {"failed": 0},
            "indices": [f"index_{name}"],
            **options,
        }

    state.add = add
    return env, state


@pytest.fixture
def catalog_server():
    documents = {}
    requested = Counter()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requested[self.path] += 1
            document = documents.get(self.path)
            self.send_response(document if isinstance(document, int) else 200 if document is not None else 404)
            self.end_headers()
            if document is not None and not isinstance(document, int):
                self.wfile.write(json.dumps(document).encode())

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield SimpleNamespace(url=f"http://127.0.0.1:{server.server_port}", documents=documents, requested=requested)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def release(snapshot, repository):
    return {
        "type": "full",
        "build_version": "20241020",
        "metadata": {"snapshot_name": snapshot, "repository": {"name": repository}},
    }


@pytest.mark.parametrize("configured", ["drug_repository-$(Y)", "drug_%(build_config.name)s"])
def test_searches_registered_family_without_formatting_creation_settings(target, configured):
    env, state = target
    env.repcfg["name"] = configured
    repository = "drug_repository-2024" if "$(Y)" in configured else "drug_old_build"
    state.add(repository)
    state.add("unrelated_repository")
    row = env._restore_plan("snapshot_alpha")["indices"][0]
    assert (row["repository"], row["repository_source"], row["action"]) == (repository, "repository_family", "restore")
    assert [call.kwargs["repository"] for call in env.client.snapshot.get.call_args_list] == [repository]
    env.client.snapshot.create_repository.assert_not_called()


@pytest.mark.parametrize("failure", [None, "missing_repository", "missing_snapshot", "incomplete"])
def test_explicit_override_is_strict_and_never_reads_metadata(target, monkeypatch, failure):
    env, state = target
    state.add("drug_repository-2024")
    if failure != "missing_repository":
        state.add("override")
    if failure == "missing_snapshot":
        state.snapshots["override"].clear()
    elif failure == "incomplete":
        state.snapshots["override"]["snapshot_alpha"]["state"] = "PARTIAL"
    monkeypatch.setattr(
        snapshot_restore, "get_src_build", Mock(side_effect=AssertionError("Explicit overrides skip metadata"))
    )
    row = env._restore_plan("snapshot_alpha", repository="override")["indices"][0]
    assert row["repository"] == "override"
    assert row["action"] == ("restore" if failure is None else "blocked")
    if failure:
        assert {
            "missing_repository": "not registered",
            "missing_snapshot": "snapshot is missing",
            "incomplete": "incomplete",
        }[failure] in row["reason"]
    assert all(call.kwargs["name"] == "override" for call in env.client.snapshot.get_repository.call_args_list)


def test_recorded_repository_wins_even_outside_family(target):
    env, state = target
    state.add("historical_recorded_repo")
    state.add("drug_repository-2024")
    state.builds.insert_one(
        {"snapshot": {"snapshot_alpha": {"conf": {"repository": {"name": "historical_recorded_repo"}}}}}
    )
    row = env._restore_plan("snapshot_alpha")["indices"][0]
    assert row["repository"] == "historical_recorded_repo" and row["repository_source"] == "snapshot_record"
    assert env.client.snapshot.get_repository.call_count == 1


@pytest.mark.parametrize("missing_snapshot", [False, True])
def test_stale_record_falls_back_to_family(target, missing_snapshot):
    env, state = target
    state.add("drug_repository-2024")
    if missing_snapshot:
        state.add("stale_repo", name="another_snapshot")
    state.builds.insert_one({"snapshot": {"snapshot_alpha": {"conf": {"repository": {"name": "stale_repo"}}}}})
    row = env._restore_plan("snapshot_alpha")["indices"][0]
    assert row["repository"] == "drug_repository-2024" and row["repository_source"] == "repository_family"


def test_recorded_incomplete_snapshot_blocks_instead_of_switching_to_complete_copy(target):
    env, state = target
    state.add("recorded_repo", state="PARTIAL")
    state.add("drug_repository-2024")
    state.builds.insert_one({"snapshot": {"snapshot_alpha": {"conf": {"repository": {"name": "recorded_repo"}}}}})
    row = env._restore_plan("snapshot_alpha")["indices"][0]
    assert row["repository"] == "recorded_repo" and row["action"] == "blocked"
    assert "incomplete" in row["reason"]
    assert env.client.snapshot.get.call_count == 1


@pytest.mark.parametrize("source", ["snapshot_record", "repository_family"])
def test_ambiguous_matches_require_override_even_if_one_is_incomplete(target, source):
    env, state = target
    for repository in ("drug_repository-2024", "drug_repository-2025"):
        state.add(repository)
        if source == "snapshot_record":
            state.builds.insert_one({"snapshot": {"snapshot_alpha": {"conf": {"repository": {"name": repository}}}}})
    state.snapshots["drug_repository-2025"]["snapshot_alpha"]["state"] = "PARTIAL"
    row = env._restore_plan("snapshot_alpha")["indices"][0]
    assert row["action"] == "blocked" and "specify --repository" in row["reason"]
    assert row["repositories"] == ["drug_repository-2024", "drug_repository-2025"]


@pytest.mark.parametrize("failure", ["missing_repository", "missing_snapshot", "incomplete", "failed_shards"])
def test_distinguishes_repository_snapshot_and_completeness_errors(target, failure):
    env, state = target
    if failure != "missing_repository":
        state.add("drug_repository-2024")
        if failure == "missing_snapshot":
            state.snapshots["drug_repository-2024"].clear()
        elif failure == "incomplete":
            state.snapshots["drug_repository-2024"]["snapshot_alpha"]["state"] = "IN_PROGRESS"
        else:
            state.snapshots["drug_repository-2024"]["snapshot_alpha"]["shards"]["failed"] = 1
    row = env._restore_plan("snapshot_alpha")["indices"][0]
    assert row["action"] == "blocked"
    expected = (
        "not registered"
        if failure == "missing_repository"
        else "snapshot is missing" if failure == "missing_snapshot" else "incomplete"
    )
    assert expected in row["reason"]


@pytest.mark.parametrize("operation", ["get_repository", "get"])
@pytest.mark.parametrize(
    "error", [RuntimeError("connection lost"), AuthorizationException("forbidden", Mock(status=403), {})]
)
def test_connection_and_permission_errors_stop_resolution(target, operation, error):
    env, state = target
    state.add("drug_repository-2024")
    getattr(env.client.snapshot, operation).side_effect = error
    with pytest.raises(type(error)) as caught:
        env._restore_plan("snapshot_alpha")
    assert caught.value is error


def test_catalog_matches_snapshot_field_and_reuses_documents_for_batch(target, catalog_server, monkeypatch):
    env, state = target
    server = catalog_server
    state.add("recorded_release_repo")
    state.add("recorded_release_repo", name="snapshot_beta")
    server.documents.update(
        {
            "/versions.json": {
                "format": "1.0",
                "versions": [
                    {"type": "full", "url": "20241020.json"},
                    {"type": "full", "url": "20241020.json"},
                    {"type": "full", "url": "20241021.json"},
                    {"type": "incremental", "url": "do-not-fetch.json"},
                ],
            },
            "/20241020.json": release("snapshot_alpha", "recorded_release_repo"),
            "/20241021.json": release("snapshot_beta", "recorded_release_repo"),
        }
    )
    monkeypatch.setattr(
        snapshot_restore.config, "VERSION_URLS", [{"name": "mychem", "url": server.url + "/versions.json"}]
    )
    rows = env._restore_plan(["snapshot_alpha", "snapshot_beta"])["indices"]
    assert [row["repository_source"] for row in rows] == ["release_document"] * 2
    assert [row["repository"] for row in rows] == ["recorded_release_repo"] * 2
    assert server.requested == {"/versions.json": 1, "/20241020.json": 1, "/20241021.json": 1}
    assert env.client.snapshot.get.call_count == 1


def test_known_release_url_does_not_need_catalog_configuration(target, catalog_server):
    env, state = target
    state.add("published_repo")
    catalog_server.documents["/20241020.json"] = release("snapshot_alpha", "published_repo")
    state.builds.insert_one(
        {"publish": {"full": {"snapshot_alpha": {"metadata": {"url": catalog_server.url + "/20241020.json"}}}}}
    )
    row = env._restore_plan("snapshot_alpha")["indices"][0]
    assert row["repository"] == "published_repo" and row["repository_source"] == "release_document"


def test_release_documents_with_multiple_available_repositories_are_ambiguous(target, catalog_server, monkeypatch):
    env, state = target
    for repository in ("published_one", "published_two"):
        state.add(repository)
        catalog_server.documents[f"/{repository}.json"] = release("snapshot_alpha", repository)
    catalog_server.documents["/versions.json"] = {
        "format": "1.0",
        "versions": [
            {"type": "full", "url": "published_one.json"},
            {"type": "full", "url": "published_two.json"},
        ],
    }
    monkeypatch.setattr(snapshot_restore.config, "VERSION_URLS", [catalog_server.url + "/versions.json"])
    row = env._restore_plan("snapshot_alpha")["indices"][0]
    assert row["action"] == "blocked" and row["repositories"] == ["published_one", "published_two"]


def test_nonmatching_release_document_falls_back_to_family(target, catalog_server, monkeypatch):
    env, state = target
    state.add("drug_repository-2024")
    server = catalog_server
    server.documents["/versions.json"] = {"format": "1.0", "versions": [{"type": "full", "url": "20241020.json"}]}
    server.documents["/20241020.json"] = release("snapshot_alpha_extra", "unrelated_repo")
    monkeypatch.setattr(snapshot_restore.config, "VERSION_URLS", [server.url + "/versions.json"])
    assert env._restore_plan("snapshot_alpha")["indices"][0]["repository_source"] == "repository_family"


@pytest.mark.parametrize("status", [403, 500])
def test_catalog_http_errors_are_reported_instead_of_silently_switching_repositories(
    target, catalog_server, monkeypatch, status
):
    env, state = target
    state.add("drug_repository-2024")
    catalog_server.documents["/versions.json"] = status
    monkeypatch.setattr(snapshot_restore.config, "VERSION_URLS", [catalog_server.url + "/versions.json"])
    with pytest.raises(requests.HTTPError, match=str(status)):
        env._restore_plan("snapshot_alpha")
    env.client.snapshot.get.assert_not_called()


def test_missing_release_catalog_falls_back_to_registered_family(target, catalog_server, monkeypatch):
    env, state = target
    state.add("drug_repository-2024")
    monkeypatch.setattr(snapshot_restore.config, "VERSION_URLS", [catalog_server.url + "/missing.json"])
    row = env._restore_plan("snapshot_alpha")["indices"][0]
    assert row["repository"] == "drug_repository-2024" and row["repository_source"] == "repository_family"


@pytest.mark.parametrize(
    "url",
    [
        "https://releases.s3.us-west-2.amazonaws.com/mychem/20241020.json",
        "https://releases.s3.amazonaws.com/mychem/20241020.json",
        "https://s3.amazonaws.com/releases/mychem/20241020.json",
    ],
)
def test_private_s3_release_metadata_uses_existing_install_credentials(target, monkeypatch, url):
    env, _ = target
    monkeypatch.setattr(
        snapshot_restore.config,
        "STANDALONE_AWS_CREDENTIALS",
        {"AWS_ACCESS_KEY_ID": "test-key", "AWS_SECRET_ACCESS_KEY": "test-secret"},
    )
    client = boto3.client("s3", aws_access_key_id="test-key", aws_secret_access_key="test-secret")
    document = release("snapshot_alpha", "published_repo")
    payload = json.dumps(document).encode()
    factory = Mock(return_value=client)
    monkeypatch.setattr(snapshot_restore.boto3, "client", factory)
    with Stubber(client) as stubber:
        stubber.add_response(
            "get_object",
            {"Body": StreamingBody(io.BytesIO(payload), len(payload))},
            {"Bucket": "releases", "Key": "mychem/20241020.json"},
        )
        resolver = snapshot_restore.RestoreRepositoryResolver(env.client, env.repcfg)
        session = Mock()
        assert resolver._load_json(url, session) == document
        assert resolver._load_json(url, session) == document
        session.get.assert_not_called()
        stubber.assert_no_pending_responses()
    assert factory.call_args.kwargs["aws_access_key_id"] == "test-key"


@pytest.mark.parametrize("configured", ["%(build_config.name)s", "$(Y)"])
def test_fully_variable_name_requires_recorded_metadata_or_override(target, configured):
    env, state = target
    env.repcfg["name"] = configured
    with pytest.raises(ValueError, match="specify --repository"):
        env._restore_plan("snapshot_alpha")
    state.add("chosen_repo")
    assert env._restore_plan("snapshot_alpha", repository="chosen_repo")["indices"][0]["action"] == "restore"


@pytest.mark.parametrize("document", [None, [], "invalid"])
def test_invalid_json_metadata_is_not_treated_as_a_missing_document(target, document):
    env, _ = target
    session = Mock()
    session.get.return_value.status_code = 200
    session.get.return_value.headers = {}
    session.get.return_value.json.return_value = document
    resolver = snapshot_restore.RestoreRepositoryResolver(env.client, env.repcfg)
    with pytest.raises(ValueError, match="Expected a JSON object"):
        resolver._load_json("https://releases.example.org/version.json", session)
