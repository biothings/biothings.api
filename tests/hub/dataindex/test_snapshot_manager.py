import asyncio
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import mongomock
import pytest
import pytest_asyncio
from elasticsearch import Elasticsearch

from biothings.hub import HubServer, datarelease
from biothings.hub.dataindex import snapshooter
from biothings.hub.dataindex.snapshooter import (
    CumulativeResult,
    ProcessInfo,
    RepositoryConfig,
    SnapshotBatchError,
    SnapshotEnv,
    SnapshotManager,
)
from tests.hub.terminal_fakes import make_terminal


def test_snapshot_in_unknown_environment():
    manager = SimpleNamespace(register={})  # no SNAPSHOT_CONFIG
    with pytest.raises(ValueError, match="Unknown snapshot environment 'local' \\(see SNAPSHOT_CONFIG\\)"):
        SnapshotManager.snapshot(manager, "local", "mygene_20240101_abcdefgh")


class FakeSnapshotEnv:
    def __init__(self, loop):
        self.idxenv = "source"
        self.job_manager = SimpleNamespace(loop=loop)
        self.client = Mock()
        self.existing = {"alpha_v1": {"aliases": {"builder_alias": {}}}, "beta_v1": {}}
        self.client.indices.get.side_effect = lambda index, **kwargs: SimpleNamespace(
            body={name: info for name, info in self.existing.items() if name in index}
        )
        self.repcfg = RepositoryConfig({"name": "test_repository", "type": "s3", "settings": {}})
        self.builds = {
            f"build-{name}": {"_id": f"build-{name}", "index": {f"{name}_v1": {"environment": "source"}}}
            for name in ("alpha", "beta")
        }
        self.states = {}
        self.calls = []
        self.tasks = []
        self.workers = []
        self.events = []
        self.failures = {}
        self.started = asyncio.Event()
        self.wait = None
        self.logger = None

    def _doc(self, index):
        for build in self.builds.values():
            if build.get("index", {}).get(index, {}).get("environment") == self.idxenv:
                return build
        raise ValueError("Not a hub-managed index.")

    def snapshot(self, index, snapshot=None, recreate_repo=False):
        self.calls.append((index, snapshot, recreate_repo))
        self.workers.append(self)
        if self.failures.get(index) == "submit":
            raise ValueError(f"Cannot submit {index}")
        self.logger = index

        async def create():
            self.events.append(("start", index))
            self.started.set()
            if self.wait is not None:
                await self.wait.wait()
            await asyncio.sleep(0)
            assert self.logger == index
            self.events.append(("end", index))
            if self.failures.get(index) == "runtime":
                raise RuntimeError(f"Snapshot failed for {index}")
            return CumulativeResult({"index_name": index})

        task = self.job_manager.loop.create_task(create())
        self.tasks.append(task)
        return task


@pytest_asyncio.fixture
async def manager(monkeypatch):
    env = FakeSnapshotEnv(asyncio.get_running_loop())
    manager = SnapshotManager.__new__(SnapshotManager)
    manager.register = {"source_snapshots": env}
    manager.job_manager = env.job_manager
    state = Mock(
        side_effect=lambda client, repository, name: SimpleNamespace(state=lambda: env.states.get(name, "MISSING"))
    )
    monkeypatch.setattr(snapshooter, "Snapshot", state)
    return manager, env


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["snapshot", "run_snapshot"])
async def test_single_index_preserves_native_task_and_arguments(manager, method):
    manager, env = manager
    task = getattr(manager, method)("source_snapshots", "alpha_v1", "custom_snapshot", True)
    assert task is env.tasks[0]
    assert env.calls == [("alpha_v1", "custom_snapshot", True)]
    assert (await task)["index_name"] == "alpha_v1"
    env.client.indices.get.assert_not_called()


def test_command_delegates_single_index_to_native_override(manager, monkeypatch):
    manager, _ = manager
    native = Mock(return_value=object())
    monkeypatch.setattr(manager, "snapshot", native)
    result = manager.run_snapshot("source_snapshots", "alpha_v1", "custom_snapshot", True)
    assert result is native.return_value
    native.assert_called_once_with("source_snapshots", "alpha_v1", "custom_snapshot", recreate_repo=True)


@pytest.mark.parametrize("indices", [[], ["alpha_v1"], ["alpha_v1", "beta_v1"]])
def test_command_delegates_batch_to_public_snapshots_method(manager, monkeypatch, indices):
    manager, _ = manager
    batch = Mock(return_value=object())
    monkeypatch.setattr(manager, "snapshots", batch)
    result = manager.run_snapshot("source_snapshots", indices)
    assert result is batch.return_value
    batch.assert_called_once_with("source_snapshots", indices, None, recreate_repo=False)


def test_hub_snapshot_command_is_registered_to_wrapper(manager):
    manager, _ = manager
    server = HubServer(source_list=[], name="Snapshot test Hub")
    server.features = []
    server.managers = {"dump_manager": Mock(), "upload_manager": Mock(), "snapshot_manager": manager}
    server.configure_commands()
    assert server.commands["snapshot"] == manager.run_snapshot


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["snapshots", "run_snapshot"])
async def test_batch_uses_native_snapshot_method(manager, monkeypatch, method):
    manager, _ = manager
    calls = []
    native = SnapshotManager.snapshot

    def record(worker, snapshot_env, index, snapshot=None, recreate_repo=False):
        calls.append((snapshot_env, index, snapshot, recreate_repo))
        return native(worker, snapshot_env, index, snapshot, recreate_repo)

    monkeypatch.setattr(SnapshotManager, "snapshot", record)
    await getattr(manager, method)("source_snapshots", ["alpha_v1", "beta_v1"])
    assert calls == [
        ("source_snapshots", "alpha_v1", "alpha_v1", False),
        ("source_snapshots", "beta_v1", "beta_v1", False),
    ]


def test_dry_run_previews_each_index_without_mutation(manager):
    manager, env = manager
    env.states["alpha_v1"] = "SUCCESS"
    preview = manager.run_snapshot("source_snapshots", ["alpha_v1", "beta_v1", "missing"], dry_run=True)
    assert preview["snapshot_env"] == "source_snapshots"
    assert preview["indexer_env"] == "source"
    assert preview["dry_run"] is True
    assert [row["action"] for row in preview["indices"]] == ["replace", "create", "skip"]
    assert preview["indices"][0]["aliases"] == ["builder_alias"]
    assert preview["indices"][0]["build_id"] == "build-alpha"
    assert env.calls == []
    assert env.client.snapshot.mock_calls == []


@pytest.mark.parametrize("state", ["MISSING", "N/A"])
def test_single_index_dry_run_uses_custom_name_and_build_registration(manager, state):
    manager, env = manager
    env.states["custom_snapshot"] = state
    env.repcfg["name"] = "snapshots-%(_id)s"
    preview = manager.run_snapshot("source_snapshots", "alpha_v1", snapshot="custom_snapshot", dry_run=True)
    row = preview["indices"][0]
    assert row["snapshot"] == "custom_snapshot"
    assert row["repository"] == "snapshots-build-alpha"
    assert row["snapshot_state"] == state
    assert row["action"] == "create"
    assert env.calls == []


@pytest.mark.parametrize("index", [None, {}, ("alpha_v1",), [1], [""], ["*"], ["a,b"], ["a b"], ["alpha_v1"] * 2])
def test_invalid_batches_are_rejected_before_queries(manager, index):
    manager, env = manager
    with pytest.raises((TypeError, ValueError)):
        manager.snapshots("source_snapshots", index)
    env.client.indices.get.assert_not_called()
    assert env.calls == []


@pytest.mark.parametrize("options", [{"snapshot": "one_name"}, {"recreate_repo": True}])
def test_ambiguous_batch_options_are_rejected(manager, options):
    manager, env = manager
    with pytest.raises(ValueError, match="single index"):
        manager.snapshots("source_snapshots", ["alpha_v1", "beta_v1"], **options)
    env.client.indices.get.assert_not_called()


def test_empty_batch_does_not_query_elasticsearch(manager):
    manager, env = manager
    assert manager.snapshots("source_snapshots", []) == []
    assert manager.run_snapshot("source_snapshots", [], dry_run=True)["indices"] == []
    env.client.indices.get.assert_not_called()


@pytest.mark.parametrize("reason", ["unregistered", "wrong_environment", "read_only", "running"])
def test_preflight_blocks_entire_batch_before_submission(manager, reason):
    manager, env = manager
    if reason == "unregistered":
        del env.builds["build-beta"]
    elif reason == "wrong_environment":
        env.builds["build-beta"]["index"]["beta_v1"]["environment"] = "other"
    elif reason == "read_only":
        env.repcfg["settings"]["readonly"] = "true"
    else:
        env.states["beta_v1"] = "IN_PROGRESS"
    preview = manager.run_snapshot("source_snapshots", ["alpha_v1", "beta_v1"], dry_run=True)
    assert preview["indices"][1]["action"] == "blocked"
    with pytest.raises(ValueError, match="Cannot create snapshots"):
        manager.snapshots("source_snapshots", ["alpha_v1", "beta_v1"])
    assert env.calls == []


def test_alias_resolution_and_elasticsearch_errors_propagate(manager):
    manager, env = manager
    env.client.indices.get.side_effect = lambda **kwargs: SimpleNamespace(body={"different_concrete_index": {}})
    with pytest.raises(ValueError, match="concrete index names"):
        manager.snapshots("source_snapshots", ["alpha_v1"])
    env.client.indices.get.side_effect = RuntimeError("ES unavailable")
    with pytest.raises(RuntimeError, match="ES unavailable"):
        manager.snapshots("source_snapshots", ["alpha_v1"])
    assert env.calls == []


@pytest.mark.asyncio
async def test_batch_reports_each_result_and_isolates_worker_logs(manager):
    manager, env = manager
    task = manager.snapshots("source_snapshots", ["alpha_v1", "missing", "beta_v1"])
    assert isinstance(task, asyncio.Task)
    result = await task
    assert [row["status"] for row in result["indices"]] == ["SUCCESS", "SKIPPED", "SUCCESS"]
    assert env.calls == [("alpha_v1", "alpha_v1", False), ("beta_v1", "beta_v1", False)]
    assert task.progress == ["alpha_v1: SUCCESS", "missing: SKIPPED", "beta_v1: SUCCESS"]
    assert env.workers[0] is not env.workers[1]
    assert all(worker.client is env.client for worker in env.workers)
    assert env.logger is None


@pytest.mark.asyncio
async def test_missing_indices_are_reported_without_snapshot_jobs(manager):
    manager, env = manager
    result = await manager.snapshots("source_snapshots", ["missing"])
    assert result["indices"][0]["status"] == "SKIPPED"
    assert env.calls == []


@pytest.mark.asyncio
async def test_one_element_batch_supports_existing_single_index_options(manager):
    manager, env = manager
    result = await manager.snapshots("source_snapshots", ["alpha_v1"], "custom_snapshot", True)
    assert env.calls == [("alpha_v1", "custom_snapshot", True)]
    assert result["indices"][0]["snapshot"] == "custom_snapshot"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["submit", "runtime"])
async def test_batch_failure_retains_successful_and_failed_outcomes(manager, failure):
    manager, env = manager
    env.failures["beta_v1"] = failure
    task = manager.snapshots("source_snapshots", ["alpha_v1", "beta_v1"])
    with pytest.raises(SnapshotBatchError) as error:
        await task
    assert [row["status"] for row in error.value.results] == ["SUCCESS", "FAILED"]
    assert error.value.results[1]["index_name"] == "beta_v1"
    assert "beta_v1" in error.value.results[1]["error"]
    assert '"status": "SUCCESS"' in str(error.value)
    assert all(task.done() for task in env.tasks)


@pytest.mark.asyncio
async def test_indices_from_same_build_run_sequentially(manager):
    manager, env = manager
    env.builds = {"shared": {"_id": "shared", "index": {name: {"environment": "source"} for name in env.existing}}}
    await manager.snapshots("source_snapshots", ["alpha_v1", "beta_v1"])
    assert env.events == [("start", "alpha_v1"), ("end", "alpha_v1"), ("start", "beta_v1"), ("end", "beta_v1")]


@pytest.mark.asyncio
async def test_cancelling_batch_cancels_pending_snapshot_tasks(manager):
    manager, env = manager
    env.wait = asyncio.Event()
    task = manager.snapshots("source_snapshots", ["alpha_v1", "beta_v1"])
    await env.started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert env.tasks and all(task.cancelled() for task in env.tasks)


@pytest.mark.asyncio
@pytest.mark.parametrize("fail", [False, True])
async def test_terminal_accepts_json_batches_and_tracks_all_results(manager, fail):
    manager, env = manager
    if fail:
        env.failures["beta_v1"] = "submit"
    terminal, shell, _ = make_terminal(snapshot=manager.run_snapshot)
    argv = ["snapshot", "source_snapshots", '["alpha_v1", "beta_v1"]']
    preview = terminal.run(argv=[*argv, "--dry-run"])
    assert preview["is_done"] is True and preview["failed"] is False
    assert len(preview["result"]["indices"]) == 2
    assert env.calls == []
    response = terminal.run(argv=argv)
    assert response["is_done"] is False
    await asyncio.gather(*shell.launched_commands[response["id"]]["jobs"], return_exceptions=True)
    shell.refresh_commands()
    info = shell.command_info(response["id"])
    assert info["failed"] is fail
    if fail:
        assert '"status": "SUCCESS"' in info["results"][0]
        assert '"status": "FAILED"' in info["results"][0]
    else:
        assert [row["index_name"] for row in info["results"][0]["indices"]] == ["alpha_v1", "beta_v1"]


@pytest.mark.asyncio
async def test_batch_creates_real_snapshots_in_filesystem_repository(monkeypatch, tmp_path):
    """Exercise the native workflow on ES; the S3 resource is unused with an existing fs repository."""
    client = Elasticsearch("http://localhost:9200")
    paths = [
        node["settings"]["path.repo"]
        for node in client.nodes.info(metric="settings", flat_settings=True)["nodes"].values()
        if node["settings"].get("path.repo")
    ]
    if not paths:
        client.close()
        pytest.skip("Elasticsearch needs path.repo configured for filesystem snapshot tests")

    suffix = uuid4().hex
    repository = f"biothings-batch-{suffix}"
    names = [f"biothings-batch-{part}-{suffix}" for part in ("alpha", "beta")]
    root = paths[0][0] if isinstance(paths[0], list) else paths[0]
    builds = mongomock.MongoClient().db.builds
    collection = SimpleNamespace(find_one=builds.find_one, replace_one=builds.replace_one, update=builds.update_one)
    monkeypatch.setattr(snapshooter, "get_src_build", lambda: collection)
    monkeypatch.setattr(datarelease, "get_src_build", lambda: collection)
    monkeypatch.setattr(snapshooter, "btconfig", SimpleNamespace(LOG_FOLDER=str(tmp_path)))

    class LocalRepositoryConfig(RepositoryConfig):
        @property
        def bucket(self):
            return "unused"

        @property
        def region(self):
            return None

        def format(self, doc=None):
            return self

    async def defer(pinfo, operation):
        return asyncio.create_task(asyncio.to_thread(operation))

    try:
        client.snapshot.create_repository(
            name=repository, body={"type": "fs", "settings": {"location": f"{root.rstrip('/')}/{repository}"}}
        )
        for position, name in enumerate(names):
            client.indices.create(index=name, settings={"number_of_shards": 1, "number_of_replicas": 0})
            client.index(index=name, id="1", document={"value": position}, refresh=True)
            builds.insert_one(
                {
                    "_id": f"build-{position}",
                    "target_name": f"build-{position}",
                    "index": {name: {"environment": "local"}},
                }
            )
        env = SnapshotEnv.__new__(SnapshotEnv)
        env.name = "local_snapshots"
        env.idxenv = "local"
        env.client = client
        env.cloud = Mock()
        env.cloud.Bucket.return_value.creation_date = True
        env.repcfg = LocalRepositoryConfig(
            {"name": repository, **client.snapshot.get_repository(name=repository)[repository]}
        )
        env.job_manager = SimpleNamespace(loop=asyncio.get_running_loop(), defer_to_thread=defer)
        env.pinfo = ProcessInfo(env.name)
        env.wtime = 0.05
        manager = SnapshotManager.__new__(SnapshotManager)
        manager.register = {env.name: env}
        manager.job_manager = env.job_manager

        preview = manager.run_snapshot(env.name, names, dry_run=True)
        assert [row["action"] for row in preview["indices"]] == ["create", "create"]
        assert client.snapshot.get(repository=repository, snapshot="_all")["snapshots"] == []
        result = await asyncio.wait_for(manager.snapshots(env.name, names), timeout=60)
        assert [row["status"] for row in result["indices"]] == ["SUCCESS", "SUCCESS"]
        snapshots = client.snapshot.get(repository=repository, snapshot="_all")["snapshots"]
        assert {item["snapshot"] for item in snapshots} == set(names)
        assert all(item["state"] == "SUCCESS" and item["indices"] == [item["snapshot"]] for item in snapshots)
        for build in builds.find():
            assert [job["status"] for job in build["jobs"]] == ["success"] * 3
            assert "release_note" in build["pending"]

        replacement = await asyncio.wait_for(manager.run_snapshot(env.name, names), timeout=60)
        assert all(row["result"]["replaced"] for row in replacement["indices"])
        env.cloud.create_bucket.assert_not_called()
    finally:
        try:
            client.options(ignore_status=404).snapshot.delete(repository=repository, snapshot="_all")
            client.options(ignore_status=404).snapshot.delete_repository(name=repository)
            client.options(ignore_status=404).indices.delete(index=names)
        finally:
            client.close()
