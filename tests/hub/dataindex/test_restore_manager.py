import asyncio
import copy
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest
import pytest_asyncio
from elasticsearch import Elasticsearch

from biothings.hub import HubServer
from biothings.hub.dataindex import snapshooter
from biothings.hub.dataindex.snapshooter import (
    ProcessInfo,
    RepositoryConfig,
    RestoreBatchError,
    SnapshotEnv,
    SnapshotManager,
)
from biothings.utils.hub_db import restore as restore_hub_database
from tests.hub.terminal_fakes import make_terminal


@pytest_asyncio.fixture
async def manager(monkeypatch):
    env = SnapshotEnv.__new__(SnapshotEnv)
    env.name = "target_snapshots"
    env.idxenv = "destination"
    env.pinfo = ProcessInfo(env.name)
    env.wtime = 0.001
    env.client = Mock()
    env.client.options.return_value = env.client
    env.repcfg = RepositoryConfig({"name": "shared_repository", "settings": {"readonly": True}})
    env._doc = Mock(side_effect=AssertionError("Restoration must not need source build records"))
    state = SimpleNamespace(metadata={}, existing={}, calls=[], polls=[], steps=[], failures={}, recoveries={})
    for part in ("alpha", "beta"):
        state.metadata[f"snapshot_{part}"] = {
            "snapshot": f"snapshot_{part}",
            "uuid": f"uuid-{part}",
            "state": "SUCCESS",
            "indices": [f"index_{part}"],
            "shards": {"failed": 0},
        }
    env.client.snapshot.get.side_effect = lambda repository, snapshot, **kwargs: {
        "snapshots": [copy.deepcopy(state.metadata[name]) for name in snapshot if name in state.metadata]
    }
    env.client.indices.get.side_effect = lambda **kwargs: copy.deepcopy(state.existing)

    async def defer(pinfo, operation):
        state.steps.append(pinfo["step"])
        return asyncio.create_task(asyncio.to_thread(operation))

    env.job_manager = SimpleNamespace(loop=asyncio.get_running_loop(), defer_to_thread=defer)

    class Indexer:
        def __init__(self, index, *, client, check_index):
            assert client is env.client and check_index is False
            self.index = index

        def restore(self, repository, snapshot, index, **options):
            state.calls.append((repository, snapshot, index, options))
            if state.failures.get(snapshot) == "submit":
                raise RuntimeError(f"Submission failed: {snapshot}")
            return {"accepted": state.failures.get(snapshot) != "unaccepted"}

        def get_restore_status(self):
            state.polls.append(self.index)
            statuses = state.recoveries.get(self.index, [{"status": "DONE", "progress": "100%"}])
            status = statuses.pop(0) if len(statuses) > 1 else statuses[0]
            if isinstance(status, Exception):
                raise status
            return status

    monkeypatch.setattr(snapshooter, "ESIndexer", Indexer)
    manager = SnapshotManager.__new__(SnapshotManager)
    manager.register = {env.name: env}
    manager.job_manager = env.job_manager
    return manager, env, state


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["restore", "run_restore"])
async def test_single_restore_reads_metadata_and_waits_for_recovery(manager, method):
    manager, env, state = manager
    state.recoveries["index_alpha"] = [
        {"status": "INIT", "progress": "0%"},
        {"status": "IN_PROGRESS", "progress": "50%"},
        {"status": "DONE", "progress": "100%"},
    ]
    task = getattr(manager, method)(env.name, "snapshot_alpha")
    assert isinstance(task, asyncio.Task) and state.calls == []
    result = await task
    assert (result["index_name"], result["snapshot"], result["status"]) == ("index_alpha", "snapshot_alpha", "DONE")
    assert (result["snapshot_env"], result["indexer_env"]) == (env.name, "destination")
    assert state.polls == ["index_alpha"] * 3
    assert state.calls[0][0:3] == ("shared_repository", "snapshot_alpha", "index_alpha")
    assert state.calls[0][3]["include_aliases"] is False
    assert task.progress == ["index_alpha: DONE 100%"]
    assert "restore:snapshot_alpha" in state.steps and "recovery:snapshot_alpha" in state.steps
    env.client.options.assert_called_once_with(max_retries=0, retry_on_timeout=False)
    env.client.close.assert_not_called()
    env._doc.assert_not_called()


@pytest.mark.parametrize("snapshots", ["snapshot_alpha", ["snapshot_alpha", "snapshot_beta"]])
def test_dry_run_previews_metadata_destinations_without_starting_jobs(manager, snapshots):
    manager, env, state = manager
    preview = manager.run_restore(env.name, snapshots, dry_run=True)
    assert preview["dry_run"] is True
    assert preview["indices"][0]["index_name"] == "index_alpha"
    assert all(row["action"] == "restore" for row in preview["indices"])
    assert state.calls == state.polls == state.steps == []
    env.client.options.assert_not_called()


@pytest.mark.parametrize(
    "reason", ["missing", "incomplete", "failed_shards", "multiple_indices", "index", "alias", "duplicate_destination"]
)
def test_entire_batch_is_blocked_before_submission(manager, reason):
    manager, env, state = manager
    metadata = state.metadata["snapshot_beta"]
    if reason == "missing":
        del state.metadata["snapshot_beta"]
    elif reason == "incomplete":
        metadata["state"] = "IN_PROGRESS"
    elif reason == "failed_shards":
        metadata["shards"]["failed"] = 1
    elif reason == "multiple_indices":
        metadata["indices"].append("another_index")
    elif reason == "index":
        state.existing["index_beta"] = {}
    elif reason == "alias":
        state.existing["other_index"] = {"aliases": {"index_beta": {}}}
    else:
        metadata["indices"] = ["index_alpha"]
    preview = manager.run_restore(env.name, ["snapshot_alpha", "snapshot_beta"], dry_run=True)
    assert preview["indices"][1]["action"] == "blocked"
    with pytest.raises(ValueError, match="Cannot restore snapshots"):
        manager.restore_many(env.name, ["snapshot_alpha", "snapshot_beta"])
    assert state.calls == state.steps == []


@pytest.mark.parametrize(
    "snapshots,options",
    [
        (["snapshot_alpha"] * 2, {}),
        (["*"], {}),
        (None, {}),
        (["snapshot_alpha", "snapshot_beta"], {"index": "renamed"}),
        (["snapshot_alpha"], {"index": "alias*"}),
        (["snapshot_alpha"], {"include_aliases": "false"}),
        (["snapshot_alpha"], {"index_settings": []}),
        (["snapshot_alpha"], {"ignore_index_settings": "index.routing.*"}),
        (["snapshot_alpha"], {"recovery_timeout": 0}),
        (["snapshot_alpha"], {"recovery_timeout": float("inf")}),
    ],
)
def test_invalid_input_is_rejected_before_queries(manager, snapshots, options):
    manager, env, _ = manager
    with pytest.raises((TypeError, ValueError)):
        manager.run_restore(env.name, snapshots, **options)
    env.client.snapshot.get.assert_not_called()
    env.client.indices.get.assert_not_called()


def test_empty_batch_and_unknown_environment(manager):
    manager, env, _ = manager
    assert manager.restore_many(env.name, []) == []
    assert manager.run_restore(env.name, [], dry_run=True)["indices"] == []
    env.client.snapshot.get.assert_not_called()
    env.client.indices.get.assert_not_called()
    for method in (manager.restore, manager.restore_many, manager.run_restore):
        with pytest.raises(ValueError, match="Unknown snapshot environment"):
            method("missing", "snapshot_alpha")


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["restore", "restore_many"])
async def test_restore_can_rename_and_forward_settings_without_mutating_them(manager, method):
    manager, env, state = manager
    settings = {"index.number_of_replicas": 0}
    ignored = ["index.routing.allocation.*"]
    selected = "snapshot_alpha" if method == "restore" else ["snapshot_alpha"]
    task = getattr(manager, method)(
        env.name, selected, "renamed", include_aliases=True, index_settings=settings, ignore_index_settings=ignored
    )
    settings["index.number_of_replicas"] = 2
    ignored.append("index.refresh_interval")
    result = await task
    if method == "restore_many":
        result = result["indices"][0]
    assert result["index_name"] == "renamed"
    assert state.calls[0][3] == {
        "include_aliases": True,
        "index_settings": {"index.number_of_replicas": 0},
        "ignore_index_settings": ["index.routing.allocation.*"],
    }


@pytest.mark.asyncio
async def test_snapshot_and_destination_are_rechecked_before_submission(manager):
    manager, env, state = manager
    task = manager.restore(env.name, "snapshot_alpha")
    state.metadata["snapshot_alpha"]["uuid"] = "replacement"
    with pytest.raises(ValueError, match="Snapshot changed after preflight"):
        await task
    assert state.calls == []
    task = manager.restore(env.name, "snapshot_alpha")
    state.existing["index_alpha"] = {}
    with pytest.raises(ValueError, match="already exists"):
        await task
    assert state.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["submit", "unaccepted", "recovery"])
async def test_batch_failure_keeps_successes_and_never_resubmits(manager, failure):
    manager, env, state = manager
    state.failures["snapshot_beta"] = failure
    if failure == "recovery":
        state.recoveries["index_beta"] = [{"status": "FAILED", "reason": "disk watermark"}]
    task = manager.restore_many(env.name, ["snapshot_alpha", "snapshot_beta"])
    with pytest.raises(RestoreBatchError) as error:
        await task
    assert [row["status"] for row in error.value.results] == ["SUCCESS", "FAILED"]
    assert error.value.results[0]["result"]["status"] == "DONE"
    assert error.value.results[1]["snapshot"] == "snapshot_beta"
    assert len(state.calls) == 2
    assert '"status": "SUCCESS"' in str(error.value)
    assert task.progress == ["index_alpha: SUCCESS", "index_beta: FAILED"]


@pytest.mark.asyncio
async def test_monitoring_retries_without_resubmitting_and_has_a_deadline(manager):
    manager, env, state = manager
    state.recoveries["index_alpha"] = [RuntimeError("transient"), {"status": "DONE", "progress": "100%"}]
    await manager.restore(env.name, "snapshot_alpha")
    assert len(state.calls) == 1 and len(state.polls) == 2
    state.recoveries["index_alpha"] = [{"status": "IN_PROGRESS", "progress": "50%"}]
    with pytest.raises(TimeoutError, match="Recovery timed out"):
        await manager.restore(env.name, "snapshot_alpha", recovery_timeout=0.01)
    assert len(state.calls) == 2
    state.recoveries["index_alpha"] = [RuntimeError("persistent")]
    state.polls.clear()
    with pytest.raises(RuntimeError, match="persistent"):
        await manager.restore(env.name, "snapshot_alpha")
    assert len(state.calls) == 3 and len(state.polls) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["restore_many", "run_restore"])
async def test_batch_reuses_single_restore_and_preserves_order(manager, monkeypatch, method):
    manager, env, _ = manager
    native = Mock(wraps=manager.restore)
    monkeypatch.setattr(manager, "restore", native)
    result = await getattr(manager, method)(env.name, ["snapshot_beta", "snapshot_alpha"])
    assert [row["snapshot"] for row in result["indices"]] == ["snapshot_beta", "snapshot_alpha"]
    assert [call.args for call in native.call_args_list] == [
        (env.name, "snapshot_beta", "index_beta"),
        (env.name, "snapshot_alpha", "index_alpha"),
    ]


@pytest.mark.asyncio
async def test_terminal_tracks_batch_and_preserves_database_restore_command(manager):
    manager, env, state = manager
    server = HubServer(source_list=[], name="Restore test Hub")
    server.features = []
    server.managers = {"dump_manager": Mock(), "upload_manager": Mock(), "snapshot_manager": manager}
    server.configure_commands()
    assert server.commands["restore_snapshot"] == manager.run_restore
    terminal, shell, _ = make_terminal(restore_snapshot=server.commands["restore_snapshot"])
    assert shell.commands["restore"] is restore_hub_database
    argv = ["restore_snapshot", env.name, '["snapshot_alpha", "snapshot_beta"]']
    preview = terminal.run(argv=[*argv, "--dry-run"])
    assert preview["is_done"] is True and len(preview["result"]["indices"]) == 2
    assert state.calls == []
    started = terminal.run(argv=argv)
    assert started["is_done"] is False
    await asyncio.gather(*shell.launched_commands[started["id"]]["jobs"])
    shell.refresh_commands()
    done = shell.command_info(started["id"])
    assert done["failed"] is False
    assert [row["status"] for row in done["results"][0]["indices"]] == ["SUCCESS", "SUCCESS"]


@pytest.mark.asyncio
async def test_restore_real_snapshots_and_settings_on_target(monkeypatch):
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
    repository = f"biothings-restore-{suffix}"
    names = [f"biothings-restore-{part}-{suffix}" for part in ("alpha", "beta")]
    snapshots = [f"snapshot-{part}-{suffix}" for part in ("alpha", "beta")]
    renamed = f"renamed-{suffix}"
    alias = f"source-alias-{suffix}"
    root = paths[0][0] if isinstance(paths[0], list) else paths[0]

    async def defer(pinfo, operation):
        return asyncio.create_task(asyncio.to_thread(operation))

    try:
        client.snapshot.create_repository(
            name=repository, body={"type": "fs", "settings": {"location": f"{root.rstrip('/')}/{repository}"}}
        )
        for position, (name, snapshot) in enumerate(zip(names, snapshots)):
            client.indices.create(index=name, settings={"number_of_shards": 1, "number_of_replicas": 1})
            client.index(index=name, id="1", document={"value": position}, refresh=True)
            if position == 0:
                client.indices.put_alias(index=name, name=alias)
            client.snapshot.create(
                repository=repository,
                snapshot=snapshot,
                indices=name,
                include_global_state=False,
                wait_for_completion=True,
            )
        client.indices.delete(index=names)
        env = SnapshotEnv.__new__(SnapshotEnv)
        env.name = "target_snapshots"
        env.idxenv = "target"
        env.client = client
        env.repcfg = RepositoryConfig({"name": repository, "settings": {"readonly": True}})
        env.pinfo = ProcessInfo(env.name)
        env.wtime = 0.01
        env.job_manager = SimpleNamespace(loop=asyncio.get_running_loop(), defer_to_thread=defer)
        env._doc = Mock(side_effect=AssertionError("No build lookup on the target"))
        manager = SnapshotManager.__new__(SnapshotManager)
        manager.register = {env.name: env}
        manager.job_manager = env.job_manager
        options = {"index_settings": {"index.number_of_replicas": 0}}
        preview = manager.run_restore(env.name, snapshots, dry_run=True, **options)
        assert [row["index_name"] for row in preview["indices"]] == names
        assert not any(client.indices.exists(index=name) for name in names)
        result = await asyncio.wait_for(manager.restore_many(env.name, snapshots, **options), 60)
        assert [row["status"] for row in result["indices"]] == ["SUCCESS", "SUCCESS"]
        assert all(row["result"]["status"] == "DONE" for row in result["indices"])
        for position, name in enumerate(names):
            assert client.get(index=name, id="1")["_source"] == {"value": position}
            assert client.indices.get_settings(index=name)[name]["settings"]["index"]["number_of_replicas"] == "0"
        assert not client.indices.exists_alias(name=alias)
        single = await asyncio.wait_for(manager.run_restore(env.name, snapshots[0], renamed, **options), 60)
        assert single["status"] == "DONE" and single["index_name"] == renamed
        assert client.get(index=renamed, id="1")["_source"] == {"value": 0}
        env._doc.assert_not_called()
    finally:
        try:
            client.options(ignore_status=404).indices.delete(index=[*names, renamed])
            client.options(ignore_status=404).snapshot.delete(repository=repository, snapshot="_all")
            client.options(ignore_status=404).snapshot.delete_repository(name=repository)
        finally:
            client.close()
