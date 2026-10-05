"""
Readable summaries of the hub's state for terminals (Studio's terminal, "biothings-cli hub"): builds,
build configurations, sources and environments, without the bulky parts of their records (mappings,
release notes...) and without secrets (see biothings.utils.redact)
"""

from biothings import config
from biothings.utils.hub_db import get_src_build, get_src_build_config, get_src_dump
from biothings.utils.redact import redact_secrets


def last_line(text):
    lines = str(text or "").strip().splitlines()
    return lines[-1] if lines else None


def compact(**fields):
    """fields, without the empty ones"""
    return {key: value for key, value in fields.items() if value not in (None, "", {}, [])}


def repository_summary(repository):
    """A snapshot repository: name, type and settings (eg. bucket, base_path)"""
    repository = repository or {}
    return compact(**dict(repository.get("settings") or {}, name=repository.get("name"), type=repository.get("type")))


def data_sources(dump_manager=None, upload_manager=None):
    """
    Names of the data sources: not the private dumpers ("__..."), nor the data release installers
    (see biothings.hub.autoupdate), like dump_all() and upload_all()
    """
    from biothings.hub.autoupdate import BiothingsDumper, BiothingsUploader  # (imports dataload modules)

    names = set()
    for manager, release_class in ((dump_manager, BiothingsDumper), (upload_manager, BiothingsUploader)):
        for name, klasses in (manager.register.items() if manager else []):
            if not name.startswith("__") and not (klasses and all(issubclass(k, release_class) for k in klasses)):
                names.add(name)
    return sorted(names)


def records(value):
    """The records of a mapping, eg. a build's indices (name => record), without its other values (eg. dates)"""
    return {key: record for key, record in value.items() if isinstance(record, dict)} if isinstance(value, dict) else {}


def build_summary(name):
    """Summary of a build: configuration, sources, documents, steps, index, snapshot, release, pending actions"""
    doc = get_src_build().find_one({"_id": name})
    if not doc:
        raise ValueError("No build named '%s' (see lsmerge)" % name)
    meta = doc.get("_meta") or {}
    summary = compact(
        name=doc["_id"],
        build_config=(doc.get("build_config") or {}).get("_id"),
        status=doc.get("status"),
        archived=doc.get("archived"),
        started_at=doc.get("started_at"),
        version=meta.get("build_version"),
        documents=(meta.get("stats") or {}).get("total", doc.get("count")),
        sources={source: info.get("version") for source, info in records(meta.get("src")).items()},
        steps=[
            compact(
                step=job.get("step"),
                status=job.get("status"),
                time=job.get("time"),
                started_at=job.get("step_started_at"),
                # why it failed: an error, or its details (eg. snapshots)
                error=last_line(job.get("err")) or (job.get("detail") if job.get("status") == "failed" else None),
            )
            for job in doc.get("jobs") or []
            if isinstance(job, dict)
        ],
        index={
            index: compact(
                environment=info.get("environment"), created_at=info.get("created_at"), count=info.get("count")
            )
            for index, info in records(doc.get("index")).items()
        },
        snapshot={
            snapshot: compact(
                environment=info.get("environment"),
                index=info.get("index_name"),
                created_at=info.get("created_at"),
                repository=repository_summary((info.get("conf") or {}).get("repository")),
            )
            for snapshot, info in records(doc.get("snapshot")).items()
        },
        diff={old: compact(folder=info.get("diff_folder")) for old, info in records(doc.get("diff")).items()},
        # builds the release notes were made against
        release_note={
            old: compact(folder=info.get("release_folder")) for old, info in records(doc.get("release_note")).items()
        },
        # full (snapshot) and incremental (diff) releases: each step's result, and when it was last published
        publish={
            kind: {
                release: (
                    {step: result for step, result in steps.items() if step != "conf"}
                    if isinstance(steps, dict)
                    else steps
                )
                for release, steps in releases.items()
            }
            for kind, releases in records(doc.get("publish")).items()
        },
        # actions queued, eg. the release note created after a snapshot
        pending=doc.get("pending"),
    )
    return redact_secrets(summary)


def build_config(name=None):
    """Build configurations (or one of them): sources, root sources, builder... and their builds"""
    configs = list(get_src_build_config().find({"_id": name} if name else {}))
    if name and not configs:
        raise ValueError("No build configuration named '%s'" % name)
    builds = {}
    for build in get_src_build().find({}, {"build_config": 1, "archived": 1}):
        if not build.get("archived"):
            builds.setdefault((build.get("build_config") or {}).get("_id"), []).append(build["_id"])
    summaries = {conf["_id"]: dict(conf, builds=sorted(builds.get(conf["_id"], []))) for conf in configs}
    return redact_secrets(summaries[name] if name else summaries)


def source_summary(dump_manager=None, upload_manager=None):
    """Download and upload status of each data source: release, dates, documents, errors"""
    sources = data_sources(dump_manager, upload_manager)
    docs = {doc["_id"]: doc for doc in get_src_dump().find({"_id": {"$in": sources}})}
    summary = {}
    for source in sources:
        doc = docs.get(source) or {}
        download = doc.get("download") or {}
        uploads = (doc.get("upload") or {}).get("jobs") or {}
        summary[source] = compact(
            release=download.get("release"),
            download=compact(
                status=download.get("status"), at=download.get("started_at"), error=last_line(download.get("error"))
            ),
            upload={
                name: compact(
                    status=upload.get("status"),
                    documents=upload.get("count"),
                    at=upload.get("started_at"),
                    error=last_line(upload.get("error")),
                )
                for name, upload in uploads.items()
            },
        )
    return redact_secrets(summary)


def envs(release_installers=None):
    """Index, snapshot and release environments (INDEX_CONFIG, SNAPSHOT_CONFIG, RELEASE_CONFIG)"""
    index = (getattr(config, "INDEX_CONFIG", None) or {}).get("env") or {}
    snapshot = (getattr(config, "SNAPSHOT_CONFIG", None) or {}).get("env") or {}
    release = (getattr(config, "RELEASE_CONFIG", None) or {}).get("env") or {}
    summary = {
        "index": {name: compact(host=env.get("host")) for name, env in index.items()},
        "snapshot": {
            name: compact(
                repository=repository_summary(env.get("repository")), index_env=(env.get("indexer") or {}).get("env")
            )
            for name, env in snapshot.items()
        },
        "release": {
            name: compact(
                release={key: value for key, value in (env.get("release") or {}).items() if key != "auto"},
                diff={key: value for key, value in (env.get("diff") or {}).items() if key != "auto"},
            )
            for name, env in release.items()
        },
    }
    if release_installers:
        # data releases installed with "install" (VERSION_URLS, see biothings.hub.autoupdate)
        summary["release_installers"] = release_installers()
    return redact_secrets(summary)
