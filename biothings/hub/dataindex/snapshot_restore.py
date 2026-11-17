"""Find snapshots in registered destination repositories without formatting creation settings."""

import json
import re
from urllib.parse import unquote, urljoin, urlparse

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError
from elasticsearch import NotFoundError

from biothings import config
from biothings.utils.hub_db import get_src_build


def validate_repository(name):
    """A command override or recorded repository must be one exact, rendered name."""
    if (
        not isinstance(name, str)
        or not name
        or any(char.isspace() or char in '*?,/\\#<>|"[]' for char in name)
        or "$(" in name
        or "%(" in name
    ):
        raise ValueError("repository must be one exact repository name, without templates or wildcards")


def repository_family(name):
    """Turn supported date/build placeholders in the name alone into a search pattern."""
    if not isinstance(name, str) or not name:
        raise ValueError("Repository name is missing; specify --repository")
    pattern = re.sub(r"\$\([a-zA-Z]\)|%\([^)]+\)[#0+ -]*\d*(?:\.\d+)?[diouxXeEfFgGcrsa]", "*", name)
    if "$(" in pattern or "%(" in pattern or not pattern.replace("*", ""):
        raise ValueError("Repository template has no searchable family; specify --repository")
    validate_repository(pattern.replace("*", "placeholder"))
    if "*" in name or "?" in name:
        raise ValueError("Repository configuration must name a repository or template; specify --repository")
    return pattern


class RestoreRepositoryResolver:
    """Resolve a whole preflight together, caching ES and release-document reads for that batch."""

    def __init__(self, client, repository_config):
        self.client = client
        self.repository_config = repository_config
        self.repositories = {}
        self.snapshots = {}
        self.documents = {}

    def _registered(self, name):
        if name not in self.repositories:
            try:
                self.repositories.update(self.client.snapshot.get_repository(name=name))
            except NotFoundError:
                pass
            self.repositories.setdefault(name, None)
        return self.repositories[name] is not None

    def _snapshots(self, repository, names):
        if not self._registered(repository):
            return {}
        cached = self.snapshots.setdefault(repository, {})
        missing = [name for name in names if name not in cached]
        if missing:
            try:
                response = self.client.snapshot.get(repository=repository, snapshot=missing, ignore_unavailable=True)
            except NotFoundError:
                # Distinguish a vanished repository from a missing snapshot.
                self.repositories.pop(repository, None)
                self._registered(repository)
                response = {"snapshots": []}
            cached.update({name: None for name in missing})
            cached.update({item["snapshot"]: item for item in response["snapshots"]})
        return {name: cached[name] for name in names if cached.get(name) is not None}

    def _choose(self, names, hints, source):
        found = {name: {} for name in names}
        repositories = sorted({repository for candidates in hints.values() for repository in candidates})
        for repository in repositories:
            selected = [name for name in names if repository in hints.get(name, ())]
            for name, metadata in self._snapshots(repository, selected).items():
                found[name][repository] = metadata
        rows = {}
        for name, matches in found.items():
            if len(matches) == 1:
                repository, metadata = next(iter(matches.items()))
                rows[name] = {
                    "snapshot": name,
                    "repository": repository,
                    "repository_source": source,
                    "action": "restore",
                    "metadata": metadata,
                }
            elif matches:
                rows[name] = {
                    "snapshot": name,
                    "action": "blocked",
                    "reason": "snapshot exists in multiple repositories; specify --repository",
                    "repositories": sorted(matches),
                }
        return rows

    def _load_json(self, url, session):
        if url not in self.documents:
            missing = False
            parsed = urlparse(url)
            credentials = getattr(config, "STANDALONE_AWS_CREDENTIALS", {}) or {}
            access_key = credentials.get("AWS_ACCESS_KEY_ID")
            secret_key = credentials.get("AWS_SECRET_ACCESS_KEY")
            hostname = parsed.hostname or ""
            is_s3 = hostname.endswith(".amazonaws.com") and (
                hostname.startswith(("s3.", "s3-")) or ".s3." in hostname or ".s3-" in hostname
            )
            if access_key and secret_key and is_s3:
                # Use the same credentials as install. S3 handles regional redirects.
                if hostname.startswith(("s3.", "s3-")):
                    bucket, _, key = parsed.path.lstrip("/").partition("/")
                else:
                    bucket = hostname.split(".s3", 1)[0]
                    key = parsed.path.lstrip("/")
                client = boto3.client(
                    "s3",
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    config=Config(connect_timeout=10, read_timeout=30, retries={"max_attempts": 2}),
                )
                try:
                    response = client.get_object(Bucket=bucket, Key=unquote(key))
                    with response["Body"] as body:
                        document = json.load(body)
                except ClientError as exc:
                    if exc.response.get("Error", {}).get("Code") not in {"NoSuchKey", "NoSuchBucket", "404"}:
                        raise
                    document = None
                    missing = True
                finally:
                    client.close()
            else:
                response = session.get(url, timeout=(10, 30))
                redirect = response.headers.get("x-amz-website-redirect-location")
                if redirect:
                    response.raise_for_status()
                    response = session.get(urljoin(url, redirect), timeout=(10, 30))
                if response.status_code == 404:
                    document = None
                    missing = True
                else:
                    response.raise_for_status()
                    document = response.json()
            if not missing and not isinstance(document, dict):
                raise ValueError(f"Expected a JSON object in release metadata at {url}")
            self.documents[url] = document
        return self.documents[url]

    @staticmethod
    def _release_hint(document, names, hints):
        if document is None or document.get("type") != "full":
            return
        metadata = document.get("metadata") or {}
        name = metadata.get("snapshot_name")
        repository = (metadata.get("repository") or {}).get("name")
        if name in names and repository:
            validate_repository(repository)
            hints[name].add(repository)

    def resolve(self, names, repository=None):
        if repository is not None:
            validate_repository(repository)
            rows = self._choose(names, {name: {repository} for name in names}, "explicit")
            for name in names:
                if name not in rows:
                    missing = (
                        self._missing_repository(repository)
                        if not self._registered(repository)
                        else "snapshot is missing"
                    )
                    rows[name] = {"snapshot": name, "repository": repository, "action": "blocked", "reason": missing}
            return [rows[name] for name in names]

        hints = {name: set() for name in names}
        urls = set()
        for build in get_src_build().find({}, projection={"snapshot": 1, "publish.full": 1}):
            for name in names:
                record = (build.get("snapshot") or {}).get(name) or {}
                repository = ((record.get("conf") or {}).get("repository") or {}).get("name")
                if repository:
                    validate_repository(repository)
                    hints[name].add(repository)
                published = ((build.get("publish") or {}).get("full") or {}).get(name) or {}
                url = (published.get("metadata") or {}).get("url")
                if url:
                    urls.add(url)
        rows = self._choose(names, hints, "snapshot_record")
        pending = [name for name in names if name not in rows]
        if pending and (urls or getattr(config, "VERSION_URLS", [])):
            hints = {name: set() for name in pending}
            with requests.Session() as session:
                for url in sorted(urls):
                    self._release_hint(self._load_json(url, session), pending, hints)
                rows.update(self._choose(pending, hints, "release_document"))
                pending = [name for name in pending if name not in rows]
                if pending:
                    for catalog in getattr(config, "VERSION_URLS", []):
                        url = catalog["url"] if isinstance(catalog, dict) else catalog
                        document = self._load_json(url, session)
                        if document is None:
                            continue
                        if document.get("format") != "1.0" or not isinstance(document.get("versions"), list):
                            raise ValueError(f"Unsupported release catalog at {url}")
                        for version in document["versions"]:
                            if version.get("type") == "full":
                                release_url = urljoin(url, version["url"])
                                self._release_hint(self._load_json(release_url, session), pending, hints)
                    rows.update(self._choose(pending, hints, "release_document"))
                    pending = [name for name in pending if name not in rows]
        if pending:
            pattern = repository_family(self.repository_config.get("name"))
            try:
                repositories = self.client.snapshot.get_repository(name=pattern)
            except NotFoundError:
                repositories = {}
            self.repositories.update(repositories)
            rows.update(self._choose(pending, {name: set(repositories) for name in pending}, "repository_family"))
            for name in pending:
                if name not in rows:
                    registered = sorted(repo for repo in repositories if self.repositories.get(repo) is not None)
                    rows[name] = {
                        "snapshot": name,
                        "repository_pattern": pattern,
                        "repositories": registered,
                        "action": "blocked",
                        "reason": (
                            "snapshot is missing from registered repositories"
                            if registered
                            else self._missing_repository(pattern)
                        ),
                    }
        return [rows[name] for name in names]

    @staticmethod
    def _missing_repository(name):
        return (
            f"repository '{name}' is not registered on the destination cluster; "
            "register it (read-only for restores) or specify --repository with an existing repository name"
        )
