import asyncio
import datetime
import json
import os
from functools import partial
from typing import Optional

from elasticsearch import Elasticsearch, NotFoundError
from requests_aws4auth import AWS4Auth

import biothings.hub.dataload.uploader as uploader
from biothings import config as btconfig
from biothings.utils.backend import DocESBackend
from biothings.utils.common import get_random_string
from biothings.utils.es import IndexerException


class BiothingsUploader(uploader.BaseSourceUploader):
    name = None

    # Specify the backend this uploader should work with. Must be defined before instantiation
    # (can be an instance or a partial() returning an instance)
    TARGET_BACKEND = None

    # Specify the syncer function this uploader will use to apply diff
    # (can be an instance or a partial() returning an instance)
    SYNCER_FUNC = None

    # should we delete index before restoring snapshot if index already exist ?
    AUTO_PURGE_INDEX = False

    def __init__(self, *args, **kwargs):
        super(BiothingsUploader, self).__init__(*args, **kwargs)
        self._target_backend = None
        self._syncer_func = None

    @property
    def target_backend(self):
        if not self._target_backend:
            if type(self.__class__.TARGET_BACKEND) == partial:
                self._target_backend = self.__class__.TARGET_BACKEND()
            else:
                self._target_backend = self.__class__.TARGET_BACKEND
            assert (
                type(self._target_backend) == DocESBackend
            ), "Only ElasticSearch backend is supported (got %s)" % type(self._target_backend)
        return self._target_backend

    @property
    def syncer_func(self):
        if not self._syncer_func:
            self._syncer_func = self.__class__.SYNCER_FUNC
        return self._syncer_func

    async def load(self, *args, **kwargs):
        return await super().load(steps=["data"], *args, **kwargs)

    async def update_data(self, batch_size, job_manager, **kwargs):
        """
        Look in data_folder and either restore a snapshot to ES
        or apply diff to current ES index
        """
        # determine if it's about a snapshot/full and diff/incremental
        # we should have a json metadata matching the release
        self.prepare_src_dump()  # load infor from src_dump
        release = self.src_doc.get("download", {}).get("release")
        assert release, "Can't find release information in src_dump document"
        build_meta = json.load(open(os.path.join(self.data_folder, "%s.json" % release)))
        if build_meta["type"] == "full":
            res = await self.restore_snapshot(build_meta, job_manager=job_manager, **kwargs)
        elif build_meta["type"] == "incremental":
            res = await self.apply_diff(build_meta, job_manager=job_manager, **kwargs)
        return res

    def get_snapshot_repository_config(self, build_meta):
        """Return (name,config) tuple from build_meta, where
        name is the repo name, and config is the repo config"""
        # repo_name, repo_settings = list(
        #     build_meta["metadata"]["repository"].items())[0]
        # TODO
        repo_name = build_meta["metadata"]["repository"]["name"]
        repo_settings = build_meta["metadata"]["repository"]
        return (repo_name, repo_settings)

    def _get_es_client(self, es_host: str, auth: Optional[dict]):
        """
        Get Elasticsearch Client

        Used by self._get_repository, self._create_repository
        """
        es_conf = {
            "timeout": 120,
            "max_retries": 3,
            "retry_on_timeout": False,
        }
        if auth:
            # see https://git.io/JoAE4 on BioThings.API Wiki
            if auth["type"] == "aws":
                auth_args = (
                    auth["properties"]["access_id"],
                    auth["properties"]["secret_key"],
                    auth["properties"]["region"],
                    "es",
                )
                es_conf["http_auth"] = AWS4Auth(*auth_args)
                # RequestsHttpConnection is not available in elasticsearch 8.x
                # es_conf["connection_class"] = RequestsHttpConnection
            elif auth["type"] == "http":
                auth_args = (
                    auth["properties"]["username"],
                    auth["properties"]["password"],
                )
                es_conf["http_auth"] = auth_args
            else:
                raise RuntimeError("Auth settings not recognized")
        es = Elasticsearch(es_host, **es_conf)
        return es

    def _get_repository(self, es_host: str, repo_name: str, auth: Optional[dict]):
        es = self._get_es_client(es_host, auth)
        try:
            repo = es.snapshot.get_repository(repository=repo_name)
        except NotFoundError:
            repo = None
        return repo

    def _create_repository(self, es_host: str, repo_name: str, repo_settings: dict, auth: Optional[dict]):
        """
        Create Elasticsearch Snapshot repository
        """
        es = self._get_es_client(es_host, auth)
        es.snapshot.create_repository(repository=repo_name, body=repo_settings)

    async def restore_snapshot(self, build_meta, job_manager, **kwargs):
        self.logger.debug("Restoring snapshot...")
        idxr = self.target_backend.target_esidxer
        es_host = idxr.es_host
        self.logger.debug("Got ES Host: %s", es_host)
        repo_name, repo_settings = self.get_snapshot_repository_config(build_meta)
        self.logger.debug("Got repo name: %s", repo_name)
        self.logger.debug("With settings: %s", repo_settings)
        # pull authentication settings from config
        auth = btconfig.STANDALONE_CONFIG.get(self.name, {}).get(
            "auth", btconfig.STANDALONE_CONFIG["_default"].get("auth")
        )
        if auth:
            self.logger.debug("Obtained Auth settings, using them.")
        else:
            self.logger.debug("No Auth settings found")

        # all restore repos should be r/o
        repo_settings["settings"]["readonly"] = True

        # populate additional settings
        additional_settings = btconfig.STANDALONE_CONFIG.get(self.name, {}).get(
            "repo_settings", btconfig.STANDALONE_CONFIG["_default"].get("repo_settings")
        )
        if additional_settings:
            self.logger.debug("Adding additional settings: %s", additional_settings)
            repo_settings["settings"].update(additional_settings)

        if "client" not in repo_settings["settings"]:
            self.logger.warning("\"client\" not set in repository settings. The 'default' " "client will be used.")
            self.logger.warning(
                "Make sure keys are in the Elasticsearch keystore. "
                "If you are trying to work with EOL versions of "
                "Elasticsearch, or if you intentionally enabled "
                'allow_insecure_settings, set "access_key", "secret_key",'
                " and potentially \"region\" in additional 'repo_settings'."
            )

        # first check if snapshot repo exists
        self.logger.info("Getting current repository settings")
        existing_repo_settings = self._get_repository(es_host, repo_name, auth)
        if existing_repo_settings:
            if existing_repo_settings[repo_name] != repo_settings:
                # TODO update comparison logic
                self.logger.info(
                    f"Repository '{repo_name}' was found but settings are different, " "it may need to be created again"
                )
                self.logger.debug("Existing setting: %s", existing_repo_settings[repo_name])
                self.logger.debug("Required (new) setting: %s" % repo_settings)
            else:
                self.logger.info("Repo exists with correct settings")
        else:
            # ok, it doesn't exist let's try to create it
            self.logger.info("Repo does not exist")
            try:
                self.logger.info("Creating repo...")
                self._create_repository(es_host, repo_name, repo_settings, auth)
            except Exception as e:
                self.logger.info("Creation failed: %s", e)
                if "url" in repo_settings["settings"]:
                    raise uploader.ResourceError(
                        "Could not create snapshot repository. Check elasticsearch.yml configuration "
                        + "file, you should have a line like this: "
                        + 'repositories.url.allowed_urls: "%s*" ' % repo_settings["settings"]["url"]
                        + "allowing snapshot to be restored from this URL. Error was: %s" % e
                    )
                else:
                    raise uploader.ResourceError("Could not create snapshot repository: %s" % e)

        # repository is now ready, let's trigger the restore
        snapshot_name = build_meta["metadata"]["snapshot_name"]

        # backup the original value of indexer's replica
        original_number_of_replicas = idxr.get_internal_number_of_replicas() or 0
        alias_name = idxr.canonical_index_name

        use_no_downtime_method = kwargs.get("use_no_downtime_method", True)
        append_ts = kwargs.get("append_ts", True)
        if use_no_downtime_method:
            base_index_name = snapshot_name
            if append_ts:
                ts = datetime.datetime.now().strftime("%Y%m%d%H%M")
                base_index_name = f"{snapshot_name}_{ts}"
            if len(base_index_name) >= 255:
                raise RuntimeError("Deterministic part of index name already too long")

            index_name = base_index_name
            append_random_str = False
            while True:
                if append_random_str:
                    index_name += "_" + get_random_string()
                    index_name = index_name[:255]  # elasticsearch restriction
                if not idxr.exists_index(index=index_name):
                    break
        else:
            index_name = alias_name

        pinfo = self.get_pinfo()
        pinfo["step"] = "restore"
        pinfo["description"] = snapshot_name

        def get_status_info():
            try:
                res = idxr.get_restore_status(index_name)
                return res
            except Exception as e:
                # somethng went wrong, report as failure
                return {"status": "FAILED %s" % e}

        def done_callback(f, step: str):
            try:
                self.logger.info("%s launched: %s" % (step, f.result()))
            except Exception as e:
                self.logger.error("Error while launching %s: %s" % (step, e))
                raise e

        self.logger.info(
            "Restoring snapshot '%s' to index '%s' on host '%s'" % (snapshot_name, index_name, idxr.es_host)
        )
        # ESIndexer.restore is synchronous but should return relatively
        # quickly
        job = await job_manager.defer_to_thread(
            pinfo, partial(idxr.restore, repo_name, snapshot_name, index_name, purge=self.__class__.AUTO_PURGE_INDEX)
        )
        job.add_done_callback(partial(done_callback, step="restore"))
        await job

        def update_alias_and_delete_old_indices():
            # Find indices which starts with snapshot_name, and sort by creation date and order by asc
            old_indices = []
            try:
                old_indices.extend(
                    idxr.get_indice_names_by_settings(index=alias_name + "*", sort_by_creation_date=True, reverse=False)
                )
            except Exception:
                pass
            self.logger.debug("Alias '%s' points to '%s'" % (alias_name, old_indices))
            if index_name in old_indices:
                self.logger.warning("new index name in old alias, something is not right")
                self.logger.warning("continuing alias swap despite potential problem")
                old_indices.remove(index_name)
            try:
                idxr.update_alias(alias_name, index_name)
                self.logger.info(f"Alias '{alias_name}' updated to " f"associate with index '{index_name}'")
            except IndexerException as e:
                self.logger.warning(f"Alias index swap ran into a problem {e}")
                self.logger.warning(f"Deleting new index '{index_name}'")
                idxr.delete_index(index_name)
                raise

            # have ESIndexer look at the correct index after snapshot restore
            idxr.check_index()

            # after successful swap, delete old indices
            # only issue messages on errors
            # we only keep n recent indices depends on the config.RELEASE_KEEP_N_RECENT_INDICES
            # n < 0: keep all
            # n == 0: only keep the new created indice
            # n > 0: only keep at most n latest indices
            number_indexes_to_keep = btconfig.RELEASE_KEEP_N_RECENT_INDICES
            if number_indexes_to_keep > 0:
                old_indices = old_indices[:-number_indexes_to_keep]
            elif number_indexes_to_keep < 0:
                old_indices = []

            try:
                for rm_idx_name in old_indices:
                    idxr.delete_index(rm_idx_name)
                    self.logger.info("Deleted old index '%s'" % rm_idx_name)
            except Exception:  # nosec
                # just inform the user that deletion failed, not that harmful
                self.logger.error("Failed to delete old indices, try deleting " f"{old_indices} manually")

            # restore indexer's replica to original value
            idxr.set_internal_number_of_replicas(original_number_of_replicas)

        while True:
            status_info = get_status_info()
            status = status_info["status"]
            self.logger.info("Recovery status for index '%s': %s" % (index_name, status_info))
            if status in ["INIT", "IN_PROGRESS"]:
                await asyncio.sleep(getattr(btconfig, "MONITOR_SNAPSHOT_DELAY", 60))
            else:
                if status == "DONE":
                    self.logger.info(
                        "Snapshot '%s' successfully restored to index '%s' (host: '%s')"
                        % (snapshot_name, index_name, idxr.es_host),
                        extra={"notify": True},
                    )
                    if use_no_downtime_method:
                        job = await job_manager.defer_to_thread(pinfo={}, func=update_alias_and_delete_old_indices)
                        job.add_done_callback(partial(done_callback, step="alias"))
                        await job
                else:
                    e = uploader.ResourceError(
                        "Failed to restore snapshot '%s' on index '%s', status: %s"
                        % (snapshot_name, idxr._index, status)
                    )
                    self.logger.error(e)
                    raise e
                break
        # return current number of docs in index
        return self.target_backend.count()

    async def apply_diff(self, build_meta, job_manager, **kwargs):
        self.logger.info("Applying incremental update from diff folder: %s" % self.data_folder)
        meta = json.load(open(os.path.join(self.data_folder, "metadata.json")))
        # old: index we want to update
        old = (
            self.target_backend.target_esidxer.es_host,
            meta["old"]["backend"],
            # TODO
            # target name can be release index name,
            # maybe should refer to old backend name
            # ----------------------------------------
            #  self.target_backend.target_name,
            # ----------------------------------------
            self.target_backend.target_esidxer._doc_type,
        )
        # new: index's data we will reach once updated (just informative)
        new = (
            self.target_backend.target_esidxer.es_host,
            meta["new"]["backend"],
            self.target_backend.target_esidxer._doc_type,
        )
        await self.syncer_func(old_db_col_names=old, new_db_col_names=new, diff_folder=self.data_folder)
        # return current number of docs in index (even if diff update)
        return self.target_backend.count()

    def clean_archived_collections(self):
        pass
