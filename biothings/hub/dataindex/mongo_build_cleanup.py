from datetime import datetime
from functools import partial

from config import logger as logging

from biothings import config as btconfig
from biothings.hub.manager import BaseManager
from biothings.utils import mongo
from biothings.utils.hub_db import get_src_build


class MongoBuildCleaner:
    def __init__(self, job_manager):
        self.job_manager = job_manager

    def list_builds(self, build_config=None, build_name=None, year=None):
        collection = get_src_build()

        filters = {}
        if build_config:
            filters["build_config._id"] = build_config
        if build_name:
            filters["_id"] = build_name
        if year:
            year = int(year)
            filters["started_at"] = {
                "$gte": datetime(year, 1, 1),
                "$lt": datetime(year + 1, 1, 1),
            }

        projection = {
            "_id": 1,
            "build_config": 1,
            "started_at": 1,
            "archived": 1,
            "target_name": 1,
        }
        builds = list(collection.find(filters, projection).sort("started_at", -1))

        grouped = {}
        for build in builds:
            group_name = build.get("build_config", {}).get("_id") or "N/A"
            grouped.setdefault(group_name, []).append(build)

        return [{"_id": key, "items": items} for key, items in grouped.items()]

    async def delete_builds(self, build_ids):
        if not build_ids:
            return {
                "deleted_count": 0,
                "target_collections_deleted_count": 0,
                "target_collections_deleted": [],
            }

        conn = mongo.get_hub_db_async_conn()
        try:
            src_build = mongo.get_src_build_async(conn)

            build_docs = []
            async for doc in src_build.find({"_id": {"$in": build_ids}}, {"_id": 1, "target_name": 1}):
                build_docs.append(doc)

            target_collection_candidates = set()
            for doc in build_docs:
                build_id = doc["_id"]
                target_name = doc.get("target_name")
                target_collection_candidates.add(build_id)
                if target_name and target_name != build_id:
                    target_collection_candidates.add(target_name)

            target_collections_deleted = []
            if target_collection_candidates:
                target_db = conn[btconfig.DATA_TARGET_DATABASE]
                existing_collections = await target_db.list_collection_names(
                    filter={"name": {"$in": list(target_collection_candidates)}}
                )

                for collection_name in existing_collections:
                    await target_db[collection_name].drop()
                    target_collections_deleted.append(collection_name)

            result = await src_build.delete_many({"_id": {"$in": build_ids}})
            return {
                "deleted_count": result.deleted_count,
                "target_collections_deleted_count": len(target_collections_deleted),
                "target_collections_deleted": sorted(target_collections_deleted),
            }
        finally:
            await conn.close()

    async def validate_builds(self):
        """Validate that target collections exist for each build record.

        Checks every build in src_build to see if its target collection still
        exists in the target database.  Build records whose target collections
        have been removed are deleted, keeping the database in sync with the
        actual data.

        Returns a dict with ``builds_removed`` (count) and ``builds_removed_names``.
        """
        logging.info("Starting validation of MongoDB builds...")
        conn = mongo.get_hub_db_async_conn()
        try:
            src_build = mongo.get_src_build_async(conn)
            target_db = conn[btconfig.DATA_TARGET_DATABASE]

            existing_collections = set(await target_db.list_collection_names())

            orphaned_ids = []
            async for doc in src_build.find({}, {"_id": 1, "target_name": 1}):
                build_id = doc["_id"]
                target_name = doc.get("target_name") or build_id
                if target_name not in existing_collections:
                    orphaned_ids.append(build_id)

            if orphaned_ids:
                result = await src_build.delete_many({"_id": {"$in": orphaned_ids}})
                deleted_count = result.deleted_count
            else:
                deleted_count = 0

            logging.info(
                "Build validation complete: removed %d orphaned build record(s)",
                deleted_count,
                extra={"notify": True},
            )
            return {
                "builds_removed": deleted_count,
                "builds_removed_names": sorted(orphaned_ids),
            }
        finally:
            await conn.close()

    async def done(self, job):
        try:
            result = await job
            logging.info(
                "Deleted %d MongoDB builds and dropped %d target collections",
                result.get("deleted_count", 0),
                result.get("target_collections_deleted_count", 0),
                extra={"notify": True},
            )
        except Exception as exc:
            logging.exception("Failed to delete MongoDB builds: %s", exc, extra={"notify": True})

    async def validate_done(self, job):
        try:
            result = await job
            logging.info(
                "Build validation complete: removed %d orphaned build record(s)",
                result.get("builds_removed", 0),
                extra={"notify": True},
            )
        except Exception as exc:
            logging.exception("Failed to validate MongoDB builds: %s", exc, extra={"notify": True})


class MongoBuildCleanupManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cleaner = MongoBuildCleaner(self.job_manager)

    def list_mongo_builds(self, build_config=None, build_name=None, year=None):
        return self.cleaner.list_builds(build_config=build_config, build_name=build_name, year=year)

    def delete_mongo_builds(self, build_ids):
        try:
            job = self.job_manager.submit(partial(self.cleaner.delete_builds, build_ids))
            self.job_manager.loop.create_task(self.cleaner.done(job))
        except Exception as ex:
            logging.exception("Error while submitting MongoDB build deletion job: %s", ex, extra={"notify": True})
            raise
        return job

    def validate_mongo_builds(self):
        try:
            job = self.job_manager.submit(partial(self.cleaner.validate_builds))
            self.job_manager.loop.create_task(self.cleaner.validate_done(job))
        except Exception as ex:
            logging.exception("Error while submitting MongoDB build validation job: %s", ex, extra={"notify": True})
            raise
        return job
