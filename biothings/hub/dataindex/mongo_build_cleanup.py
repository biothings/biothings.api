from functools import partial

from biothings import config as btconfig
from biothings.hub.manager import BaseManager
from biothings.utils.hub_db import get_src_build
from config import logger as logging


class MongoBuildCleaner:
    def __init__(self, job_manager):
        self.job_manager = job_manager

    def list_builds(self, build_config=None, build_name=None):
        collection = get_src_build()

        filters = {}
        if build_config:
            filters["build_config._id"] = build_config
        if build_name:
            filters["_id"] = build_name

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

        from biothings.utils import mongo

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

    def done(self, future):
        try:
            result = future.result()
            logging.info(
                "Deleted %d MongoDB builds and dropped %d target collections",
                result.get("deleted_count", 0),
                result.get("target_collections_deleted_count", 0),
                extra={"notify": True},
            )
        except Exception as exc:
            logging.exception("Failed to delete MongoDB builds: %s", exc, extra={"notify": True})


class MongoBuildCleanupManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cleaner = MongoBuildCleaner(self.job_manager)

    def list_mongo_builds(self, build_config=None, build_name=None):
        return self.cleaner.list_builds(build_config=build_config, build_name=build_name)

    def delete_mongo_builds(self, build_ids):
        try:
            job = self.job_manager.submit(partial(self.cleaner.delete_builds, build_ids))
            job.add_done_callback(self.cleaner.done)
        except Exception as ex:
            logging.exception("Error while submitting MongoDB build deletion job: %s", ex, extra={"notify": True})
            raise
        return job
