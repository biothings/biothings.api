from functools import partial

from biothings import config as btconfig
from biothings.utils.manager import BaseManager

logger = btconfig.logger


class AutoSnapshotCleanupManager(BaseManager):
    """This feature will add a new console command "auto_snapshot_cleanup", a new api "/auto_snapshot_cleanup".
    It is intended to allow automatically cleanup old snapshot, based on configuration.

    This feature can be configurated by using AUTO_SNAPSHOT_CLEANUP_CONFIG variable, to determine:
        - schedule: how frequency this task should run
        - days: how old a snapshot should be deleted

    AUTO_SNAPSHOT_CLEANUP_CONFIG = {
        "environment_name": {
            "schedule": "* 0 * * *",      # run daily at 0am UTC
            "keep": 3,                   # the number of most recent snapshots to keep in one group
            "group_by": "build_config",  # the attr of which its values form groups
            "extra_filters": {}          # a set of criterions to limit which snapshots are to be cleaned
        },
        ...
    }
    """

    DEFAULT_SCHEDULE = "* 0 * * *"  # run daily at 0am UTC

    def __init__(self, snapshot_manager, job_manager, *args, **kwargs):
        super().__init__(job_manager, *args, **kwargs)

        self.snapshot_manager = snapshot_manager

    def configure(self, conf=None):
        self.auto_snapshot_cleaner_config = conf or {}

        for env_name in self.snapshot_manager.register.keys():
            cleaner_config = self.auto_snapshot_cleaner_config.get(env_name)

            if not isinstance(cleaner_config, dict):
                logger.info(f"Snapshot environment: {env_name}: No cleaner config found!")
                continue

            schedule = cleaner_config.get("schedule") or self.DEFAULT_SCHEDULE
            keep = cleaner_config.get("keep")
            group_by = cleaner_config.get("group_by")
            extra_filters = cleaner_config.get("extra_filters")

            self.job_manager.submit(
                partial(
                    self.snapshot_manager.cleanup,
                    env=env_name,
                    keep=keep,
                    group_by=group_by,
                    dryrun=False,
                    **extra_filters,
                ),
                schedule=schedule,
            )
