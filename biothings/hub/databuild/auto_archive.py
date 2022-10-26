import datetime
from functools import partial

from dateutil import parser as dtparser

from biothings import config as btconfig
from biothings.utils.manager import BaseManager

logger = btconfig.logger


class AutoArchiveManager(BaseManager):
    """This feature will add a new console command "auto_archive", a new api "/auto_archive".
    It is intended to allow automatically archive old build, based on configuration.

    This feature can be configurated by using AUTO_ARCHIVE_CONFIG variable, to determine:
        - schedule: how frequency a build configuration should run
        - days: how old a build should be archived

    AUTO_ARCHIVE_CONFIG = {
        "build_configuration_name": {
            "schedule: "* 0 * * *",  # run daily at 0am UTC
            "days": 3
        },
        ...
    }
    """

    DEFAULT_ARCHIVE_AFTER_DAYS = 3
    DEFAULT_ARCHIVE_SCHEDULE = "* 0 * * *"  # run daily at 0am UTC

    def __init__(
        self, build_manager, job_manager, days=None, auto_archive_config=None, *args, **kwargs
    ):
        super().__init__(job_manager, *args, **kwargs)

        self.build_manager = build_manager
        self.archive_after_days = self.DEFAULT_ARCHIVE_AFTER_DAYS
        if days is not None:
            self.archive_after_days = days
        self.auto_archive_config = auto_archive_config or {}

    def configure(self):
        for build_config_name in self.build_manager.register.keys():
            archive_config = self.auto_archive_config.get(build_config_name) or {}
            schedule = archive_config.get("schedule") or self.DEFAULT_ARCHIVE_SCHEDULE
            days = archive_config.get("days")
            if days is None:
                days = self.archive_after_days

            self.job_manager.submit(
                partial(self.archive, build_config_name, days=days, dryrun=False),
                schedule=schedule,
            )

    def archive(self, build_config_name, days=None, dryrun=True):
        """
        Archive any builds which build date is older than today's date
        by "days" day.
        """
        if days is None:
            days = self.archive_after_days

        if days < 0:
            logger.info("days is negative value. So nothing to do!")
            return

        logger.info("Auto-archive builds older than %s days" % days)
        builds = self.build_manager.list_merge(build_config_name)
        today = datetime.datetime.now().astimezone()
        at_least_one = False
        for build_id in builds:
            build = self.build_manager.build_info(build_id)
            try:
                bdate = dtparser.parse(build["_meta"]["build_date"]).astimezone()
            except KeyError:
                logger.warning('Build "{}" missing "_meta" key.'.format(build_id))
                continue
            deltadate = today - bdate
            if deltadate.days > days:
                logger.info("Archiving build %s (older than %s)" % (build_id, deltadate))
                if dryrun:
                    logger.info(
                        'This is a dryrun of "archive(%s)", no real changes were made.', build_id
                    )
                else:
                    self.build_manager.archive_merge(build_id)
                at_least_one = True
        if not at_least_one:
            logger.info("Nothing to archive")
