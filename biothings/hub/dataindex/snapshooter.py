"""
Elasticsearch Snapshot Feature

Snapshot Config Example:
{
    "cloud": {
        "type": "aws",  # default, only one supported by now
        "access_key": None,
        "secret_key": None,
    },
    "repository": {
        "name": "s3-$(Y)",
        "type": "s3",
        "settings": {
            "bucket": "<SNAPSHOT_BUCKET_NAME>",
            "base_path": "mynews.info/$(Y)",  # per year
            "region": "us-west-2",
        },
        "acl": "private",
    },
    #----------------------------- inferred from build doc from now on
    "indexer": {
        # reference to INDEX_CONFIG
        "env": "local",
    },
    #-----------------------------
    # when creating a snapshot, how long should we wait before querying ES
    # to check snapshot status/completion ? (in seconds)
    "monitor_delay": 60 * 5,
}

SnapshotManager => SnapshotEnvConfig(s)
SnapshotEnvConfig + (Build) -> SnapshotEnv
SnapshotEnv + Index + Snapshot -> SnapshotTaskEnv
"""

import asyncio
import copy
import json
from functools import partial
from pprint import pformat
import time
import biothings.utils.aws as aws
try:
    from biothings import config as btconfig
    from config import logger as logging
except ImportError:
    import sys
    sys.path.insert(1, '/home/biothings/mychem.info/src')
    import config
    from biothings import config_for_app
    config_for_app(config)
    from biothings import config as btconfig
    from config import logger as logging


from biothings.hub import SNAPSHOOTER_CATEGORY, SNAPSHOTMANAGER_CATEGORY
from biothings.hub.databuild.buildconfig import AutoBuildConfig
from biothings.hub.datarelease import set_pending_to_release_note
from biothings.utils.es import ESIndexer
from biothings.utils.es import IndexerException as ESIndexerException
from biothings.utils.hub import template_out
from biothings.utils.hub_db import get_src_build
from biothings.utils.loggers import get_logger
from biothings.utils.manager import BaseManager, BaseStatusRegisterer


class BuildSpecificEnv(dict):
    """
    Snapshot Env % Build
    """

    def __init__(self, env_conf, build_doc):
        super().__init__(self._template_out_config(env_conf, build_doc))

    def _template_out_config(self, env_conf, build_doc):
        """
        Template out for special value using build_doc
        Templated values can look like:
            "base_path" : "onefolder/%(_meta.build_version)s"
        where "_meta.build_version" value is taken from build_doc dictionary
        (dot field notation).  In other words, such repo config are dynamic
        and potentially change for each index/snapshot created.
        """
        try:
            strconf = template_out(json.dumps(env_conf), build_doc)
            return json.loads(strconf)
        except Exception as exc:
            logging.exception("Coudn't template out configuration: %s", exc)
            raise

class SnapshotTaskStatusRegister(BaseStatusRegisterer):

    STATUS_NAME = {
        "pre": "pre-snapshot",
        "snapshot": "snapshot",
        "post": "post-snapshot"
    }

    def __init__(self, index_name, snapshot_name, env_conf, build_doc):

        self.index_name = index_name
        self.snapshot_name = snapshot_name
        self.build_doc = build_doc
        self.env_conf = env_conf

        self.logger, self.logfile = get_logger(
            SNAPSHOOTER_CATEGORY, btconfig.LOG_FOLDER)

    @property  # override
    def collection(self):
        return get_src_build()

    def reset_repository_info(self, snapshot_name):
        bdoc = self.build_doc
        if bdoc.get("snapshot", {}).get(snapshot_name):
            if bdoc["snapshot"][snapshot_name].pop("repository", None):
                self.collection.save(bdoc)

    # override
    def register_status(
            self, status, job_info, snapshot_info,
            transient=False, init=False, **extra):

        super().register_status(
            self.build_doc, "snapshot", status,
            transient=transient, init=init, job=job_info,
            snapshot={self.snapshot_name: snapshot_info},
            **extra
        )

    def register_start(self, step):

        return self.register_status(
            status='in progress',
            transient=True, init=True,
            job_info={"step": self.STATUS_NAME[step]},
            snapshot_info={}
        )

    def register_result(self, step, success, res):

        params = {
            'status': 'success' if success else 'failed',
            'job_info': {
                "step": self.STATUS_NAME[step],
            },
            'snapshot_info': {
                "conf": self.env_conf
            }
        }
        if success:
            params['job_info']['result'] = res
            params['snapshot_info'][step] = res
        else:
            params['job_info']['err'] = res
            params['snapshot_info'][step] = None

        return self.register_status(**params)


class SnapshotTaskProcessInfo():
    """
    Generate information about the current process.
    (used to report in the hub)
    """

    def __init__(self, task_env):
        self.source = task_env.index_name
        self.category = SNAPSHOOTER_CATEGORY,
        self.predicates = self.get_predicates()
        self.es_host = task_env.indexer.es_host

    def get_predicates(self):
        return []

    def get_pinfo(self, step, description):
        pinfo = {
            "category": self.category,
            "source": self.source,
            "step": step,
            "description": description
        }
        if self.predicates:
            pinfo["__predicates__"] = self.predicates
        return pinfo

    def get_pinfo_for(self, step):
        step_info = {
            "pre": ("pre-snapshot", None),
            "snapshot": ("snapshot", self.es_host),
            "post": ("post-snapshot", None)
        }
        return self.get_pinfo(*step_info[step])


class SnapshotTaskESStatusInterpreter():

    def __init__(self, res, task_env):

        assert "snapshots" in res,\
            "Can't find snapshot '%s' in repository '%s'" % \
            (task_env.snapshot_name, task_env.repo)

        # assuming only one index in the snapshot,
        # so only check first element
        info = res["snapshots"][0]

        assert "state" in info, \
            "Can't find state in snapshot '%s'" \
            % task_env.snapshot_name

        self.info = info
        self.state = info["state"]
        self.failed_shards = info["shards_stats"]["failed"]

    def is_running(self):
        return self.state in ("INIT", "IN_PROGRESS", "STARTED")

    def is_succeed(self):
        return self.state == "SUCCESS" and self.failed_shards == 0

    def is_partially_succeed(self):
        return self.state == "SUCCESS" and self.failed_shards != 0

class SnapshotTaskEnv():
    """
    SnapshotEnv + Index Name + Snapshot Name
    Access to status register and ES indexer.
    """

    def __init__(self, env, index, snapshot):

        self.env = env
        self.index_name = index
        self.snapshot_name = snapshot
        self.repo = env.env_config['repository']['name']

        self.status = SnapshotTaskStatusRegister(
            index, self.snapshot_name, env.env_config, env.build_doc
        )
        self.indexer = ESIndexer(
            index=index,
            doc_type=env.build_doc['index'][index]['doc_type'],
            es_host=env.build_doc['index'][index]['host'],
            check_index=index is not None
        )

    def snapshot(self):
        return self.indexer.snapshot(self.repo, self.snapshot_name)

    def wait_for_error(self):
        """
        Blocking, execute in a thread.
        """

        while True:
            info = self.indexer.get_snapshot_status(self.repo, self.snapshot_name)
            status = SnapshotTaskESStatusInterpreter(info, self)

            if status.is_running():
                time.sleep(self.env.env_config.get('monitor_delay', 60))

            elif status.is_succeed():
                logging.info(
                    "Snapshot '%s' successfully created (host: '%s', repository: '%s')" %
                    (self.snapshot_name, self.indexer.es_host, self.repo),
                    extra={"notify": True}
                )
                return None

            else:  # failed
                if status.is_partially_succeed():
                    e = ESIndexerException(
                        "Snapshot '%s' partially failed: state is %s but %s shards failed" %
                        (self.snapshot_name, status.state, status.failed_shards))
                else:
                    e = ESIndexerException(
                        "Snapshot '%s' failed: state is %s" %
                        (self.snapshot_name, status.state))

                logging.error(
                    "Failed creating snapshot '%s' (host: %s, repository: %s), state: %s" %
                    (self.snapshot_name, self.indexer.es_host, self.repo, status.state),
                    extra={"notify": True})

                return e


class SnapshotEnv():
    """
    Corresponds to an ES repository for a specific build.
    The repository type can be what are supported by ES.
    """

    def __init__(self, job_manager, env_config, build_doc):

        self.job_manager = job_manager
        self.env_config = env_config  # already build specific
        self.build_doc = build_doc

    def snapshot(self, index, snapshot=None, steps=("pre", "snapshot", "post")):
        """
        Make a snapshot of 'index', with name 'snapshot' in this env.
        Typically used to make a snapshot of an index created by the hub.
        """

        # process params
        if isinstance(steps, str):
            steps = (steps,)
        snapshot = snapshot or index

        # create a task env, so we have access to indexers, etc.
        task_env = SnapshotTaskEnv(self, index, snapshot)

        @asyncio.coroutine
        def do():
            pinfo = SnapshotTaskProcessInfo(task_env)

            # we only allow one repo conf per snapshot name
            task_env.status.reset_repository_info(snapshot)

            funcs = {
                'pre': partial(self.pre_snapshot, task_env),
                'snapshot': partial(self._snapshot, task_env),
                'post': partial(self.post_snapshot, task_env)
            }

            for step in ('pre', 'snapshot', 'post'):
                if step in steps:
                    task_env.status.register_start(step)
                    job = yield from self.job_manager.defer_to_thread(
                        pinfo.get_pinfo_for(step), funcs[step])
                    try:
                        result = yield from job
                    except Exception as e:
                        task_env.status.register_result(step, False, str(e))
                        logging.error("Error running %s.", step)
                        raise
                    else:
                        task_env.status.register_result(step, True, result)
                        logging.info("Step %s done: %s.", step, result)

            set_pending_to_release_note(self.build_doc['_id'])  # TODO: conditional

        return asyncio.ensure_future(do())

    def _snapshot(self, task_env):
        task_env.snapshot()
        logging.info("Snapshot successfully launched.")

        error = task_env.wait_for_error()
        if error:
            raise RuntimeError("Snapshot failed: %s" % error)

        return "created"

    def pre_snapshot(self, task_env):
        pass

    def post_snapshot(self, task_env):
        pass


class SnapshotFSEnv(SnapshotEnv):

    def __init__(self, job_manager, env_config, build_doc):
        super().__init__(job_manager, env_config, build_doc)
        assert env_config['repository']['type'] == 'fs'
        raise NotImplementedError

class SnapshotS3Env(SnapshotEnv):
    """
    Relevent Config Entries:
    {
        "cloud": {
            "type": "aws",  # default, only one supported by now
            "access_key": None,
            "secret_key": None,
        },
        "repository": {
            "name": "s3-$(Y)",
            "type": "s3",
            "settings": {
                "bucket": "<SNAPSHOT_BUCKET_NAME>",
                "base_path": "mynews.info/$(Y)",  # per year
                "region": "us-west-2",
            },
            "acl": "private",
        }
    }
    """

    def __init__(self, job_manager, env_config, build_doc):
        super().__init__(job_manager, env_config, build_doc)
        assert env_config['repository']['type'] == 's3'

    def pre_snapshot(self, task_env):
        """
        Ensure the destination repository is ready.
        Create the bucket and repository if necessary.
        """
        cloud = dict(self.env_config.get("cloud", {}))
        repository = dict(self.env_config.get("repository", {}))

        try:  # check if already created
            task_env.indexer.get_repository(repository["name"])

        except ESIndexerException:
            # first make sure bucket exists
            aws.create_bucket(
                name=repository["settings"]["bucket"],
                region=repository["settings"]["region"],
                aws_key=cloud.get("access_key"),
                aws_secret=cloud.get("secret_key"),
                acl=repository.get("acl", None),  # let aws.create_bucket default it
                ignore_already_exists=True
            )
            logging.info("Create repository:\n%s" % pformat(repository))
            task_env.indexer.create_repository(repository.pop("name"), repository)

        return "repo_ready"


class SnapshotEnvConfig():
    """
    Snapshot Env before Combining with Build Info.
    """

    def __init__(self, name, env_class, env_config):
        self.name = name
        self.env_class = env_class
        self.env_config = env_config

    def get_env(self, job_manager, build_doc=None):
        """
        Get build specific snapshot environment.
        """
        env = BuildSpecificEnv(self.env_config, build_doc or {})
        return self.env_class(job_manager, env, build_doc or {})

class SnapshotManager(BaseManager):

    SNAPSHOT_ENV = {
        "s3": SnapshotS3Env,
        "fs": SnapshotFSEnv
    }

    def __init__(self, index_manager, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.index_manager = index_manager
        self.snapshot_config = {}  # user specified config

    @staticmethod
    def pending_snapshot(build_name):
        src_build = get_src_build()
        src_build.update({"_id": build_name}, {"$addToSet": {"pending": "snapshot"}})

    @staticmethod
    def get_build_doc(index_name):
        src_build = get_src_build()
        doc = src_build.find_one({"index." + index_name: {"$exists": True}})
        if not doc:
            logging.error("No build associated with index %s.", index_name)
        return doc

    # override
    def clean_stale_status(self):
        src_build = get_src_build()
        for build in src_build.find():
            for job in build.get("jobs", []):
                if job.get("status", "") == "in progress":
                    logging.warning(
                        "Found stale build '%s', marking snapshot status as 'canceled'"
                        % build["_id"])
                    job["status"] = "canceled"
            src_build.replace_one({"_id": build["_id"]}, build)

    # override
    def poll(self, state, func):
        super().poll(state, func, col=get_src_build())

    def configure(self, snapshot_confdict):
        """
        Configure manager with snapshot config dict.
        See SNAPSHOT_CONFIG in config_hub.py for the format.
        """
        self.snapshot_config = copy.deepcopy(snapshot_confdict)
        for env, envconf in self.snapshot_config.get("env", {}).items():
            if envconf.get("cloud"):
                assert envconf["cloud"]["type"] == "aws", \
                    "Only Amazon AWS cloud is supported at the moment"
            repo_type = envconf.get("repository", {}).get("type")
            if not repo_type:
                raise ValueError("Repository type not specified.")
            if repo_type not in self.SNAPSHOT_ENV.keys():
                raise ValueError("Unsupported repository type %s.", repo_type)
            try:
                self.register[env] = SnapshotEnvConfig(
                    name=env,
                    env_class=self.SNAPSHOT_ENV[repo_type],
                    env_config=envconf
                )
            except Exception as e:
                logging.exception(
                    "Couldn't setup snapshot environment '%s' because: %s" %
                    (env, e))

    def snapshot(self, env, index, snapshot=None,
                 steps=("pre", "snapshot", "post")):
        """
        Create a snapshot named "snapshot" (or, by default, same name as the index)
        from "index" according to environment definition (repository, etc...) "env".
        """
        if env not in self.register:
            raise ValueError("Unknown snapshot environment '%s'." % env)
        build_doc = self.get_build_doc(index)
        if not build_doc:
            logging.warning("The index is not created by the hub.")
        env_for_build = self[env].get_env(self.job_manager, build_doc)
        return env_for_build.snapshot(index, snapshot=snapshot, steps=steps)

    def snapshot_build(self, build_doc):
        """
        Create a snapshot basing on the autobuild settings in the build config.
        If the build config associated with this build has:
        {
            "autobuild": {
                "type": "snapshot", // implied when env is set. env must be set.
                "env": "local" // which es env to make the snapshot.
            },
            ...
        }
        Attempt to make a snapshot for this build on the specified es env "local".
        """
        @asyncio.coroutine
        def _():
            autoconf = AutoBuildConfig(build_doc['build_config'])
            env = autoconf.auto_build.get('env')
            assert env, "Unknown autobuild env."
            try:
                latest_index = list(build_doc['index'].keys())[-1]
            except Exception:
                logging.info("No index already created, now create one.")
                yield from self.index_manager.index(env, build_doc['_id'])
                latest_index = build_doc['_id']
            return self.snapshot(env, latest_index)
        return asyncio.ensure_future(_())

    def snapshot_info(self, env=None, remote=False):
        return copy.deepcopy(self.snapshot_config)


def test():
    from biothings.utils.manager import JobManager
    from biothings.hub.dataindex.indexer import IndexManager
    loop = asyncio.get_event_loop()
    job_manager = JobManager(loop)
    index_manager = IndexManager(job_manager=job_manager)
    index_manager.configure(config.INDEX_CONFIG)
    snapshot_manager = SnapshotManager(
        index_manager=index_manager,
        job_manager=job_manager,
        poll_schedule="* * * * * */10"
    )
    snapshot_manager.configure(config.SNAPSHOT_CONFIG)
    # snapshot_manager.poll("snapshot",snapshot_manager.snapshot_build)

    async def test_code():
        snapshot_manager.snapshot('prod', 'mynews_202009170234_fjvg7skx', steps="post")

    asyncio.ensure_future(test_code())
    loop.run_forever()


if __name__ == '__main__':
    test()
