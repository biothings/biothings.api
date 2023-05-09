import asyncio
import copy
import datetime
import inspect
import os
import time
from functools import partial

from biothings import config
from biothings.hub import BUILDER_CATEGORY, DUMPER_CATEGORY, UPLOADER_CATEGORY
from biothings.utils.common import get_random_string, get_timestamp, timesofar
from biothings.utils.hub_db import get_src_conn, get_src_dump, get_src_master
from biothings.utils.loggers import get_logger
from biothings.utils.manager import BaseSourceManager, ResourceNotFound
from biothings.utils.storage import (
    BasicStorage,
    IgnoreDuplicatedStorage,
    MergerStorage,
    NoBatchIgnoreDuplicatedStorage,
    NoStorage,
)
from biothings.utils.version import get_source_code_info
from biothings.utils.workers import upload_worker

logging = config.logger


class ResourceNotReady(Exception):
    pass


class ResourceError(Exception):
    pass


class BaseSourceUploader(object):
    """
    Default datasource uploader. Database storage can be done
    in batch or line by line. Duplicated records aren't not allowed
    """

    # TODO: fix this delayed import
    from biothings import config

    __database__ = config.DATA_SRC_DATABASE

    # define storage strategy, override in subclass as necessary
    storage_class = BasicStorage

    # Will be override in subclasses
    # name of the resource and collection name used to store data
    # (see regex_name though for exceptions)
    name = None
    # if several resources, this one if the main name,
    # it's also the _id of the resource in src_dump collection
    # if set to None, it will be set to the value of variable "name"
    main_source = None
    # in case resource used split collections (so data is spread accross
    # different colleciton, regex_name should be specified so all those split
    # collections can be found using it (used when selecting mappers for instance)
    regex_name = None

    keep_archive = 10  # number of archived collection to keep. Oldest get dropped first.

    def __init__(self, db_conn_info, collection_name=None, log_folder=None, *args, **kwargs):
        """db_conn_info is a database connection info tuple (host,port) to fetch/store
        information about the datasource's state."""
        # non-pickable attributes (see __getattr__, prepare() and unprepare())
        self.init_state()
        self.db_conn_info = db_conn_info
        self.timestamp = datetime.datetime.now()
        self.t0 = time.time()
        # main_source at object level so it's part of pickling data
        # otherwise it won't be set properly when using multiprocessing
        # note: "name" is always defined at class level so pickle knows
        # how to restore it
        self.main_source = self.__class__.main_source or self.__class__.name
        self.log_folder = log_folder or config.LOG_FOLDER
        self.logfile = None
        self.temp_collection_name = None
        self.collection_name = collection_name or self.name
        self.data_folder = None
        self.prepared = False
        self.src_doc = {}  # will hold src_dump's doc

    @property
    def fullname(self):
        if self.main_source != self.name:
            name = "%s.%s" % (self.main_source, self.name)
        else:
            name = self.name
        return name

    @classmethod
    def create(klass, db_conn_info, *args, **kwargs):
        """
        Factory-like method, just return an instance of this uploader
        (used by SourceManager, may be overridden in sub-class to generate
        more than one instance per class, like a true factory.
        This is usefull when a resource is splitted in different collection but the
        data structure doesn't change (it's really just data splitted accros
        multiple collections, usually for parallelization purposes).
        Instead of having actual class for each split collection, factory
        will generate them on-the-fly.
        """
        return klass(db_conn_info, *args, **kwargs)

    def init_state(self):
        self._state = {
            "db": None,
            "conn": None,
            "collection": None,
            "src_dump": None,
            "logger": None,
        }

    def prepare(self, state={}):  # noqa: B006
        """Sync uploader information with database (or given state dict)"""
        if self.prepared:
            return
        if state:
            # let's be explicit, _state takes what it wants
            for k in self._state:
                self._state[k] = state[k]
            return
        self._state["conn"] = get_src_conn()
        self._state["db"] = self._state["conn"][self.__class__.__database__]
        self._state["collection"] = self._state["db"][self.collection_name]
        self._state["src_dump"] = self.prepare_src_dump()
        self._state["src_master"] = get_src_master()
        self._state["logger"], self.logfile = self.setup_log()
        self.data_folder = self.src_doc.get("download", {}).get("data_folder") or self.src_doc.get("data_folder")
        # flag ready
        self.prepared = True

    def unprepare(self):
        """
        reset anything that's not pickable (so self can be pickled)
        return what's been reset as a dict, so self can be restored
        once pickled
        """
        state = {
            "db": self._state["db"],
            "conn": self._state["conn"],
            "collection": self._state["collection"],
            "src_dump": self._state["src_dump"],
            "src_master": self._state["src_master"],
            "logger": self._state["logger"],
        }
        for k in state:
            self._state[k] = None
        self.prepared = False
        return state

    def get_predicates(self):
        """
        Return a list of predicates (functions returning true/false, as in math logic)
        which instructs/dictates if job manager should start a job (process/thread)
        """

        def no_dumper_running(job_manager):
            """
            Dumpers could change the files uploader is currently using
            """
            return (
                len(
                    [
                        j
                        for j in job_manager.jobs.values()
                        if j["source"] == self.fullname.split(".")[0] and j["category"] == DUMPER_CATEGORY
                    ]
                )
                == 0
            )

        def no_builder_running(job_manager):
            """
            Builders (mergers) read data from single datasource under control of uploader
            don't change the data while it's being used
            """
            return len([j for j in job_manager.jobs.values() if j["category"] == BUILDER_CATEGORY]) == 0

        # TODO: can't use this one below for parallized uploader
        # def no_same_uploader_running(job_manager):
        #    """
        #    Avoid collision at mongo's level (and what's the point anyway?)
        #    """
        #    return len([j for j in job_manager.jobs.values() if \
        #            j["source"] == self.fullname and j["category"] == UPLOADER_CATEGORY]) == 0
        return [no_dumper_running, no_builder_running]

    def get_pinfo(self):
        """
        Return dict containing information about the current process
        (used to report in the hub)
        """
        pinfo = {
            "category": UPLOADER_CATEGORY,
            "source": self.fullname,
            "step": "",
            "description": "",
        }
        preds = self.get_predicates()
        if preds:
            pinfo["__predicates__"] = preds
        return pinfo

    def check_ready(self, force=False):
        if not self.src_doc:
            raise ResourceNotReady(f"Missing information for source '{self.main_source}' to start upload")
        if not self.src_doc.get("download", {}).get("data_folder"):
            raise ResourceNotReady("No data folder found for resource '%s'" % self.name)
        if not force and not self.src_doc.get("download", {}).get("status") == "success":
            raise ResourceNotReady("No successful download found for resource '%s'" % self.name)
        if not os.path.exists(self.data_folder):
            raise ResourceNotReady(f"Data folder '{self.data_folder}' doesn't exist for resource '{self.name}'")
        job = self.src_doc.get("upload", {}).get("job", {}).get(self.name)
        if not force and job:
            raise ResourceNotReady(f"Resource '{self.name}' is already being uploaded (job: {job})")

    def load_data(self, data_path):
        """
        Parse data from data_path and return structure ready to be inserted in database
        In general, data_path is a folder path.
        But in parallel mode (use parallelizer option), data_path is a file path
        :param data_path: It can be a folder path or a file path
        :return: structure ready to be inserted in database
        """
        raise NotImplementedError("Implement in subclass")

    @classmethod
    def get_mapping(self):
        """Return ES mapping"""
        return {}  # default to nothing...

    def make_temp_collection(self):
        """Create a temp collection for dataloading, e.g., entrez_geneinfo_INEMO."""
        if self.temp_collection_name:
            # already set
            return
        self.temp_collection_name = self.collection_name + "_temp_" + get_random_string()
        return self.temp_collection_name

    def clean_archived_collections(self):
        # archived collections look like...
        prefix = "%s_archive_" % self.name
        cols = [c for c in self.db.collection_names() if c.startswith(prefix)]
        tmp_prefix = "%s_temp_" % self.name
        tmp_cols = [c for c in self.db.collection_names() if c.startswith(tmp_prefix)]
        # timestamp is what's after _archive_, YYYYMMDD, so we can sort it safely
        cols = sorted(cols, reverse=True)
        to_drop = cols[self.keep_archive :] + tmp_cols  # noqa: E203
        for colname in to_drop:
            self.logger.info("Cleaning old archive/temp collection '%s'" % colname)
            self.db[colname].drop()

    def switch_collection(self):
        """after a successful loading, rename temp_collection to regular collection name,
        and renaming existing collection to a temp name for archiving purpose.
        """
        if self.temp_collection_name and self.db[self.temp_collection_name].count() > 0:
            if self.collection_name in self.db.collection_names():
                # renaming existing collections
                new_name = "_".join([self.collection_name, "archive", get_timestamp(), get_random_string()])
                self.logger.info(
                    "Renaming collection '%s' to '%s' for archiving purpose." % (self.collection_name, new_name)
                )
                self.collection.rename(new_name, dropTarget=True)
            self.logger.info("Renaming collection '%s' to '%s'", self.temp_collection_name, self.collection_name)
            self.db[self.temp_collection_name].rename(self.collection_name)
        else:
            raise ResourceError("No temp collection (or it's empty)")

    def post_update_data(self, steps, force, batch_size, job_manager, **kwargs):
        """Override as needed to perform operations after
        data has been uploaded"""
        pass

    async def update_data(self, batch_size, job_manager):
        """
        Iterate over load_data() to pull data and store it
        """
        pinfo = self.get_pinfo()
        pinfo["step"] = "update_data"
        got_error = False
        self.unprepare()
        job = await job_manager.defer_to_process(
            pinfo,
            partial(
                upload_worker,
                self.fullname,
                self.__class__.storage_class,
                self.load_data,
                self.temp_collection_name,
                batch_size,
                1,  # no batch, just #1
                self.data_folder,
            ),
        )

        def uploaded(f):
            nonlocal got_error
            if type(f.result()) != int:
                got_error = Exception(f"upload error (should have a int as returned value got {repr(f.result())}")

        job.add_done_callback(uploaded)
        await job
        if got_error:
            raise got_error
        self.switch_collection()

    def generate_doc_src_master(self):
        _doc = {
            "_id": str(self.name),
            "name": self.regex_name and self.regex_name or str(self.name),
            "timestamp": datetime.datetime.now(),
        }
        # store mapping
        _map = self.__class__.get_mapping()
        if _map:
            _doc["mapping"] = _map
        # type of id being stored in these docs
        if hasattr(self.__class__, "__metadata__"):
            _doc.update(self.__class__.__metadata__)
        # try to find information about the uploader source code
        from biothings.hub.dataplugin.assistant import AssistedUploader

        if issubclass(self.__class__, AssistedUploader):
            # it's a plugin, we'll just point to the plugin folder
            src_file = self.__class__.DATA_PLUGIN_FOLDER
        else:
            src_file = inspect.getfile(self.__class__)
        info = get_source_code_info(src_file)
        if info:
            _doc.setdefault("src_meta", {}).update({"code": info})
        return _doc

    def get_current_and_new_master(self):
        new = self.generate_doc_src_master() or {}
        dkey = {"_id": new["_id"]}
        current = self.src_master.find_one(dkey) or {}
        if current.get("src_meta") != new.get("src_meta"):
            return {
                "kclass": f"{self.__class__.__module__}.{self.__class__.__name__}",
                "current": current.get("src_meta"),
                "new": new.get("src_meta"),
            }

    def update_master(self):
        _doc = self.generate_doc_src_master()
        self.save_doc_src_master(_doc)

    def save_doc_src_master(self, _doc):
        dkey = {"_id": _doc["_id"]}
        prev = self.src_master.find_one(dkey)
        if prev:
            self.src_master.update(dkey, {"$set": _doc})
        else:
            self.src_master.insert_one(_doc)

    def register_status(self, status, subkey="upload", **extra):
        """
        Register step status, ie. status for a sub-resource
        """
        upload_info = {"status": status}
        upload_info.update(extra)
        job_key = "%s.jobs.%s" % (subkey, self.name)

        # TODO: should use the same approach as Builder.register_status
        # with arguments like 'init' and 'transient'...
        if status.endswith("ing"):
            # record some "in-progress" information

            upload_info["step"] = self.name  # this is the actual collection name
            upload_info["temp_collection"] = self.temp_collection_name
            upload_info["pid"] = os.getpid()
            upload_info["logfile"] = self.logfile
            upload_info["started_at"] = datetime.datetime.now().astimezone()

            # We should use the last_success from the last upload time as a default value for the current's last_success
            # If last_success from the last upload doesn't exist or is None, and last upload's status is success,
            # the last upload's started_at will be used.
            last_upload_info = self.src_doc.get(subkey, {}).get("jobs", {}).setdefault(self.name, {})
            last_success = last_upload_info.get("last_success")
            last_status = last_upload_info.get("status")
            if not last_success and last_status == "success":
                last_success = last_upload_info.get("started_at")
            if last_success:
                upload_info["last_success"] = last_success

            self.src_dump.update_one({"_id": self.main_source}, {"$set": {job_key: upload_info}})
        else:
            # get release that's been uploaded from download part
            src_doc = self.src_dump.find_one({"_id": self.main_source}) or {}
            # back-compatibility while searching for release
            release = src_doc.get("download", {}).get("release") or src_doc.get("release")
            data_folder = src_doc.get("download", {}).get("data_folder") or src_doc.get("data_folder")
            # only register time when it's a final state
            # also, keep previous uploading information
            upd = {}
            for k, v in upload_info.items():
                upd["%s.%s" % (job_key, k)] = v
            t1 = round(time.time() - self.t0, 0)
            upd["%s.status" % job_key] = status
            upd["%s.time" % job_key] = timesofar(self.t0)
            upd["%s.time_in_s" % job_key] = t1
            upd["%s.step" % job_key] = self.name  # collection name
            upd["%s.release" % job_key] = release
            upd["%s.data_folder" % job_key] = data_folder
            # Update last success upload time only when the success
            if status == "success":
                upd["%s.last_success" % job_key] = (src_doc["upload"]["jobs"].get(self.name) or {}).get("started_at")
            self.src_dump.update_one({"_id": self.main_source}, {"$set": upd})

    async def load(
        self,
        steps=("data", "post", "master", "clean"),
        force=False,
        batch_size=10000,
        job_manager=None,
        **kwargs,
    ):
        """
        Main resource load process, reads data from doc_c using chunk sized as batch_size.
        steps defines the different processes used to laod the resource:
        - "data"   : will store actual data into single collections
        - "post"   : will perform post data load operations
        - "master" : will register the master document in src_master
        """
        try:
            # check what to do
            if isinstance(steps, tuple):
                steps = list(
                    steps
                )  # may not be necessary, but previous steps default is a list, so let's be consistent
            elif isinstance(steps, str):
                steps = steps.split(",")

            update_data = "data" in steps
            update_master = "master" in steps
            post_update_data = "post" in steps
            clean_archives = "clean" in steps
            strargs = "[steps=%s]" % ",".join(steps)
            cnt = None
            if not self.temp_collection_name:
                self.make_temp_collection()
            if self.db[self.temp_collection_name]:
                self.db[self.temp_collection_name].drop()  # drop all existing records just in case.
            # sanity check before running
            self.check_ready(force)
            self.logger.info("Uploading '%s' (collection: %s)" % (self.name, self.collection_name))
            self.register_status("uploading")
            if update_data:
                # unsync to make it pickable
                state = self.unprepare()
                cnt = await self.update_data(batch_size, job_manager, **kwargs)
                self.prepare(state)
            if update_master:
                self.update_master()
            if post_update_data:
                got_error = False
                self.unprepare()
                pinfo = self.get_pinfo()
                pinfo["step"] = "post_update_data"
                f2 = await job_manager.defer_to_thread(
                    pinfo,
                    partial(self.post_update_data, steps, force, batch_size, job_manager, **kwargs),
                )

                def postupdated(f):
                    nonlocal got_error
                    if f.exception():
                        got_error = f.exception()

                f2.add_done_callback(postupdated)
                await f2
                if got_error:
                    raise got_error
            # take the total from update call or directly from collection
            cnt = cnt or self.db[self.collection_name].count()
            if clean_archives:
                self.clean_archived_collections()
            self.register_status("success", count=cnt, err=None, tb=None)
            self.logger.info("success %s" % strargs, extra={"notify": True})
        except Exception as e:
            self.logger.exception("failed %s: %s" % (strargs, e), extra={"notify": True})
            import traceback

            self.logger.error(traceback.format_exc())
            self.register_status("failed", err=str(e), tb=traceback.format_exc())
            raise

    def prepare_src_dump(self):
        """Sync with src_dump collection, collection information (src_doc)
        Return src_dump collection"""
        src_dump = get_src_dump()
        self.src_doc = src_dump.find_one({"_id": self.main_source}) or {}
        return src_dump

    def setup_log(self):
        log_folder = os.path.join(config.LOG_FOLDER, "dataload") if config.LOG_FOLDER else None
        return get_logger("upload_%s" % self.fullname, log_folder=log_folder)

    def __getattr__(self, attr):
        """This catches access to unpicabkle attributes. If unset,
        will call sync to restore them."""
        # tricky: self._state will always exist when the instance is create
        # through __init__(). But... when pickling the instance, __setstate__
        # is used to restore attribute on an instance that's hasn't been though
        # __init__() constructor. So we raise an error here to tell pickle not
        # to restore this attribute (it'll be set after)
        if attr == "_state":
            raise AttributeError(attr)
        if attr in self._state:
            if not self._state[attr]:
                self.prepare()
            return self._state[attr]
        else:
            raise AttributeError(attr)


class NoBatchIgnoreDuplicatedSourceUploader(BaseSourceUploader):
    """Same as default uploader, but will store records and ignore if
    any duplicated error occuring (use with caution...). Storage
    is done line by line (slow, not using a batch) but preserve order
    of data in input file.
    """

    storage_class = NoBatchIgnoreDuplicatedStorage


class IgnoreDuplicatedSourceUploader(BaseSourceUploader):
    """Same as default uploader, but will store records and ignore if
    any duplicated error occuring (use with caution...). Storage
    is done using batch and unordered bulk operations.
    """

    storage_class = IgnoreDuplicatedStorage


class MergerSourceUploader(BaseSourceUploader):
    storage_class = MergerStorage


class DummySourceUploader(BaseSourceUploader):
    """
    Dummy uploader, won't upload any data, assuming data is already there
    but make sure every other bit of information is there for the overall process
    (usefull when online data isn't available anymore)
    """

    def prepare_src_dump(self):
        src_dump = get_src_dump()
        # just populate/initiate an src_dump record (b/c no dump before) if needed
        self.src_doc = src_dump.find_one({"_id": self.main_source})
        if not self.src_doc:
            src_dump.save({"_id": self.main_source})
            self.src_doc = src_dump.find_one({"_id": self.main_source})
        return src_dump

    def check_ready(self, force=False):
        # bypass checks about src_dump
        pass

    async def update_data(self, batch_size, job_manager=None, release=None):
        assert release is not None, "Dummy uploader requires 'release' argument to be specified"
        self.logger.info("Dummy uploader, nothing to upload")
        # dummy uploaders have no dumper associated b/c it's collection-only resource,
        # so fill minimum information so register_status() can set the proper release
        self.src_dump.update_one({"_id": self.main_source}, {"$set": {"download.release": release}})
        # sanity check, dummy uploader, yes, but make sure data is there
        assert self.collection.count() > 0, "No data found in collection '%s' !!!" % self.collection_name


class ParallelizedSourceUploader(BaseSourceUploader):
    def jobs(self):
        """Return list of (`*arguments`) passed to self.load_data, in order. for
        each parallelized jobs. Ex: [(x,1),(y,2),(z,3)]
        If only one argument is required, it still must be passed as a 1-element tuple
        """
        raise NotImplementedError("implement me in subclass")

    async def update_data(self, batch_size, job_manager=None):
        jobs = []
        job_params = self.jobs()
        got_error = False
        # make sure we don't use any of self reference in the following loop
        fullname = copy.deepcopy(self.fullname)
        storage_class = copy.deepcopy(self.__class__.storage_class)
        load_data = copy.deepcopy(self.load_data)
        temp_collection_name = copy.deepcopy(self.temp_collection_name)
        self.unprepare()
        # important: within this loop, "self" should never be used to make sure we don't
        # instantiate unpicklable attributes (via via autoset attributes, see prepare())
        # because there could a race condition where an error would cause self to log a statement
        # (logger is unpicklable) while at the same another job from the loop would be
        # subtmitted to job_manager causing a error due to that logger attribute)
        # in other words: once unprepared, self should never be changed until all
        # jobs are submitted
        for bnum, args in enumerate(job_params):
            pinfo = self.get_pinfo()
            pinfo["step"] = "update_data"
            pinfo["description"] = "%s" % str(args)
            job = await job_manager.defer_to_process(
                pinfo,
                partial(
                    # pickable worker
                    upload_worker,
                    # worker name
                    fullname,
                    # storage class
                    storage_class,
                    # loading func
                    load_data,
                    # dest collection name
                    temp_collection_name,
                    # batch size
                    batch_size,
                    # batch num
                    bnum,
                    # and finally *args passed to loading func
                    *args,
                ),
            )
            jobs.append(job)

            # raise error as soon as we know
            if got_error:
                raise got_error

            def batch_uploaded(f, name, batch_num):
                # important: don't even use "self" ref here to make sure jobs can be submitted
                # (see comment above, before loop)
                nonlocal got_error
                try:
                    if type(f.result()) != int:
                        got_error = Exception(
                            "Batch #%s failed while uploading source '%s' [%s]" % (batch_num, name, f.result())
                        )
                except Exception as e:
                    got_error = e

            job.add_done_callback(partial(batch_uploaded, name=fullname, batch_num=bnum))
        if jobs:
            await asyncio.gather(*jobs)
            if got_error:
                raise got_error
            self.switch_collection()
            self.clean_archived_collections()


class NoDataSourceUploader(BaseSourceUploader):
    """
    This uploader won't upload any data and won't even assume
    there's actual data (different from DummySourceUploader on this point).
    It's usefull for instance when mapping need to be stored (get_mapping())
    but data doesn't comes from an actual upload (ie. generated)
    """

    storage_class = NoStorage

    async def update_data(self, batch_size, job_manager=None):
        self.logger.debug("No data to upload, skip")


class UploaderManager(BaseSourceManager):
    """
    After registering datasources, manager will orchestrate source uploading.
    """

    SOURCE_CLASS = BaseSourceUploader

    def __init__(self, poll_schedule=None, *args, **kwargs):
        super(UploaderManager, self).__init__(*args, **kwargs)
        self.poll_schedule = poll_schedule

    def get_source_ids(self):
        """Return displayable list of registered source names (not private)"""
        # skip private ones starting with __
        # skip those deriving from bt.h.autoupdate.uploader.BiothingsUploader, they're used for autohub
        # and considered internal (note: while there could be more than 1 uploader per source, when it's
        # an autoupdate one, there's only one, so [0])
        from biothings.hub.autoupdate.uploader import BiothingsUploader  # prevent circular imports

        registered = sorted(
            [
                src
                for src, klasses in self.register.items()
                if not src.startswith("__") and not issubclass(klasses[0], BiothingsUploader)
            ]
        )
        return registered

    def __repr__(self):
        return "<%s [%d registered]: %s>" % (
            self.__class__.__name__,
            len(self.register),
            self.get_source_ids(),
        )

    def clean_stale_status(self):
        src_dump = get_src_dump()
        srcs = src_dump.find()
        for src in srcs:
            jobs = src.get("upload", {}).get("jobs", {})
            dirty = False
            for subsrc in jobs:
                if jobs[subsrc].get("status") == "uploading":
                    logging.warning("Found stale datasource '%s', marking upload status as 'canceled'", src["_id"])
                    jobs[subsrc]["status"] = "canceled"
                    dirty = True
            if dirty:
                src_dump.replace_one({"_id": src["_id"]}, src)

    def filter_class(self, klass):
        if klass.name is None:
            # usually a base defined in an uploader, which then is subclassed in same
            # module. Kind of intermediate, not fully functional class
            logging.debug("%s has no 'name' defined, skip it" % klass)
            return None
        else:
            return klass

    def create_instance(self, klass):
        inst = klass.create(db_conn_info=self.conn.address)
        return inst

    def register_classes(self, klasses):
        for klass in klasses:
            config.supersede(klass)  # monkey-patch from DB
            if klass.main_source:
                self.register.setdefault(klass.main_source, []).append(klass)
            else:
                self.register.setdefault(klass.name, []).append(klass)

    def upload_all(self, raise_on_error=False, **kwargs):
        """
        Trigger upload processes for all registered resources.
        `**kwargs` are passed to upload_src() method
        """
        jobs = []
        for src in self.register:
            job = self.upload_src(src, **kwargs)
            jobs.extend(job)
        return asyncio.gather(*jobs)

    def upload_src(self, src, *args, **kwargs):
        """
        Trigger upload for registered resource named 'src'.
        Other args are passed to uploader's load() method
        """
        try:
            klasses = self[src]
        except KeyError:
            raise ResourceNotFound(f"Can't find '{src}' in registered sources (whether as main or sub-source)")

        jobs = []
        try:
            for _, klass in enumerate(klasses):
                kwargs["job_manager"] = self.job_manager
                job = self.job_manager.submit(
                    # partial(self.create_and_load, klass, job_manager=self.job_manager, *args, **kwargs)
                    partial(self.create_and_load, klass, *args, **kwargs)  # Fix Flake8 B026
                )
                jobs.append(job)
            tasks = asyncio.gather(*jobs)

            def done(f):
                try:
                    # just consume the result to raise exception
                    # if there were an error... (what an api...)
                    f.result()
                    logging.info("success", extra={"notify": True})
                except Exception as e:
                    logging.exception("failed: %s" % e, extra={"notify": True})

            tasks.add_done_callback(done)
            return jobs
        except Exception as e:
            logging.exception("Error while uploading '%s': %s" % (src, e), extra={"notify": True})
            raise

    def update_source_meta(self, src, dry=False):
        """
        Trigger update for registered resource named 'src'.
        """
        try:
            klasses = self[src]
        except KeyError:
            raise ResourceNotFound(f"Can't find '{src}' in registered sources (whether as main or sub-source)" % src)

        jobs = []
        try:
            for _, klass in enumerate(klasses):
                job = self.job_manager.submit(partial(self.create_and_update_master, klass, dry=dry))
                jobs.append(job)
            tasks = asyncio.gather(*jobs)

            def done(f):
                try:
                    # just consume the result to raise exception
                    # if there were an error... (what an api...)
                    f.result()
                    logging.info("success", extra={"notify": True})
                except Exception as e:
                    logging.exception("failed: %s" % e, extra={"notify": True})

            tasks.add_done_callback(done)
            return jobs
        except Exception as e:
            logging.exception("Error while update src meta '%s': %s" % (src, e), extra={"notify": True})
            raise

    async def create_and_update_master(self, klass, dry=False):
        compare_data = None
        inst = self.create_instance(klass)
        inst.prepare()
        if dry:
            compare_data = inst.get_current_and_new_master()
        else:
            inst.update_master()
        inst.unprepare()
        return compare_data

    async def create_and_load(self, klass, *args, **kwargs):
        insts = self.create_instance(klass)
        if type(insts) != list:
            insts = [insts]
        for inst in insts:
            await inst.load(*args, **kwargs)

    def poll(self, state, func):
        super(UploaderManager, self).poll(state, func, col=get_src_dump())

    def source_info(self, source=None):
        src_dump = get_src_dump()
        src_ids = self.get_source_ids()
        if source:
            if source in src_ids:
                src_ids = [source]
            else:
                return None
        res = []
        cur = src_dump.find({"_id": {"$in": src_ids}})
        bysrcs = {}
        [bysrcs.setdefault(src["_id"], src) for src in cur]
        for _id in src_ids:
            src = bysrcs.get(_id, {})
            uploaders = self.register[_id]
            src.setdefault("upload", {})
            for uploader in uploaders:
                upl = {
                    "name": "%s.%s" % (inspect.getmodule(uploader).__name__, uploader.__name__),
                    "bases": [
                        "%s.%s" % (inspect.getmodule(k).__name__, k.__name__)
                        for k in uploader.__bases__
                        if inspect.getmodule(k)
                    ],
                    "dummy": issubclass(uploader, DummySourceUploader),
                }
                src["upload"].setdefault("jobs", {}).setdefault(uploader.name, {})
                src["upload"]["jobs"][uploader.name]["uploader"] = upl
            src["name"] = _id
            src["_id"] = _id
            res.append(src)
        if source:
            if res:
                return res.pop()
            else:
                # no information, just return what was passed to honor return type
                # + minimal information
                return {"name": source, "_id": source}
        else:
            return res

    def upload_info(self):
        res = {}
        for name, klasses in self.register.items():
            res[name] = [klass.__name__ for klass in klasses]
        return res


def set_pending_to_upload(src_name):
    src_dump = get_src_dump()
    src_dump.update({"_id": src_name}, {"$addToSet": {"pending": "upload"}})
