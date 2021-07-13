import asyncio
import cgi
import inspect
import os
import pprint
import re
import subprocess
import time
from datetime import datetime, timezone
from functools import partial
from typing import Optional, Union, List

from biothings import config as btconfig
from biothings.hub import DUMPER_CATEGORY, UPLOADER_CATEGORY
from biothings.hub.dataload.uploader import set_pending_to_upload
from biothings.utils.common import timesofar, rmdashfr
from biothings.utils.hub_db import get_src_dump
from biothings.utils.loggers import get_logger
from biothings.utils.manager import BaseSourceManager, ResourceError

logging = btconfig.logger


class DumperException(Exception):
    pass


class BaseDumper(object):
    # override in subclass accordingly
    SRC_NAME = None
    SRC_ROOT_FOLDER = None  # source folder (without version/dates)

    # Should an upload be triggered after dump ?
    AUTO_UPLOAD = True

    # attribute used to generate data folder path suffix
    SUFFIX_ATTR = "release"

    # Max parallel downloads (None = no limit).
    MAX_PARALLEL_DUMP = None
    # waiting time between download (0.0 = no waiting)
    SLEEP_BETWEEN_DOWNLOAD = 0.0

    # keep all release (True) or keep only the latest ?
    ARCHIVE = True

    SCHEDULE = None  # crontab format schedule, if None, won't be scheduled

    def __init__(self,
                 src_name=None,
                 src_root_folder=None,
                 log_folder=None,
                 archive=None):
        # unpickable attrs, grouped
        self.init_state()
        self.src_name = src_name or self.SRC_NAME
        self.src_root_folder = src_root_folder or self.SRC_ROOT_FOLDER
        self.log_folder = log_folder or btconfig.LOG_FOLDER
        self.archive = archive or self.ARCHIVE
        self.to_dump = []
        self.release = None
        self.t0 = time.time()
        self.logfile = None
        self.prev_data_folder = None
        self.timestamp = time.strftime('%Y%m%d')
        self.prepared = False
        self.steps = ["dump", "post"]

    def init_state(self):
        self._state = {
            "client": None,
            "src_dump": None,
            "logger": None,
            "src_doc": None,
        }

    # specific setters for attrs that can't be pickled
    # note: we can't use a generic __setattr__ as it collides
    # (infinite recursion) with __getattr__, and we can't use
    # __getattr__ as well as @x.setter required @property(x) to
    # be defined. We'll be explicit there...
    @property
    def client(self):
        if not self._state["client"]:
            try:
                self.prepare_client()
            except Exception as e:
                # if accessed but not ready, then just ignore and return invalid value for a client
                logging.exception("Can't prepare client: %s" % e)
                return None
        return self._state["client"]

    @property
    def src_dump(self):
        if not self._state["src_dump"]:
            self.prepare()
        return self._state["src_dump"]

    @property
    def logger(self):
        if not self._state["logger"]:
            self.prepare()
        return self._state["logger"]

    @property
    def src_doc(self):
        # this one is pickable but it's a lazy load
        # (based on non-pickable src_dump)
        if not self._state["src_doc"]:
            self.prepare()
        return self._state["src_doc"]

    @client.setter
    def client(self, value):
        self._state["client"] = value

    @src_dump.setter
    def src_dump(self, value):
        self._state["src_dump"] = value

    @logger.setter
    def logger(self, value):
        self._state["logger"] = value

    @src_doc.setter
    def src_doc(self, value):
        self._state["src_doc"] = value

    def create_todump_list(self, force=False, **kwargs):
        """Fill self.to_dump list with dict("remote":remote_path,"local":local_path)
        elements. This is the todo list for the dumper. It's a good place to
        check whether needs to be downloaded. If 'force' is True though, all files
        will be considered for download"""
        raise NotImplementedError("Define in subclass")

    def prepare_client(self):
        """do initialization to make the client ready to dump files"""
        raise NotImplementedError("Define in subclass")

    def need_prepare(self):
        """check whether some prepare step should executed
        before running dump"""

    def release_client(self):
        """Do whatever necessary (like closing network connection)
        to "release" the client"""
        raise NotImplementedError("Define in subclass")

    def remote_is_better(self, remotefile, localfile):
        '''Compared to local file, check if remote file is worth downloading.
        (like either bigger or newer for instance)'''
        raise NotImplementedError("Define in subclass")

    def download(self, remotefile, localfile):
        """
        Download "remotefile' to local location defined by 'localfile'
        Return relevant information about remotefile (depends on the actual client)
        """
        raise NotImplementedError("Define in subclass")

    def post_download(self, remotefile, localfile):
        """Placeholder to add a custom process once a file is downloaded.
        This is a good place to check file's integrity. Optional"""
        pass

    def post_dump(self, *args, **kwargs):
        """
        Placeholder to add a custom process once the whole resource
        has been dumped. Optional.
        """
        pass

    def setup_log(self):
        self.logger, self.logfile = get_logger("dump_%s" % self.src_name)

    def prepare(self, state={}):
        if self.prepared:
            return
        if state:
            # let's be explicit, _state takes what it wants
            for k in self._state:
                self._state[k] = state[k]
            return
        self.prepare_src_dump()
        self.setup_log()

    def unprepare(self):
        """
        reset anything that's not pickable (so self can be pickled)
        return what's been reset as a dict, so self can be restored
        once pickled
        """
        state = {
            "client": self._state["client"],
            "src_dump": self._state["src_dump"],
            "logger": self._state["logger"],
            "src_doc": self._state["src_doc"],
        }
        for k in state:
            self._state[k] = None
        self.prepared = False
        return state

    def prepare_src_dump(self):
        # Mongo side
        self.src_dump = get_src_dump()
        self.src_doc = self.src_dump.find_one({'_id': self.src_name}) or {}

    def register_status(self, status, transient=False, **extra):
        try:
            # if status is "failed" and depending on where it failed,
            # we may not be able to get the new_data_folder (if dumper didn't reach
            # the release information for instance). Default to current if failing
            data_folder = self.new_data_folder
        except DumperException:
            data_folder = self.current_data_folder
        release = getattr(self, self.__class__.SUFFIX_ATTR)
        if release is None:
            # it has not been set by the dumper before while exploring
            # remote site. maybe we're just running post step ?
            # back-compatibility; use "release" at root level if not found under "download"
            release = self.src_doc.get(
                "download", {}).get("release") or self.src_doc.get("release")
            self.logger.error(
                "No release set, assuming: data_folder: %s, release: %s" %
                (data_folder, release))
        # make sure to remove old "release" field to get back on track
        for field in ["release", "data_folder"]:
            if self.src_doc.get(field):
                self.logger.warning("Found '%s'='%s' at root level, convert to new format" % (field, self.src_doc[field]))
                self.src_doc.pop(field)

        self.src_doc.update({
            '_id': self.src_name,
            'download': {
                'release': release,
                'data_folder': data_folder,
                'logfile': self.logfile,
                'started_at': datetime.now().astimezone(),
                'status': status
            }
        })
        # only register time when it's a final state
        if transient:
            self.src_doc["download"]["pid"] = os.getpid()
        else:
            self.src_doc["download"]["time"] = timesofar(self.t0)
        if "download" in extra:
            self.src_doc["download"].update(extra["download"])
        else:
            self.src_doc.update(extra)
        self.src_dump.save(self.src_doc)

    @asyncio.coroutine
    def dump(self,
             steps=None,
             force=False,
             job_manager=None,
             check_only=False,
             **kwargs):
        '''
        Dump (ie. download) resource as needed
        this should be called after instance creation
        'force' argument will force dump, passing this to
        create_todump_list() method.
        '''
        # signature says it's optional but for now it's not...
        assert job_manager
        # check what to do
        self.steps = steps or self.steps
        if type(self.steps) == str:
            self.steps = [self.steps]
        strargs = "[steps=%s]" % ",".join(self.steps)
        try:
            if "dump" in self.steps:
                pinfo = self.get_pinfo()
                pinfo["step"] = "check"
                # if last download failed (or was interrupted), we want to force the dump again
                try:
                    if self.src_doc["download"]["status"] in [
                            "failed", "downloading"
                    ]:
                        self.logger.info(
                            "Forcing dump because previous failed (so let's try again)"
                        )
                        force = True
                except (AttributeError, KeyError):
                    # no src_doc or no download info
                    pass
                # TODO: blocking call for now, FTP client can't be properly set in thread after
                self.create_todump_list(force=force, **kwargs)
                # make sure we release (disconnect) client so we don't keep an open
                # connection for nothing
                self.release_client()
                if self.to_dump:
                    if check_only:
                        self.logger.info("New release available, '%s', %s file(s) to download" %
                                         (self.release, len(self.to_dump)), extra={"notify": True})
                        return self.release
                    # mark the download starts
                    self.register_status("downloading", transient=True)
                    # unsync to make it pickable
                    state = self.unprepare()
                    yield from self.do_dump(job_manager=job_manager)
                    # then restore state
                    self.prepare(state)
                else:
                    # if nothing to dump, don't do post process
                    self.logger.debug("Nothing to dump",
                                      extra={"notify": True})
                    return "Nothing to dump"
            if "post" in self.steps:
                got_error = False
                pinfo = self.get_pinfo()
                pinfo["step"] = "post_dump"
                # for some reason (like maintaining object's state between pickling).
                # we can't use process there. Need to use thread to maintain that state without
                # building an unmaintainable monster
                job = yield from job_manager.defer_to_thread(
                    pinfo, partial(self.post_dump, job_manager=job_manager))

                def postdumped(f):
                    nonlocal got_error
                    if f.exception():
                        got_error = f.exception()

                job.add_done_callback(postdumped)
                yield from job
                if got_error:
                    raise got_error
                # set it to success at the very end
                self.register_status("success")
                if self.__class__.AUTO_UPLOAD:
                    set_pending_to_upload(self.src_name)
                self.logger.info("success %s" % strargs,
                                 extra={"notify": True})
        except (KeyboardInterrupt, Exception) as e:
            self.logger.error("Error while dumping source: %s" % e)
            import traceback
            self.logger.error(traceback.format_exc())
            self.register_status("failed", download={"err": str(e), "tb": traceback.format_exc()})
            self.logger.error("failed %s: %s" % (strargs, e),
                              extra={"notify": True})
            raise
        finally:
            if self.client:
                self.release_client()

    def get_predicates(self):
        """
        Return a list of predicates (functions returning true/false, as in math logic)
        which instructs/dictates if job manager should start a job (process/thread)
        """
        def no_corresponding_uploader_running(job_manager):
            """
            Don't download data if the associated uploader is running
            """
            return len([j for j in job_manager.jobs.values() if
                       j["source"].split(".")[0] == self.src_name and j["category"] == UPLOADER_CATEGORY]) == 0

        return [no_corresponding_uploader_running]

    def get_pinfo(self):
        """
        Return dict containing information about the current process
        (used to report in the hub)
        """
        pinfo = {
            "category": DUMPER_CATEGORY,
            "source": self.src_name,
            "step": None,
            "description": None
        }
        preds = self.get_predicates()
        if preds:
            pinfo["__predicates__"] = preds
        return pinfo

    @property
    def new_data_folder(self):
        """Generate a new data folder path using src_root_folder and
        specified suffix attribute. Also sync current (aka previous) data
        folder previously registeted in database.
        This method typically has to be called in create_todump_list()
        when the dumper actually knows some information about the resource,
        like the actual release.
        """
        if self.archive:
            if getattr(
                    self,
                    self.__class__.SUFFIX_ATTR) is None:  # defined but not set
                # if step is "post" only, it means we didn't even check a new version and we
                # want to run "post" step on current version again
                if self.steps == ["post"]:
                    return self.current_data_folder
                else:
                    raise DumperException(
                        "Can't generate new data folder, attribute used for suffix (%s) isn't set"
                        % self.__class__.SUFFIX_ATTR)
            suffix = getattr(self, self.__class__.SUFFIX_ATTR)
            return os.path.join(self.src_root_folder, suffix)
        else:
            return os.path.join(self.src_root_folder, 'latest')

    @property
    def current_data_folder(self):
        try:
            return self.src_doc.get(
                "download", {}).get("data_folder") or self.new_data_folder
        except DumperException:
            # exception raied from new_data_folder generation, we give up
            return None

    @property
    def current_release(self):
        return self.src_doc.get("download", {}).get("release")

    @asyncio.coroutine
    def do_dump(self, job_manager=None):
        self.logger.info("%d file(s) to download" % len(self.to_dump))
        # should downloads be throttled ?
        max_dump = self.__class__.MAX_PARALLEL_DUMP and asyncio.Semaphore(
            self.__class__.MAX_PARALLEL_DUMP)
        courtesy_wait = self.__class__.SLEEP_BETWEEN_DOWNLOAD
        got_error = None
        jobs = []
        self.unprepare()
        for todo in self.to_dump:
            remote = todo["remote"]
            local = todo["local"]

            def done(f):
                try:
                    _ = f.result()
                    nonlocal max_dump
                    nonlocal got_error
                    if max_dump:
                        #self.logger.debug("Releasing download semaphore: %s" % max_dump)
                        max_dump.release()
                    self.post_download(remote, local)
                except Exception as e:
                    self.logger.exception("Error downloading '%s': %s" %
                                          (remote, e))
                    got_error = e

            pinfo = self.get_pinfo()
            pinfo["step"] = "dump"
            pinfo["description"] = remote
            if max_dump:
                yield from max_dump.acquire()
            if courtesy_wait:
                yield from asyncio.sleep(courtesy_wait)
            job = yield from job_manager.defer_to_process(
                pinfo, partial(self.download, remote, local))
            job.add_done_callback(done)
            jobs.append(job)
            # raise error as soon as we get it:
            # 1. it prevents from launching things for nothing
            # 2. if we gather the error at the end of the loop *and* if we
            #    have more errors than the queue size, we get stuck
            if got_error:
                raise got_error
        yield from asyncio.gather(*jobs)
        if got_error:
            raise got_error
        self.logger.info("%s successfully downloaded" % self.SRC_NAME)
        self.to_dump = []

    def prepare_local_folders(self, localfile):
        localdir = os.path.dirname(localfile)
        if not os.path.exists(localdir):
            try:
                os.makedirs(localdir)
            except FileExistsError:
                # ignore, might exist now (parallelization occuring...)
                pass


from ftplib import FTP


class FTPDumper(BaseDumper):
    FTP_HOST = ''
    CWD_DIR = ''
    FTP_USER = ''
    FTP_PASSWD = ''
    FTP_TIMEOUT = 10 * 60.0  # we want dumper to timout if necessary
    BLOCK_SIZE: Optional[int] = None  # default is still kept at 8KB

    # TODO: should we add a __del__ to make sure to close ftp connection ?
    # ftplib has a context __enter__, but we don't use it that way ("with ...")

    def _get_optimal_buffer_size(self) -> int:
        if self.BLOCK_SIZE is not None:
            return self.BLOCK_SIZE
        # else:
        known_optimal_sizes = {
            'ftp.ncbi.nlm.nih.gov': 33554432,
            # see https://ftp.ncbi.nlm.nih.gov/README.ftp for reason
            # add new ones above
            'DEFAULT': 8192,
        }
        normalized_host = self.FTP_HOST.lower()
        if normalized_host in known_optimal_sizes:
            return known_optimal_sizes[normalized_host]
        else:
            return known_optimal_sizes['DEFAULT']

    def prepare_client(self):
        # FTP side
        self.client = FTP(self.FTP_HOST, timeout=self.FTP_TIMEOUT)
        self.client.login(self.FTP_USER, self.FTP_PASSWD)
        if self.CWD_DIR:
            self.client.cwd(self.CWD_DIR)

    def need_prepare(self):
        return not self.client or (self.client and not self.client.file)

    def release_client(self):
        assert self.client
        self.client.close()
        self.client = None

    def download(self, remotefile, localfile):
        self.prepare_local_folders(localfile)
        self.logger.debug("Downloading '%s' as '%s'" % (remotefile, localfile))
        block_size = self._get_optimal_buffer_size()
        if self.need_prepare():
            self.prepare_client()
        try:
            with open(localfile, "wb") as out_f:
                self.client.retrbinary(cmd='RETR %s' % remotefile,
                                       callback=out_f.write,
                                       blocksize=block_size)
            # set the mtime to match remote ftp server
            response = self.client.sendcmd('MDTM ' + remotefile)
            code, lastmodified = response.split()
            # an example: 'last-modified': '20121128150000'
            lastmodified = time.mktime(
                datetime.strptime(lastmodified, '%Y%m%d%H%M%S').timetuple())
            os.utime(localfile, (lastmodified, lastmodified))
            return code
        except Exception as e:
            self.logger.error("Error while downloading %s: %s" %
                              (remotefile, e))
            self.release_client()
            raise
        finally:
            self.release_client()

    def remote_is_better(self, remotefile, localfile):
        """'remotefile' is relative path from current working dir (CWD_DIR),
        'localfile' is absolute path"""
        try:
            res = os.stat(localfile)
        except FileNotFoundError:
            # no local file, remote is always better
            return True
        local_lastmodified = int(res.st_mtime)
        self.logger.info("Getting modification time for '%s'" % remotefile)
        response = self.client.sendcmd('MDTM ' + remotefile)
        code, remote_lastmodified = response.split()
        remote_lastmodified = int(
            time.mktime(
                datetime.strptime(remote_lastmodified,
                                  '%Y%m%d%H%M%S').timetuple()))

        if remote_lastmodified > local_lastmodified:
            self.logger.debug(
                "Remote file '%s' is newer (remote: %s, local: %s)" %
                (remotefile, remote_lastmodified, local_lastmodified))
            return True
        local_size = res.st_size
        self.client.sendcmd("TYPE I")
        response = self.client.sendcmd('SIZE ' + remotefile)
        code, remote_size = map(int, response.split())
        if remote_size > local_size:
            self.logger.debug(
                "Remote file '%s' is bigger (remote: %s, local: %s)" %
                (remotefile, remote_size, local_size))
            return True
        self.logger.debug("'%s' is up-to-date, no need to download" %
                          remotefile)
        return False


class LastModifiedBaseDumper(BaseDumper):
    '''
    Use SRC_URLS as a list of URLs to download and
    implement create_todump_list() according to that list.
    Shoud be used in parallel with a dumper talking the
    actual underlying protocol
    '''
    SRC_URLS = []  # must be overridden in subclass

    def set_release(self):
        """
        Set self.release attribute as the last-modified datetime found in
        the last SRC_URLs element (so releae is the datetime of the last file
        to download)
        """
        raise NotImplementedError("Implement me in sub-class")

    def create_todump_list(self, force=False):
        assert type(
            self.__class__.SRC_URLS) is list, "SRC_URLS should be a list"
        assert self.__class__.SRC_URLS, "SRC_URLS list is empty"
        self.set_release()  # so we can generate new_data_folder
        for src_url in self.__class__.SRC_URLS:
            filename = os.path.basename(src_url)
            new_localfile = os.path.join(self.new_data_folder, filename)
            try:
                current_localfile = os.path.join(self.current_data_folder,
                                                 filename)
            except TypeError:
                # current data folder doesn't even exist
                current_localfile = new_localfile

            remote_better = self.remote_is_better(src_url, current_localfile)
            if force or current_localfile is None or remote_better:
                new_localfile = os.path.join(self.new_data_folder, filename)
                self.to_dump.append({
                    "remote": src_url,
                    "local": new_localfile
                })


class LastModifiedFTPDumper(LastModifiedBaseDumper):
    """
    SRC_URLS containing a list of URLs pointing to files to download,
    use FTP's MDTM command to check whether files should be downloaded
    The release is generated from the last file's MDTM in SRC_URLS, and
    formatted according to RELEASE_FORMAT.
    See also LastModifiedHTTPDumper, working the same way but for HTTP
    protocol.
    Note: this dumper is a wrapper over FTPDumper, one URL will give
    one FTPDumper instance.
    """

    RELEASE_FORMAT = "%Y-%m-%d"

    def prepare_client(self):
        pass

    def release_client(self):
        pass

    def get_client_for_url(self, url):
        split = urlparse.urlsplit(url)
        klass = type(
            "dynftpdumper", (FTPDumper, ), {
                "FTP_HOST": split.hostname,
                "CWD_DIR": "/".join(split.path.split("/")[:-1]),
                "FTP_USER": split.username or '',
                "FTP_PASSWD": split.password or '',
                "SRC_NAME": self.__class__.SRC_NAME,
                "SRC_ROOT_FOLDER": self.__class__.SRC_ROOT_FOLDER,
            })
        ftpdumper = klass()
        ftpdumper.prepare_client()
        return ftpdumper

    def get_remote_file(self, url):
        split = urlparse.urlsplit(url)
        remotef = split.path.split("/")[-1]
        return remotef

    def set_release(self):
        url = self.__class__.SRC_URLS[-1]
        ftpdumper = self.get_client_for_url(url)
        remotefile = self.get_remote_file(url)
        response = ftpdumper.client.sendcmd('MDTM ' + remotefile)
        code, lastmodified = response.split()
        lastmodified = time.mktime(
            datetime.strptime(lastmodified, '%Y%m%d%H%M%S').timetuple())
        dt = datetime.fromtimestamp(lastmodified)
        self.release = dt.strftime(self.__class__.RELEASE_FORMAT)
        ftpdumper.release_client()

    def remote_is_better(self, urlremotefile, localfile):
        ftpdumper = self.get_client_for_url(urlremotefile)
        remotefile = self.get_remote_file(urlremotefile)
        isitbetter = ftpdumper.remote_is_better(remotefile, localfile)
        ftpdumper.release_client()
        return isitbetter

    def download(self, urlremotefile, localfile, headers={}):
        ftpdumper = self.get_client_for_url(urlremotefile)
        remotefile = self.get_remote_file(urlremotefile)
        return ftpdumper.download(remotefile, localfile)


import requests


class HTTPDumper(BaseDumper):
    """Dumper using HTTP protocol and "requests" library"""

    VERIFY_CERT = True
    IGNORE_HTTP_CODE = [
    ]  # list of HTTP code to ignore in case on non-200 response
    RESOLVE_FILENAME = False  # global trigger to get filenames from headers

    # when available

    def prepare_client(self):
        self.client = requests.Session()
        self.client.verify = self.__class__.VERIFY_CERT

    def need_prepare(self):
        return not self.client

    def release_client(self):
        self.client.close()
        self.client = None

    def remote_is_better(self, remotefile, localfile):
        return True

    def download(self, remoteurl, localfile, headers={}):
        self.prepare_local_folders(localfile)
        res = self.client.get(remoteurl, stream=True, headers=headers)
        if not res.status_code == 200:
            if res.status_code in self.__class__.IGNORE_HTTP_CODE:
                self.logger.info("Remote URL gave http code %s, ignored" %
                                 (remoteurl, res.status_code))
                return
            else:
                raise DumperException("Error while downloading '%s' (status: %s, reason: %s)" %
                                      (remoteurl, res.status_code, res.reason))
        # issue biothings.api #3: take filename from header if specified
        # note: this has to explicit, either on a globa (class) level or per file to dump
        if self.__class__.RESOLVE_FILENAME and res.headers.get(
                "content-disposition"):
            parsed = cgi.parse_header(res.headers["content-disposition"])
            # looks like: ('attachment', {'filename': 'the_filename.txt'})
            if parsed and parsed[0] == "attachment" and parsed[1].get(
                    "filename"):
                # localfile is an absolute path, replace last part
                localfile = os.path.join(os.path.dirname(localfile),
                                         parsed[1]["filename"])
        self.logger.debug("Downloading '%s' as '%s'" % (remoteurl, localfile))
        fout = open(localfile, 'wb')
        for chunk in res.iter_content(chunk_size=512 * 1024):
            if chunk:
                fout.write(chunk)
        fout.close()
        return res


class LastModifiedHTTPDumper(HTTPDumper, LastModifiedBaseDumper):
    """Given a list of URLs, check Last-Modified header to see
    whether the file should be downloaded. Sub-class should only have
    to declare SRC_URLS. Optionally, another field name can be used instead
    of Last-Modified, but date format must follow RFC 2616. If that header
    doesn't exist, it will always download the data (bypass)
    The release is generated from the last file's Last-Modified in SRC_URLS, and
    formatted according to RELEASE_FORMAT.
    """
    LAST_MODIFIED = "Last-Modified"
    ETAG = "ETag"
    RELEASE_FORMAT = "%Y-%m-%d"
    RESOLVE_FILENAME = True  # resolve by default as this dumper is called

    # with a list of URLs only, without any information
    # about the local filename to store data in

    def remote_is_better(self, remotefile, localfile):
        res = self.client.head(remotefile, allow_redirects=True)
        if self.__class__.LAST_MODIFIED not in res.headers:
            self.logger.warning("Header '%s' doesn't exist, can determine if remote is better, assuming it is..." % self.__class__.LAST_MODIFIED)
            return True
        # In accordance with RFC 7231
        # The reason we are not using strptime is that it's locale sensitive
        # and changing locale and then changing it bace is not thread safe.
        # Although at the moment we are using Tornado which uses coroutines,
        # we still have threads (for reasons) and down the line we may move
        # to a different framework. The Dumper should be agnostic of what
        # it is running on. Hence parsing the dates manually
        last_modified_str = res.headers[self.LAST_MODIFIED]
        http_date_regex = re.compile(
            r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), '
            r'(?P<day>\d{2}) '
            r'(?P<month>Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) '
            r'(?P<year>\d{4}) '
            r'(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2}) '
            r'GMT'
        )
        match = http_date_regex.match(last_modified_str)
        if not match:
            self.logger.warning("Last-Modified is in an obsolete format")
            return True
        month_map = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr': 4, 'May': 5, 'Jun':6,
                     'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}
        dt_kwargs = {'tzinfo': timezone.utc}
        for k in ('year', 'day', 'hour', 'minute', 'second'):
            dt_kwargs[k] = int(match.group(k))
        dt_kwargs['month'] = month_map[match.group('month')]
        # RFC 7231 defines leap seconds
        if dt_kwargs['second'] == 60:
            dt_kwargs['second'] = 59
        dt = datetime(**dt_kwargs)
        remote_lastmodified = dt.timestamp()
        try:
            res = os.stat(localfile)
            local_lastmodified = int(res.st_mtime)
        except (FileNotFoundError, TypeError):
            return True  # doesn't even exist, need to dump
        if remote_lastmodified > local_lastmodified:
            self.logger.debug(
                "Remote file '%s' is newer (remote: %s, local: %s)" %
                (remotefile, remote_lastmodified, local_lastmodified))
            return True
        else:
            return False

    def set_release(self):
        url = self.__class__.SRC_URLS[-1]
        res = self.client.head(url, allow_redirects=True)
        for h in self.__class__.LAST_MODIFIED:
            try:
                remote_dt = datetime.strptime(
                    res.headers[self.__class__.LAST_MODIFIED],
                    '%a, %d %b %Y %H:%M:%S GMT')
                # also set release attr
                self.release = remote_dt.strftime(
                    self.__class__.RELEASE_FORMAT)
            except KeyError:
                self.release = res.headers[self.__class__.ETAG]


class WgetDumper(BaseDumper):
    def create_todump_list(self, force=False, **kwargs):
        """Fill self.to_dump list with dict("remote":remote_path,"local":local_path)
        elements. This is the todo list for the dumper. It's a good place to
        check whether needs to be downloaded. If 'force' is True though, all files
        will be considered for download"""
        raise NotImplementedError("Define in subclass")

    def prepare_client(self):
        """Check if 'wget' executable exists"""
        ret = os.system("type wget 2>&1 > /dev/null")
        if not ret == 0:
            raise DumperException("Can't find wget executable")

    def need_prepare(self):
        return False

    def release_client(self):
        pass

    def remote_is_better(self, remotefile, localfile):
        return True

    def download(self, remoteurl, localfile):
        self.prepare_local_folders(localfile)
        cmdline = "wget %s -O %s" % (remoteurl, localfile)
        return_code = os.system(cmdline)
        if return_code == 0:
            self.logger.info("Success.")
        else:
            self.logger.error("Failed with return code (%s)." % return_code)


class FilesystemDumper(BaseDumper):
    """
    This dumpers works locally and copy (or move) files to datasource folder
    """

    FS_OP = "cp"  # or 'mv' if file needs to be delete from original folder,

    # or 'ln' is a symlink should be created

    def prepare_client(self):
        """Check if 'cp' and 'mv' executable exists..."""
        for cmd in ["cp", "mv", "ln"]:
            ret = os.system("type %s 2>&1 > /dev/null" % cmd)
            if not ret == 0:
                raise DumperException("Can't find '%s' executable" % cmd)

    def need_prepare(self):
        return False

    def release_client(self):
        pass

    def remote_is_better(self, remotefile, localfile):
        res = os.stat(remotefile)
        remote_lastmodified = int(res.st_mtime)
        res = os.stat(localfile)
        local_lastmodified = int(res.st_mtime)
        if remote_lastmodified > local_lastmodified:
            return True
        else:
            return False

    def download(self, remotefile, localfile):
        self.prepare_local_folders(localfile)
        if self.__class__.FS_OP == "ln":
            cmdline = "rm -f %s && ln -s %s %s" % (localfile, remotefile,
                                                   localfile)
        else:
            cmdline = "%s -f %s %s" % (self.__class__.FS_OP, remotefile,
                                       localfile)
        return_code = os.system(cmdline)
        if return_code == 0:
            self.logger.info("Success.")
        else:
            self.logger.error("Failed with return code (%s)." % return_code)


class DummyDumper(BaseDumper):
    """DummyDumper will do nothing...
    (useful for datasources that can't be downloaded anymore
    but still need to be integrated, ie. fill src_dump, etc...)
    """
    def __init__(self, *args, **kwargs):
        # make sure we don't create empty directory each time it's launched
        # so create a non-archiving dumper
        super(DummyDumper, self).__init__(archive=False, *args, **kwargs)
        self.release = ""

    def prepare_client(self):
        self.logger.info("Dummy dumper, will do nothing")
        pass

    @asyncio.coroutine
    def dump(self, force=False, job_manager=None, *args, **kwargs):
        self.logger.debug("Dummy dumper, nothing to download...")
        self.prepare_local_folders(
            os.path.join(self.new_data_folder, "dummy_file"))
        # this is the only interesting thing happening here
        pinfo = self.get_pinfo()
        pinfo["step"] = "post_dump"
        job = yield from job_manager.defer_to_thread(
            pinfo, partial(self.post_dump, job_manager=job_manager))
        yield from asyncio.gather(job)  # consume future
        self.logger.info("Registering success")
        self.register_status("success")
        if self.__class__.AUTO_UPLOAD:
            set_pending_to_upload(self.src_name)
        self.logger.info("success", extra={"notify": True})


class ManualDumper(BaseDumper):
    '''
    This dumper will assist user to dump a resource. It will usually expect the files
    to be downloaded first (sometimes there's no easy way to automate this process).
    Once downloaded, a call to dump() will make sure everything is fine in terms of
    files and metadata
    '''
    def __init__(self, *args, **kwargs):
        super(ManualDumper, self).__init__(*args, **kwargs)
        # overide @property, it'll be set manually in this case (ie. not dynamically generated)
        # because it's a manual dumper and user specifies data folder path explicitely
        # (and see below)
        self._new_data_folder = None

    @property
    def new_data_folder(self):
        return self._new_data_folder

    @new_data_folder.setter
    def new_data_folder(self, value):
        self._new_data_folder = value

    def prepare(self, state={}):
        self.setup_log()
        if self.prepared:
            return
        if state:
            # let's be explicit, _state takes what it wants
            for k in self._state:
                self._state[k] = state[k]
            return
        self.prepare_src_dump()

    def prepare_client(self):
        self.logger.info(
            "Manual dumper, assuming data will be downloaded manually")

    @asyncio.coroutine
    def dump(self,
             path,
             release=None,
             force=False,
             job_manager=None,
             **kwargs):
        if os.path.isabs(path):
            self.new_data_folder = path
        elif path:
            self.new_data_folder = os.path.join(self.src_root_folder, path)
        else:
            self.new_data_folder = self.src_root_folder
        if release is None:
            # take latest path part, usually it's the release
            self.release = os.path.basename(self.new_data_folder)
        else:
            self.release = release
        # sanity check
        if not os.path.exists(self.new_data_folder):
            raise DumperException(
                "Can't find folder '%s' (did you download data first ?)" %
                self.new_data_folder)
        if not os.listdir(self.new_data_folder):
            raise DumperException(
                "Directory '%s' is empty (did you download data first ?)" %
                self.new_data_folder)

        pinfo = self.get_pinfo()
        pinfo["step"] = "post_dump"
        strargs = "[path=%s,release=%s]" % (self.new_data_folder, self.release)
        job = yield from job_manager.defer_to_thread(
            pinfo, partial(self.post_dump, job_manager=job_manager))
        yield from asyncio.gather(job)  # consume future
        # ok, good to go
        self.register_status("success")
        if self.__class__.AUTO_UPLOAD:
            set_pending_to_upload(self.src_name)
        self.logger.info("success %s" % strargs, extra={"notify": True})
        self.logger.info("Manually dumped resource (data_folder: '%s')" %
                         self.new_data_folder)


from urllib import parse as urlparse
from bs4 import BeautifulSoup


class GoogleDriveDumper(HTTPDumper):
    def prepare_client(self):
        # FIXME: this is not very useful...
        super(GoogleDriveDumper, self).prepare_client()

    def remote_is_better(self, remotefile, localfile):
        return True

    def get_document_id(self, url):
        pr = urlparse.urlparse(url)
        if "drive.google.com/open" in url or "docs.google.com/uc" in url:
            q = urlparse.parse_qs(pr.query)
            doc_id = q.get("id")
            if not doc_id:
                raise DumperException(
                    "Can't extract document ID from URL '%s'" % url)
            return doc_id.pop()
        elif "drive.google.com/file" in url:
            frags = pr.path.split("/")
            ends = ["view", "edit"]
            if frags[-1] in ends:
                doc_id = frags[-2]
                return doc_id
            else:
                raise DumperException(
                    "URL '%s' doesn't end with %s, can't extract document ID" %
                    (url, ends))

        raise DumperException(
            "Don't know how to extract document ID from URL '%s'" % url)

    def download(self, remoteurl, localfile):
        '''
        remoteurl is a google drive link containing a document ID, such as:
            - https://drive.google.com/open?id=<1234567890ABCDEF>
            - https://drive.google.com/file/d/<1234567890ABCDEF>/view

        It can also be just a document ID
        '''
        self.prepare_local_folders(localfile)
        if remoteurl.startswith("http"):
            doc_id = self.get_document_id(remoteurl)
        else:
            doc_id = remoteurl
        self.logger.info("Found document ID: %s" % doc_id)
        # first pass: get download URL with "confirm" code
        dl_url = "https://docs.google.com/uc?id=%s" % doc_id
        res = requests.get(dl_url)
        html = BeautifulSoup(res.text, "html.parser")
        link = html.find("a", {"id": "uc-download-link"})
        if not link:
            raise DumperException("Can't find a download link from '%s': %s" %
                                  (dl_url, html))
        href = link.get("href")
        # now build the final GET request, using cookies to simulate browser
        return super(GoogleDriveDumper, self).download(
            "https://docs.google.com" + href,
            localfile,
            headers={"cookie": res.headers["set-cookie"]})


class GitDumper(BaseDumper):
    """
    Git dumper gets data from a git repo. Repo is stored in SRC_ROOT_FOLDER
    (without versioning) and then versions/releases are fetched in
    SRC_ROOT_FOLDER/<release>
    """

    GIT_REPO_URL = None
    DEFAULT_BRANCH = None

    def _get_remote_default_branch(self) -> Optional[bytes]:
        # expect bytes to work when invoking commands via subprocess
        # git doesn't really care about the encoding of refnames, it only
        # seems to be limited by the underlying filesystem
        # (don't count on the above statement, it's just an educated guess)
        cmd = ['git', 'ls-remote', '--symref', self.GIT_REPO_URL, 'HEAD']
        try:
            # set locale to C so the output may have more reliable format
            result = subprocess.run(cmd, stdout=subprocess.PIPE,  # nosec
                                    timeout=5, check=True, env={'LC_ALL': 'C'})
            r = re.compile(rb'^ref:\s+refs\/heads\/(.*)\s+HEAD$',
                           flags=re.MULTILINE)
            m = r.match(result.stdout)
            if m is not None:
                return m[1]
        except (TimeoutError, subprocess.CalledProcessError):
            pass
        return None

    def _get_remote_branches(self) -> List[bytes]:
        cmd = ['git', 'ls-remote', '--heads', self.GIT_REPO_URL]
        ret = []
        try:
            # set locale to C so the output may have more reliable format
            result = subprocess.run(cmd, stdout=subprocess.PIPE,  # nosec
                                    timeout=5, check=True, env={'LC_ALL': 'C'})
            # user controls the URL anyways, and we don't use a shell
            # so it is safe
            r = re.compile(rb'^[0-9a-f]{40}\s+refs\/heads\/(.*)$',
                           flags=re.MULTILINE)
            for m in re.findall(r, result.stdout):
                ret.append(m)
        except (TimeoutError, subprocess.CalledProcessError):
            pass
        return ret

    def _get_default_branch(self) -> Union[bytes, str]:
        # pylint: disable=bare-except
        # TODO: check TODO in _pull regarding original exclusive use of
        #  class variable
        # Case 1, default is explicitly set
        if self.DEFAULT_BRANCH is not None:
            return self.DEFAULT_BRANCH
        try:
            # Case 2, remote HEAD exists
            branch = self._get_remote_default_branch()
            if branch is not None:
                return branch
            # Case 3, 'main' exists but not 'master'
            branches = self._get_remote_branches()
            if b'main' in branches and b'master' not in branches:
                return 'main'
        except:  # nosec  # noqa
            # fallback anything goes wrong
            pass
        # Case 4, use 'master' for compatibility reasons
        return 'master'


    def _clone(self, repourl, localdir):
        self.logger.info("git clone '%s' into '%s'" % (repourl, localdir))
        subprocess.check_call(["git", "clone", repourl, localdir])

    def _pull(self, localdir, commit):
        # fetch+merge
        self.logger.info("git pull data (commit %s) into '%s'" % (commit,localdir))
        old = os.path.abspath(os.curdir)
        try:
            os.chdir(localdir)
            # discard changes, we don't want to activate a conflit resolution session...
            cmd = ["git", "reset", "--hard", "HEAD"]
            subprocess.check_call(cmd)
            # then fetch latest code (local repo, not applied to code base yet)
            cmd = ["git", "fetch", "--all"]
            subprocess.check_call(cmd)
            if commit != "HEAD":
                # first get the latest code from repo
                # (if a newly created branch is avail in remote, we can't check it out)
                self.logger.info("git checkout to commit %s" % commit)
                cmd = ["git", "checkout", commit]
                subprocess.check_call(cmd)
            else:
                # if we were on a detached branch (due to specific commit checkout)
                # we need to make sure to go back to master (re-attach)
                # TODO: figure out why it was originally using the class
                #  variable exclusively. Changed to prefer instance varaibles.
                branch = self._get_default_branch()
                cmd = ["git", "checkout", branch]
                subprocess.check_call(cmd)
                # then merge
                cmd = ["git", "merge"]
                subprocess.check_call(cmd)
                # and then get the commit hash
                out = subprocess.check_output(["git", "rev-parse", "HEAD"])
                self.release = "HEAD (%s)" % out.decode().strip()
        finally:
            os.chdir(old)
        pass

    @property
    def new_data_folder(self):
        # we don't keep release in data folder path
        # as it's a git repo
        return self.src_root_folder

    @asyncio.coroutine
    def dump(self, release="HEAD", force=False, job_manager=None, **kwargs):
        assert self.__class__.GIT_REPO_URL, "GIT_REPO_URL is not defined"
        #assert self.__class__.ARCHIVE == False, "Git dumper can't keep multiple versions (but can move to a specific commit hash)"
        got_error = None
        self.release = release

        def do():
            do_clone = False
            if force:
                # force is also a way to clean and start from scratch
                rmdashfr(self.src_root_folder)
            if not os.path.exists(self.src_root_folder):
                # data folder doesn't even exist, no git files yet, we need to clone
                os.makedirs(self.src_root_folder)
                do_clone = True
            self.register_status("downloading", transient=True)
            if do_clone:
                self._clone(self.__class__.GIT_REPO_URL, self.src_root_folder)
            self._pull(self.src_root_folder, release)

        pinfo = self.get_pinfo()
        job = yield from job_manager.defer_to_thread(pinfo, partial(do))

        def done(f):
            nonlocal got_error
            try:
                _ = f.result()
                self.register_status("success")
            except Exception as e:
                got_error = e
                self.logger.exception("failed: %s" % e, extra={"notify": True})
                self.register_status("failed", download={"err": str(e)})
                raise

        job.add_done_callback(done)
        yield from job

    def prepare_client(self):
        """Check if 'git' executable exists"""
        ret = os.system("type git 2>&1 > /dev/null")
        if not ret == 0:
            raise DumperException("Can't find 'git' executable")

    def need_prepare(self):
        return True

    def release_client(self):
        pass

    def remote_is_better(self, remotefile, localfile):
        return True

    def download(self, remotefile, localfile):
        self.prepare_local_folders(localfile)
        cmdline = "wget %s -O %s" % (remoteurl, localfile)
        return_code = os.system(cmdline)
        if return_code == 0:
            self.logger.info("Success.")
        else:
            self.logger.error("Failed with return code (%s)." % return_code)


####################


class DumperManager(BaseSourceManager):

    SOURCE_CLASS = BaseDumper

    def get_source_ids(self):
        """Return displayable list of registered source names (not private)"""
        # skip private ones starting with __
        # skip those deriving from bt.h.autoupdate.dumper.BiothingsDumper, they're used for autohub
        # and considered internal (note: only one dumper per source, so [0])
        from biothings.hub.autoupdate.dumper import BiothingsDumper
        registered = sorted([src for src,klasses in self.register.items() if not src.startswith("__") and
                             not issubclass(klasses[0],BiothingsDumper)])
        return registered

    def __repr__(self):
        return "<%s [%d registered]: %s>" % (self.__class__.__name__, len(self.register), self.get_source_ids())

    def clean_stale_status(self):
        # not uysing mongo query capabilities as hub backend could be ES, SQLlite, etc...
        # so manually iterate
        src_dump = get_src_dump()
        srcs = src_dump.find()
        for src in srcs:
            if src.get("download", {}).get("status", None) == "downloading":
                logging.warning(
                    "Found stale datasource '%s', marking download status as 'canceled'"
                    % src["_id"])
                src["download"]["status"] = "canceled"
                src_dump.replace_one({"_id": src["_id"]}, src)

    def create_instance(self, klass):
        logging.debug("Creating new %s instance" % klass.__name__)
        inst = klass()
        return inst

    def register_classes(self, klasses):
        for klass in klasses:
            # supersede/monkey-patch klass with potiential existing conf values from DB
            btconfig.supersede(klass)
            if klass.SRC_NAME:
                if len(self.register.get(klass.SRC_NAME, [])) >= 1:
                    raise ResourceError("Can't register %s for source '%s', dumper already registered: %s" %
                                        (klass, klass.SRC_NAME, self.register[klass.SRC_NAME]))
                self.register.setdefault(klass.SRC_NAME, []).append(klass)
            else:
                try:
                    self.register[klass.name] = klass
                except AttributeError as e:
                    logging.error("Can't register class %s: %s" % (klass, e))
                    continue

    def dump_all(self, force=False, **kwargs):
        """
        Run all dumpers, except manual ones
        """
        jobs = []
        for src in self.register:
            job = self.dump_src(src, force=force, skip_manual=True, **kwargs)
            jobs.extend(job)
        return asyncio.gather(*jobs)

    def dump_src(self,
                 src,
                 force=False,
                 skip_manual=False,
                 schedule=False,
                 check_only=False,
                 **kwargs):
        if src in self.register:
            klasses = self.register[src]
        else:
            raise DumperException(
                "Can't find '%s' in registered sources (whether as main or sub-source)"
                % src)

        jobs = []
        try:
            for i, klass in enumerate(klasses):
                if issubclass(klass, ManualDumper) and skip_manual:
                    logging.warning("Skip %s, it's a manual dumper" % klass)
                    continue
                crontab = None
                if schedule:
                    if klass.SCHEDULE:
                        crontab = klass.SCHEDULE
                    else:
                        raise DumperException("Missing scheduling information")
                job = self.job_manager.submit(
                    partial(self.create_and_dump,
                            klass,
                            force=force,
                            job_manager=self.job_manager,
                            check_only=check_only,
                            **kwargs),
                    schedule=crontab)
                jobs.append(job)
            return jobs
        except Exception as e:
            logging.error("Error while dumping '%s': %s" % (src, e))
            raise

    def call(self, src, method_name, *args, **kwargs):
        """
        Create a dumper for datasource "src" and call method "method_name" on it,
        with given arguments. Used to create arbitrary calls on a dumper.
        "method_name" within dumper definition must a coroutine.
        """
        if src in self.register:
            klasses = self.register[src]
        else:
            raise DumperException(
                "Can't find '%s' in registered sources (whether as main or sub-source)"
                % src)

        jobs = []
        try:
            for i, klass in enumerate(klasses):
                pfunc = partial(self.create_and_call, klass, method_name,
                                *args, **kwargs)
                job = asyncio.ensure_future(pfunc())
                jobs.append(job)
            return jobs
        except Exception as e:
            logging.error("Error while dumping '%s': %s" % (src, e))
            raise

    @asyncio.coroutine
    def create_and_dump(self, klass, *args, **kwargs):
        inst = self.create_instance(klass)
        res = yield from inst.dump(*args, **kwargs)
        return res

    @asyncio.coroutine
    def create_and_call(self, klass, method_name, *args, **kwargs):
        inst = self.create_instance(klass)
        res = yield from getattr(inst, method_name)(*args, **kwargs)
        return res

    def schedule_all(self, raise_on_error=False, **kwargs):
        """
        Run all dumpers, except manual ones
        """
        errors = {}
        for src in self.register:
            try:
                self.dump_src(src, skip_manual=True, schedule=True, **kwargs)
            except Exception as e:
                errors[src] = e
                if raise_on_error:
                    raise
        if errors:
            logging.warning("Found errors while scheduling:\n%s" %
                            pprint.pformat(errors))
            return errors

    def source_info(self, source=None):
        src_dump = get_src_dump()
        src_ids = self.get_source_ids()
        if source:
            if source in src_ids:
                src_ids = [source]
            else:
                return None
        res = []
        for _id in src_ids:
            src = src_dump.find_one({"_id": _id}) or {}
            assert len(
                self.register[_id]
            ) == 1, "Found more than one dumper for source '%s': %s" % (
                _id, self.register[_id])
            dumper = self.register[_id][0]
            src.setdefault("download", {})
            src["download"]["dumper"] = {"name": "%s.%s" % (inspect.getmodule(dumper).__name__, dumper.__name__),
                                         "bases": ["%s.%s" % (inspect.getmodule(k).__name__, k.__name__) for k in dumper.__bases__
                                                   if inspect.getmodule(k)],
                                         "manual": issubclass(dumper, ManualDumper)}
            src["name"] = _id
            src["_id"] = _id
            res.append(src)
        if source:
            return res.pop()
        else:
            return res

    def dump_info(self):
        res = {}
        for name, klasses in self.register.items():
            res[name] = [klass.__name__ for klass in klasses]
        return res
