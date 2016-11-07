import time
import os, pprint
from datetime import datetime
import asyncio
from functools import partial

from biothings.utils.mongo import get_src_dump
from biothings.utils.common import timesofar
from config import logger as logging


class DumperException(Exception):
    pass

class BaseDumper(object):
    # override in subclass accordingly
    SRC_NAME = None
    SRC_ROOT_FOLDER = None # source folder (without version/dates)

    # attribute used to generate data folder path suffix
    SUFFIX_ATTR = "release"

    SCHEDULE = None # crontab format schedule, if None, won't be scheduled

    def __init__(self, src_name=None, src_root_folder=None, no_confirm=True, archive=True):
        # unpickable attrs, grouped
        self.init_state()
        self.src_name = src_name or self.SRC_NAME
        self.src_root_folder = src_root_folder or self.SRC_ROOT_FOLDER
        self.no_confirm = no_confirm
        self.archive = archive
        self.to_dump = []
        self.release = None
        self.t0 = time.time()
        self.logfile = None
        self.prev_data_folder = None
        self.timestamp = time.strftime('%Y%m%d')
        self.prepared = False

    def init_state(self):
        self._state = {
                "client" : None,
                "src_dump" : None,
                "logger" : None,
                "src_doc" : None,
        }
    # specific setters for attrs that can't be pickled
    # note: we can't use a generic __setattr__ as it collides
    # (infinite recursion) with __getattr__, and we can't use
    # __getattr__ as well as @x.setter required @property(x) to
    # be defined. We'll be explicit there...
    @property
    def client(self):
        if not self._state["client"]:
            self.prepare()
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

    def create_todump_list(self,force=False):
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

    def remote_is_better(self,remotefile,localfile):
        '''Compared to local file, check if remote file is worth downloading.
        (like either bigger or newer for instance)'''
        raise NotImplementedError("Define in subclass")

    def download(self,remotefile,localfile):
        """Download "remotefile' to local location defined by 'localfile'"""
        raise NotImplementedError("Define in subclass")

    def post_download(self, remotefile, localfile):
        """Placeholder to add a custom process once a file is downloaded.
        This is a good place to check file's integrity. Optional"""
        pass

    def post_dump(self):
        """
        Placeholder to add a custom process once the whole resource 
        has been dumped. Optional.
        """
        pass

    def setup_log(self):
        import logging as logging_mod
        if not os.path.exists(self.src_root_folder):
            os.makedirs(self.src_root_folder)
        self.logfile = os.path.join(self.src_root_folder, '%s_%s_dump.log' % (self.src_name,self.timestamp))
        fh = logging_mod.FileHandler(self.logfile)
        fh.setFormatter(logging_mod.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        fh.name = "logfile"
        sh = logging_mod.StreamHandler()
        sh.name = "logstream"
        self.logger = logging_mod.getLogger("%s_dump" % self.src_name)
        self.logger.setLevel(logging_mod.DEBUG)
        if not fh.name in [h.name for h in self.logger.handlers]:
            self.logger.addHandler(fh)
        if not sh.name in [h.name for h in self.logger.handlers]:
            self.logger.addHandler(sh)

    def prepare(self,state={}):
        if self.prepared:
            return
        if state:
            # let's be explicit, _state takes what it wants
            for k in self._state:
                self._state[k] = state[k]
            return
        self.prepare_client()
        self.prepare_src_dump()
        self.setup_log()

    def unprepare(self):
        """
        reset anything that's not pickable (so self can be pickled)
        return what's been reset as a dict, so self can be restored
        once pickled
        """
        state = {
            "client" : self._state["client"],
            "src_dump" : self._state["src_dump"],
            "logger" : self._state["logger"],
            "src_doc" : self._state["src_doc"],
        }
        for k in state:
            self._state[k] = None
        self.prepared = False
        return state

    def prepare_src_dump(self):
        # Mongo side
        self.src_dump = get_src_dump()
        self.src_doc = self.src_dump.find_one({'_id': self.src_name}) or {}

    def register_status(self,status,transient=False,**extra):
        self.src_doc = {
                '_id': self.src_name,
               'data_folder': self.new_data_folder,
               'release': self.release,
               'download' : {
                   'logfile': self.logfile,
                   'started_at': datetime.now(),
                   'status': status}
               }
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
    def dump(self,force=False,loop=None):
        '''
        Dump (ie. download) resource as needed
        this should be called after instance creation
        'force' argument will force dump, passing this to
        create_todump_list() method.
        '''
        # signature says it's optional but for now it's not...
        assert loop
        try:
            self.create_todump_list(force=force)
            if self.to_dump:
                # mark the download starts
                self.register_status("downloading",transient=True)
                # unsync to make it pickable
                state = self.unprepare()
                yield from self.do_dump(loop=loop)
                # then restore state
                self.prepare(state)
                self.post_dump()
                self.register_status("success",pending_to_upload=True)
        except (KeyboardInterrupt,Exception) as e:
            self.logger.error("Error while dumping source: %s" % e)
            import traceback
            self.logger.error(traceback.format_exc())
            self.register_status("failed",download={"err" : repr(e)})
        finally:
            if self.client:
                self.release_client()

    @property
    def new_data_folder(self):
        """Generate a new data folder path using src_root_folder and
        specified suffix attribute. Also sync current (aka previous) data
        folder previously registeted in database.
        This method typically has to be called in create_todump_list()
        when the dumper actually knows some information about the resource,
        like the actual release.
        """
        if not getattr(self,self.__class__.SUFFIX_ATTR):
            raise DumperException("Can't generate new data folder, attribute used to suffix (%s) isn't set" % self.__class__.SUFFIX_ATTR)
        suffix = getattr(self,self.__class__.SUFFIX_ATTR)
        if self.archive:
            return os.path.join(self.src_root_folder, suffix)
        else:
            return  os.path.join(self.src_root_folder, 'latest')

    @property
    def current_data_folder(self):
        return self.src_doc.get("data_folder") or self.new_data_folder

    @asyncio.coroutine
    def do_dump(self,loop=None):
        self.logger.info("%d file(s) to download" % len(self.to_dump))
        jobs = []
        state = self.unprepare()
        for todo in self.to_dump:
            remote = todo["remote"]
            local = todo["local"]
            def done(job):
                self.post_download(remote,local)
            job = loop.run_in_executor(None,partial(self.download,remote,local))
            job.add_done_callback(done)
            jobs.append(job)
        yield from asyncio.wait(jobs)
        self.logger.info("%s successfully downloaded" % self.SRC_NAME)
        self.to_dump = []

    def prepare_local_folders(self,localfile):
        localdir = os.path.dirname(localfile)
        if not os.path.exists(localdir):
            try:
                os.makedirs(localdir)
            except FileExistsError:
                # ignore, might exist now (parallelization occuring...)
                pass


from ftplib import FTP

class FTPDumper(BaseDumper):
    FTP_HOST = 'ftp.ncbi.nlm.nih.gov'
    CWD_DIR = '/snp/organisms'
    FTP_USER = ''
    FTP_PASSWD = ''

    def prepare_client(self):
        # FTP side
        self.client = FTP(self.FTP_HOST)
        self.client.login(self.FTP_USER,self.FTP_PASSWD)
        if self.CWD_DIR:
            self.client.cwd(self.CWD_DIR)

    def need_prepare(self):
        return not self.client or (self.client and not self.client.file)

    def release_client(self):
        assert self.client
        self.client.close()

    def download(self,remotefile,localfile):
        self.prepare_local_folders(localfile)
        self.logger.debug("Downloading '%s'" % remotefile)
        with open(localfile,"wb") as out_f:
            self.client.retrbinary('RETR %s' % remotefile, out_f.write)
        # set the mtime to match remote ftp server
        response = self.client.sendcmd('MDTM ' + remotefile)
        code, lastmodified = response.split()
        # an example: 'last-modified': '20121128150000'
        lastmodified = time.mktime(datetime.strptime(lastmodified, '%Y%m%d%H%M%S').timetuple())
        os.utime(localfile, (lastmodified, lastmodified))

    def remote_is_better(self,remotefile,localfile):
        """'remotefile' is relative path from current working dir (CWD_DIR), 
        'localfile' is absolute path"""
        res = os.stat(localfile)
        local_lastmodified = int(res.st_mtime)
        response = self.client.sendcmd('MDTM ' + remotefile)
        code, remote_lastmodified = response.split()
        remote_lastmodified = int(time.mktime(datetime.strptime(remote_lastmodified, '%Y%m%d%H%M%S').timetuple()))

        if remote_lastmodified > local_lastmodified:
            self.logger.debug("Remote file '%s' is newer (remote: %s, local: %s)" %
                    (remotefile,remote_lastmodified,local_lastmodified))
            return True
        local_size = res.st_size
        self.client.sendcmd("TYPE I")
        response = self.client.sendcmd('SIZE ' + remotefile)
        code, remote_size= map(int,response.split())
        if remote_size > local_size:
            self.logger.debug("Remote file '%s' is bigger (remote: %s, local: %s)" % (remotefile,remote_size,local_size))
            return True
        self.logger.debug("'%s' is up-to-date, no need to download" % remotefile)
        return False


import requests

class HTTPDumper(BaseDumper):
    """Dumper using HTTP protocol and "requests" library"""

    def prepare_client(self):
        self.client = requests.Session()

    def need_prepare(self):
        return not self.client

    def release_client(self):
        self.client.close()
        self.client = None

    def remote_is_better(self,remotefile,localfile):
        return True

    def download(self,remoteurl,localfile,headers={}):
        """kwargs will be passed to requests.Session.get()"""
        self.prepare_local_folders(localfile)
        self.logger.debug("Downloading '%s'" % remoteurl)
        res = self.client.get(remoteurl,stream=True,headers=headers)
        fout = open(localfile, 'wb')
        for chunk in res.iter_content(chunk_size=512 * 1024):
            if chunk:
                fout.write(chunk)
        fout.close()

class WgetDumper(BaseDumper):

    def create_todump_list(self,force=False):
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

    def remote_is_better(self,remotefile,localfile):
        return True

    def download(self,remoteurl,localfile):
        self.prepare_local_folders(localfile)
        cmdline = "wget %s -O %s" % (remoteurl, localfile)
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
        super(DummyDumper,self).__init__(archive=False, *args, **kwargs)
        self.release = "dummy"

    def prepare_client(self):
        self.logger.info("Dummy dumper, will do nothing")
        pass

    @asyncio.coroutine
    def dump(self,force=False,loop=None):
        self.logger.debug("Dummy dumper, nothing to download...")
        self.prepare_local_folders(os.path.join(self.new_data_folder,"dummy_file"))
        # this is the only interesting thing happening here
        self.post_dump()
        self.logger.info("Registering success")
        self.register_status("success",pending_to_upload=True)

class ManualDumper(BaseDumper):
    '''
    This dumper will assist user to dump a resource. It will usually expect the files
    to be downloaded first (sometimes there's no easy way to automate this process).
    Once downloaded, a call to dump() will make sure everything is fine in terms of
    files and metadata
    '''

    def __init__(self,*args,**kwargs):
        super(ManualDumper,self).__init__(*args,**kwargs)
        # overide @property, it'll be set manually in this case (ie. not dynamically generated)
        # because it's a manual dumper and user specifies data folder path explicitely
        # (and see below)
        self._new_data_folder = None

    @property
    def new_data_folder(self):
        return self._new_data_folder
    @new_data_folder.setter
    def new_data_folder(self,value):
        self._new_data_folder = value

    def prepare(self,state={}):
        self.setup_log()
        if self.prepared:
            return
        if state:
            # let's be explicit, _state takes what it wants
            for k in self._state:
                self._state[k] = state[k]
            return
        self.prepare_client()
        self.prepare_src_dump()

    def prepare_client(self):
        self.logger.info("Manual dumper, assuming data will be downloaded manually")

    @asyncio.coroutine
    def dump(self,path, release=None, force=False, loop=None):
        if os.path.isabs(path):
            self.new_data_folder = path
        elif path:
            self.new_data_folder = os.path.join(self.src_root_folder,path)
        else:
            self.new_data_folder = self.src_root_folder
        if release is None:
            # take latest path part, usually it's the release
            self.release = os.path.basename(self.new_data_folder)
        else:
            self.release = release
        # sanity check
        if not os.path.exists(self.new_data_folder):
            raise DumperException("Can't find folder '%s' (did you download data first ?)" % self.new_data_folder)
        if not os.listdir(self.new_data_folder):
            raise DumperException("Directory '%s' is empty (did you download data first ?)" % self.new_data_folder)

        self.post_dump()
        # ok, good to go
        self.register_status("success",pending_to_upload=True)
        self.logger.info("Manually dumped resource (data_folder: '%s')" % self.new_data_folder)


from urllib import parse as urlparse
from bs4 import BeautifulSoup

class GoogleDriveDumper(HTTPDumper):

    def prepare_client(self):
        super(GoogleDriveDumper,self).prepare_client()

    def remote_is_better(self,remotefile,localfile):
        return True

    def get_document_id(self,url):
        pr = urlparse.urlparse(url)
        if "drive.google.com/open" in url or "docs.google.com/uc" in url:
            q = urlparse.parse_qs(pr.query)
            doc_id = q.get("id")
            if not doc_id:
                raise DumperException("Can't extract document ID from URL '%s'" % url)
            return doc_id.pop()
        elif "drive.google.com/file" in url:
            frags = pr.path.split("/")
            ends = ["view","edit"]
            if frags[-1] in ends:
                doc_id = frags[-2]
                return doc_id
            else:
                raise DumperException("URL '%s' doesn't end with %s, can't extract document ID" % (url,ends))

        raise DumperException("Don't know how to extract document ID from URL '%s'" % url)


    def download(self,remoteurl,localfile):
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
        html = BeautifulSoup(res.text,"html.parser")
        link = html.find("a",{"id":"uc-download-link"})
        if not link:
            raise DumperException("Can't find a download link from '%s': %s" % (dl_url,html))
        href = link.get("href")
        # now build the final GET request, using cookies to simulate browser
        super(GoogleDriveDumper,self).download("https://docs.google.com" + href, localfile, headers={"cookie": res.headers["set-cookie"]})

####################

from biothings.utils.manager import BaseSourceManager

class DumperManager(BaseSourceManager):

    SOURCE_CLASS = BaseDumper

    def create_instance(self,klass):
        logging.debug("Creating new %s instance" % klass.__name__)
        return klass()

    def register_classes(self,klasses):
        for klass in klasses:
            if klass.SRC_NAME:
                self.register.setdefault(klass.SRC_NAME,[]).append(klass)
            else:
                self.register[klass.name] = klass 
            self.conn.register(klass)

    def dump_all(self,force=False,raise_on_error=False,**kwargs):
        """
        Run all dumpers, except manual ones
        """
        errors = {}
        for src in self.register:
            try:
                self.dump_src(src, force=force, skip_manual=True, **kwargs)
            except Exception as e:
                errors[src] = e
                if raise_on_error:
                    raise
        if errors:
            logging.warning("Found errors while dumping:\n%s" % pprint.pformat(errors))
            return errors

    def dump_src(self, src, force=False, skip_manual=False, schedule=False, **kwargs):
        if src in self.register:
            klasses = self.register[src]
        else:
            raise ResourceError("Can't find '%s' in registered sources (whether as main or sub-source)" % src)

        jobs = []
        try:
            for i,klass in enumerate(klasses):
                if issubclass(klass,ManualDumper) and skip_manual:
                    logging.warning("Skip %s, it's a manual dumper" % klass)
                    continue
                crontab = None
                if schedule:
                    if klass.SCHEDULE:
                        crontab = klass.SCHEDULE
                    else:
                        raise DumperException("Missing scheduling information")
                job = self.submit(partial(self.create_and_dump,klass,force=force,loop=self.loop,**kwargs),schedule=crontab)
                jobs.append(job)
            return jobs
        except Exception as e:
            logging.error("Error while dumping '%s': %s" % (src,e))
            raise

    @asyncio.coroutine
    def create_and_dump(self,klass,*args,**kwargs):
        inst = self.create_instance(klass)
        yield from inst.dump(*args,**kwargs)

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
            logging.warning("Found errors while scheduling:\n%s" % pprint.pformat(errors))
            return errors
