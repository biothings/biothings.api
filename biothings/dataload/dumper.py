import time
import os
from datetime import datetime

from biothings.utils.mongo import get_src_dump
from biothings.utils.common import timesofar


class BaseDumper(object):
    # override in subclass accordingly
    SRC_NAME = None
    SRC_ROOT_FOLDER = None #se  source folder (without version/dates)

    def __init__(self, no_confirm=True, archive=True):
        self.client = None
        self.logger = None
        self.src_dump = None
        self.src_doc = None
        self.no_confirm = no_confirm
        self.archive = archive
        self.to_dump = []
        self.release = None
        self.t0 = time.time()
        self.logfile = None
        self.prev_data_folder = None
        self.timestamp = time.strftime('%Y%m%d')
        # init
        self.setup_log()
        self.prepare()

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

    def setup_log(self):
        import logging as logging_mod
        if not os.path.exists(self.SRC_ROOT_FOLDER):
            os.makedirs(self.SRC_ROOT_FOLDER)
        self.logfile = os.path.join(self.SRC_ROOT_FOLDER, '%s_%s_dump.log' % (self.SRC_NAME,self.timestamp))
        fh = logging_mod.FileHandler(self.logfile)
        fh.setFormatter(logging_mod.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        fh.name = "logfile"
        sh = logging_mod.StreamHandler()
        sh.name = "logstream"
        self.logger = logging_mod.getLogger("%s_dump" % self.SRC_NAME)
        self.logger.setLevel(logging_mod.DEBUG)
        if not fh.name in [h.name for h in self.logger.handlers]:
            self.logger.addHandler(fh)
        if not sh.name in [h.name for h in self.logger.handlers]:
            self.logger.addHandler(sh)

    def prepare(self):
        self.prepare_client()
        self.prepare_src_dump()
        self.new_data_folder = self.get_new_data_folder()
        self.current_data_folder = self.src_doc.get("data_folder")
        self.to_dump = []

    def prepare_src_dump(self):
        # Mongo side
        self.src_dump = get_src_dump()
        self.src_doc = self.src_dump.find_one({'_id': self.SRC_NAME}) or {}

    def register_status(self,status,transient=False,**extra):
        self.src_doc = {'_id': self.SRC_NAME,
               'timestamp': self.timestamp,
               'data_folder': self.new_data_folder,
               'release': self.release,
               'logfile': self.logfile,
               'status': status}
        # only register time when it's a final state
        if not transient:
            self.src_doc["time"] = timesofar(self.t0)
        self.src_doc.update(extra)
        self.src_dump.save(self.src_doc)

    def dump(self,force=False):
        '''
        Dump (ie. download) resource as needed
        this should be called after instance creation
        'force' argument will force dump, passing this to
        create_todump_list() method.
        '''
        try:
            if self.need_prepare():
                self.prepare()
            self.create_todump_list(force=force)
            if self.to_dump:
                # mark the download starts
                self.register_status("downloading",transient=True)
                self.do_dump()
                self.register_status("success",pending_to_upload=True)
        except (KeyboardInterrupt,Exception) as e:
            self.logger.error("Error while dumping source: %s" % e)
            import traceback
            self.logger.error(traceback.format_exc())
            self.register_status("failed")
        finally:
            if self.client:
                self.release_client()

    def get_new_data_folder(self):
        if self.archive:
            return os.path.join(self.SRC_ROOT_FOLDER, self.timestamp)
        else:
            return os.path.join(self.SRC_ROOT_FOLDER, 'latest')

    def do_dump(self):
        self.logger.info("%d files to download" % len(self.to_dump))
        for todo in self.to_dump:
            remote = todo["remote"]
            local = todo["local"]
            self.download(remote,local)

    def prepare_local_folders(self,localfile):
        localdir = os.path.dirname(localfile)
        if not os.path.exists(localdir):
            os.makedirs(localdir)



from ftplib import FTP

class FTPDumper(BaseDumper):
    FTP_HOST = 'ftp.ncbi.nlm.nih.gov'
    CWD_DIR = '/snp/organisms'

    def prepare_client(self):
        # FTP side
        self.client = FTP(self.FTP_HOST)
        self.client.login()
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
            raise Exception("Can't find wget executable")

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

