import os, sys, time, datetime, json, re
import asyncio
from urllib.parse import urlparse, urljoin
from functools import partial
import boto3
from requests_aws4auth import AWS4Auth

from biothings import config as btconfig
from biothings.hub.dataload.dumper import HTTPDumper, DumperException
from biothings.utils.common import uncompressall, md5sum


class BiothingsDumper(HTTPDumper):
    """
    This dumper is used to maintain a BioThings API up-to-date. BioThings data
    is available as either as an ElasticSearch snapshot when full update,
    and a collection of diff files for incremental updates.
    It will either download incremental updates and apply diff, or trigger an ElasticSearch
    restore if the latest version is a full update.
    This dumper can also be configured with precedence rules: when a full and a incremental 
    update is available, rules can set so full is preferably used over incremental (size can also
    be considered when selecting the preferred way).
    """
    # URL pointing to versions.json file, this is the main entry point
    VERSION_URL = None

    # set during autohub init
    SRC_NAME = None
    SRC_ROOT_FOLDER = None

    # Auto-deploy data update ?
    AUTO_UPLOAD = False

    # Optionally, a schedule can be used to automatically check new version
    #SCHEDULE = "0 9 * * *"

    # what backend the dumper should work with. Must be defined before instantiation
    # (can be an instance or a partial() returning an instance)
    TARGET_BACKEND = None

    # TODO: should we ensure ARCHIVE is always true ?
    # (ie we have to keep all versions to apply them in order)

    # must be set before use if accessing restricted bucket
    AWS_ACCESS_KEY_ID = None
    AWS_SECRET_ACCESS_KEY = None


    def __init__(self, *args, **kwargs):
        super(BiothingsDumper,self).__init__(*args,**kwargs)
        # list of build_version to download/apply, in order
        self._target_backend = None

    @property
    def base_url(self):
        # add trailing / so urljoin won't remove folder from path
        return os.path.dirname(self.__class__.VERSION_URL) + "/"

    @property
    def target_backend(self):
        if not self._target_backend:
            if type(self.__class__.TARGET_BACKEND) == partial:
                self._target_backend = self.__class__.TARGET_BACKEND()
            else:
                 self._target_backend = self.__class__.TARGET_BACKEND
        return self._target_backend

    @asyncio.coroutine
    def get_target_backend(self):
        return {
                "host" : self.target_backend.target_esidxer.es_host,
                "index" : self.target_backend.target_name,
                "version" : self.target_backend.version,
                "count" : self.target_backend.count(),
                }

    def download(self,remoteurl,localfile,headers={}):
        self.prepare_local_folders(localfile)  
        parsed = urlparse(remoteurl)
        if self.__class__.AWS_ACCESS_KEY_ID and self.__class__.AWS_SECRET_ACCESS_KEY:
            # accessing diffs controled by auth
            key = parsed.path.strip("/") # s3 key are relative, not / at beginning
            # extract bucket name from URL (reliable?)
            pat = re.compile("^(.*?)\..*\.amazonaws.com")
            m = pat.match(parsed.netloc)
            if m:
                bucket_name = m.groups()[0]
            else:
                raise DumperException("Can't extract bucket name from URL '%s'" % remote_url)

            return self.auth_download(bucket_name,key,localfile,headers)
        else:
            return self.anonymous_download(remoteurl,localfile,headers)

    def anonymous_download(self,remoteurl,localfile,headers={}):
        res = super(BiothingsDumper,self).download(remoteurl,localfile,headers=headers)
        # use S3 metadata to set local mtime
        # we add 1 second to make sure we wouldn't download remoteurl again
        # because remote is older by just a few milliseconds
        lastmodified = int(res.headers["x-amz-meta-lastmodified"]) + 1
        os.utime(localfile, (lastmodified, lastmodified))
        return res

    def auth_download(self,bucket_name,key,localfile,headers={}):
        session = boto3.Session(
            aws_access_key_id = self.__class__.AWS_ACCESS_KEY_ID,
            aws_secret_access_key = self.__class__.AWS_SECRET_ACCESS_KEY)
        bucket = session.resource("s3").Bucket(bucket_name)
        res = bucket.download_file(key,localfile)
        return res

    def check_compat(self,build_meta):
        if hasattr(btconfig,"SKIP_CHECK_COMPAT") and btconfig.SKIP_CHECK_COMPAT:
            return

        msg = []
        for version_field in ["app_version","standalone_version","biothings_version"]:
            VERSION_FIELD = version_field.upper()
            version = build_meta.get(version_field)
            # some releases use dict (most recent) some use string
            if isinstance(version,dict):
                version = version["branch"]
            if type(version) != list:
                version = [version]
            # remove hash from versions (only useful when version is a string,
            # not a dict, see above
            version = [re.sub("( \[.*\])","",v) for v in version]
            version = set(version)
            if version == set([None]):
                raise DumperException("Remote data is too old and can't be handled with current app (%s not defined)" % version_field)
            versionfromconf = re.sub("( \[.*\])","",getattr(btconfig,VERSION_FIELD).get("branch"))
            VERSION = set()
            VERSION.add(versionfromconf)
            found_compat_version = VERSION.intersection(version)
            assert found_compat_version, "Remote data requires %s to be %s, but current app is %s" % (version_field,version,VERSION)
            msg.append("%s=%s:OK" % (version_field,version))
        self.logger.debug("Compat: %s" % ", ".join(msg))

    def load_remote_json(self,url):
        if self.__class__.AWS_ACCESS_KEY_ID:
            if ".s3-website-" in url:
                raise DumperException("Can't access s3 static website using authentication")
            # extract region from URL (reliable ?)
            pat = re.compile(".*\.(.*)\.amazonaws.com.*")
            m = pat.match(url)
            if m:
                frag = m.groups()[0]
                # looks like "s3-us-west-2"
                # whether static website is activated or not
                region = frag.replace("s3-","")
                auth = AWS4Auth(self.__class__.AWS_ACCESS_KEY_ID,
                        self.__class__.AWS_SECRET_ACCESS_KEY,
                        region,
                        's3')
                # since it's not a static website, redirections don't work (that's
                # how s3 works) so we need to deal with that manually. We allow only
                # one hop (basically, it's for latest.json file/symlink)
                res = self.client.get(url,auth=auth)
                redirect = res.headers.get('x-amz-website-redirect-location')
                if redirect:
                    parsed = urlparse(url)
                    newurl = parsed._replace(path=redirect)
                    res = self.client.get(newurl.geturl(),auth=auth)
            else:
                raise DumperException("Couldn't determine s3 region from url '%s'" % url)
        else:
            auth = None
            res = self.client.get(url,allow_redirects=True,auth=auth)
        if res.status_code != 200:
            return None
        try:
            jsondat = json.loads(res.text)
            return jsondat
        except json.JSONDecodeError:
            return None

    def compare_remote_local(self,remote_version,local_version,orig_remote_version,orig_local_version):
            # we need have to some data locally. do we already have ?
            if remote_version > local_version:
                self.logger.info("Remote version '%s' is more recent than local version '%s', download needed" % \
                        (orig_remote_version,orig_local_version))
                return True
            else:
                self.logger.info("Remote version '%s' is the same as " % orig_remote_version + \
                        "local version '%s'. " % orig_local_version + "Dump is waiting to be applied")                      
                return False

    def remote_is_better(self,remotefile,localfile):
        remote_dat = self.load_remote_json(remotefile) 
        if not remote_dat:
            self.logger.info("Couldn't find any build metadata at url '%s'" % remotefile)
            return False
        orig_remote_version = remote_dat["build_version"]

        local_dat = json.load(open(localfile))
        orig_local_version = local_dat["build_version"]

        # if diff version, we want to compatr the right part (destination version)
        # local: "3.4", backend: "4". It's actually the same (4==4)
        local_version = orig_local_version.split(".")[-1]
        remote_version = orig_remote_version.split(".")[-1]
        if remote_version != orig_remote_version:
            self.logger.debug("Remote version '%s' converted to '%s' " % (orig_remote_version,remote_version) + \
                    "(version that will be reached once incremental update has been applied)")
        if local_version != orig_local_version:
            self.logger.debug("Local version '%s' converted to '%s' " % (orig_local_version,local_version) + \
                    "(version that had been be reached using incremental update files)")

        backend_version = self.target_backend.version
        if backend_version == None:
            self.logger.info("No backend version found")
            return self.compare_remote_local(remote_version,local_version,
                    orig_remote_version,orig_local_version)
        elif remote_version > backend_version:
            self.logger.info("Remote version '%s' is more recent than backend version '%s'" % \
                    (orig_remote_version,backend_version))
            return self.compare_remote_local(remote_version,local_version,
                    orig_remote_version,orig_local_version)
        else:
            self.logger.info("Backend version '%s' is up-to-date" % backend_version)
            return False

    def choose_best_version(self,versions):
        """
        Out of all compatible versions, choose the best:
        1. choose incremental vs. full according to preferences
        2. version must be the highest (most up-to-date)
        """
        # 1st pass
        # TODO: implemente inc/full preferences, for now prefer incremental
        if not versions:
            raise DumperException("No compatible version found")
        preferreds = [v for v in versions if "." in v]
        if preferreds:
            self.logger.info("Preferred versions (according to preferences): %s" % preferreds)
            versions = preferreds
        # we can directly take the max because:
        # - version is a string
        # - format if YYYYMMDD 
        # - when incremental, it's always old_version.new_version
        return max(versions,key=lambda e: e["build_version"])

    def find_update_path(self, version, backend_version=None):
        """
        Explore available versions and find the path to update the hub up to "version",
        starting from given backend_version (typically current version found in ES index).
        If backend_version is None (typically no index yet), a complete path will be returned,
        from the last compatible "full" release up-to the latest "diff" update.
        Returned is a list of dict, where each dict is a build metadata element containing
        information about each update (see versions.json), the order of the list describes
        the order the updates should be performed.
        """
        avail_versions = self.load_remote_json(self.__class__.VERSION_URL)
        assert avail_versions["format"] == "1.0", "versions.json format has changed: %s" % avail_versions["format"]
        if version == "latest":
            version = avail_versions["versions"][-1]["build_version"]
            self.logger.info("Asking for latest version, ie. '%s'" % version)
        self.logger.info("Find update path to bring data from version '%s' up-to version '%s'" % (backend_version,version))
        file_url = urljoin(self.base_url,"%s.json" % version)
        build_meta = self.load_remote_json(file_url)
        if not build_meta:
            raise Exception("Can't get remote build information about version '%s' (url was '%s')" % \
                             (version,file_url))
        self.check_compat(build_meta)

        if build_meta["target_version"] == backend_version:
            self.logger.info("Backend is up-to-date, version '%s'" % backend_version)
            return []

        # from older to older, each required_version being compatible with the next target_version
        # except when version is a full update, ie. no previous version required (end of the path)
        path = [build_meta]

        # latest points to a new version, 2 options there:
        # - update is a full snapshot: nothing to download if type=s3, one archive to download if type=fs, we then need to trigger a restore
        # - update is an incremental, we need to check if the incremental is compatible with current version
        if build_meta["type"] == "incremental":
            # require_version contains the compatible version for which we can apply the diff
            # let's compare...
            if backend_version == build_meta["require_version"]:
                self.logger.info("Diff update '%s' requires version '%s', which is compatible with current backend version, download update" % \
                        (build_meta["build_version"],build_meta["require_version"]))
            else:
                self.logger.info("Diff '%s' update requires version '%s'" % (build_meta["build_version"],build_meta["require_version"]))
                self.logger.info("Now looking for a compatible version")
                # by default we'll check directly the required version
                required_version = build_meta["require_version"]
                compatibles = [v for v in avail_versions["versions"] if v["target_version"] == version.split(".")[0]]
                self.logger.info("Compatible versions from which we can apply this update: %s" % compatibles)
                best_version = self.choose_best_version(compatibles)
                self.logger.info("Best version found: '%s'" % best_version)
                required_version = best_version
                # keep this version as part of the update path
                # fill the path from older to newer (no extend or append)
                path = self.find_update_path(best_version["build_version"], backend_version) + path
        else:
            # full, just keep it as-is, it's a full (and it's already part of path during init, see above
            pass

        return path

    def create_todump_list(self, force=False, version="latest", url=None):
        assert self.__class__.VERSION_URL, "VERSION_URL class attribute is not set"
        self.logger.info("Dumping version '%s'" % version)
        file_url = url or urljoin(self.base_url,"%s.json" % version)
        # if "latest", we compare current json file we have (because we don't know what's behind latest)
        # otherwise json file should match version explicitely in current folder.
        version = version == "latest" and self.current_release or version
        try:
            current_localfile = os.path.join(self.current_data_folder,"%s.json" % version)
            # check it actually exists (if data folder was deleted by src_dump still refers to 
            # this folder, this file won't exist)
            if not os.path.exists(current_localfile):
                self.logger.error("Local file '%s' doesn't exist" % current_localfile)
                raise FileNotFoundError
            dump_status = self.src_doc.get("download",{}).get("status")
            if dump_status != "success":
                self.logger.error("Found dump information but status is '%s', will ignore current dump" % dump_status)
                raise TypeError
        except (TypeError, FileNotFoundError) as e:
            # current data folder doesn't even exist
            current_localfile = None
        self.logger.info("Local file: %s" % current_localfile)
        self.logger.info("Remote url : %s" % file_url)
        self.logger.info("Force: %s" % force)
        if force or current_localfile is None or self.remote_is_better(file_url,current_localfile):
            # manually get the diff meta file (ie. not using download() because we don't know the version yet,
            # it's in the diff meta
            build_meta = self.load_remote_json(file_url)
            self.check_compat(build_meta)
            if not build_meta:
                raise Exception("Can't get remote build information about version '%s' (url was '%s')" % \
                                (version,file_url))
            if build_meta["type"] == "incremental":
                self.release = build_meta["build_version"]
                # ok, now we can use download()
                # we will download it again during the normal process so we can then compare
                # when we have new data release
                new_localfile = os.path.join(self.new_data_folder,"%s.json" % self.release)
                self.to_dump.append({"remote":file_url, "local":new_localfile})
                # get base url (used later to get diff files)
                metadata_url = build_meta["metadata"]["url"]
                base_url = os.path.dirname(metadata_url) + "/" # "/" or urljoin will remove previous fragment...
                new_localfile = os.path.join(self.new_data_folder,os.path.basename(metadata_url))
                self.download(metadata_url,new_localfile)
                metadata = json.load(open(new_localfile))
                remote_files = metadata["diff"]["files"]
                if metadata["diff"]["mapping_file"]:
                    remote_files.append(metadata["diff"]["mapping_file"])
                for md5_fname in remote_files:
                    fname = md5_fname["name"]
                    p = urlparse(fname)
                    if not p.scheme:
                        # this is a relative path
                        furl = urljoin(base_url,fname)
                    else:
                        # this is a true URL
                        furl = fname
                    new_localfile = os.path.join(self.new_data_folder,os.path.basename(fname))
                    self.to_dump.append({"remote":furl, "local":new_localfile}) 

            else:
                # it's a full snapshot release, it always can be applied
                self.release = build_meta["build_version"]
                new_localfile = os.path.join(self.new_data_folder,"%s.json" % self.release)
                self.to_dump.append({"remote":file_url, "local":new_localfile})
                # if repo type is fs, we assume metadata contains url to archive
                repo_name = list(build_meta["metadata"]["repository"].keys())[0]
                if build_meta["metadata"]["repository"][repo_name]["type"] == "fs":
                    archive_url = build_meta["metadata"]["archive_url"]
                    archive = os.path.basename(archive_url)
                    new_localfile = os.path.join(self.new_data_folder,archive)
                    self.to_dump.append({"remote":archive_url, "local":new_localfile})

            # unset this one, as it may not be pickelable (next step is "download", which
            # uses different processes and need workers to be pickled)
            self._target_backend = None
            return self.release

    def post_dump(self, *args, **kwargs):
        if not self.release:
            # wasn't set before, means no need to post-process (ie. up-to-date, already done)
            return
        build_meta = json.load(open(os.path.join(self.new_data_folder,"%s.json" % self.release)))
        if build_meta["type"] == "incremental":
            self.logger.info("Checking md5sum for files in '%s'" % self.new_data_folder) 
            metadata = json.load(open(os.path.join(self.new_data_folder,"metadata.json")))
            for md5_fname in metadata["diff"]["files"]:
                spec_md5 = md5_fname["md5sum"]
                fname = md5_fname["name"]
                compute_md5 = md5sum(os.path.join(self.new_data_folder,fname))
                if compute_md5 != spec_md5:
                    self.logger.error("md5 check failed for file '%s', it may be corrupted" % fname)
                    e = DumperException("Bad md5sum for file '%s'" % fname)
                    self.register_status("failed",download={"err" : repr(e)})
                    raise e
                else:
                    self.logger.debug("md5 check success for file '%s'" % fname)
        elif build_meta["type"] == "full":
            # if type=fs, check if archive must be uncompressed
            repo_name = list(build_meta["metadata"]["repository"].keys())[0]
            if build_meta["metadata"]["repository"][repo_name]["type"] == "fs":
                uncompressall(self.new_data_folder)


    @asyncio.coroutine
    def info(self,version="latest"):
        """Display version information (release note, etc...) for given version"""
        txt = ">>> Current local version: '%s'\n" % self.target_backend.version
        txt += ">>> Release note for remote version '%s':\n" % version
        file_url = urljoin(self.base_url,"%s.json" % version)
        build_meta = self.load_remote_json(file_url)
        if not build_meta:
            raise DumperException("Can't find version '%s'" % version)
        if build_meta.get("changes") and build_meta["changes"].get("txt"):
            relnote_url = build_meta["changes"]["txt"]["url"]
            res = self.client.get(relnote_url)
            if res.status_code == 200:
                return txt + res.text
            else:
                raise DumperException("Error while downloading release note '%s': %s" % (version,res))
        else:
            return txt + "No information found for release '%s'" % version

    @asyncio.coroutine
    def versions(self):
        """Display all available versions"""
        avail_versions = self.load_remote_json(self.__class__.VERSION_URL)
        if not avail_versions:
            raise DumperException("Can't find any versions available...'")
        assert avail_versions["format"] == "1.0", "versions.json format has changed: %s" % avail_versions["format"]
        return avail_versions["versions"]

