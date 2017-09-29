import os, sys, time, datetime, json
import asyncio
from urllib.parse import urlparse, urljoin
from functools import partial

import biothings, config
biothings.config_for_app(config)

from config import DATA_ARCHIVE_ROOT
from biothings.hub.dataload.dumper import HTTPDumper, DumperException
from biothings.utils.common import gunzipall, md5sum

HUB_ENV = hasattr(config,"HUB_ENV") and config.HUB_ENV or "" # default to prod (normal)
LATEST = HUB_ENV and "%s-latest" % HUB_ENV or "latest"
VERSIONS = HUB_ENV and "%s-versions" % HUB_ENV or "versions"

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
    # App name for this biothings API. Must be set when using this dumper
    BIOTHINGS_S3_FOLDER = None

    SRC_NAME = "biothings"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)

    # URL is always the same, but headers change (template for app + version)
    SRC_URL = "http://biothings-releases.s3-website-us-west-2.amazonaws.com/%s/%s.json"

    # Auto-deploy data update ?
    AUTO_UPLOAD = False

    # Optionally, a schedule can be used to automatically check new version
    #SCHEDULE = "0 9 * * *"

    # what backend the dumper should work with. Must be defined before instantiation
    # (can be an instance or a partial() returning an instance)
    TARGET_BACKEND = None

    # TODO: should we ensure ARCHIVE is always true ?
    # (ie we have to keep all versions to apply them in order)


    def __init__(self, *args, **kwargs):
        super(BiothingsDumper,self).__init__(*args,**kwargs)
        # list of build_version to download/apply, in order
        self._target_backend = None

    @property
    def target_backend(self):
        if not self._target_backend:
            if type(self.__class__.TARGET_BACKEND) == partial:
                self._target_backend = self.__class__.TARGET_BACKEND()
            else:
                 self._target_backend = self.__class__.TARGET_BACKEND
        return self._target_backend

    def download(self,remoteurl,localfile,headers={}):
        res = super(BiothingsDumper,self).download(remoteurl,localfile,headers=headers)
        # use S3 metadata to set local mtime
        # we add 1 second to make sure we wouldn't download remoteurl again
        # because remote is older by just a few milliseconds
        lastmodified = int(res.headers["x-amz-meta-lastmodified"]) + 1
        os.utime(localfile, (lastmodified, lastmodified))

    def load_remote_json(self,url):
        res = self.client.get(url)
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

    def create_todump_list(self, force=False, version=LATEST, url=None):
        assert self.__class__.BIOTHINGS_S3_FOLDER, "BIOTHINGS_S3_FOLDER class attribute is not set"
        self.logger.info("Dumping version '%s'" % version)
        file_url = url or self.__class__.SRC_URL % (self.__class__.BIOTHINGS_S3_FOLDER,version)
        filename = os.path.basename(self.__class__.SRC_URL)
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
            if not build_meta:
                raise Exception("Can't get remote build information about version '%s' (url was '%s')" % \
                        (version,file_url))
            # latest poins to a new version, 2 options there:
            # - update is a full snapshot: nothing to download, but we need to trigger a restore
            # - update is an incremental: 
            #   * we first need to check if the incremental is compatible with current version
            #     if not, we need to find the previous update (full or incremental) compatible
            #   * if compatible, we need to download metadata file which contains the list of files
            #     we need to download and then trigger a sync using those diff files
            if build_meta["type"] == "incremental":
                # require_version contains the compatible version for which we can apply the diff
                # let's compare...
                if self.target_backend.version == build_meta["require_version"]:
                    self.logger.info("Diff update version '%s' is compatible with current version, download update" % \
                            build_meta["require_version"])
                else:
                    self.logger.info("Diff update requires version '%s' but target_backend is '%s'" % \
                            (build_meta["require_version"],self.target_backend.version))
                    # TODO: we could keep track of what's needed to update, recursively. But
                    # note sure if it's a good idea because we'd need to mix dumper and
                    # uploader processes together, orchestrate them which can be tricky.
                    # If we just let dumper and uploader works on their own, and
                    # kind of force dumper to check more regularly when we know we have more than
                    # one update to go through, we would respect dumper/uploade decoupling and things
                    # we would keep things simple (it'd be a little bit longer though)
                    # keep track on this version, we'll need to apply it later
                    self.logger.info("Now looking for a compatible version")
                    # by default we'll check directly the required version
                    required_version = build_meta["require_version"]
                    versions_url = self.__class__.SRC_URL % (self.__class__.BIOTHINGS_S3_FOLDER,VERSIONS)
                    avail_versions = self.load_remote_json(versions_url)
                    assert avail_versions["format"] == "1.0", "versions.json format has changed: %s" % avail_versions["format"]
                    if not avail_versions:
                        self.logger.error("Can't find versions information from URL %s, will try '%s'" % \
                                (versions_url,required_version))
                    else:
                        # if any of available versions end with "require_version", then it means it's compatible
                        compatibles = [v for v in avail_versions["versions"] if v["require_version"] == self.target_backend.version] #v["target_version"] == build_meta["require_version"]]
                        self.logger.info("Compatible versions from which we can apply this update: %s" % compatibles)
                        best_version = self.choose_best_version(compatibles)
                        self.logger.info("Best version found: '%s'" % best_version)
                        required_version = best_version
                    # let's get what we need
                    return self.create_todump_list(force=force,
                            version=required_version["build_version"],url=required_version["url"])
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
                for md5_fname in metadata["diff"]["files"]:
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

    @asyncio.coroutine
    def info(self,version=LATEST):
        """Display version information (release note, etc...) for given version"""
        txt = ">>> Current local version: '%s'\n" % self.target_backend.version
        txt += ">>> Release note for remote version '%s':\n" % version
        file_url = self.__class__.SRC_URL % (self.__class__.BIOTHINGS_S3_FOLDER,version)
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
        versions_url = self.__class__.SRC_URL % (self.__class__.BIOTHINGS_S3_FOLDER,VERSIONS)
        avail_versions = self.load_remote_json(versions_url)
        if not avail_versions:
            raise DumperException("Can't find any versions available...'")
        res = []
        assert avail_versions["format"] == "1.0", "versions.json format has changed: %s" % avail_versions["format"]
        for ver in avail_versions["versions"]:
            res.append("version=%s date=%s type=%s" % ('{0: <20}'.format(ver["build_version"]),'{0: <20}'.format(ver["release_date"]),
            '{0: <16}'.format(ver["type"])))
        return "\n".join(res)

