import json
import os
import re
from functools import partial
from urllib.parse import urljoin, urlparse

import boto3
import requests
from requests_aws4auth import AWS4Auth

from biothings import config as btconfig
from biothings.hub.dataload.dumper import DumperException, HTTPDumper
from biothings.utils.common import md5sum, uncompressall


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
    # SCHEDULE = "0 9 * * *"

    # what backend the dumper should work with. Must be defined before instantiation
    # (can be an instance or a partial() returning an instance)
    TARGET_BACKEND = None

    # TODO: should we ensure ARCHIVE is always true ?
    # (ie we have to keep all versions to apply them in order)

    # must be set before use if accessing restricted bucket
    AWS_ACCESS_KEY_ID = None
    AWS_SECRET_ACCESS_KEY = None

    def __init__(self, *args, **kwargs):
        super(BiothingsDumper, self).__init__(*args, **kwargs)
        # list of build_version to download/apply, in order
        self._target_backend = None

    def prepare_client(self):
        """
        Depending on presence of credentials, inject authentication
        in client.get()
        """
        super().prepare_client()
        if self.__class__.AWS_ACCESS_KEY_ID and self.__class__.AWS_SECRET_ACCESS_KEY:
            self._client = requests.Session()
            self._client.verify = self.__class__.VERIFY_CERT

            def auth_get(url, *args, **kwargs):
                if ".s3-website-" in url:
                    raise DumperException("Can't access s3 static website using authentication")
                # extract region from URL (reliable ?)
                pat = re.compile(r"https?://(.*)\.(.*)\.amazonaws.com.*")
                m = pat.match(url)
                if m:
                    bucket_name, frag = m.groups()
                    # looks like "s3-us-west-2"
                    # whether static website is activated or not
                    region = frag.replace("s3-", "")
                    if region == "s3":  # url doesn't contain a region, we need to query the bucket
                        s3client = boto3.client(
                            "s3",
                            aws_access_key_id=self.__class__.AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=self.__class__.AWS_SECRET_ACCESS_KEY,
                        )
                        bucket_info = s3client.get_bucket_location(Bucket=bucket_name)
                        region = bucket_info["LocationConstraint"]
                    auth = AWS4Auth(
                        self.__class__.AWS_ACCESS_KEY_ID, self.__class__.AWS_SECRET_ACCESS_KEY, region, "s3"
                    )
                    return self._client.get(url, auth=auth, *args, **kwargs)
                else:
                    raise DumperException(f"Couldn't determine s3 region from url '{url}'")

            self.client.get = auth_get

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

    async def get_target_backend(self):
        """
        Example:
        [{
            'host': 'es6.mygene.info:9200',
            'index': 'mygene_allspecies_20200823_ufkwdv79',
            'index_alias': 'mygene_allspecies',
            'version': '20200906',
            'count': 38729977
        }]
        """

        async def do():
            cnt = self.target_backend.count()
            return {
                "host": self.target_backend.target_esidxer.es_host,
                "index": self.target_backend.target_name,
                "index_alias": self.target_backend.target_alias,
                "version": self.target_backend.version,
                "count": cnt,
            }

        result = await do()
        return result

    async def reset_target_backend(self):
        async def do():
            if self.target_backend.target_esidxer.exists_index():
                self.target_backend.target_esidxer.delete_index()

        await do()

    def download(self, remoteurl, localfile, headers=None):
        headers = headers or {}
        self.prepare_local_folders(localfile)
        parsed = urlparse(remoteurl)
        if self.__class__.AWS_ACCESS_KEY_ID and self.__class__.AWS_SECRET_ACCESS_KEY:
            # accessing diffs controled by auth
            key = parsed.path.strip("/")  # s3 key are relative, not / at beginning
            # extract bucket name from URL (reliable?)
            pat = re.compile(r"^(.*?)\..*\.amazonaws.com")
            m = pat.match(parsed.netloc)
            if m:
                bucket_name = m.groups()[0]
            else:
                raise DumperException(f"Can't extract bucket name from URL '{remoteurl}'")

            return self.auth_download(bucket_name, key, localfile, headers)
        else:
            return self.anonymous_download(remoteurl, localfile, headers)

    def anonymous_download(self, remoteurl, localfile, headers=None):
        headers = headers or {}
        res = super(BiothingsDumper, self).download(remoteurl, localfile, headers=headers)
        # use S3 metadata to set local mtime
        # we add 1 second to make sure we wouldn't download remoteurl again
        # because remote is older by just a few milliseconds
        lastmodified = int(res.headers["x-amz-meta-lastmodified"]) + 1
        os.utime(localfile, (lastmodified, lastmodified))
        return res

    def auth_download(self, bucket_name, key, localfile, headers=None):
        headers = headers or {}
        session = boto3.Session(
            aws_access_key_id=self.__class__.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=self.__class__.AWS_SECRET_ACCESS_KEY,
        )
        bucket = session.resource("s3").Bucket(bucket_name)
        res = bucket.download_file(key, localfile)
        return res

    def check_compat(self, build_meta):
        if hasattr(btconfig, "SKIP_CHECK_COMPAT") and btconfig.SKIP_CHECK_COMPAT:
            return

        msg = []
        for version_field in ["app_version", "standalone_version", "biothings_version"]:
            VERSION_FIELD = version_field.upper()
            version = build_meta.get(version_field)
            assert version is not None, "Version field '%s' is None" % VERSION_FIELD
            # some releases use dict (most recent) some use string
            if isinstance(version, dict):
                version = version["branch"]
            if type(version) != list:
                version = [version]
            # remove hash from versions (only useful when version is a string,
            # not a dict, see above
            version = [re.sub(r"( \[.*\])", "", v) for v in version]
            version = set(version)
            if version == set([None]):
                raise DumperException(
                    f"Remote data is too old and can't be handled with current app ({version_field} not defined)"
                )
            versionfromconf = re.sub(r"( \[.*\])", "", getattr(btconfig, VERSION_FIELD).get("branch"))
            VERSION = set()
            VERSION.add(versionfromconf)
            found_compat_version = VERSION.intersection(version)
            assert found_compat_version, "Remote data requires %s to be %s, but current app is %s" % (
                version_field,
                version,
                VERSION,
            )
            msg.append("%s=%s:OK" % (version_field, version))

    def load_remote_json(self, url):
        res = self.client.get(url, allow_redirects=True)
        redirect = res.headers.get("x-amz-website-redirect-location")
        if redirect:
            parsed = urlparse(url)
            newurl = parsed._replace(path=redirect)
            res = self.client.get(newurl.geturl())
        if res.status_code != 200:
            self.logger.error(res)
            return None
        try:
            jsondat = json.loads(res.text)
            return jsondat
        except json.JSONDecodeError:
            self.logger.error(res.headers)
            self.logger.error(res)
            return None

    def compare_remote_local(self, remote_version, local_version, orig_remote_version, orig_local_version):
        # we need have to some data locally. do we already have ?
        if remote_version > local_version:
            self.logger.info(
                "Remote version '%s' is more recent than local version '%s', download needed"
                % (orig_remote_version, orig_local_version)
            )
            return True
        else:
            self.logger.info(
                "Remote version '%s' is the same as " % orig_remote_version
                + "local version '%s'. " % orig_local_version
                + "Dump is waiting to be applied"
            )
            return False

    def remote_is_better(self, remotefile, localfile):
        remote_dat = self.load_remote_json(remotefile)
        if not remote_dat:
            self.logger.info("Couldn't find any build metadata at url '%s'", remotefile)
            return False
        orig_remote_version = remote_dat["build_version"]

        local_dat = json.load(open(localfile))
        orig_local_version = local_dat["build_version"]

        # if diff version, we want to compatr the right part (destination version)
        # local: "3.4", backend: "4". It's actually the same (4==4)
        local_version = orig_local_version.split(".")[-1]
        remote_version = orig_remote_version.split(".")[-1]
        if remote_version != orig_remote_version:
            self.logger.debug(
                "Remote version '%s' converted to '%s' (version that will be reached once incremental update has been applied)",
                orig_remote_version,
                remote_version,
            )
        if local_version != orig_local_version:
            self.logger.debug(
                "Local version '%s' converted to '%s' (version that had been be reached using incremental update files)",
                orig_local_version,
                local_version,
            )

        backend_version = self.target_backend.version
        if backend_version is None:
            self.logger.info("No backend version found")
            return self.compare_remote_local(
                remote_version,
                local_version,
                orig_remote_version,
                orig_local_version,
            )
        elif remote_version > backend_version:
            self.logger.info(
                "Remote version '%s' is more recent than backend version '%s'", orig_remote_version, backend_version
            )
            return self.compare_remote_local(remote_version, local_version, orig_remote_version, orig_local_version)
        else:
            self.logger.info("Backend version '%s' is up-to-date", backend_version)
            return False

    def choose_best_version(self, versions):
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
            self.logger.info("Preferred versions (according to preferences): %s", preferreds)
            versions = preferreds
        # we can directly take the max because:
        # - version is a string
        # - format if YYYYMMDD
        # - when incremental, it's always old_version.new_version
        return max(versions, key=lambda e: e["build_version"])

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
        self.logger.info(
            "Find update path to bring data from version '%s' up-to version '%s'", backend_version, version
        )
        file_url = urljoin(self.base_url, "%s.json" % version)
        build_meta = self.load_remote_json(file_url)
        if not build_meta:
            raise Exception(f"Can't get remote build information about version '{version}' (url was '{file_url}')")
        self.check_compat(build_meta)

        if build_meta["target_version"] == backend_version:
            self.logger.info("Backend is up-to-date, version '%s'", backend_version)
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
                self.logger.info(
                    "Diff update '%s' requires version '%s', which is compatible with current backend version, download update",
                    build_meta["build_version"],
                    build_meta["require_version"],
                )
            else:
                self.logger.info(
                    "Diff '%s' update requires version '%s'", build_meta["build_version"], build_meta["require_version"]
                )
                self.logger.info("Now looking for a compatible version")
                # by default we'll check directly the required version
                compatibles = [v for v in avail_versions["versions"] if v["target_version"] == version.split(".")[0]]
                self.logger.info("Compatible versions from which we can apply this update: %s", compatibles)
                best_version = self.choose_best_version(compatibles)
                self.logger.info("Best version found: '%s'" % best_version)
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
        file_url = url or urljoin(self.base_url, "%s.json" % version)
        # if "latest", we compare current json file we have (because we don't know what's behind latest)
        # otherwise json file should match version explicitely in current folder.
        version = version == "latest" and self.current_release or version
        try:
            current_localfile = os.path.join(self.current_data_folder, f"{version}.json")
            # check it actually exists (if data folder was deleted by src_dump still refers to
            # this folder, this file won't exist)
            if not os.path.exists(current_localfile):
                self.logger.error("Local file '%s' doesn't exist", current_localfile)
                raise FileNotFoundError
            dump_status = self.src_doc.get("download", {}).get("status")
            if dump_status != "success":
                self.logger.error("Found dump information but status is '%s', will ignore current dump", dump_status)
                raise TypeError
        except (TypeError, FileNotFoundError):
            # current data folder doesn't even exist
            current_localfile = None
        self.logger.info("Local file: %s" % current_localfile)
        self.logger.info("Remote url : %s" % file_url)
        self.logger.info("Force: %s" % force)
        if force or current_localfile is None or self.remote_is_better(file_url, current_localfile):
            # manually get the diff meta file (ie. not using download() because we don't know the version yet,
            # it's in the diff meta
            build_meta = self.load_remote_json(file_url)
            self.check_compat(build_meta)
            if not build_meta:
                raise Exception(f"Can't get remote build information about version '{version}' (url was '{file_url}')")

            if build_meta["type"] == "incremental":
                self.release = build_meta["build_version"]
                # ok, now we can use download()
                # we will download it again during the normal process so we can then compare
                # when we have new data release
                new_localfile = os.path.join(self.new_data_folder, f"{self.release}.json")
                self.to_dump.append({"remote": file_url, "local": new_localfile})
                # get base url (used later to get diff files)
                metadata_url = build_meta["metadata"]["url"]
                base_url = os.path.dirname(metadata_url) + "/"  # "/" or urljoin will remove previous fragment...
                new_localfile = os.path.join(self.new_data_folder, os.path.basename(metadata_url))
                self.download(metadata_url, new_localfile)
                metadata = json.load(open(new_localfile))
                remote_files = metadata["diff"]["files"]
                if metadata["diff"]["mapping_file"]:
                    remote_files.append(metadata["diff"]["mapping_file"])
                for md5_fname in remote_files:
                    fname = md5_fname["name"]
                    p = urlparse(fname)
                    if not p.scheme:
                        # this is a relative path
                        furl = urljoin(base_url, fname)
                    else:
                        # this is a true URL
                        furl = fname
                    new_localfile = os.path.join(self.new_data_folder, os.path.basename(fname))
                    self.to_dump.append({"remote": furl, "local": new_localfile})

            else:
                # it's a full snapshot release, it always can be applied
                self.release = build_meta["build_version"]
                new_localfile = os.path.join(self.new_data_folder, "%s.json" % self.release)
                self.to_dump.append({"remote": file_url, "local": new_localfile})
                # -------------------------------
                # TODO review
                # -------------------------------
                # if repo type is fs, we assume metadata contains url to archive
                # repo_name = list(
                #     build_meta["metadata"]["repository"].keys())[0]
                # if build_meta["metadata"]["repository"][repo_name]["type"] == "fs":
                if build_meta["metadata"]["repository"]["type"] == "fs":
                    archive_url = build_meta["metadata"]["archive_url"]
                    archive = os.path.basename(archive_url)
                    new_localfile = os.path.join(self.new_data_folder, archive)
                    self.to_dump.append({"remote": archive_url, "local": new_localfile})

            # unset this one, as it may not be pickelable (next step is "download", which
            # uses different processes and need workers to be pickled)
            self._target_backend = None
            return self.release

    def post_dump(self, *args, **kwargs):
        if not self.release:
            # wasn't set before, means no need to post-process (ie. up-to-date, already done)
            return
        build_meta = json.load(open(os.path.join(self.new_data_folder, f"{self.release}.json")))
        if build_meta["type"] == "incremental":
            self.logger.info("Checking md5sum for files in '%s'" % self.new_data_folder)
            metadata = json.load(open(os.path.join(self.new_data_folder, "metadata.json")))
            for md5_fname in metadata["diff"]["files"]:
                spec_md5 = md5_fname["md5sum"]
                fname = md5_fname["name"]
                compute_md5 = md5sum(os.path.join(self.new_data_folder, fname))
                if compute_md5 != spec_md5:
                    self.logger.error("md5 check failed for file '%s', it may be corrupted", fname)
                    e = DumperException(f"Bad md5sum for file '{fname}'")
                    self.register_status("failed", download={"err": repr(e)})
                    raise e
                else:
                    self.logger.debug(f"md5 check success for file '{fname}'")
        elif build_meta["type"] == "full":
            # if type=fs, check if archive must be uncompressed
            # TODO

            # repo_name = list(build_meta["metadata"]["repository"].keys())[0]
            if build_meta["metadata"]["repository"]["type"] == "fs":
                uncompressall(self.new_data_folder)

    async def info(self, version="latest"):
        """
        Display version information (release note, etc...) for given version
        {
            "info": ...
            "release_note": ...
        }
        """
        file_url = urljoin(self.base_url, "%s.json" % version)
        result = {}
        build_meta = self.load_remote_json(file_url)
        if not build_meta:
            raise DumperException("Can't find version '%s'" % version)
        result["info"] = build_meta
        if build_meta.get("changes"):
            result["release_note"] = {}
            for filtyp in build_meta["changes"]:
                relnote_url = build_meta["changes"][filtyp]["url"]
                res = self.client.get(relnote_url)
                if res.status_code == 200:
                    if filtyp == "json":
                        result["release_note"][filtyp] = res.json()
                    else:
                        result["release_note"][filtyp] = res.text
                else:
                    raise DumperException(f"Error while downloading release note '{version} ({res})': {res.text}")
        return result

    async def versions(self):
        """
        Display all available versions.
        Example:
        [{
            'build_version': '20171003',
            'url': 'https://biothings-releases.s3.amazonaws.com:443/mygene.info/20171003.json',
            'release_date': '2017-10-06T11:58:39.749357',
            'require_version': None,
            'target_version': '20171003',
            'type': 'full'
        }, ...]
        """
        avail_versions = self.load_remote_json(self.__class__.VERSION_URL)
        if not avail_versions:
            raise DumperException("Can't find any versions available...")
        assert avail_versions["format"] == "1.0", "versions.json format has changed: %s" % avail_versions["format"]
        return avail_versions["versions"]
