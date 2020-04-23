''' Functions to return versions of things. '''
import functools
import logging
# import pip
import os
import re
import shlex
import sys
from subprocess import DEVNULL, check_output

from git import (Git, GitCommandError, InvalidGitRepositoryError,
                 NoSuchPathError, Repo)

import biothings
from biothings.utils.dataload import dict_sweep

# cache these
BIOTHINGS_REPO_DATA = {}
APP_REPO_DATA = {}


def get_python_version():
    ''' Get a list of python packages installed and their versions. '''
    try:
        output = check_output('pip list', shell=True, stderr=DEVNULL)
        return output.decode('utf-8').replace('\r', '').split('\n')[2: -1]
    except Exception:
        return []


def get_biothings_commit():
    ''' Gets the biothings commit information. '''
    global BIOTHINGS_REPO_DATA
    if BIOTHINGS_REPO_DATA:
        return BIOTHINGS_REPO_DATA

    try:
        with open(os.path.join(os.path.dirname(biothings.__file__), '.git-info'), 'r') as f:
            lines = [l.strip('\n') for l in f.readlines()]
        BIOTHINGS_REPO_DATA = {
            'repository-url': lines[0],
            'commit-hash': lines[1],
            'master-commits': lines[2],
            'version': biothings.get_version()}
    except Exception:
        BIOTHINGS_REPO_DATA = {'repository-url': '', 'commit-hash': '',
                               'master-commits': '', 'version': biothings.get_version()}

    return BIOTHINGS_REPO_DATA

@functools.lru_cache()
def get_repository_information(app_dir=None):
    """
    Get the repository information for the local repository, if it exists.
    """
    commit_hash = ''
    repository_url = ''

    if app_dir:
        app_dir = os.path.abspath(app_dir)

        try:
            args = shlex.split("git rev-parse HEAD")
            output = check_output(args, cwd=app_dir, shell=True, stderr=DEVNULL)
            commit_hash = output.decode('utf-8').strip('\n')
        except BaseException:
            pass

        try:
            args = shlex.split("git config --get remote.origin.url")
            output = check_output(args, cwd=app_dir, shell=True, stderr=DEVNULL)
            repository_url = output.decode('utf-8').strip('\n')
        except BaseException:
            pass

    codebase = {
        'repository-url': repository_url,
        'commit-hash': commit_hash
    }
    return codebase


def get_python_exec_version():
    return {
        'version': sys.version,
        'version_info': {
            "major": sys.version_info[0],
            "minor": sys.version_info[1],
            "micro": sys.version_info[2]
        }
    }


def get_software_info(app_dir=None):
    return {
        'python-package-info': get_python_version(),
        'codebase': get_repository_information(app_dir=app_dir),
        'biothings': get_biothings_commit(),
        'python-info': get_python_exec_version(),
    }


def check_new_version(folder, max_commits=10):
    """
    Given a folder pointing to a Git repo, return a dict containing info
    about remote commits not qpplied yet to the repo, or empty dict if nothing
    new.
    """
    # from https://stackoverflow.com/questions/8290233/gitpython-get-list-of-remote-commits-not-yet-applied
    repo = Repo(folder)
    try:

        repo_url = re.sub("\.git$","",repo.remotes.origin.url)
    except Exception as e:
        logging.debug("Can't determine repository URL: %s" % e)
        repo_url = None
    new_info = {}
    try:
        head = repo.head.ref
        tracking = head.tracking_branch()
        new_commits = [commit for commit in tracking.commit.iter_items(repo, f'{head.path}..{tracking.path}')]
        if new_commits:
            new_info = {
                "latest": new_commits[0].hexsha[:6],
                "commits": [
                    {
                        "hash": c.hexsha[:6],
                        "url" : repo_url and os.path.join(repo_url,"commit",c.hexsha) or None,
                        "date": c.committed_datetime.isoformat(),
                        "message": c.message
                    } for c in new_commits][:max_commits],
                "total": len(new_commits),
            }
    except Exception as e:

        logging.warning("Can't check for new version: %s" % e)
        raise e

    return new_info

def get_version(folder):
    repo = Repo(folder)  # app or lib dir
    url = repo.remotes.origin.url
    try:
        commit = repo.head.object.hexsha[:6]
        commitdate = repo.head.object.committed_datetime.isoformat()
    except Exception as e:
        logging.warning("can't determine app commit hash: %s" % e)
        commit = "unknown"
        commitdate = "unknown"

    try:
        return {"branch": repo.active_branch.name,
                "commit": commit,
                "date": commitdate,
                "giturl" : url}
    except Exception as e:
        logging.warning("can't determine app version, assuming HEAD detached': %s" % e)
        return {"branch": "HEAD detached",
                "commit": commit,
                "date": commitdate,
                "giturl" : url}


def set_versions(config, app_folder):
    """
    Propagate versions (git branch name) in config module.
    Also set app and biothings folder paths (though not
    exposed as a config param since they are lower-cased,
    see biothings.__init__.py, regex PARAM_PAT)
    """
    # app_version: version of the API application
    if not hasattr(config, "APP_VERSION"):
        config.APP_VERSION = get_version(app_folder)
        config.app_folder = app_folder
    else:
        logging.info("app_version '%s' forced in configuration file" % config.APP_VERSION)

    # biothings_version: version of BioThings SDK
    if not hasattr(config, "BIOTHINGS_VERSION"):
        import biothings
        # .../biothings.api/biothings/__init__.py
        bt_folder, _bt = os.path.split(os.path.split(os.path.realpath(biothings.__file__))[0])
        assert _bt == "biothings", "Expectig 'biothings' dir in biothings lib path"
        config.BIOTHINGS_VERSION = get_version(bt_folder)
        config.biothings_folder = bt_folder
    else:
        logging.info("biothings_version '%s' forced in configuration file" %
                     config.BIOTHINGS_VERSION)

    logging.info("Running app_version=%s with biothings_version=%s" %
                 (repr(config.APP_VERSION), repr(config.BIOTHINGS_VERSION)))


def get_source_code_info(src_file):
    """
    Given a path to a source code, try to find information
    about repository, revision, URL pointing to that file, etc...
    Return None if nothing can be determined.
    Tricky cases:
      - src_file could refer to another repo, within current repo
        (namely a remote data plugin, cloned within the api's plugins folder
      - src_file could point to a folder, when for instance a dataplugin is
        analized. This is because we can't point to an uploader file since
        it's dynamically generated
    """
    # need to be absolute to build proper github URL
    abs_src_file = os.path.abspath(src_file)
    try:
        repo = Repo(abs_src_file, search_parent_directories=True)
    except (InvalidGitRepositoryError, NoSuchPathError):
        logging.exception("Can't find a github repository for file '%s'" % src_file)
        return None
    try:
        gcmd = Git(repo)
        hash = gcmd.rev_list(-1, repo.active_branch, abs_src_file)
        rel_src_file = abs_src_file.replace(repo.working_dir, "").strip("/")
        if not hash:
            # seems to be a repo cloned within a repo, change directory
            curdir = os.path.abspath(os.curdir)
            try:
                if os.path.isdir(abs_src_file):
                    os.chdir(abs_src_file)
                    hash = gcmd.rev_list(-1, repo.active_branch)
                else:
                    dirname, filename = os.path.split(abs_src_file)
                    os.chdir(dirname)
                    hash = gcmd.rev_list(-1, repo.active_branch, filename)
                rel_src_file = ""  # will point to folder by commit hash
            finally:
                os.chdir(curdir)
        if hash:
            short_hash = gcmd.rev_parse(hash, short=7)
        else:
            logging.warning("Couldn't determine commit hash for file '%s'" % src_file)
            hash = None
            short_hash = None
        # could have more than one URLs for origin, only take first
        repo_url = next(repo.remote().urls)
        info = {
            "repo": repo_url,
            "commit": short_hash,
            "branch": repo.active_branch.name,
        }
        if os.path.isdir(src_file):
            info["folder"] = rel_src_file
        else:
            info["file"] = rel_src_file
        info = dict_sweep(info)
        # rebuild URL to that file
        if "github.com" in repo_url:
            info["url"] = os.path.join(re.sub(r"\.git$", "", repo_url),
                                       "tree", hash, rel_src_file)

        return info

    except GitCommandError:
        logging.exception("Error while getting git information for file '%s'" % src_file)
        return None
    except TypeError as e:
        # happens with biothings symlink, just ignore
        logging.debug("Can't determine source code info (but that's fine): %s" % e)
        return None
