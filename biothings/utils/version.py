''' Functions to return versions of things. '''
from subprocess import check_output
from io import StringIO
from contextlib import redirect_stdout
#import pip
import os, sys, logging
from git import Repo, Git, GitCommandError, NoSuchPathError, InvalidGitRepositoryError
import re
import biothings
from biothings.utils.dataload import dict_sweep

# cache these
BIOTHINGS_REPO_DATA = {}
APP_REPO_DATA = {}

def get_python_version():
    ''' Get a list of python packages installed and their versions. '''
    try:
        return check_output('pip list', shell=True).decode('utf-8').split('\n')[2:-1]
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
        BIOTHINGS_REPO_DATA = {'repository-url': lines[0], 'commit-hash': lines[1], 'master-commits': lines[2], 'version': biothings.get_version()}
    except:
        BIOTHINGS_REPO_DATA = {'repository-url': '', 'commit-hash': '', 'master-commits': '', 'version': biothings.get_version()}

    return BIOTHINGS_REPO_DATA

def get_repository_information(app_dir=None):
    ''' Get the repository information for the local repository, if it exists. '''
    global APP_REPO_DATA
    if APP_REPO_DATA:
        return APP_REPO_DATA

    if not app_dir:
        APP_REPO_DATA = {'repository-url': '', 'commit-hash': ''}
        return APP_REPO_DATA

    try:
        commit_hash = check_output("cd {};git rev-parse HEAD".format(os.path.abspath(app_dir)), 
                        shell=True).decode('utf-8').strip('\n')
    except:
        commit_hash = ''

    try:
        repository_url = check_output("cd {};git config --get remote.origin.url".format(os.path.abspath(app_dir)), 
                        shell=True).decode('utf-8').strip('\n')
    except:
        repository_url = ''

    APP_REPO_DATA = {'repository-url': repository_url, 'commit-hash': commit_hash}

    return APP_REPO_DATA

def get_python_exec_version():
    return {
            'version' : sys.version,
            'version_info' : {
                "major" : sys.version_info[0],
                "minor" : sys.version_info[1],
                "micro" : sys.version_info[2]
                }
            }

def get_software_info(app_dir=None):
    return {
            'python-package-info': get_python_version(),
            'codebase': get_repository_information(app_dir=app_dir),
            'biothings': get_biothings_commit(),
            'python-info' : get_python_exec_version(),
            }



def set_versions(config, app_folder):
    """
    Propagate versions (git branch name) in config module
    """
    # app_version: version of the API application
    if not hasattr(config,"APP_VERSION"):
        repo = Repo(app_folder) # app dir (mygene, myvariant, ...)
        try:
            commit = repo.head.object.hexsha[:6]
            commitdate = repo.head.object.committed_datetime.isoformat()
        except Exception as e:
            logging.warning("Can't determine app commit hash: %s" % e)
            commit = "unknown"
            commitdate = "unknown"
        try:
            config.APP_VERSION = "%s [%s] [%s]" % (repo.active_branch.name,commit,commitdate)
        except Exception as e:
            logging.warning("Can't determine app version, defaulting to 'master': %s" % e)
            config.APP_VERSION = "master [%s] [%s]" % (commit,commitdate)
    else:
        logging.info("app_version '%s' forced in configuration file" % config.APP_VERSION)

    # biothings_version: version of BioThings SDK
    if not hasattr(config,"BIOTHINGS_VERSION"):
        import biothings
        # .../biothings.api/biothings/__init__.py
        bt_folder,_bt = os.path.split(os.path.split(os.path.realpath(biothings.__file__))[0])
        assert _bt == "biothings", "Expectig 'biothings' dir in biothings lib path"
        repo = Repo(bt_folder) # app dir (mygene, myvariant, ...)
        try:
            commit = repo.head.object.hexsha[:6]
            commitdate = repo.head.object.committed_datetime.isoformat()
        except Exception as e:
            logging.warning("Can't determine biothings commit hash: %s" % e)
            commit = "unknown"
            commitdate = "unknown"
        try:
            config.BIOTHINGS_VERSION = "%s [%s] [%s]" % (repo.active_branch.name,commit,commitdate)
        except Exception as e:
            logging.warning("Can't determine biothings version, defaulting to 'master': %s" % e)
            config.BIOTHINGS_VERSION = "master [%s] [%s]" % (commit,commitdate)
    else:
        logging.info("biothings_version '%s' forced in configuration file" % config.BIOTHINGS_VERSION)

    logging.info("Running app_version=%s with biothings_version=%s" % (repr(config.APP_VERSION),repr(config.BIOTHINGS_VERSION)))


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
        repo = Repo(abs_src_file,search_parent_directories=True)
    except (InvalidGitRepositoryError,NoSuchPathError) as e:
        logging.exception("Can't find a github repository for file '%s'" % src_file)
        return None
    try:
        gcmd = Git(repo)
        hash = gcmd.rev_list(-1,repo.active_branch,abs_src_file)
        rel_src_file = abs_src_file.replace(repo.working_dir,"").strip("/")
        if not hash:
            # seems to be a repo cloned within a repo, change directory
            curdir = os.path.abspath(os.curdir)
            try:
                if os.path.isdir(abs_src_file):
                    os.chdir(abs_src_file)
                    hash = gcmd.rev_list(-1,repo.active_branch)
                else:
                    dirname,filename = os.path.split(abs_src_file)
                    os.chdir(dirname)
                    hash = gcmd.rev_list(-1,repo.active_branch,filename)
                rel_src_file = "" # will point to folder by commit hash
            finally:
                os.chdir(curdir)
        if hash:
            short_hash = gcmd.rev_parse(hash,short=7)
        else:
            logging.warning("Couldn't determine commit hash for file '%s'" % src_file)
            hash = None
            short_hash = None
        # could have more than one URLs for origin, only take first
        repo_url = next(repo.remote().urls)
        info = {
                "repo" : repo_url,
                "commit" : short_hash,
                "branch" : repo.active_branch.name,
                }
        if os.path.isdir(src_file):
            info["folder"] = rel_src_file
        else:
            info["file"] = rel_src_file
        info = dict_sweep(info)
        # rebuild URL to that file
        if "github.com" in repo_url:
            info["url"] = os.path.join(re.sub("\.git$","",repo_url),
                                            "tree",hash,rel_src_file)

        return info

    except GitCommandError as e:
        logging.exception("Error while getting git information for file '%s'" % src_file)
        return None
    except TypeError as e:
        # happens with biothings symlink, just ignore
        logging.debug("Can't determine source code info (but that's fine): %s" % e)
        return None
