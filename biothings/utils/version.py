''' Functions to return versions of things. '''
from subprocess import check_output
from io import StringIO
from contextlib import redirect_stdout
#import pip
import os, sys, logging
from git import Repo
import biothings

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
        except Exception as e:
            logging.warning("Can't determine app commit hash: %s" % e)
            commit = "unknown"
        try:
            config.APP_VERSION = "%s [%s]" % (repo.active_branch.name,commit)
        except Exception as e:
            logging.warning("Can't determine app version, defaulting to 'master': %s" % e)
            config.APP_VERSION = "master [%s]" % commit
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
        except Exception as e:
            logging.warning("Can't determine biothings commit hash: %s" % e)
            commit = "unknown"
        try:
            config.BIOTHINGS_VERSION = "%s [%s]" % (repo.active_branch.name,commit)
        except Exception as e:
            logging.warning("Can't determine biothings version, defaulting to 'master': %s" % e)
            config.BIOTHINGS_VERSION = "master [%s]" % commit
    else:
        logging.info("biothings_version '%s' forced in configuration file" % config.BIOTHINGS_VERSION)
    
    logging.info("Running app_version=%s with biothings_version=%s" % (repr(config.APP_VERSION),repr(config.BIOTHINGS_VERSION)))
