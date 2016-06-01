''' Functions to return versions of things. '''
from subprocess import check_output

def get_python_version():
    ''' Get a list of python packages installed and their versions. '''
    try:
        return check_output("pip freeze", shell=True).decode('utf-8').strip('\n').split('\n')
    except:
        return []

def get_repository_information():
    ''' Get the repository information for the local repository. '''
    try:
        repository_url = check_output("git config --get remote.origin.url", shell=True).decode('utf-8').strip('\n')
    except:
        repository_url = ''

    try:
        commit_hash = check_output("git rev-parse HEAD", shell=True).decode('utf-8').strip('\n')
    except:
        commit_hash = ''

    return {'repository-url': repository_url, 'commit-hash': commit_hash}
