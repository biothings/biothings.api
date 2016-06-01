''' Functions to return versions of things. '''
from subprocess import check_output
from io import StringIO
from contextlib import redirect_stdout
import pip

def get_python_version():
    ''' Get a list of python packages installed and their versions. '''
    so = StringIO()

    with redirect_stdout(so):
        pip.main(['freeze'])
    
    if so.getvalue():
        return so.getvalue().strip('\n').split('\n')
    
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
