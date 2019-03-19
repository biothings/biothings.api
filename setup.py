import os
import glob
from subprocess import check_output
from subprocess import CalledProcessError
from setuptools import setup, find_packages

setup_path = os.path.dirname(__file__)


def read(fname):
    return open(os.path.join(setup_path, fname), encoding="utf8").read()


REPO_URL = "https://github.com/biothings/biothings.api"


# get version
version = __import__('biothings').get_version()

# should fail if installed from source or from pypi,
# version gets set to MAJOR.MINOR.# commits on master branch if installed from pip repo
# otherwise to MAJOR.MINOR.MICRO as defined in biothings.version
try:
    num_commits = check_output("git rev-list --count master", shell=True).strip().decode('utf-8')
except CalledProcessError:
    num_commits = ''

# Calculate commit hash, should fail if installed from source or from pypi
try:
    commit_hash = check_output("git rev-parse HEAD", shell=True).strip().decode('utf-8')
except CalledProcessError:
    commit_hash = ''

# Write commit to file inside package, that can be read later
if commit_hash or num_commits:
    with open('biothings/.git-info', 'w') as git_file:
        git_file.write("{}.git\n{}\n{}".format(REPO_URL, commit_hash, num_commits))


# very minimal requirement for running biothings.web
install_requires = [
    'requests>=2.8.1',
    'tornado==5.1.1',
    'elasticsearch==6.1.1',
    'gitpython==2.1.11'
]

# extra requirements for biothings.web
web_extra_requires = [
    'msgpack>=0.6.0',   # support format=msgpack
    'PyYAML>=3.13'      # support format=yaml
]

# extra requirements to run biothings.hub
hub_requires = [
    'beautifulsoup4',
    'aiocron',
    'asyncssh==1.7.1',  # needs libffi-dev installed (apt-get)
    'pymongo',
    'psutil',
    'jsonpointer',
    'IPython',
    'boto',
    'boto3',
    'multiprocessing_on_dill',  # can replace pickler in concurrent.futures
    'dill',
    'pyinotify',        # hub reloader
    'prettytable',      # diff report renderer
    'sockjs-tornado==1.0.6',
    'networkx>=2.1',
    'jsonschema>=2.6.0',
    'pip',              # auto-install requirements from plugins
]

# extra requirements for building docs
docs_requires = [
    'sphinx',
    'sphinx_rtd_theme'
]

setup(
    name="biothings",
    version=version,
    author="Cyrus Afrasiabi, Sebastien Lelong, Chunlei Wu",
    author_email="cwu@scripps.edu",
    description="a toolkit for building high-performance data APIs in biology",
    license="Apache License, Version 2.0",
    keywords="biology annotation web service client api",
    url=REPO_URL,
    packages=find_packages(),
    include_package_data=True,
    scripts=list(glob.glob('biothings/bin/*')),
    long_description=read('README.md'),
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Operating System :: POSIX",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Topic :: Utilities",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
    ],
    install_requires=install_requires,
    extras_require={
        'web_extra': web_extra_requires,
        'hub': hub_requires,
        'dev':  web_extra_requires + hub_requires + docs_requires
    },
)
