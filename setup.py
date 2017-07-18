import os
from setuptools import setup, find_packages
from subprocess import check_output
from subprocess import CalledProcessError

setup_path = os.path.dirname(__file__)

def read(fname):
    return open(os.path.join(setup_path, fname)).read()

# get version
with open('biothings/version.py', 'r') as bt_ver:
    exec(bt_ver.read())

# should fail if installed form source or from pypi
try:
    MICRO_VER = int(check_output("git rev-list --count master", shell=True).decode('utf-8').strip('\n'))
except:
    pass

REPO_URL = "https://github.com/biothings/biothings.api"

# Calculate commit hash, should fail if installed from source or from pypi
try:
    commit_hash = check_output("git rev-parse HEAD", shell=True).decode('utf-8').strip('\n')
except CalledProcessError:
    # put commit hash for current release
    commit_hash = ''

# Write commit to file inside package, that can be read later
with open('biothings/.git-commit-hash', 'w') as git_file:
    git_file.write("{}.git\n{}".format(REPO_URL, commit_hash))

install_requires = [
    'requests>=2.3.0',
    'tornado',
    'elasticsearch==2.4.0',
]
hub_requires = [
    'beautifulsoup4',
    'aiocron',
    'asyncssh==1.7.1',
    'pymongo',
    'psutil',
    'jsonpointer',
    'IPython',
    'boto'
]
    
setup(
    name="biothings",
    version="{}.{}.{}".format(MAJOR_VER, MINOR_VER, MICRO_VER),
    author="Cyrus Afrasiabi, Sebastien Lelong, Chunlei Wu",
    author_email="cwu@scripps.edu",
    description="a toolkit for building high-performance data APIs in biology",
    license="Apache License, Version 2.0",
    keywords="biology annotation web service client api",
    url=REPO_URL,
    packages=find_packages(),
    include_package_data=True,
    scripts=['biothings/bin/biothings-admin.py'],
    long_description=read('README.md'),
    classifiers=[
        "Programming Language :: Python",
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
        'hub': hub_requires,
        'dev':  hub_requires + ['sphinx' + 'sphinx_rtd_theme']
    },
)
