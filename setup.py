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

setup(
    name="biothings",
    version="{}.{}.{}".format(MAJOR_VER, MINOR_VER, MICRO_VER),
    author="Cyrus Afrasiabi, Chunlei Wu, Sebastien Lelong, Kevin Xin",
    author_email="cyrus@scripps.edu",
    description="Python package for biothings framework",
    license="BSD",
    keywords="biology annotation web service client api",
    url=REPO_URL,
    packages=find_packages(),
    include_package_data=True,
    scripts=['biothings/bin/biothings-admin.py','biothings/bin/generate-client.py'],
    long_description=read('README.md'),
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: BSD License",
        "Operating System :: POSIX",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        "Intended Audience :: Science/Research",
        "Topic :: Utilities",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
    ],
    install_requires=[
        'requests>=2.3.0',
        'tornado',
    ],
)
