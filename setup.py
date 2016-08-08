import os
from setuptools import setup, find_packages
from subprocess import check_output
from subprocess import CalledProcessError

setup_path = os.path.dirname(__file__)

print("setup_path: {}".format(setup_path))

def read(fname):
    return open(os.path.join(setup_path, fname)).read()

MAJOR_VERSION = 0
MINOR_VERSION = 0
REPO_URL = "https://github.com/SuLab/biothings.api"

try:
    MICRO_VERSION = int(check_output("git rev-list --count master", shell=True).decode('utf-8').strip('\n'))
except:
    MICRO_VERSION = 2

# Calculate commit hash 
try:
    commit_hash = check_output("git rev-parse HEAD", shell=True).decode('utf-8').strip('\n')
except CalledProcessError:
    commit_hash = ''

f = open('biothings/.git-commit-hash', 'w')
f.write("{}.git\n{}".format(REPO_URL, commit_hash))
f.close()

setup(
    name="biothings",
    version="{}.{}.{}".format(MAJOR_VERSION, MINOR_VERSION, MICRO_VERSION),
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
