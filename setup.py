import glob
import os
from subprocess import CalledProcessError, check_output

from setuptools import find_packages, setup

setup_path = os.path.dirname(__file__)


def read(fname):
    return open(os.path.join(setup_path, fname), encoding="utf8").read()


REPO_URL = "https://github.com/biothings/biothings.api"

# should fail if installed from source or from pypi,
# version gets set to MAJOR.MINOR.# commits on master branch if installed from pip repo
# otherwise to MAJOR.MINOR.MICRO as defined in biothings.version
try:
    NUM_COMMITS = check_output("git rev-list --count master", shell=True).strip().decode("utf-8")
except CalledProcessError:
    NUM_COMMITS = ""

# Calculate commit hash, should fail if installed from source or from pypi
try:
    COMMIT_HASH = check_output("git rev-parse HEAD", shell=True).strip().decode("utf-8")
except CalledProcessError:
    COMMIT_HASH = ""

# Write commit to file inside package, that can be read later
if COMMIT_HASH or NUM_COMMITS:
    with open("biothings/.git-info", "w", encoding="utf-8") as git_file:
        git_file.write(f"{REPO_URL}.git\n{COMMIT_HASH}\n{NUM_COMMITS}")


# very minimal requirement for running biothings.web
install_requires = [
    "boto3",
    "requests>=2.21.0",
    "requests-aws4auth",
    'tornado==6.1.0; python_version < "3.7.0"',
    'tornado==6.2.0; python_version >= "3.7.0"',
    "gitpython>=3.1.0",
    "elasticsearch[async]>=7, <8",
    "elasticsearch-dsl>=7, <8",
    'singledispatchmethod; python_version < "3.8.0"',
    'dataclasses; python_version < "3.7.0"',
    "PyYAML>=5.1",
    "orjson>=3.6.1",  # a faster json lib supports inf/nan and datetime, v3.6.1 is the last version supports Python 3.6
]

# extra requirements for biothings.web
web_extra_requires = [
    "msgpack>=0.6.1",  # support format=msgpack
    "sentry-sdk>=1.5.3",  # new sentry package
]

# extra requirements to run biothings.hub
hub_requires = [
    "beautifulsoup4",  # used in dumper.GoogleDriveDumper
    "aiocron==1.8",  # setup scheduled jobs
    "aiohttp==3.8.3",  # elasticsearch requires aiohttp>=3,<4
    "asyncssh==2.11.0",  # needs libffi-dev installed (apt-get)
    "pymongo>=4.1.0,<5.0",  # support MongoDB 5.0 since v3.12.0
    "psutil",
    "jsonpointer",  # for utils.jsonpatch
    "IPython",  # for interactive hub console
    "multiprocessing_on_dill",  # can replace pickler in concurrent.futures
    "dill",  # a pickle alternative with extra object type support
    "prettytable",  # diff report renderer
    "sockjs-tornado==1.0.7",  # websocket server for HubServer
    "jsonschema>=2.6.0",
    "pip",  # auto-install requirements from plugins
    # 'pandas==1.0.1',    # json with inf/nan and more to come (replaced by orjson below now)
    # 'orjson>=3.5.2',    # this is a faster json lib support inf/nan and datetime
    "yapf",  # code reformatter, better results than autopep8
    "requests-aws4auth",  # aws s3 auth requests for autohub
    "networkx>=2.1,<2.6",  # datatransform
    "biothings_client>=0.2.6",  # datatransform (api client)
]

# extra requirements to develop biothings
dev_requires = [
    "pytest",
    "pytest-mock",
    "pre-commit==2.17.0",
]

# extra requirements for building docs
docs_requires = ["sphinx>=2.4.3", "sphinx_rtd_theme>=1.0.0", "sphinx_reredirects>=0.0.1"]

setup(
    name="biothings",
    version=__import__("biothings").__version__,
    author="Sebastien Lelong, Zhongchao Qian, Xinghua Zhou, Chunlei Wu",
    author_email="cwu@scripps.edu",
    description="a toolkit for building high-performance data APIs in biology",
    license="Apache License, Version 2.0",
    keywords="biology annotation web service client api",
    url=REPO_URL,
    packages=find_packages(exclude=["tests"]),
    package_data={"": ["*.html", "*.py.tpl"]},
    include_package_data=True,
    scripts=list(glob.glob("biothings/bin/*")),
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    python_requires=">=3.6",
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
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
        "web_extra": web_extra_requires,
        "hub": hub_requires,
        "dev": web_extra_requires + hub_requires + dev_requires + docs_requires,
    },
)
