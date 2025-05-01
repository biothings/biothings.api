from subprocess import CalledProcessError, check_output

from setuptools import setup

REPO_URL = "https://github.com/biothings/biothings.api"

# should fail if installed from source or from pypi,
# version gets set to MAJOR.MINOR.# commits on master branch if installed from pip repo
# otherwise to MAJOR.MINOR.MICRO as defined in biothings.version
try:
    command = ["git", "rev-list", "--count", "master"]
    NUM_COMMITS = check_output(command).strip().decode("utf-8")
except CalledProcessError:
    NUM_COMMITS = ""

# Calculate commit hash, should fail if installed from source or from pypi
try:
    command = ["git", "rev-parse", "HEAD"]
    COMMIT_HASH = check_output(command).strip().decode("utf-8")
except CalledProcessError:
    COMMIT_HASH = ""

# Write commit to file inside package, that can be read later
if COMMIT_HASH or NUM_COMMITS:
    with open("biothings/.git-info", "w", encoding="utf-8") as git_file:
        git_file.write(f"{REPO_URL}.git\n{COMMIT_HASH}\n{NUM_COMMITS}")

setup(
    name="biothings"
)
