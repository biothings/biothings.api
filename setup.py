import logging
import os.path

from git import GitCommandError, InvalidGitRepositoryError, NoSuchPathError, Repo
from setuptools import setup

REPO_URL = "https://github.com/biothings/biothings.api"


def get_git_info():
    """
    Get the git commit hash and number of commits in the current repository.
    Returns a tuple (commit_hash, num_commits).
    """
    folder = os.path.dirname(__file__)
    commit_hash, num_commits = "", ""
    try:
        repo = Repo(folder)
        commit_hash = repo.head.object.hexsha
        num_commits = repo.rev_parse("master").count()
    except (InvalidGitRepositoryError, NoSuchPathError):
        logging.warning("Not a valid git repository for folder '%s', skipped for checking new version.", folder)
    except GitCommandError:
        logging.exception("Error while getting git information for '%s'", folder)
    except Exception as err:
        logging.debug("Can't determine source code info (but that's fine) for: %s", err)

    return commit_hash, num_commits


COMMIT_HASH, NUM_COMMITS = get_git_info()

# Write commit to file inside package, that can be read later
if COMMIT_HASH or NUM_COMMITS:
    with open(os.path.join(os.path.dirname(__file__), "biothings/.git-info"), "w", encoding="utf-8") as git_file:
        git_file.write(f"{REPO_URL}.git\n{COMMIT_HASH}\n{NUM_COMMITS}")

setup(
    name="biothings"
)
