* This is the procedure we use for "biothings" package release"

0. requires both `wheel` and `twine` packages installed

1. Update version number in [version.py](biothings/version.py).

2. Check and update [setup.py](setup.py) if needed (dependcies, metadata etc.).

3. Build the package locally:

    python setup.py sdist bdist_wheel

   Note: No need to add `--unversal` parameter, since `biothings` package now requires Python3, with no support of Python2 any more.

4. Test the package built locally:

    pip install dist/biothings-0.3.0-py3-none-any.whl

   And run any local test as needed (e.g. run nosetests on a local BioThings API instance).

5. Prepare github repo for the release:

   * Create a version branch (no need for every micro versions):

        git branch v0.3.x

     Note: future version-specific bug-fixes (with incread micro versions) will go to this branch (possibly cherry-picked from `master` branch).

   * Create a tag for each released version:

        git tag -a "v0.3.0"

   * If everything looks good, push to the remote:

        git push --tags

6. `master` branch is our dev branch. After a successful release, update major and/or minor version to the next release version, and update micro as "dev". Here is an example after "v0.3.0" release:

    ```
    MAJOR_VER=0
    MINOR_VER=4
    MICRO_VER=dev
    ```
