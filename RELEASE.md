#### This is the procedure we use for "biothings" package release:

1. requires both `wheel` and `twine` packages installed
    ```
    pip install wheel twine
    ```

2. Update version number in [version.py](biothings/version.py).

3. Check and update [setup.py](setup.py) if needed (dependencies, metadata etc.).

4. Build the package locally:

    ```
    python setup.py sdist bdist_wheel
    ```

   Note: No need to add `--unversal` parameter, since `biothings` package now requires Python3, with no support of Python2 any more.

5. Test the package built locally:

    ```
    pip install dist/biothings-0.3.0-py3-none-any.whl
    ```

   And run any local test as needed (e.g. run nosetests on a local BioThings API instance).

6. Prepare github repo for the release:

   * Create a version branch (no need for every micro versions):
        ```
        git checkout -b 0.3.x
        git push -u origin "0.3.x"
        ```
     Note: future version-specific bug-fixes (with increased micro versions) will go to this branch (possibly cherry-picked from `master` branch).

   * Create a tag for each released version (with "v" prefix):
        ```
        git tag -a "v0.3.0" -m "tagging v0.3.0 for release"
        ```
   * If everything looks good, push to the remote:
        ```
        git push --tags
        ```

7. Upload to PyPI:

    ```
    twine upload dist/*
    ```

    Note: for now, this step needs to be done by @newgene under ["newgene" PyPI account](https://pypi.org/user/newgene/).

8. `master` branch is our dev branch. After a successful release, update major and/or minor version to the next release version, and update micro as "dev". Here is an example after "v0.3.0" release:

    ```
    MAJOR_VER=0
    MINOR_VER=4
    MICRO_VER="dev"
    ```
