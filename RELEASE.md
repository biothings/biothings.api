### This is the procedure we use for "biothings" package release

1. requires both `wheel` and `twine` packages installed

    ```bash
    pip install wheel twine
    ```

2. Update version number in [biothings/__init__.py](biothings/__init__.py).

3. Check and update [setup.py](setup.py) if needed (dependencies, metadata etc.).

4. Build the package locally:

    ```bash
    python setup.py sdist bdist_wheel
    ```

   Note: No need to add `--unversal` parameter, since `biothings` package now requires Python3, with no support of Python2 any more.

5. Test the package built locally:

    ```bash
    pip install dist/biothings-0.9.0-py3-none-any.whl
    ```

   And run any local test as needed (e.g. run nosetests on a local BioThings API instance).

6. Prepare github repo for the release:

   * Create a version branch if not already (no need for every micro versions):

        ```bash
        git checkout -b 0.11.x
        git push -u origin "0.11.x"
        ```

     Note: future version-specific bug-fixes (with increased micro versions) will go to this branch (possibly cherry-picked from `master` branch).

   * Create a tag for each released version (with "v" prefix):

        ```bash
        git tag -a "v0.9.0" -m "tagging v0.9.0 for release"
        ```

   * If everything looks good, push to the remote:

        ```bash
        git push --tags
        ```

7. Publish a new release using Github Action

   * [Draft a new release](https://github.com/biothings/biothings.api/releases/new) in Github Releases interface using the latest tag.
   * If everything looks good, click "Publish release". [A github action workflow](.github/workflows/pypi-publish.yml) will be triggered automatically to build and publish the new release to PyPI.

     Note: this Github action workflow requires a `PYPI_API_TOKEN` secret stored in the repository. You can create a PyPI token following [this instruction](https://pypi.org/help/#apitoken).

8. Alternatively, upload manually a new release to PyPI:

    ```bash
    twine upload dist/*
    ```

    Note: make sure `dist` folder contains only the new versions you want to publish.

    Note: for now, this step needs to be done by @newgene under ["newgene" PyPI account](https://pypi.org/user/newgene/). Ask for a token generated from "newgene" PyPI account.

9. Make it ready to work on the next development cycle

* Create a new development branch for the next major release, e.g. `0.12.x` after the `0.11.0` release:

   ```bash
   git checkout -b 0.11.x
   git push -u origin "0.11.x"
   ```

* Three active branches for future developemnt:

  * `0.12.x` - new features/fixes for next `0.12.x` release
  * `master` - basically a staging branch for the current `0.11.x` branch
  * `0.11.x` - the branch is corresponding to the current published release till the next new `0.12.x` release

  Note: typically, all future new commits should be merged into `0.12.x` branch, and for those bug-fixes commits relevant to published `0.11.x` releases, they can be merged or cherry-picked into `master` and then merged into `0.11.x` branch. When necessary, a bug-fix micro release of `0.11.x` (e.g. `0.11.1` release can be made from the `master` or `0.11.x` branch.
