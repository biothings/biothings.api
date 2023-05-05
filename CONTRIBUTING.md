# Contribution Guidelines

## Code style

### Code style we follow

The Python code style used by BioThings projects generally folllows [PEP8](https://pep8.org/).

In addition to the standards outlined in PEP 8, we have a few guidelines:

* Max line length

  Line-length can exceed 79 characters, we set max-line-length to 120 to ignore most of line-length warning,
  But try to keep the line-length within 100 characters.

* Import order and grouping

  Imports at the beginning of the code should be grouped into three groups in this order:
    1. Python's builtin modules
    2. Third-party modules
    3. The modules from this project
  Leave an empty line to separate each import group.

  The only exception is `import logging`, where we often need to add logging settings immediately after import.
  We recommend to put this `logging` block at the end of the imports.

  See [this code](biothings/utils/es.py) as an example.

  Note: You can use [isort](https://pypi.org/project/isort/) package or [its vscode extension](https://github.com/microsoft/vscode-isort) to format import order and grouping easily.

* docstring format

  A typical docstring should follow this [Google style](http://google.github.io/styleguide/pyguide.html#381-docstrings):

      """[Summary]

      Args:
          path (str): The path of the file to wrap
          field_storage (FileStorage): The :class:`FileStorage` instance to wrap
          temporary (bool): Whether or not to delete the file when the File
            instance is destructed

      Returns:
          BufferedFileStorage: A buffered writable file descriptor
      """

  This is enabled by using [sphinx.ext.napoleon](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/) extension in the `docs/conf.py` file.

  Alternatively, a default `sphinx` docstring style is also supported:

      """[Summary]

      :param [ParamName]: [ParamDescription], defaults to [DefaultParamVal]
      :type [ParamName]: [ParamType](, optional)
      ...
      :raises [ErrorType]: [ErrorDescription]
      ...
      :return: [ReturnDescription]
      :rtype: [ReturnType]
      """
  See examples and additional details [here](https://sphinx-rtd-tutorial.readthedocs.io/en/latest/docstrings.html).

Please also check this [CODING_STYLE_EXAMPLES.md](CODING_STYLE_EXAMPLES.md) doc for some common styling issues we should avoid.

In some particular cases, the style guidelines may not be suitable. Use your own best judgment,
and also raise the issue with the team members for their opinions. Read more about when to ignore a
particular guideline from [PEP8 doc](https://pep8.org/#a-foolish-consistency-is-the-hobgoblin-of-little-minds).

### Check your code style

We recommand to setup [fourmat](https://github.com/4Catalyzer/fourmat) as your code-style checker/formmater with your editor.
Fix the styling issue before you push the code to the github. [fourmat](https://github.com/4Catalyzer/fourmat) conviniently combines
three code-styling tools: [flake8](http://flake8.pycqa.org), [black](https://github.com/python/black) and [isort](https://pypi.org/project/isort/).

We have already included config files for code styling check and formatting: [.flake8](.flake8) for flake8 and [pyproject.toml] for black and isort,
so that we all use the same settings when run `fourmat`.

You can check out more config options for each tool:

* [flake8 configuration](http://flake8.pycqa.org/en/latest/user/configuration.html)
* [black configuration](https://black.readthedocs.io/en/stable/usage_and_configuration/the_basics.html#configuration-via-a-file)
* [isort configuration](https://github.com/microsoft/vscode-isort#settings)

#### run fourmat in CLI

* install fourmat

  ```bash
  pip install fourmat
  ```

* check code

  ```bash
  fourmat check <file_or_folder>
  ```

* format code

  ```bash
  fourmat fix <file_or_folder>
  ```

#### setup black and isort formatter in VSCode

* install both [Black Formatter](https://marketplace.visualstudio.com/items?itemName=ms-python.black-formatter) and
  [isort](https://marketplace.visualstudio.com/items?itemName=ms-python.isort) vscode extensions

* setup auto formatting on file dave:

  Add this settings to your vscode's `settings.json` file (up to you to set it at the project, user or workspace level):

  ```json
  "[python]": {
      "editor.defaultFormatter": "ms-python.black-formatter",
      "editor.formatOnSave": true,
      "editor.codeActionsOnSave": {
          "source.organizeImports": true
      },
  },
  "isort.args":["--profile", "black"],
  ```

#### Ignore some code-styling check errors

For some particular cases, if you think the reported errors are false alerts, you can [ignore specific errors/warnings at the specific code](https://flake8.pycqa.org/en/latest/user/violations.html#ignoring-violations-with-flake8) instead. A few common examples below:

```python
# ignore one or muliple errors
example = lambda: 'example'  # noqa: E731

example = lambda: 'example'  # noqa: E731,E123

# ignore all errors, but not recommended, better to include specific errors
example = lambda: 'example'  # noqa

# ignore the whole file if this is at the top:
# flake8: noqa
```

### Other recommended but not required style checks

You can always do extra checks on your code before commits. Some checkers may give you extra code style suggestions which will make your code more readable or more efficient (or not). Use your own judgement to decide to use it or not. For beginners, you may found it could be a good learning process to know some Python syntax you may not know before.

* Extra [flake8](http://flake8.pycqa.org) plugins

  You may find these flake plugins can be very useful (install them using `pip`):

      pip install flake8-builtins flake8-comprehensions flake8-logging-format pep8-naming

* [PyLint](https://www.pylint.org/)

      pip install pylint

  When there are conflicts with our style guidelines above from these extra checkers, follow our own rules.

### Setup pre-commit

Optionally, you can also setup [pre-commit](https://github.com/pre-commit/pre-commit) for your
local repository. We have included a [.pre-commit-config.yaml](.pre-commit-config.yaml) file in this repository.
You just need to install it on your local git repository:

    pre-commit install

### Some useful references

* [fourmat](https://github.com/4Catalyzer/fourmat):  Flake8 + Black + isort = ❤️
* [The list of Flake8 rules](https://lintlyci.github.io/Flake8Rules/)
* [The Black code style](https://black.readthedocs.io/en/stable/the_black_code_style/current_style.html)
* [**requests** code style](http://python-requests.org//en/latest/dev/contributing/#kenneth-reitz-s-code-style)
* [Google's Python code style guide](http://flake8.pycqa.org/en/latest/)
