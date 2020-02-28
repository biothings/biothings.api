# Contribution Guidelines

## Code style

### Code style we follow

The Python code style used by BioThings projects generally folllows [PEP8](https://pep8.org/).

In addition to the standards outlined in PEP 8, we have a few guidelines:

* Max line length

  Line-length can exceed 79 characters, we set max-line-length to 160 to ignore most of line-length warning,
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

* Inline comments

  When you have multiple inline comments, try to align them by groups:

  **Yes**

        self.number_of_shards = number_of_shards            # set number_of_shards when create_index
        self.number_of_replicas = int(number_of_replicas)   # set number_of_replicas when create_index
        self.step = step   # the bulk size when doing bulk operation.
        self.s = None      # number of records to skip, useful to continue indexing after an error.

  **No**

        self.number_of_shards = number_of_shards    # set number_of_shards when create_index
        self.number_of_replicas = int(number_of_replicas)    # set number_of_replicas when create_index
        self.step = step    # the bulk size when doing bulk operation.
        self.s = None    # number of records to skip, useful to continue indexing after an error.

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

In some particular cases, the style guidelines may not be suitable. Use your own best judgment,
and also raise the issue with the team members for their opinions. Read more about when to ignore a
particular guideline from [PEP8 doc](https://pep8.org/#a-foolish-consistency-is-the-hobgoblin-of-little-minds).

### Check your code style

We recommand to setup [flake8](http://flake8.pycqa.org) as your code-style checker with your editor.
Fix the styling issue before you push the code to the github.

You can use this as your [flake8](http://flake8.pycqa.org) [config file](http://flake8.pycqa.org/en/latest/user/configuration.html):

    [flake8]
    ignore=E226,E265,E302,E402,E731,F821,W503
    max-line-length=160

There are project-level [flake8](http://flake8.pycqa.org) settings provided in this top-level [setup.cfg](setup.cfg) file, so you can just run `flake8 <your file>` to check your code, or explicitly `flake8 --config ./setup.cfg <your file>`.

In VSCode, you can add this to settings.json:

    "python.linting.flake8Args": [
        "--max-line-length=160",
        "--ignore=E226,E265,E302,E402,E731,F821,W503"
            //E226  Missing whitespace around arithmetic operator
            //E265  block comment should start with '# '
            //E302  Expected 2 blank lines, found 0
            //E402  Module level import not at top of file
            //E731  Do not assign a lambda expression, use a def
            //F821  Undefined name name
            //W503  Line break occurred before a binary operator
    ],

### Other recommended but not required style checks

You can always do extra checks on your code before commits. Some checkers may give you extra code style suggestions which will make your code more readable or more efficient (or not). Use your own judgement to decide to use it or not. For beginners, you may found it could be a good learning process to know some Python syntax you may not know before.

* Extra [flake8](http://flake8.pycqa.org) plugins

  You may find these flake plugins can be very useful (install them using `pip`):

      pip install flake8-builtins flake8-comprehensions flake8-logging-format pep8-naming flake8-import-order

* [PyLint](https://www.pylint.org/)

      pip install pylint

  When there are conflicts with our style guidelines above from these extra checkers, follow our own rules.

### Python code formatter

Some Python code formatters can be useful, but use them with caution. Double check the converted code to make sure you don't break your own code.

* [black](https://github.com/python/black)
* [yapf](https://github.com/google/yapf/)

### Some useful references

* [**requests** code style](http://python-requests.org//en/latest/dev/contributing/#kenneth-reitz-s-code-style)
* [The list of Flake8 rules](https://lintlyci.github.io/Flake8Rules/)
* [Google's Python code style guide](http://flake8.pycqa.org/en/latest/)
