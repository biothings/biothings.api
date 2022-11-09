# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))


# -- Project information -----------------------------------------------------
import datetime

current_year = datetime.datetime.now().year
project = 'BioThings SDK'
copyright = f'{current_year}, BioThings'
author = 'BioThings team'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',   # support numpy and google style docstring
    'sphinx_reredirects'     # handle old page redirection see "redirects" setting below
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
# html_theme = 'alabaster'    #  this is the sphinx default

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']


# on_rtd is whether we are on readthedocs.org
import os
on_rtd = os.environ.get('READTHEDOCS', None) == 'True'

if not on_rtd:
    # only import and set the theme if we're building docs locally
    try:
        import sphinx_rtd_theme
        html_theme = 'sphinx_rtd_theme'
        html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]
    except ImportError:
        print('Warning: "sphinx_rtd_theme" is not installed, fall back to default theme.')
# otherwise, readthedocs.org uses their theme by default, so no need to specify it


# Both the class’ and the __init__ method’s docstring are concatenated and inserted.
# Ref: http://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#confval-autoclass_content
autoclass_content = 'both'

redirects = {
    "doc/studio": "../tutorial/studio.html"
}

# Reference Code
# ---------------------
# Previous attemps to configure biothings.hub
# to support automatically generating documentation.
# This would not directly work for 0.10.x.
# Use the code below as a reference in the future
# if we decide to want to generate hub docs.

# create a dummy config object
# class DummyConfig(object):
#     def __getattr__(self, item):
#         setattr(self, item, None)
#         return getattr(self, item)


# this allows "import config" or "from biothings import config"
#  to work without actually creating a config.py file
# sys.modules["config"] = DummyConfig()
# sys.modules["biothings.config"] = DummyConfig()
