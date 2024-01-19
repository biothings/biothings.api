*************
BioThings CLI
*************

Introduction
============

The BioThings CLI (Command Line Interface) provides a set of conveniance command
line tools for developers to create and test data plugins locally. Compared to the
option of setting up a local Hub running in docker containers, the CLI further lowers the
entry barrier by NOT requiring docker or any external databases installed locally.
It is particularly suitable for data plugin developers to build and test their data plugin
independantly. When a data plugin is ready, they can then pass the data plugin to a running
BioThings Hub to build the data plugin into a BioThings API.

This tutorial aims to provide a comprehensive guide to the BioThings CLI,
covering its essential commands and functionalities. We will explore a range of
topics including installation, initial setup, and core features such as data
plugin management, data plugin hub management, and utility commands.
Additionally, we will delve into practical applications of the CLI, demonstrating
how to work with the API server for data access and manipulation.

=============
Prerequisites
=============

To use the BioThings CLI, you need to have Python installed on your system, specifically version 3.7 or higher.

Ensure that your Python version meets this requirement by running:

.. code-block:: bash

    python --version

If you need to install or upgrade Python, visit the official Python website at https://www.python.org/downloads/ for the latest version.

In addition to Python 3.8 or higher, having Git installed on your system is essential for using the BioThings CLI, particularly if you need to clone repositories or manage version-controlled code.

To check if Git is installed on your system, run:

.. code-block:: bash

    git --version

If Git is not installed, you can download and install it from the official Git website:

- For Windows and macOS: Visit [Git's official download page](https://git-scm.com/downloads).
- For Linux: Use your distribution's package manager (e.g., `apt-get install git` for Ubuntu, `yum install git` for Fedora).

After installing Git, you can proceed with the setup and usage of the BioThings CLI.

===========
Setting Up
==========
Clone the tutorials repository on our Biothings group.

.. code:: bash
   git clone https://github.com/biothings/tutorials.git
   cd tutorials
   git checkout pharmgkb_v5

Now we will need to install the requirements to run our Biothings CLI. We will first create a virtual environment and then install a Biothings Hub CLI environemt.
.. code:: bash
   python -m venv .venv
   source ./.venv/bin/activate
   pip install "biothings[cli]"


======================================
Create and Test the dataplugin via CLI
======================================

Lets check out our command line inputs. Here is a quick summary of every command we will be using in this tutorial.

* biothings-cli dataplugin dump: Download source data files to local
* biothings-cli dataplugin list: Listing dumped files or uploaded sources
* biothings-cli dataplugin upload: Convert downloaded data from dump step into JSON documents and upload the to the source database
* biothings-cli dataplugin serve: *serve* command runs a simple API server for serving documents from the source database.
* biothings-cli dataplugin clean: Delete all dumped files and drop uploaded sources tables

If you have any futher questions on what other options are available in our biothings-cli. You can check out more using the ``--help`` or ``-h`` flag on any attribute. Examples:

* ``biothings-cli --help``
* ``biothings-cli dataplugin --help``
* ``biothings-cli dataplugin dump -h``

The Biothings CLI can only be used for a manifest based plugin. Looking at our manifest file, we are using a JSON based manifest with multiple uploaders.
Check out our `manifest section <studio.html#manifest-plugins>`_ to know more about the different types of manifest files that can be used with our Hub.

.. code:: bash
    {
        "version": "0.3",
        "requires" : ["pandas", "numpy"],
        "dumper" : {
            "data_url" : ["https://s3.pgkb.org/data/annotations.zip",
                        "https://s3.pgkb.org/data/drugLabels.zip",
                        "https://s3.pgkb.org/data/occurrences.zip"],
            "uncompress" : true
        },
        "uploaders" : [
            {
                "name" : "annotations",
                "parser" : "parser:load_annotations",
                "mapping" : "parser:custom_annotations_mapping",
                "on_duplicates" : "error"
            },
            {
                "name" : "druglabels",
                "parser" : "parser:load_druglabels",
                "on_duplicates" : "error"
            },
            {
                "name" : "occurrences",
                "parser" : "parser:load_occurrences",
                "on_duplicates" : "error"
            }
        ]
    }




----------
Subsection
----------
