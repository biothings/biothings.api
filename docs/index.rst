.. biothings.api documentation master file, created by
   sphinx-quickstart on Thu Jul  7 15:58:06 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

BioThings SDK
=============

The BioThings SDK is a `Python-based <https://www.python.org/>`_ collection of data-loading and web tools used to create and customize high-performance data APIs.  The BioThings data-loading tools allow you to automate the process of uploading your data to an `Elasticsearch <https://www.elastic.co/products/elasticsearch>`_ backend.  From here, the BioThings web tools - built on the high-concurrency `Tornado Web Server <http://www.tornadoweb.org/en/stable/>`_ - allows you to easily setup high-performance API endpoints to query your data using `Elasticsearch's full-text query capabilities and query language <https://www.elastic.co/guide/en/elasticsearch/reference/2.3/query-dsl-query-string-query.html#query-string-syntax>`_.  The `MyGene.Info <http://mygene.info/>`_ and `MyVariant.Info <http://myvariant.info/>`_ web services are examples of data APIs built and maintained using tools from the BioThings SDK.

Installing BioThings
--------------------

You can install the BioThings SDK with pip, like:

::
    
    pip install git+https://github.com/SuLab/biothings.api.git#egg=biothings

Alternatively, you can download the source code, or clone the `BioThings SDK repository <http://github.com/biothings/biothings.api>`_ and run:

::
    
    python setup.py install

BioThings Tutorials
-----------------------

Check out a simple ``"Hello, World!"`` example, or more advanced tutorials `here <doc/tutorial.html>`_.

.. toctree::
    :maxdepth: 3
    :caption: BioThings Tutorials
 
    doc/single_source_tutorial
    doc/multiple_sources_tutorial

.. toctree::
    :maxdepth: 3
    :caption: BioThings Code Documentation

    doc/hub
    doc/web

.. Indices and tables
.. ==================

.. * :ref:`genindex`
.. * :ref:`modindex`
.. * :ref:`search`

