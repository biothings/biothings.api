
Introduction
------------

What's BioThings?
*****************
We use "**BioThings**" to refer to objects of any biomedical entity-type represented in the biological knowledge space,
such as genes, genetic variants, drugs, chemicals, diseases, etc.

BioThings SDK
*************

SDK represents "Software Development Kit". BioThings SDK provides a `Python-based <https://www.python.org/>`_ toolkit to build high-performance data APIs (or web services) from a single data source or multiple data sources. It has the particular focus on building data APIs for biomedical-related entities, a.k.a "*BioThings*", though it's not necessarily limited to the biomedical scope.  For any given "*BioThings*" type, BioThings SDK helps developers to aggregate annotations from multiple data sources, and expose them as a clean and high-performance web API.

The BioThings SDK can be roughly divided into two main components: data hub (or just "hub") component and web component. The hub component allows developers to automate the process of monitoring, parsing and uploading your data source to an `Elasticsearch <https://www.elastic.co/products/elasticsearch>`_ backend. From here, the web component, built on the high-concurrency `Tornado Web Server <http://www.tornadoweb.org/en/stable/>`_ , allows you to easily setup a live high-performance API. The API endpoints expose simple-to-use yet powerful query features using `Elasticsearch's full-text query capabilities and query language <https://www.elastic.co/guide/en/elasticsearch/reference/2.4/query-dsl-query-string-query.html#query-string-syntax>`_.

BioThings API
*************
We also use "*BioThings API*" (or *BioThings APIs*) to refer to an API (or a collection of APIs) built with BioThings SDK. For example, both our popular `MyGene.Info <http://mygene.info/>`_ and `MyVariant.Info <http://myvariant.info/>`_ APIs are built and maintained using this BioThings SDK.

BioThings Studio
****************
*BioThings Studio* is a buildin, pre-configured environment used to build and administer BioThings API. At its core is the *Hub*,
a backend service responsible for maintaining data up-to-date, producing data releases and update API frontends.


Installation
------------

You can install the latest stable BioThings SDK release with pip from `PyPI <https://pypi.python.org/pypi>`_, like:

::

    pip install biothings

You can install the latest development version of BioThings SDK directly from our github repository like:

::

    pip install git+https://github.com/biothings/biothings.api.git#egg=biothings

Alternatively, you can download the source code, or clone the `BioThings SDK repository <http://github.com/biothings/biothings.api>`_ and run:

::

    python setup.py install

Quick Start
-----------

We recommend to follow `this tutorial <tutorial/studio.html>`_ to develop your first BioThings API in our pre-configured `BioThings Studio <doc/studio.html>`_ development environment.


.. toctree::
    :hidden:
    :caption: Products

    tutorial/studio
    tutorial/cli
    tutorial/standalone
    tutorial/hub_tutorial
    tutorial/web
    tutorial/datatransform

.. toctree::
   :hidden:
   :caption: Documentation

   apidoc/biothings.web
   apidoc/biothings.tests
   apidoc/biothings.utils
   apidoc/biothings.hub
   apidoc/biothings.cli
