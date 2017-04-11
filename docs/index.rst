.. biothings.api documentation master file, created by
   sphinx-quickstart on Thu Jul  7 15:58:06 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

BioThings.API
=============

BioThings.API is a `Python-based <https://www.python.org/>`_ combined web and data-loading framework used to create and customize high-performance data APIs.  The BioThings.API data-loading tools allow you to automate the process of uploading your data to an `Elasticsearch <https://www.elastic.co/products/elasticsearch>`_ backend.  From here, the BioThings.API web framework - built on the high-concurrency `Tornado Web Server <http://www.tornadoweb.org/en/stable/>`_ - allows you to easily setup high-performance API endpoints to powerfully query your data using `Elasticsearch's full-text query capabilities and query language <https://www.elastic.co/guide/en/elasticsearch/reference/2.3/query-dsl-query-string-query.html#query-string-syntax>`_.  The `MyGene.Info <http://mygene.info/>`_ and `MyVariant.Info <http://myvariant.info/>`_ web services are examples of data APIs built and maintained using tools from BioThings.API.

.. raw :: html

    <div>
    <center><img src="_static/biothings_overview.png" usemap="#overviewmap"></center>

    <map name="overviewmap">
        <area shape="rect" coords="7,7,314,284" href="doc/data_loading.html">
        <area shape="rect" coords="7,313,313,458" href="doc/web.html">
    </map>
    </div>

    <div id="spacer" style="height:300px"></div>

.. toctree::
   :maxdepth: 3

   doc/data_loading
   doc/web
   doc/tutorial

.. Indices and tables
.. ==================

.. * :ref:`genindex`
.. * :ref:`modindex`
.. * :ref:`search`

