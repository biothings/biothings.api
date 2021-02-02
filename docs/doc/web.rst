#############
Web component
#############

The BioThings SDK web component contains tools used to generate and customize an API, given an Elasticsearch index with data.
The web component uses the Tornado Web Server to respond to incoming API requests.

.. py:module:: biothings.web

******************
Server boot script
******************

.. automodule:: biothings.web.index_base

.. automethod:: biothings.web.index_base.main

********
Settings
********

.. automodule:: biothings.web.settings

Config module
=============

BiothingWebSettings
===================

.. autoclass:: biothings.web.settings.BiothingWebSettings
    :members:

BiothingESWebSettings
=====================

.. autoclass:: biothings.web.settings.BiothingESWebSettings
    :members:

********
Handlers
********

BaseHandler
===========

.. autoclass:: biothings.web.handlers.BaseHandler
    :members:

FrontPageHandler
----------------

.. autoclass:: biothings.web.handlers.FrontPageHandler
    :members:

StatusHandler
-------------

.. autoclass:: biothings.web.handlers.StatusHandler
    :members:


BaseESRequestHandler
====================

.. autoclass:: biothings.web.handlers.BaseESRequestHandler
    :members:

BiothingHandler
---------------

.. autoclass:: biothings.web.handlers.BiothingHandler
    :members:

QueryHandler
------------

.. autoclass:: biothings.web.handlers.QueryHandler
    :members:

MetadataFieldHandler
--------------------

.. autoclass:: biothings.web.handlers.MetadataFieldHandler
    :members:


MetadataSourceHandler
---------------------

.. autoclass:: biothings.web.handlers.MetadataSourceHandler
    :members:

ESRequestHandler
----------------

.. autoclass:: biothings.web.handlers.ESRequestHandler
    :members:

BaseAPIHandler
==============

.. autoclass:: biothings.web.handlers.BaseAPIHandler
    :members:

APISpecificationHandler
-----------------------

.. autoclass:: biothings.web.handlers.APISpecificationHandler
    :members:

********
Pipeline
********

Elasticsearch Query Builder
===========================

.. autoclass:: biothings.web.pipeline.ESQueryBuilder
    :members:

Elasticsearch Query Execution
=============================

.. autoclass:: biothings.web.pipeline.ESQueryBackend
    :members:

Elasticsearch Result Transformer
================================

.. autoclass:: biothings.web.pipeline.ESResultTransform
    :members:
