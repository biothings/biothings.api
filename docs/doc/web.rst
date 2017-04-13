#########
Web tools
#########

The BioThings.API web tools use the Tornado Web Server to respond to incoming API requests.

.. py:module:: biothings

******************
Server boot script
******************

.. automodule:: biothings.www.index_base

.. automethod:: biothings.www.index_base.main

********
Settings
********

.. automodule:: biothings.www.settings

Config module
=============

BiothingWebSettings
===================
.. autoclass:: biothings.www.settings.BiothingWebSettings
    :members:

BiothingESWebSettings
=====================

.. autoclass:: biothings.www.settings.BiothingESWebSettings
    :members:

********
Handlers
********

BaseHandler
===========

.. autoclass:: biothings.www.api.helper.BaseHandler
    :members:

BaseESRequestHandler
====================

.. autoclass:: biothings.www.api.es.handlers.base_handler.BaseESRequestHandler
    :members:

BiothingHandler
---------------

.. autoclass:: biothings.www.api.es.handlers.biothing_handler.BiothingHandler
    :members:

QueryHandler
------------

.. autoclass:: biothings.www.api.es.handlers.query_handler.QueryHandler
    :members:

MetadataHandler
---------------

.. autoclass:: biothings.www.api.es.handlers.metadata_handler.MetadataHandler
    :members:

***************************
Elasticsearch Query Builder
***************************

.. autoclass:: biothings.www.api.es.query_builder.ESQueries
    :members:

.. autoclass:: biothings.www.api.es.query_builder.ESQueryBuilder
    :members:

*******************
Elasticsearch Query
*******************

.. autoclass:: biothings.www.api.es.query.ESQuery
    :members:

********************************
Elasticsearch Result Transformer
********************************

.. autoclass:: biothings.www.api.es.transform.ESResultTransformer
    :members:
