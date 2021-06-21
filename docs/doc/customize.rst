=========================
Customize a BioThings API
=========================

With BioThings SDK, you can already start a live BioThings API with its default settings. For example,

.. code:: python

    Show some code snippets here for multiple ways to start a default BioThings API


Customize with config.py
========================

Ref. :py:class:`biothings.web.settings.default`


Customize existing handler behaviors
====================================

The default BioThings endpoints (e.g. ``/<biothing_type>``, ``/query`` and ``/metadata`` endpoints) can be further
customized when it's necessary. For example, you can add an additional query parameter and change the
underlying Elasticsearch queries.

Here we show you serveral customization examples:

 1. Add a new query parameter

    * Create your own pipeline module with subclass of ESQueryBuilder/ESQueryBackend/RSResultFormatter
    * Set it in config.py with ``ES_QUERY_BUILDER`` parameter.

 2. Example 2

Create a new handler
====================

* Create your subclass of :py:class:`biothings.web.handlers.ESRequestHandler` or :py:class:`biothings.web.handlers.BaseAPIHandler` or :py:class:`biothings.web.handlers.BaseHandler`
* Add the new handler to the routes.