BioThings Web
===============

In this tutorial we will start a Biothings API and learn to customize it, 
overriding the default behaviors and adding new features, using increasingly
more advanced techniques step by step. In the end, you will be able to make
your own Biothings API, run other production APIs, like Mygene.info, and 
additionally, customize and add more features to those projects.

.. attention::
    Before starting the tutorial, you should have the `biothings <https://pypi.org/project/biothings/>`_ package installed,
    and have an `Elasticsearch <https://www.elastic.co/guide/en/elasticsearch/reference/current/getting-started.html>`_ running 
    with only one index populated with this
    `dataset <https://github.com/biothings/biothings.api/blob/master/tests/web/handlers/test_data.ndjson>`_ using this
    `mapping <https://github.com/biothings/biothings.api/blob/master/tests/web/handlers/test_data_index.json>`_.
    You may also need a JSON Formatter browser extension for the best experience following this tutorial.
    (For `Chrome <https://chrome.google.com/webstore/detail/json-formatter/bcjindcccaagfpapjjmafapmmgkkhgoa>`_)

1. Starting an API server
---------------------------

First, assuming your Elasticsearch service is running on the default port 9200,
we can run a Biothings API with all default settings to explore the data,
simply by creating a ``config.py`` under your project folder. After creating
the file, run ``python -m biothings.web`` to start the API server. You should
be able to see the following console output::

    [I 211130 22:21:57 launcher:28] Biothings API 0.10.0
    [I 211130 22:21:57 configs:86] <module 'config' from 'C:\\Users\\Jerry\\code\\biothings.tutorial\\config.py'>
    [INFO biothings.web.connections:31] <Elasticsearch([{'host': 'localhost', 'port': 9200}])>
    [INFO biothings.web.connections:31] <AsyncElasticsearch([{'host': 'localhost', 'port': 9200}])>
    [INFO biothings.web.applications:137] API Handlers:
        [('/', <class 'biothings.web.handlers.services.FrontPageHandler'>, {}),
        ('/status', <class 'biothings.web.handlers.services.StatusHandler'>, {}),
        ('/metadata/fields/?', <class 'biothings.web.handlers.query.MetadataFieldHandler'>, {}),
        ('/metadata/?', <class 'biothings.web.handlers.query.MetadataSourceHandler'>, {}),
        ('/v1/spec/?', <class 'biothings.web.handlers.services.APISpecificationHandler'>, {}),
        ('/v1/doc(?:/([^/]+))?/?', <class 'biothings.web.handlers.query.BiothingHandler'>, {'biothing_type': 'doc'}),
        ('/v1/metadata/fields/?', <class 'biothings.web.handlers.query.MetadataFieldHandler'>, {}),
        ('/v1/metadata/?', <class 'biothings.web.handlers.query.MetadataSourceHandler'>, {}),
        ('/v1/query/?', <class 'biothings.web.handlers.query.QueryHandler'>, {})]
    [INFO biothings.web.launcher:99] Server is running on "0.0.0.0:8000"...
    [INFO biothings.web.connections:25] Elasticsearch Package Version: 7.13.4
    [INFO biothings.web.connections:27] Elasticsearch DSL Package Version: 7.3.0
    [INFO biothings.web.connections:51] localhost:9200: docker-cluster 7.9.3

Note the console log shows the API version, the config file it uses, its 
database connections, HTTP routes, service port, important python dependency
package versions, as well as the database cluster details.

.. note::
    The cluster detail appears as the last line, sometimes with a delay, 
    because it is scheduled asynchronously at start time, but executed later
    after the main program has launched. The default implementation of our
    application is `asynchronous and non-blocking <https://www.tornadoweb.org/en/stable/guide/async.html>`_
    based on `asyncio <https://docs.python.org/3/library/asyncio.html>`_
    and `tornado.ioloop <https://www.tornadoweb.org/en/stable/ioloop.html>`_ interface.
    The specific logic in this case is implemented in the :py:mod:`biothings.web.connections` module.

Of all the information provided, note that it says the server is running on
port 8000, this is the default port we use when we start a Biothings API.
It means you can acccess the API by opening http://localhost:8000/ in your
browser in most of the cases. 

.. note::
    If this port is occupied, you can pass the "port" parameter during startup to change 
    it, for example, running ``python -m biothings.web --port=9000``. 
    The links in the tutorial assume the services is running on the default port 8000.
    If you are running the service on a differnt port. You need to modify the URLs 
    provided in the tutorial before opening in the browser.

Now open the browser and access localhost:8000, we should be able to see the biothings 
welcome page, showing the public routes in `regex <https://en.wikipedia.org/wiki/Regular_expression>`_ formats reading like::

    /
    /status
    /metadata/fields/?
    /metadata/?
    /v1/spec/?
    /v1/doc(?:/([^/]+))?/?
    /v1/metadata/fields/?
    /v1/metadata/?
    /v1/query/?

2. Exploring an API endpoint
------------------------------

The last route on the welcome page shows the URL pattern of the query API.
Let's use this pattern to access the query endpoint. Accessing http://localhost:8000/v1/query/
returns a JSON document containing 10 results from our elasticsearch index.

Let's explore some Biothings API features here, adding a query parameter "fields"
to limit the fields returned by the API, and another parameter "size" to limit
the returned document number. If you used the dataset mentioned at the start
of the tutorial, accessing http://localhost:8000/v1/query?fields=symbol,alias,name&size=1
should return a document like this::

    {
        "took": 15,
        "total": 1030,
        "max_score": 1,
        "hits": [
            {
                "_id": "1017",
                "_score": 1,
                "alias": [
                    "CDKN2",
                    "p33(CDK2)"
                ],
                "name": "cyclin dependent kinase 2",
                "symbol": "CDK2"
            }
        ]
    }

The most commonly used parameter is the "q" parameter, try http://localhost:8000/v1/query?q=cdk2
and see all the returned results contain "cdk2", the value specified for the "q" parameter.

.. note::

    For a list of the supporting parameters, visit `Biothings API Specifications <https://biothings.io/specs/>`_.
    The documentation for our most popular service https://mygene.info/ also covers a lot of features also
    available in all biothings applications. Read more on
    `Gene Query Service <https://docs.mygene.info/en/latest/doc/query_service.html>`_ and
    `Gene Annotation Service <https://docs.mygene.info/en/latest/doc/annotation_service.html>`_.


3. Customizing an API through the config file
------------------------------------------------

In the previous step, we tested document exploration by search its content.
Is there a way to access individual documents directly by their "_id" or other
id fields? We can look at the annotation endpoint doing exactly that.

By default, this endpoint is accessible by an URL pattern like this: ``/<ver>/doc/<_id>``
where "ver" refers to the API version. In our case, if we want to access a document
with an id of "1017", one of those doc showing up in the previous example, 
we can try: http://localhost:8000/v1/doc/1017

.. note::

    To configure a different API version other than "v1" for your program, add a prefix
    to all API patterns, like /api/<ver>/..., or remove these patterns, make changes
    in the config file modifying the settings prefixed with "APP", as those control 
    the web application behavior. A web application is basically a collection of routes
    and settings that can be understood by a web server. See :py:mod:`biothings.web.settings.default`
    source code to look at the current configuration and refer to :py:mod:`biothings.web.applications`
    to see how the settings are turned to routes in different web frameworks.

In this dataset, we know the document type can be best described as "gene"s.
We can enable a widely-used feature, document type URL templating, by providing
more information to the biothings app in the ``config.py`` file. Write the 
following lines to the config file:

.. code-block:: python
    :linenos:

    ES_HOST = "localhost:9200" # optional
    ES_INDICES = {"gene": "<your index name>"}
    
    ANNOTATION_DEFAULT_SCOPES = ["_id", "symbol"]

.. note::

    The ``ES_HOST`` setting is a common parameter that you see in the config file.
    Although it is not making a difference here, you can configure the value of
    this setting to ask biothings.web to connect to a different Elasticsearch
    server, maybe hosted remotely on the cloud. The ``ANNOTATION_DEFAULT_SCOPES``
    setting specifies the document fields we consider as the id fields. By default,
    only the "_id" field in the document, a must-have field in Elasticsearch,
    is considered the biothings id field. We additionally added the "symbol" field,
    to allow the user to it to find documents in this demo API.

Restart your program and see the annotation route is now prefixed with /v1/gene
if you pay close attention to the console log. Now try the following URL:

| http://localhost:8000/v1/gene/1017
| http://localhost:8000/v1/gene/CDK2

See that using both of the URLs can take you straight to the document previously
mentioned. Note using the symbol field "CDK2" may yield multiple documents
because multiple documents may have the same key-value pair. This also means
"symbol" may not be a good choice of the key field we want to support in the URL.

These two endpoints, annotation and query, are the pillars for Biothings API. 
You can additionally customize these endpoints to work better with your data.

For example, if you think our returned result by default from the query endpoint
is too verbose and we want to only include limited information unless the user
specifically asked for more, we can set a default "fields" value, for this
parameter used in the previous example. Open ``config.py`` and add:

.. code-block:: python

    from biothings.web.settings.default import QUERY_KWARGS
    QUERY_KWARGS['*']['_source']['default'] = ['name', 'symbol', 'taxid', 'entrezgene']

Restart your program after changing the config file and visit http://localhost:8000/v1/query,
see the effect of specifying default fields to return. Like this::

    {
        "took": 9,
        "total": 100,
        "max_score": 1,
        "hits": [
            {
                "_id": "1017",
                "_score": 1,
                "entrezgene": "1017",
                "name": "cyclin dependent kinase 2",
                "symbol": "CDK2",
                "taxid": 9606
            },
            {
                "_id": "12566",
                "_score": 1,
                "entrezgene": "12566",
                "name": "cyclin-dependent kinase 2",
                "symbol": "Cdk2",
                "taxid": 10090
            },
            {
                "_id": "362817",
                "_score": 1,
                "entrezgene": "362817",
                "name": "cyclin dependent kinase 2",
                "symbol": "Cdk2",
                "taxid": 10116
            },
            ...
        ]
    }

4. Customizing an API through pipeline stages
-----------------------------------------------

In the previous example, the numbers in the "entrezgene" field are typed as strings. 
Let's modify the internal logic called the query pipeline to convert these values 
to integers just to show what we can do in customization. 

.. note::

    The pipeline is one of the :py:mod:`biothings.web.services`. It defines the intermediate 
    steps or stages we take to execute a query. See :py:mod:`biothings.web.query` to learn
    more about the individual stages.

Add to config.py:

.. code-block:: python

    ES_RESULT_TRANSFORM = "pipeline.MyFormatter"

And create a file ``pipeline.py`` to include:

.. code-block:: python
    :linenos:

    from biothings.web.query import ESResultFormatter


    class MyFormatter(ESResultFormatter):

        def transform_hit(self, path, doc, options):

            if path == '' and 'entrezgene' in doc:  # root level
                try:
                    doc['entrezgene'] = int(doc['entrezgene'])
                except:
                    ...

Commit your changes and restart the webserver process. Run some queries
and you should be able to see the "entrezgene" field now showing as integers::

    {
        "_id": "1017",
        "_score": 1,
        "entrezgene": 1017, # instead of the quoted "1017" (str)
        "name": "cyclin dependent kinase 2",
        "symbol": "CDK2",
        "taxid": 9606
    }

In this example, we made changes to the query transformation stage, 
controlled by the :py:class:`biothings.web.query.formatter.ESResultFormatter` class, 
this is one of the three stages that defined the query pipeline. 
The two stages coming before it are represented by 
:py:class:`biothings.web.query.engine.AsyncESQueryBackend` and
:py:class:`biothings.web.query.builder.ESQueryBuilder`.

Let's try to modify the query builder stage to add another feature. We'll incorporate 
domain knowledge here to deliver more user-friendly seach result by scoring the documents
with a few rules to increase result relevancy. Additionally add to the ``pipeline.py`` file:


.. code-block:: python

    from biothings.web.query import ESQueryBuilder 
    from elasticsearch.dsl import Search

    class MyQueryBuilder(ESQueryBuilder):

        def apply_extras(self, search, options):

            search = Search().query(
                "function_score",
                query=search.query,
                functions=[
                    {"filter": {"term": {"name": "pseudogene"}}, "weight": "0.5"},  # downgrade
                    {"filter": {"term": {"taxid": 9606}}, "weight": "1.55"},
                    {"filter": {"term": {"taxid": 10090}}, "weight": "1.3"},
                    {"filter": {"term": {"taxid": 10116}}, "weight": "1.1"},
                ], score_mode="first")

            return super().apply_extras(search, options)

Make sure our application can pick up the change by adding this line to ``config.py``:

.. code-block:: python

    ES_QUERY_BUILDER = "pipeline.MyQueryBuilder"

.. note::

    We wrapped our original query logic in an Elasticsearch compound query `fucntion score query
    <https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-function-score-query.html>`_.
    For more on writing python-friendly Elasticsearch queries, see `Elasticsearch DSL
    <https://elasticsearch-dsl.readthedocs.io/en/latest/>`_ package, one of the dependencies
    used in :py:mod:`biothings.web`.

Save the file and restart the webserver process. Search something and if you compare
with the application before, you may notice some result rankings have changed.
It is not easy to pick up this change if you are not familiar with the data,
visit http://localhost:8000/v1/query?q=kinase&rawquery instead and see that 
our code was indeed making a difference and get passed to elasticsearch, 
affecting the query result ranking. Notice the "rawquery" is a feature in 
our program to intercept the raw query we sent to elasticsearch for debugging.

5. Customizing an API through pipeline services
--------------------------------------------------

Taking it one more step further, we can add more procedures or stages to 
the pipeline by overwriting the Pipeline class. Add to the config file:

.. code-block:: python

    ES_QUERY_PIPELINE = "pipeline.MyQueryPipeline"

and add the following code to ``pipeline.py``:

.. code-block:: python

    class MyQueryPipeline(AsyncESQueryPipeline):

        async def fetch(self, id, **options):

            if id == "tutorial":
                res = {"_welcome": "to the world of biothings.api"}
                res.update(await super().fetch("1017", **options))
                return res

            res = await super().fetch(id, **options)
            return res

Now we made ourselves a tutorial page to show what annotation results
can look like, by visiting http://localhost:8000/v1/gene/tutorial, you
can see what http://localhost:8000/v1/gene/1017 would typically give you, 
and the additional welcome message::

    {
        "_welcome": "to the world of biothings.api",
        "_id": "1017",
        "_version": 1,
        "entrezgene": 1017,
        "name": "cyclin dependent kinase 2",
        "symbol": "CDK2",
        "taxid": 9606
    }

.. note::

    In this example, we modified the query pipeline's "fetch" method,
    the one used in the annotation endpoint, to include some additional
    logic before executing what we would typically do. The call to the "super" 
    function executes the typical query building, executing and formatting stages.


6. Customizing an API through the web app
-------------------------------------------

The examples above demonstrated the customizations you can make on top of
our pre-defined APIs, for the most demanding tasks, you can additionally add
your own API routes to the web app.

Modify the config file as a usual first step. Declare a new route by adding:

.. code-block:: python

    from biothings.web.settings.default import APP_LIST

    APP_LIST = [
        *APP_LIST, # keep the original ones
        (r"/{ver}/echo/(.+)", "handlers.EchoHandler"),
    ]

Let's make an echo handler that just echos what the user puts in the URL. 
Create a ``handlers.py`` and add:

.. code-block:: python
    :linenos:

    from biothings.web.handlers import BaseAPIHandler


    class EchoHandler(BaseAPIHandler):

        def get(self, text):
            self.write({
                "status": "ok",
                "result": text
            })

Now we have added a completely new feature not based on any of the existing
biothings offerings, which can be as simple and as complex as you need.
Visiting http://localhost:8000/v1/echo/hello would give you::

    {
        "status": "ok",
        "result": "hello"
    }

in which case, the "hello" in "result" field is 
the input we give the application in the URL. 

7. Customizing an API through the app launcher
-----------------------------------------------

Another convenient place to customize the API is to have a launching module, 
typically called ``index.py``, and pass parameters to the starting function,
provided as :py:func:`biothings.web.launcher.main`. Create an ``index.py``
in your project folder:

.. code-block:: python
    :linenos:

    from biothings.web.launcher import main
    from tornado.web import RedirectHandler

    if __name__ == '__main__':
        main([
            (r"/v2/query(.*)", RedirectHandler, {"url": "/v1/query{0}"})
        ], {
            "static_path": "static"
        })

Create another folder called "static" and add a file of random content
named "file.txt" under the newly created static folder. In this step,
we added a redirection of a later-to-launch v2 query API, that we 
temporarily set to redirect to the v1 API and passed a static file configuration
that asks tornado to serve files under the static folder we specified 
to the tornado webserver, the default webserver we use. The static folder is
named "static" and contains only one file in this example.

.. note::

    For more on configuring route redirections and other application features in tornado, see
    `RedirectHandler <https://www.tornadoweb.org/en/stable/web.html#tornado.web.RedirectHandler>`_ and
    `Application configuration <https://www.tornadoweb.org/en/stable/web.html#tornado.web.Application.settings>`_.

After making the changes, visiting http://localhost:8000/v2/query/?q=cdk2
would direct you back to http://localhost:8000/v1/query/?q=cdk2 and by visiting
http://localhost:8000/static/file.txt you should see the random content you
previously created. Note in this step, you should run the python launcher
module directly by calling something like ``python index.py`` instead of 
running the first command we introduced. Running the launcher directly is
also how we start most of our user-facing products that require complex 
configurations, like http://mygene.info/. ts code is publicly available at 
https://github.com/biothings/mygene.info under the `Biothings Organization
<https://github.com/biothings>`_.


The End
---------

Finishing this tutorial, you have completed the most common steps to customize
biothings.api. The customization starts from passing a different parameter at 
launch time and evolve to modifying the app code at different levels.
I hope you feel confident running biothings API now and please check out
the documentation page for more details on customizing APIs.
