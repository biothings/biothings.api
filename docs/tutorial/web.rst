BioThings Web
---------------

In this tutorial we will start a Biothings API and learn to customize the API, 
adding new features and overriding the default behaviors, using increasingly
more advanced techniques step by step. In the end, you will be able to make
your own Biothings API, run other production APIs, like Mygene.info, and 
additionally, customize and add more features to those projects.

Before starting the tutorial, you should have the biothings package installed,
and have an Elasticsearch with only one index populated with this dataset 
`data <https://github.com/biothings/biothings.api/blob/master/tests/web/handlers/test_data.ndjson>`_ using this
`mapping <https://github.com/biothings/biothings.api/blob/master/tests/web/handlers/test_data_index.json>`_.

First, assuming your Elasticsearch service is running on the default port,
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
package versions, as well as the database cluster details, showing in the very
end because it is scheduled asynchronously at start time, but executed later
after the main program has launched.

Of all the information provided, note that it says the server is running on
port 8000, this is the default port we use when we start a Biothings API.
You can pass the "port" parameter during startup to configure the port our
service will run on, for example: ``python -m biothings.web --port=9000``.
Now we open the browser and when we access localhost:8000, we should be able
to see the biothings welcome page, showing the public routes in regex formats.

The last route in the welcome page shows the url pattern of the query API.
Let's use this pattern to access the query endpoint. Accessing http://localhost:8000/v1/query/
returns a JSON document containing 10 results from our elasticsearch index.

Let's explore some Biothings API feature here, adding a query parameter "fields"
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
and see all the returned results contain "cdk2", the value specified for "q" parameter. 
Is there a way to access individual documents directly by their "_id" or other
id fields? We can support this feature by providing more information for the
program in the ``config.py`` file. In the file, write::

    ES_HOST = "localhost:9200" # optional
    ES_INDICES = {"gene": "<your index name>"}
    
    ANNOTATION_DEFAULT_SCOPES = ["_id", "symbol"]

Restart your program and see an addtional route prefixed with /v1/gene is created
if you pay close attention to the console log. Now try the follwing URL:

http://localhost:8000/v1/gene/1017  
http://localhost:8000/v1/gene/CDK2

See that using both of the URL can take you staright to the document previously
mentioned. Note using the symbol field "CDK2" may yield multiple documents
because multiple documents may have the same key-value pair. This also means
"symbol" may not be a good choice of key field we want to support in the URL.

The query endpoint we first introduced is usually used to search documents with
text keywords. When we know the document id we want to access instead, we can
use the annotation endpoint in the previous example. These two endpoints are
the pillars for Biothings API. You can additional customize these endpoints
to work better with your data.

For example, if you think our default returned result for the query endpoint
is too verbose and we want to only include limited information unless the user
specifically asked for more, we can set a default "fields" value, for this
parameter used in the previous example. Open ``config.py`` and add::

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

We also see that in this example, the "entrezgene" field are numbers formatted
as strings. Let's modify the internal logic called the query pipeline to convert
this value to an integer just to show what we can do for customization. Add to
config.py::
    ES_RESULT_TRANSFORM = "pipeline.MyFormatter"
And create a file ``pipeline.py`` to include::

    from biothings.web.query import ESResultFormatter


    class MyFormatter(ESResultFormatter):

        def transform_hit(self, path, doc, options):

            if path == '' and 'entrezgene' in doc:  # root level
                try:
                    doc['entrezgene'] = int(doc['entrezgene'])
                except:
                    ...

Commit your changes and restart the web server process. Run some queries
and you should be able to see the "entrezgene" field now become integers::
    {
        "_id": "1017",
        "_score": 1,
        "entrezgene": 1017, # instead of the quoted "1017" (str)
        "name": "cyclin dependent kinase 2",
        "symbol": "CDK2",
        "taxid": 9606
    }

In the previous example, we are making changes to the query transformation
stage, controlled by the ESResultFormatter class, this is one of the three
stages that defined the query pipeline. The two stages coming before this
are represented by ESQueryBuilder and ESQueryEngine.

Let's addtionally try to add another feature to better sort the query result
incorporating domain knowledge of the data to make sure we deliver the most
user-friendly result. Additionally import ``ESQueryBuilder`` from ``biothings.web.query``
and also add ``from elasticsearch_dsl import Search`` in the ``pipeline.py``.
Customize the sort score by adding in the same file::

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

Make sure our application can pick up the change by adding this line to ``config.py``::

    ES_QUERY_BUILDER = "pipeline.MyQueryBuilder"

Save the file and restart the web server process. Search something and if you compare
with the application before, you may notice some result rankings have changed.
It is not easy to pick up this change if you are not familiar with the data,
visit http://localhost:8000/v1/query?q=kinase&rawquery instead and see that 
our code was indeed making a difference and get passed to elasticsearch, 
affecting the query result ranking. Notice the "rawquery" is a feature in 
our program to intercept the raw query we sent to elasticsearch for debugging.

Taking it one more step further, we can add more stages to the pipeline by 
overwriting the Pipeline class. Add to the config file::

    ES_QUERY_PIPELINE = "pipeline.MyQueryPipeline"

and add the following code to ``pipeline.py``::

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

The examples above demonstrated the customizations you can make on top of
our pre-defined APIs, for the most demanding tasks, you can additionally add
your own API routes to the web app.

Declare a new route in the config file as a usual first step::

    from biothings.web.settings.default import APP_LIST
    APP_LIST = [
        *APP_LIST, # keep the original ones
        (r"/{ver}/echo/(.+)", "handlers.EchoHandler"),
    ]

Let's make an echo handler that just echos what the user put in the URL, 
create a ``handlers.py`` and add::

    from biothings.web.handlers import BaseAPIHandler


    class EchoHandler(BaseAPIHandler):

        def get(self, text):
            self.write({
                "status": "ok",
                "result": text
            })

Now we have added a completely new feature not basing on any of the existing
biothings offerings, which can be as simple and as complex as you need.
Visiting http://localhost:8000/v1/echo/hello would give you::

    {
        "status": "ok",
        "result": "hello"
    }

in which case, the "hello" in "result" is the input we give the application
in the URL. Another convenient place to combine your existing application
logic with biothings or configuring the server level settings is to provide
a launching module, typically called ``index.py``, create this file in your
project folder and add these lines::

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
we added a redirction of a later-to-launch v2 query API, that we 
temporarily set to redirect to the v1 API and passing a tornado web
server, the default web server we use, static file configuration that
asks tornado to serve files under the static folder we specified, in
this case, named "static" and containing only one file.

After making the changes, visiting http://localhost:8000/v2/query/?q=cdk2
would direct you back to http://localhost:8000/v1/query/?q=cdk2 and by visiting
http://localhost:8000/static/file.txt you should see the random content you
previously created. Note in this step, you should run the python launcher
module directly by calling something like ``python index.py`` instead of 
running the first command we introduced.

Finishing this tutorial, you have completed the most common steps to customize
biothings.api from the smallest step, by passing a different parameter at 
launch time, to modifying the app code from the lower to highest level.
I hope you feel confident running biothings API now and please check out
the documentation page for more details on customizing APIs.
