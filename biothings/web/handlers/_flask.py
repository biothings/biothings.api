from functools import wraps
from types import CoroutineType

import flask
from tornado.template import Loader

from biothings.web import templates
from biothings.web.options import OptionError
from biothings.web.query.pipeline import QueryPipelineException, QueryPipelineInterrupt

routes = []


def route(pattern, methods=("GET", "POST")):
    def A(f):
        async def B(*args, **kwargs):
            biothings = flask.current_app.biothings
            optionsets = biothings.optionsets
            optionset = optionsets.get(f.__name__)
            if optionset:
                try:
                    _args = optionset.parse(
                        flask.request.method,
                        (
                            (tuple(kwargs.values()), {}),
                            flask.request.args,
                            flask.request.form,
                            flask.request.get_json(),
                        ),
                    )
                except OptionError as err:
                    return err.info, 400
            else:
                _args = {}
            result = f(biothings, _args)
            if isinstance(result, CoroutineType):
                return await result
            return result

        B.pattern = pattern
        B.methods = methods
        B.name = f.__name__
        routes.append(B)
        return B

    return A


@route("/")
def homepage(biothings, args):
    loader = Loader(templates.__path__[0])
    template = loader.load("home.html")
    return template.generate(
        alert="Front Page Not Configured.",
        title="Biothings API",
        contents=biothings.handlers.keys(),
        support=biothings.metadata.types,
        url="http://biothings.io/",
    )


def handle_es_conn(f):
    @wraps(f)
    async def _(biothings, *args, **kwargs):
        client = biothings.elasticsearch.async_client
        # because of the flask execution model
        # each time the async function is executed
        # it is executed on a different event loop
        # reset the connections to use the active loop
        del client.transport.connection_pool
        await client.transport._async_init()
        try:
            response = await f(biothings, *args, **kwargs)
        except QueryPipelineInterrupt as itr:
            return itr.details
        except QueryPipelineException as exc:
            kwargs = exc.details if isinstance(exc.details, dict) else {}
            kwargs["success"] = False
            kwargs["status"] = exc.code
            kwargs["reason"] = exc.summary
            return kwargs, exc.code
        finally:
            await client.close()
        return response

    return _


@route("/{ver}/query")
@handle_es_conn
async def query(biothings, args):
    return await biothings.pipeline.search(**args)


@route(["/{ver}/{typ}/", "/{ver}/{typ}/<id>"])
@handle_es_conn
async def annotation(biothings, args):
    # could be a list, in which case we need jsonify.
    return flask.jsonify(await biothings.pipeline.fetch(**args))


@route("/{ver}/metadata")
@handle_es_conn
async def metadata(biothings, args):
    await biothings.metadata.refresh(None)
    return biothings.metadata.get_metadata(None)


@route("/{ver}/metadata/fields")
@handle_es_conn
async def fields(biothings, args):
    await biothings.metadata.refresh(None)
    mappings = biothings.metadata.get_mappings(None)
    return biothings.pipeline.formatter.transform_mapping(mappings)


@route("/status")
@handle_es_conn
async def status(biothings, args):
    return await biothings.health.async_check()
