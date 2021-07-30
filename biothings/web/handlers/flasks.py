# Proof of concept
# Not production ready.

from types import CoroutineType

import flask
from biothings.web.options import OptionError
from biothings.web.query.pipeline import (QueryPipelineException,
                                          QueryPipelineInterrupt)
routes = []

def route(pattern, name, methods=("GET", "POST")):
    def A(f):
        async def B(*args, **kwargs):
            biothings = flask.current_app.biothings
            optionsets = biothings.optionsets
            optionset = optionsets.get(name)
            if optionset:
                try:
                    _args = optionset.parse(flask.request.method, (
                        (tuple(kwargs.values()), {}),
                        flask.request.args,
                        flask.request.form,
                        flask.request.get_json()
                    ))
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
        B.name = name
        routes.append(B)
        return B
    return A

@route("/", "homepage")
def homepage(biothings, args):
    return "HELLO"

def handle_es_conn(f):
    async def _(*args, **kwargs):
        biothings = flask.current_app.biothings
        del biothings.elasticsearch.async_client.transport.connection_pool
        # this is inefficient, currently implemented for proof of concept only
        # should just infer the event loop for the current thread and keep
        # the other connection state instead of resetting everything.
        await biothings.elasticsearch.async_client.transport._async_init()
        try:
            response = await f(*args, **kwargs)
        except QueryPipelineInterrupt as itr:
            return itr.details
        except QueryPipelineException as exc:
            kwargs = exc.details if isinstance(exc.details, dict) else {}
            kwargs["status"] = exc.code
            kwargs["reason"] = exc.summary
            return kwargs, exc.code
        finally:
            await biothings.elasticsearch.async_client.close()
        return response
    return _

@route("/v1/query", "query")
@handle_es_conn
async def query(biothings, args):
    return await biothings.db.pipeline.search(**args)

@route([
    "/v1/doc/",
    "/v1/doc/<id>"
], "annotation")
@handle_es_conn
async def annotation(biothings, args):
    # could be a list, in which case we need jsonify.
    return flask.jsonify(await biothings.db.pipeline.fetch(**args))

@route("/v1/metadata", "metadata")
@handle_es_conn
async def metadata(biothings, args):
    await biothings.metadata.refresh(None)
    return biothings.metadata.get_metadata(None)

@route("/v1/metadata/fields", "fields")
@handle_es_conn
async def metadata(biothings, args):
    await biothings.metadata.refresh(None)
    mappings = biothings.metadata.get_mappings(None)
    return biothings.db.pipeline.formatter.transform_mapping(mappings)
