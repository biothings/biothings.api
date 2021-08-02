# Proof of concept
# Not production ready.

# These routes represent a native implementaton
# of the biothings apis with limited functionality

# A comprehensive translation of all biothings
# default parameters may require building the
# handler functions with FunctionType and AST
# https://docs.python.org/3.8/library/ast.html

from types import SimpleNamespace

from biothings.web.query.pipeline import (QueryPipelineException,
                                          QueryPipelineInterrupt)
from fastapi import HTTPException

routes = []
biothings = SimpleNamespace()

def route(*args, **kwargs):
    def _(f):
        f.name = f.__name__
        f.args = args
        f.kwargs = kwargs
        routes.append(f)
        return f
    return _

async def _capture_exc(coro):
    try:
        return await coro
    except QueryPipelineInterrupt as itr:
        return itr.details
    except QueryPipelineException as exc:
        kwargs = exc.details if isinstance(exc.details, dict) else {}
        kwargs["status"] = exc.code
        kwargs["reason"] = exc.summary
        raise HTTPException(exc.code, kwargs)

@route("/")
async def root():
    return {"message": "Hello World"}

@route("/v1/query")
async def query(q="__all__"):
    return await _capture_exc(
        biothings.db.pipeline.search(q))

@route("/v1/doc/{id}")
async def annotation(id):
    return await _capture_exc(
        biothings.db.pipeline.fetch(id))

@route("/v1/metadata")
async def metadata():
    await biothings.metadata.refresh(None)
    return biothings.metadata.get_metadata(None)

@route("/v1/metadata/fields")
async def fields():
    await biothings.metadata.refresh(None)
    mappings = biothings.metadata.get_mappings(None)
    return biothings.db.pipeline.formatter.transform_mapping(mappings)
