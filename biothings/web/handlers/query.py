"""
Elasticsearch Handlers

biothings.web.handlers.BaseESRequestHandler

    Supports: (all features above and)
    - access to biothing_type attribute
    - access to ES query pipeline stages
    - pretty print elasticsearch exceptions
    - common control option out_format

    Subclasses:
    - biothings.web.handlers.MetadataSourceHandler
    - biothings.web.handlers.MetadataFieldHandler
    - myvariant.web.beacon.BeaconHandler

biothings.web.handlers.ESRequestHandler

    Supports: (all features above and)
    - common control options (raw, rawquery)
    - common transform options (dotfield, always_list...)
    - query pipeline customization hooks
    - single query through GET
    - multiple quers through POST

    Subclasses:
    - biothings.web.handlers.BiothingHandler
    - biothings.web.handlers.QueryHandler

"""

import logging
from collections import Counter
from inspect import iscoroutinefunction
from types import CoroutineType

from tornado.web import Finish, HTTPError

from biothings.utils import serializer
from biothings.web.analytics.events import GAEvent
from biothings.web.handlers.base import BaseAPIHandler
from biothings.web.query.pipeline import QueryPipelineException, QueryPipelineInterrupt

__all__ = [
    "BaseQueryHandler",
    "MetadataSourceHandler",
    "MetadataFieldHandler",
    "BiothingHandler",
    "QueryHandler",
]

logger = logging.getLogger(__name__)


class BaseQueryHandler(BaseAPIHandler):
    def initialize(self, biothing_type=None, *args, **kwargs):
        super().initialize(*args, **kwargs)
        self.biothing_type = biothing_type
        self.pipeline = self.biothings.pipeline
        self.metadata = self.biothings.metadata

    def prepare(self):
        super().prepare()

        # provide convenient access to next stages
        self.args.biothing_type = self.biothing_type

        self.event = GAEvent(
            {
                "__secondary__": [],  # secondary analytical objective: field tracking
                "category": "{}_api".format(self.biothings.config.APP_VERSION),  # eg.'v1_api'
                "action": "_".join((self.name, self.request.method.lower())),  # eg.'query_get'
                # 'label': 'fetch_all', etc.
                # 'value': 100, # number of queries
            }
        )

        if self.args._source:
            fields = [str(field) for field in self.args._source]  # in case input is not str
            fields = [field.split(".", 1)[0] for field in fields]  # only consider root keys
            for field, cnt in Counter(fields).items():
                self.event["__secondary__"].append(
                    GAEvent({"category": "parameter_tracking", "action": "field_filter", "label": field, "value": cnt})
                )
        else:
            self.event["__secondary__"].append(
                GAEvent(
                    {
                        "category": "parameter_tracking",
                        "action": "field_filter",
                        "label": "all",
                    }
                )
            )

    def write(self, chunk):
        # add an additional header to the JSON formatter
        # with a header image and a title-like section
        # further more, show a link to documentation

        # relevant settings in config:
        # HTML_OUT_TITLE
        # HTML_OUT_HEADER_IMG
        # HTML_OUT_<ENDPOINT>_DOCS

        DEFAULT_TITLE = "<p>Biothings API</p>"
        DEFAULT_IMG = "https://biothings.io/static/favicon.ico"

        try:
            if self.format == "html":
                config = self.biothings.config
                chunk = self.render_string(
                    template_name="api.html",
                    data=serializer.to_json(chunk),
                    link=serializer.URL(self.request.full_url()).remove("format"),
                    title_div=getattr(config, "HTML_OUT_TITLE", "") or DEFAULT_TITLE,
                    header_img=getattr(config, "HTML_OUT_HEADER_IMG", "") or DEFAULT_IMG,
                    learn_more=getattr(config, f"HTML_OUT_{self.name.upper()}_DOCS", ""),
                )
                self.set_header("Content-Type", "text/html; charset=utf-8")
                return super(BaseAPIHandler, self).write(chunk)

        except Exception as exc:
            logger.warning(exc)

        super().write(chunk)


class MetadataSourceHandler(BaseQueryHandler):
    """
    GET /metadata
    """

    name = "metadata"
    kwargs = dict(BaseQueryHandler.kwargs)
    kwargs["GET"] = {
        "dev": {"type": bool, "default": False},
        "raw": {"type": bool, "default": False},
    }

    async def get(self):
        info = await self.metadata.refresh(self.biothing_type)
        meta = self.metadata.get_metadata(self.biothing_type).copy()

        if self.args.raw:
            raise Finish(info)

        elif self.args.dev:
            meta["software"] = self.biothings.devinfo.get()
            # replace each source's field mapping with a "fields" list holding only the
            # dotted field names, using the same flattening as /metadata/fields
            if "src" in meta:
                formatter = self.pipeline.formatter
                meta["src"] = self._transform_source_mappings(meta["src"], self._set_source_fields(formatter))

        else:  # remove debug info
            filtered_meta = {}
            for key, value in meta.items():
                if not key.startswith("_"):
                    filtered_meta[key] = value
            meta = filtered_meta
            # per-source field mappings (src.<source>.mapping) are only exposed in
            # dev mode (/metadata?dev); strip them from the public response
            if "src" in meta:
                meta["src"] = self._transform_source_mappings(
                    meta["src"], lambda source: source.pop("mapping", None)
                )

        if iscoroutinefunction(self.extras):
            meta = await self.extras(meta)
        else:
            meta = self.extras(meta)

        self.finish(dict(sorted(meta.items())))

    @staticmethod
    def _transform_source_mappings(src, func):
        """
        Return a copy of the ``src`` metadata after applying ``func`` to every source that
        carries a field mapping.

        The mapping lives under ``<source>.mapping`` (a single merged mapping per source,
        including multi-uploader sources). ``func`` receives the source dict that holds the
        ``"mapping"`` key and replaces or drops that key in place.

        Each source dict is copied because ``src`` is part of the metadata cache shared
        across requests, and mutating it would remove the mapping for later requests. Only
        the source dicts themselves are copied (not their nested mappings), since ``func``
        must not modify the mapping content itself.
        """
        src = dict(src)
        for name, source in src.items():
            if isinstance(source, dict) and "mapping" in source:
                source = src[name] = dict(source)  # copy before mutating the cached dict
                func(source)
        return src

    @staticmethod
    def _set_source_fields(formatter):
        """
        Build a function replacing a source's ``mapping`` with a ``fields`` list of the
        dotted field names it provides, e.g.
        """

        def _set_fields(source):
            source["fields"] = sorted(formatter.transform_mapping(source.pop("mapping")))

        return _set_fields

    def extras(self, _meta):
        """
        Override to add app specific metadata.
        """
        return _meta


class MetadataFieldHandler(BaseQueryHandler):
    """
    GET /metadata/fields
    """

    name = "fields"
    kwargs = dict(BaseQueryHandler.kwargs)
    kwargs["GET"] = {
        "raw": {"type": bool, "default": False},
        "search": {"type": str, "default": None},
        "prefix": {"type": str, "default": None},
    }

    async def get(self):
        await self.metadata.refresh(self.biothing_type)
        mapping = self.metadata.get_mappings(self.biothing_type)

        if self.args.raw:
            raise Finish(mapping)

        result = self.pipeline.formatter.transform_mapping(mapping, self.args.prefix, self.args.search)

        # annotate each field with the main source(s) that populate it
        self._annotate_field_sources(result)

        self.finish(result)

    def _annotate_field_sources(self, result):
        """
        Add a ``source`` key to each field in ``result`` listing the main sources whose
        field mapping (stored in ``_meta.src.<source>.mapping``) includes that field.

        Sources are identified by flattening each per-source mapping the same way the field
        list itself is built, so the field paths line up. If no per-source mapping data is
        available (e.g. an index built before mappings were recorded), no annotation is added.
        """
        metadata = self.metadata.get_metadata(self.biothing_type)
        sources = metadata.get("src", {}) if metadata else {}
        formatter = self.pipeline.formatter

        # map each field path to the set of main sources that define it
        field_sources = {}
        for source, info in sources.items():
            source_mapping = info.get("mapping") if isinstance(info, dict) else None
            if not source_mapping:
                continue
            for field in formatter.transform_mapping(source_mapping):
                field_sources.setdefault(field, set()).add(source)

        if not field_sources:
            return

        for field, definition in result.items():
            field_srcs = field_sources.get(field)
            # only annotate fields that actually map to a source; computed fields
            # (e.g. _id) with no source mapping are left without a "source" key
            if field_srcs and isinstance(definition, dict):
                srcs = sorted(field_srcs)
                definition["source"] = srcs[0] if len(srcs) == 1 else srcs



async def ensure_awaitable(obj):
    if isinstance(obj, CoroutineType):
        return await obj
    return obj


def capture_exceptions(coro):
    async def _method(*args, **kwargs):
        try:
            return await coro(*args, **kwargs)
        except QueryPipelineInterrupt as itr:
            raise Finish(itr.details)
        except QueryPipelineException as exc:
            raise HTTPError(exc.code, None, exc.details, reason=exc.summary)

    return _method


class BiothingHandler(BaseQueryHandler):
    r"""
    Biothings Annotation Endpoint

    URL pattern examples:

        /{pre}/{ver}/{typ}/?
        /{pre}/{ver}/{typ}/([^\/]+)/?

        queries a term against a pre-determined field that
        represents the id of a document, like _id and dbsnp.rsid

        GET -> {...} or [{...}, ...]
        POST -> [{...}, ...]
    """

    name = "annotation"

    @capture_exceptions
    async def post(self, *args, **kwargs):
        self.event["value"] = len(self.args["id"])

        result = await ensure_awaitable(self.pipeline.fetch(**self.args))
        self.finish(result)

    @capture_exceptions
    async def get(self, *args, **kwargs):
        self.event["value"] = 1

        result = await ensure_awaitable(self.pipeline.fetch(**self.args))
        self.finish(result)


class QueryHandler(BaseQueryHandler):
    """
    Biothings Query Endpoint

    URL pattern examples:

        /{pre}/{ver}/{typ}/query/?
        /{pre}/{ver}//query/?

        GET -> {...}
        POST -> [{...}, ...]
    """

    name = "query"

    @capture_exceptions
    async def post(self, *args, **kwargs):
        self.event["value"] = len(self.args["q"])

        result = await ensure_awaitable(self.pipeline.search(**self.args))
        self.finish(result)

    @capture_exceptions
    async def get(self, *args, **kwargs):
        self.event["value"] = 1
        if self.args.get("fetch_all"):
            self.event["label"] = "fetch_all"

        if self.args.get("fetch_all") or self.args.get("scroll_id") or self.args.get("q") == "__any__":
            self.clear_header("Cache-Control")

        response = await ensure_awaitable(self.pipeline.search(**self.args))
        self.finish(response)
