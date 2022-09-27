import asyncio
import logging
from collections import defaultdict
from datetime import datetime
from functools import reduce
from operator import add

import elasticsearch
from dateutil.parser import parse as dtparse

from biothings.utils.common import get_loop


logger = logging.getLogger(__name__)


class BiothingsMetadata:

    def __init__(self):

        # cached, generated from mappings
        self.biothing_metadata = defaultdict(dict)
        # {
        #     "variant": {
        #         "biothing_type": "variant",
        #         "build_date": "2020-05-08T17:33:59.756164",
        #         "build_version": "20200508",
        #         "src": {"cosmic": { ... }, ... },
        #         "stats": {"total": 928585791 } }
        #     "gene": { ... }
        # }
        self.biothing_mappings = defaultdict(dict)
        # {
        #     "variant": {
        #         'aeolus': {'properties': {'drug_code': {...}, ...}},
        #         'cadd': {'properties': {'1000g': {...}, 'alt': {...}, ...}},
        #         'cgi': {'properties': {'association': {...}, ...}},
        #         'chebi': {'properties': {'brand_names': {...}, ...}}, ... }
        #     "gene": { ... }
        # }
        self.biothing_licenses = defaultdict(dict)
        # {
        #     "variant": {
        #         'aeolus': 'http://bit.ly/2DIxWwF',
        #         'cadd': 'http://bit.ly/2TIuab9',
        #         'cgi': 'http://bit.ly/2FqS871',
        #         'chebi': 'http://bit.ly/2KAUCAm', ... }
        #     "gene": { ... }
        # }

    def get_metadata(self, biothing_type):  # hub
        return self.biothing_metadata[biothing_type]

    def get_mappings(self, biothing_type):
        return self.biothing_mappings[biothing_type]

    def get_licenses(self, biothing_type):
        return self.biothing_licenses[biothing_type]

    async def refresh(self, biothing_type):
        pass

class BiothingsESMetadata(BiothingsMetadata):

    def __init__(self, indices, client):
        super().__init__()

        if not indices:
            # all indices on the host
            indices = {None: "_all"}
        if None not in indices:
            # default index pattern when no type specified
            indices[None] = next(iter(indices.values()))

        self.indices = indices
        self.client = client

        # initial refresh
        loop = get_loop()
        for btype in self.indices:
            obj = self.refresh(btype)
            if asyncio.iscoroutine(obj):
                try:  # py3.8+
                    task = loop.create_task(obj, name=str(btype))
                except TypeError:
                    task = loop.create_task(obj)
                task.add_done_callback(logger.debug)

    @property
    def types(self):  # biothing_type(s)
        return tuple(filter(None, self.indices.keys()))

    def update(self, biothing_type, info, count):
        """
        Read ES index mappings for the corresponding biothing_type,
        Populate datasource info and field properties from mappings.
        """
        _type = biothing_type

        # try to resolve default to an equivalent
        # and concrete biothing_type (in meta) to display
        if _type is None:
            for type_, pattern in self.indices.items():
                if self.indices[None] == pattern:
                    _type = type_
                    break

        reader = _BiothingsESMetadataReader(_type, info, count)
        self.biothing_metadata[biothing_type] = reader.get_metadata()
        self.biothing_mappings[biothing_type] = reader.get_mappings()
        self.biothing_licenses[biothing_type] = reader.get_licenses()

    def refresh(self, biothing_type=None):
        from elasticsearch import AsyncElasticsearch, Elasticsearch
        if isinstance(self.client, Elasticsearch):
            return self._refresh(biothing_type)
        elif isinstance(self.client, AsyncElasticsearch):
            return self._async_refresh(biothing_type)

    def _refresh(self, biothing_type):
        index = self.indices[biothing_type]
        info = self.client.indices.get(index=index)
        count = self.client.count(index=index)
        self.update(biothing_type, info, count)
        return info

    async def _async_refresh(self, biothing_type):
        index = self.indices[biothing_type]
        info = await self.client.indices.get(index=index)
        count = await self.client.count(index=index)
        self.update(biothing_type, info, count)
        return info

class BiothingsMongoMetadata(BiothingsMetadata):

    def __init__(self, collections, client):
        super().__init__()

        self.collections = collections
        self.client = client

    @property
    def types(self):  # biothing_type(s)
        return tuple(filter(None, self.collections.keys()))

    async def refresh(self, biothing_type):
        collection = self.client[self.collections[biothing_type]]
        # https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html
        # #pymongo.collection.Collection.estimated_document_count
        self.biothing_metadata[biothing_type] = BiothingHubMeta(
            biothing_type=biothing_type,
            stats=dict(total=collection.estimated_document_count())
        ).to_dict()

    def get_mappings(self, biothing_type):
        # document database does not have data schema
        # however, it might be possible to extract those indexed fields
        # https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html
        # #pymongo.collection.Collection.list_indexes
        return {"__N/A__": True}

    def get_licenses(self, biothing_type):
        # rely on metadata storage support
        return {}

class BiothingsSQLMetadata(BiothingsMetadata):

    def __init__(self, tables, client):
        super().__init__()

        self.tables = tables
        self.client = client

    @property
    def types(self):  # biothing_type(s)
        return tuple(filter(None, self.tables.keys()))

    async def refresh(self, biothing_type):
        # https://docs.sqlalchemy.org/en/14/core/reflection.html
        # This is a temporary solution as a proof of concept.
        # The implementation should probably be refined.
        # It doesn't work with empty tables at this point.
        table = self.tables[biothing_type]
        cursor = self.client.execute(f"SELECT * FROM {table}")
        if cursor.returns_rows:
            self.biothing_mappings[biothing_type] = {
                key: {'type': type(val).__name__}
                for key, val in zip(cursor.keys(), cursor.fetchone())
            }
            self.biothing_metadata[biothing_type] = BiothingHubMeta(
                biothing_type=biothing_type,
                stats=dict(total=cursor.rowcount)
            ).to_dict()


class _BiothingsESMetadataReader:
    """
    Read http://<elasticsearch>/<index_pattern>/ and ./_stats
    If the pattern matches one index, then that index's info will be used.
    If the pattern matches multiple indices, then the results will be combined.
    If the pattern matches no index, then empty dictionaries are returned.
    """

    def __init__(self, biothing_type, info, count):

        self.biothing_type = biothing_type
        self.document_count = count

        self.indices_info = {
            index: _ESIndex(biothing_type, **index_info)
            for index, index_info in info.items()
        }

    def get_mappings(self):
        """
        Mapping properties used for metadata field endpoint. For example:
        {
            'aeolus': {'properties': {'drug_code': {...}, ...}},
            'cadd': {'properties': {'1000g': {...}, 'alt': {...}, ...}},
            'cgi': {'properties': {'association': {...}, ...}},
            'chebi': {'properties': {'brand_names': {...}, ...}},
            ...
        }
        """
        mappings = list(info.get_mappings() for info in self.indices_info.values())
        mappings = reduce(add, mappings).to_dict() if mappings else {}
        return mappings

    def get_licenses(self):
        """
        Source-URL pairs that contains the data licencing information. Example:
        {
            'aeolus': 'http://bit.ly/2DIxWwF',
            'cadd': 'http://bit.ly/2TIuab9',
            'cgi': 'http://bit.ly/2FqS871',
            'chebi': 'http://bit.ly/2KAUCAm',
            ...
        }
        """
        licenses = list(info.get_licenses() for info in self.indices_info.values())
        licenses = reduce(add, licenses).to_dict() if licenses else {}
        return licenses

    def get_metadata(self):
        """
        Provide description about the data under this type. Example:
        {
            "biothing_type": "variant",
            "build_date": "2020-05-08T17:33:59.756164",
            "build_version": "20200508",
            "src": {"cosmic": { ... }, ... },
            "stats": {"total": 928585791 }
        }
        """
        metadata = list(info.get_metadata() for info in self.indices_info.values())
        metadata = reduce(add, metadata).to_dict() if metadata else {}
        if metadata:
            if metadata.get('biothing_type') == "__multiple__":
                metadata['biothing_type'] = self.biothing_type
            metadata['stats']['total'] = self.document_count['count']
        metadata['_biothing'] = self.biothing_type
        metadata['_indices'] = list(self.indices_info.keys())
        return metadata

class _ESIndex:
    """
    Read one index's info http://<elasticsearch>/<index>.
    Return combinable BiothingMetaProp objects.
    """

    def __init__(self, biothing, aliases, mappings, settings):
        self.biothing = biothing
        self.aliases = aliases
        self.mappings = _ESIndexMappings(mappings)
        self.settings = _ESIndexSettings(settings)

    def get_metadata(self):
        """
        Return BiothingHubMetadata instance.
        Populate empty metadata basing on index settings.
        Fill in empty stats field if not provided.
        """
        if self.mappings.metadata:
            try:
                return BiothingHubMeta(**self.mappings.metadata)
            except KeyError:
                pass
        return BiothingHubMeta(
            biothing_type=self.biothing,
            build_date=self.settings.get_creation_date().isoformat(),
            build_version=self.settings.get_index_version(),
            src={},
            stats={}
        )

    def get_licenses(self):
        return BiothingLicenses(
            self.mappings.extract_licenses()
        )

    def get_mappings(self):
        return BiothingMappings(
            self.mappings.properties
        )

class _ESIndexSettings:
    """
    Object representation of ES index settings.
    {
        "index": {
            "number_of_shards": "1",
            "auto_expand_replicas": "0-1",
            "provided_name": ".tasks",
            "creation_date": "1566293197607",
            "priority": "2147483647",
            "number_of_replicas": "0",
            "uuid": "yWBk0qw0QXmEuxJFas3mIg",
            "version": {
                "created": "6050099"
            }
        }
    }
    """

    def __init__(self, setting):
        self.index = setting['index']

    def get_creation_date(self):
        return datetime.fromtimestamp(int(self.index['creation_date'])/1000)

    def get_index_version(self):
        if 'updated' in self.index['version']:
            return self.index['version']['updated']
        return self.index['version']['created']

class _ESIndexMappings:
    """
    Object representation of ES index mappings:
    {
        # this level is only available for es6
        "<doc_type> : {
            'properties': { ... },  ---> mapping
            '_meta': {
                "src" : { ... }     ---> licenses
                ...
            },              -----------> metadata
            ...
        }
    }
    """

    def __init__(self, mapping):
        # for elasticsearch version 6.x
        if len(mapping) == 1 and next(iter(mapping)) != "properties":
            # remove doc_type, support 1 type per index
            mapping = next(iter(mapping.values()))
        self.enabled = mapping.pop('enabled', True)
        self.dynamic = mapping.pop('dynamic', True)
        self.properties = mapping.get('properties', {})
        self.metadata = mapping.get('_meta', {})

    def extract_licenses(self):
        """
        Return source name - license url pairs.
        """
        licenses = {}
        for src, info in self.metadata.get('src', {}).items():
            if 'license_url_short' in info:
                licenses[src] = info['license_url_short']
            elif 'license_url' in info:
                licenses[src] = info['license_url']
        return licenses


class BiothingMetaProp:

    def __add__(self, other):
        raise NotImplementedError

        # NOTE
        # The current implementations below
        # may not be able to properly handle
        # field (key/source) collisions.

    def to_dict(self):
        raise NotImplementedError

class BiothingLicenses(BiothingMetaProp):

    def __init__(self, licenses):
        self.licenses = licenses

    def __add__(self, other):
        licenses = dict(self.licenses)
        licenses.update(other.licenses)
        return BiothingLicenses(licenses)

    def to_dict(self):
        return dict(self.licenses)

class BiothingMappings(BiothingMetaProp):

    def __init__(self, properties):
        self.properties = properties

    def __add__(self, other):
        mappings = dict(self.properties)
        mappings.update(other.properties)
        return BiothingMappings(mappings)

    def to_dict(self):
        return dict(self.properties)

class BiothingHubMeta(BiothingMetaProp):

    def __init__(self, **metadata):  # dict

        self.biothing_type = metadata.get('biothing_type')
        # self.build_date = datetime.fromisoformat(metadata['build_date']) # python3.7 syntax
        # self.build_date = datetime.strptime(metadata['build_date'], "%Y-%m-%dT%H:%M:%S.%f")
        self.build_date = metadata.get('build_date')
        self.build_version = metadata.get('build_version')
        self.src = metadata.get('src', {})
        self.stats = metadata.get('stats', {})

        if self.build_date and isinstance(self.build_date, str):
            self.build_date = dtparse(metadata['build_date']).astimezone()

    def to_dict(self):
        return {
            'biothing_type': self.biothing_type,
            'build_date': self.build_date.isoformat()
            if isinstance(self.build_date, datetime)
            else self.build_date,
            'build_version': self.build_version,
            'src': self.src,
            'stats': self.stats
        }

    def __add__(self, other):

        # combine biothing_type field
        biothing_type = self.biothing_type,
        if other.biothing_type != self.biothing_type:
            biothing_type = '__multiple__'

        # take the latest build_date
        # TODO if one of them is None
        build_date = self.build_date
        if other.build_date > self.build_date:
            build_date = other.build_date

        # combine build_version field
        build_version = self.build_version
        if other.build_version != build_version:
            build_version = '__multiple__'

        # combine source field
        src = dict(self.src)
        src.update(other.src)

        # add up stats field
        stats = dict(self.stats)
        for key, value in other.stats.items():
            if key in stats:
                stats[key] += value
            else:  # new key
                stats[key] = value

        return BiothingHubMeta(
            biothing_type=biothing_type,
            build_date=build_date.isoformat(),
            build_version=build_version,
            src=src,
            stats=stats)
