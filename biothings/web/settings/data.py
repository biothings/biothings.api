"""
    Data utility functions for web settings.
    Most of the classes are specific to elasticsearch.
    Typically one instance of each per settings class.
"""

from collections import defaultdict
from datetime import datetime
from functools import reduce
from operator import add

import elasticsearch
import elasticsearch_dsl
from elasticsearch_dsl.connections import Connections
from dateutil.parser import parse as dtparse

from biothings.utils.web.es import get_es_versions
from biothings.utils.web.es_transport import (BiothingsAsyncTransport,
                                              BiothingsTransport)
from biothings.utils.web.run import run_once

__all__ = [
    'DataConnections',
    'DataPipeline',
    'DataMetadata',
]


class DataConnections:
    """
        Elasticsearch Database Connection
        Connections are created upon first call
    """
    _should_log_es_py_ver = run_once()  # evaluate to true once
    _should_log_es_host_ver = run_once()  # evaluate to true once per host

    def __init__(self, settings):

        self.settings = settings
        self._connections = Connections()

        connection_settings = {
            "hosts": self.settings.ES_HOST,
            "timeout": self.settings.ES_CLIENT_TIMEOUT
        }
        connection_settings.update(transport_class=BiothingsTransport)
        self._connections.create_connection(alias='sync', **connection_settings)
        connection_settings.update(transport_class=BiothingsAsyncTransport)
        if self.settings.ES_SNIFF:
            connection_settings.update(sniffer_timeout=60)
            connection_settings.update(sniff_on_start=True)
            connection_settings.update(sniff_on_connection_fail=True)
            connection_settings.update(retry_on_timeout=True)
        self._connections.create_connection(alias='async', **connection_settings)

    async def log_versions(self):

        if DataConnections._should_log_es_py_ver():

            self.settings.logger.info(
                "Python Elasticsearch Version: %s",
                '.'.join(map(str, elasticsearch.__version__)))
            self.settings.logger.info(
                "Python Elasticsearch DSL Version: %s",
                '.'.join(map(str, elasticsearch_dsl.__version__)))

            if elasticsearch.__version__[0] != elasticsearch_dsl.__version__[0]:
                self.settings.logger.error(
                    "ES Pacakge Version Mismatch with ES-DSL.")

        if DataConnections._should_log_es_host_ver(self.settings.ES_HOST):

            versions = await get_es_versions(self.async_client)
            self.settings.logger.info(
                '[%s] %s: %s', self.settings.ES_HOST,
                versions["elasticsearch_cluster"],
                versions["elasticsearch_version"])

            major_version = versions["elasticsearch_version"].split('.')[0]
            if major_version.isdigit() and int(major_version) != elasticsearch.__version__[0]:
                self.settings.logger.error(
                    "ES Python Version Mismatch.")

    def get_connection(self, connection):
        return self._connections.get_connection(connection)

    @property
    def client(self):
        '''
        Return the blocking elasticsearch client.
        '''
        return self.get_connection('sync')

    @property
    def async_client(self):
        '''
        Return the async elasitcsearch client.
        API calls return awaitable objects.
        '''
        return self.get_connection('async')

class DataPipeline:

    def __init__(self, settings):

        _load_class = settings.load_class
        self.query_builder = _load_class(settings.ES_QUERY_BUILDER)(settings)
        self.query_backend = _load_class(settings.ES_QUERY_BACKEND)(settings)
        self.result_transform = _load_class(settings.ES_RESULT_TRANSFORM)(settings)

    def build(self, *args, **kwargs):
        return self.query_builder.build(*args, **kwargs)

    def execute(self, *args, **kwargs):
        return self.query_backend.execute(*args, **kwargs)

    def transform(self, *args, **kwargs):
        return self.result_transform.transform(*args, **kwargs)

    def transform_mapping(self, *args, **kwargs):
        return self.result_transform.transform_mapping(*args, **kwargs)

class DataMetadata:

    def __init__(self, settings):

        # defined in configs
        self._biothing_default = settings.ES_DOC_TYPE
        self._biothing_indices = settings.ES_INDICES

        # defined in biothings.web.settings
        self.client = settings.connections.async_client
        self.logger = settings.logger

        # cached, generated from mappings
        self.biothing_metadata = defaultdict(dict)
        self.biothing_mappings = defaultdict(dict)
        self.biothing_licenses = defaultdict(dict)

    async def refresh(self, biothing_type=None):
        """
        Read ES index mappings for the corresponding biothing_type,
        Populate datasource info and field properties from mappings.
        """
        if not biothing_type:
            biothing_type = self._biothing_default
        try:
            info = await self.client.indices.get(
                index=self._biothing_indices[biothing_type]
            )
            count = await self.client.count(
                index=self._biothing_indices[biothing_type]
            )
        except elasticsearch.TransportError as exc:
            self.logger.error('Error loading [%s] indices.', biothing_type)
            self.logger.debug(str(exc))
            return None

        reader = BiothingMetadataReader(biothing_type, info, count)
        self.biothing_metadata[biothing_type] = reader.get_metadata()
        self.biothing_mappings[biothing_type] = reader.get_mappings()
        self.biothing_licenses[biothing_type] = reader.get_licenses()

        return info  # raw index info


class BiothingMetadataReader:
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
            index: ESIndex(biothing_type, **index_info)
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
        mappings = (info.get_mappings() for info in self.indices_info.values())
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
        licenses = (info.get_licenses() for info in self.indices_info.values())
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
        metadata = (info.get_metadata() for info in self.indices_info.values())
        metadata = reduce(add, metadata).to_dict() if metadata else {}
        if metadata:
            if metadata.get('biothing_type') == "__multiple__":
                metadata['biothing_type'] = self.biothing_type
            metadata['stats']['total'] = self.document_count['count']
        metadata['_biothing'] = self.biothing_type
        metadata['_indices'] = list(self.indices_info.keys())
        return metadata


class ESIndex:
    """
    Read one index's info http://<elasticsearch>/<index>.
    Return combinable BiothingMetaProp objects.
    """

    def __init__(self, biothing, aliases, mappings, settings):
        self.biothing = biothing
        self.aliases = aliases
        self.mappings = ESIndexMappings(mappings)
        self.settings = ESIndexSettings(settings)

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


class ESIndexSettings:
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


class ESIndexMappings:
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
        # assume package version corresponds to that of es
        if mapping and elasticsearch.__version__[0] < 7:
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

    def to_dict(self):
        raise NotImplementedError

class BiothingLicenses(BiothingMetaProp):

    def __init__(self, licenses):
        self.licenses = licenses

    def __add__(self, other):
        # TODO log conflicts
        licenses = dict(self.licenses)
        licenses.update(other.licenses)
        return BiothingLicenses(licenses)

    def to_dict(self):
        return dict(self.licenses)


class BiothingMappings(BiothingMetaProp):

    def __init__(self, properties):
        self.properties = properties

    def __add__(self, other):
        # TODO conflicting fields
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
        self.build_date = dtparse(metadata['build_date']).astimezone()    # this handles with timestamp string w/wo timezone
        self.build_version = metadata.get('build_version')
        self.src = metadata.get('src', {})
        self.stats = metadata.get('stats', {})

    def to_dict(self):

        return {
            'biothing_type': self.biothing_type,
            'build_date': self.build_date.isoformat(),
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
