import json
import logging
from collections import defaultdict

import elasticsearch
import elasticsearch_dsl
from elasticsearch_dsl.connections import Connections
from tornado.ioloop import IOLoop

from biothings.utils.web.es import get_es_versions
from biothings.utils.web.es_transport import (BiothingsAsyncTransport,
                                              BiothingsTransport)
from biothings.utils.web.run import run_once
from biothings.utils.web.userquery import ESUserQuery
from biothings.web.settings import BiothingWebSettings

should_log_es_py_ver = run_once()  # evaluate to true once
should_log_es_host_ver = run_once()  # evaluate to true once per host

class BiothingESWebSettings(BiothingWebSettings):
    '''
    `BiothingWebSettings`_ subclass with functions specific to an elasticsearch backend.
    '''

    def __init__(self, config=None, parent=None, **kwargs):
        '''
        The ``config`` init parameter specifies a module that configures
        this biothing.  For more information see `config module`_ documentation.
        '''
        super(BiothingESWebSettings, self).__init__(config, parent, **kwargs)

        # elasticsearch connections
        self._connections = Connections()

        connection_settings = {
            "hosts": self.ES_HOST,
            "timeout": self.ES_CLIENT_TIMEOUT
        }
        connection_settings.update(transport_class=BiothingsTransport)
        self._connections.create_connection(alias='sync', **connection_settings)
        connection_settings.update(transport_class=BiothingsAsyncTransport)
        if self.ES_SNIFF:
            connection_settings.update(sniffer_timeout=60)
            connection_settings.update(sniff_on_start=True)
            connection_settings.update(sniff_on_connection_fail=True)
        self._connections.create_connection(alias='async', **connection_settings)

        # cached index mappings
        self.source_metadata = defaultdict(dict)
        self.source_properties = defaultdict(dict)

        # populate field notes if exist
        try:
            inf = open(self.AVAILABLE_FIELDS_NOTES_PATH, 'r')
            self._fields_notes = json.load(inf)
            inf.close()
        except Exception:
            self._fields_notes = {}

        # user query data
        self.userquery = ESUserQuery(self.USERQUERY_DIR)

        # query pipelines
        self.query_builder = self.load_class(self.ES_QUERY_BUILDER)(self)
        self.query_backend = self.load_class(self.ES_QUERY_BACKEND)(self)
        self.result_transform = self.load_class(self.ES_RESULT_TRANSFORM)(self)

        # initialize payload for standalone tracking batch
        self.tracking_payload = []

        self.ES_INDICES = dict(self.ES_INDICES)  # TODO
        self.ES_INDICES[self.ES_DOC_TYPE] = self.ES_INDEX
        self.BIOTHING_TYPES = list(self.ES_INDICES.keys())

        IOLoop.current().add_callback(self._initialize)

    async def _initialize(self):

        # failures will be logged concisely
        logging.getLogger('elasticsearch.trace').propagate = False

        if should_log_es_py_ver():
            self.logger.info("Python Elasticsearch Version: %s", elasticsearch.__version__)
            self.logger.info("Python Elasticsearch DSL Version: %s", elasticsearch_dsl.__version__)
            if elasticsearch.__version__[0] != elasticsearch_dsl.__version__[0]:
                self.logger.error("ES Pacakge Version Mismatch with ES-DSL.")
        if should_log_es_host_ver(self.ES_HOST):
            versions = await get_es_versions(self.async_es_client)
            self.logger.info('Elasticsearch Version: %s', versions["elasticsearch_version"])
            self.logger.info('Elasticsearch Cluster: %s', versions["elasticsearch_cluster"])
            major_version = versions["elasticsearch_version"].split('.')[0]
            if major_version.isdigit() and int(major_version) != elasticsearch.__version__[0]:
                self.logger.error("ES Python Version Mismatch.")

        # populate source mappings
        for biothing_type in self.ES_INDICES:
            await self.read_index_mappings(biothing_type)

        # resume normal log flow
        logging.getLogger('elasticsearch.trace').propagate = True

    def validate(self):
        '''
        Additional ES settings to validate.
        '''
        super().validate()

        assert isinstance(self.ES_INDEX, str)
        assert isinstance(self.ES_DOC_TYPE, str)
        assert isinstance(self.ES_INDICES, dict)
        assert '*' not in self.ES_DOC_TYPE

    def get_es_client(self):
        '''
        Return the default blocking elasticsearch client.
        The connection is created upon first call.
        '''
        return self._connections.get_connection('sync')

    def get_async_es_client(self):
        '''
        Return the async elasitcsearch client.
        The connection is created upon first call.
        API calls return awaitable objects.
        '''
        return self._connections.get_connection('async')

    async def read_index_mappings(self, biothing_type=None):
        """
        Read ES index mappings for the corresponding biothing_type,
        Populate datasource info and field properties from mappings.
        Return ES raw response. This implementation combines indices.

        The ES response would look like: (for es7+)
        {
            'index_1': {
                'properties': { ... },  ---> source_properties
                '_meta': {
                    "src" : { ... }     ---> source_licenses
                    ...
                },              -----------> source_metadata
                ...
            },
            'index_2': {
                ...     ---------> Combine with results above
            }
        }
        """

        biothing_type = biothing_type or self.ES_DOC_TYPE
        try:
            mappings = await self.async_es_client.indices.get_mapping(
                index=self.ES_INDICES[biothing_type],
                allow_no_indices=True,
                ignore_unavailable=True,
                local=False)
        except elasticsearch.TransportError as exc:
            self.logger.error('Error loading index mapping for [%s].', biothing_type)
            self.logger.debug(str(exc))
            return None

        metadata = self.source_metadata[biothing_type]
        properties = self.source_properties[biothing_type]
        licenses = self.result_transform.source_licenses[biothing_type]

        metadata.clear()
        properties.clear()
        licenses.clear()

        metadata['_biothing'] = biothing_type
        metadata['_indices'] = list(mappings.keys())

        for index in mappings:

            mapping = mappings[index]['mappings']

            if mapping and elasticsearch.__version__[0] < 7:
                # remove doc_type, support 1 type per index
                mapping = next(iter(mapping.values()))

            if '_meta' in mapping:
                for key, val in mapping['_meta'].items():
                    # combine dict from multiple index
                    if key in metadata and isinstance(val, dict) \
                            and isinstance(metadata[key], dict):
                        metadata[key].update(val)
                    else:  # otherwise set/replace
                        metadata[key] = val

                # metadata.update(mapping['_meta'])  # alternative, no combine

                if 'src' in mapping['_meta']:
                    for src, info in mapping['_meta']['src'].items():
                        if 'license_url_short' in info:
                            licenses[src] = info['license_url_short']
                        elif 'license_url' in info:
                            licenses[src] = info['license_url']

            if 'properties' in mapping:
                properties.update(mapping['properties'])

        return mappings

    def get_field_notes(self):
        '''
        Return the cached field notes associated with this instance.
        '''
        return self._fields_notes

    @property
    def es_client(self):
        return self.get_es_client()

    @property
    def async_es_client(self):
        return self.get_async_es_client()
