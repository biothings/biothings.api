"""
    Metadata Endpoint
"""


from tornado.web import Finish

from biothings.utils.version import get_software_info
from biothings.web.api.es.handlers.base_handler import BaseESRequestHandler


class MetadataSourceHandler(BaseESRequestHandler):
    """
    GET /metadata
    """
    name = 'metadata'
    kwarg_types = ('control', 'source')
    kwarg_methods = ('get',)

    def get(self):

        self.web_settings.read_index_mappings()
        _meta = self.web_settings.source_metadata[self.biothing_type]

        if self.kwargs.source.dev:
            _meta['software'] = get_software_info(app_dir=self.web_settings.get_git_repo_path())

        self.finish(dict(sorted(_meta.items())))


class MetadataFieldHandler(BaseESRequestHandler):
    """
    GET /metadata/fields
    """
    name = 'metadata'
    kwarg_types = ('control', 'fields')
    kwarg_methods = ('get',)

    def get(self):

        _properties = self.web_settings.source_properties[self.biothing_type]

        if self.kwargs.fields.raw:
            raise Finish(_properties)

        field_notes = self.web_settings.get_field_notes()
        excluded_keys = self.web_settings.AVAILABLE_FIELDS_EXCLUDED

        prefix = self.kwargs.fields.prefix or []
        search = self.kwargs.fields.search or []

        result = {}
        todo = list(_properties.items())
        todo.reverse()
        while todo:
            key, dic = todo.pop()
            dic = dict(dic)

            dic.pop('dynamic', None)
            dic.pop('normalizer', None)

            if key in field_notes:
                result['notes'] = field_notes[key]

            if 'copy_to' in dic:
                if 'all' in dic['copy_to']:
                    dic['searched_by_default'] = True
                del dic['copy_to']

            if 'index' not in dic:
                if 'enabled' in dic:
                    dic['index'] = dic.pop('enabled')
                else:
                    dic['index'] = True

            if 'properties' in dic:
                dic['type'] = 'object'
                subs = (('.'.join((key, k)), v) for k, v in dic['properties'].items())
                todo.extend(reversed(list(subs)))
                del dic['properties']

            if all((not excluded_keys or key not in excluded_keys,
                    not prefix or key.startswith(prefix),
                    not search or search in key)):
                result[key] = dict(sorted(dic.items()))

        self.finish(result)
