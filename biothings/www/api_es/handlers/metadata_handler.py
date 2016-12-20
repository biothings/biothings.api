import re
import json
from tornado.web import HTTPError
from biothings.www.helper import BaseHandler
from biothings.utils.common import split_ids
from biothings.utils.version import get_software_info
from collections import OrderedDict

class MetaDataHandler(BaseHandler):
    boolean_parameters = set(['dev'])

    def _fill_software_info(self,_meta):
        kwargs = self.get_query_params()
        debug = kwargs.pop('dev', False)
        if debug:
            _meta['software'] = get_software_info()

    def get(self):
        kwargs = self.get_query_params()
        _meta = self.esq.get_mapping_meta(**kwargs)
        self._fill_software_info(_meta)
        self.return_json(_meta)

# TODO: merge this fields handler into metadata handler....
class FieldsHandler(BaseHandler):

    def get(self):
        kwargs = self.get_query_params()
        search = kwargs.pop('search', None)
        prefix = kwargs.pop('prefix', None)
        es_mapping = self.esq.query_fields(**kwargs)
        if biothing_settings.field_notes_path:
            notes = json.load(open(biothing_settings.field_notes_path, 'r'))
        else:
            notes = {}

        def get_indexed_properties_in_dict(d, prefix):
            r = {}
            for (k, v) in d.items():
                r[prefix + '.' + k] = {}
                r[prefix + '.' + k]['indexed'] = False
                if 'properties' not in v:
                    r[prefix + '.' + k]['type'] = v['type']
                    if ('index' not in v) or ('index' in v and v['index'] != 'no'):
                        # indexed field
                        r[prefix + '.' + k]['indexed'] = True
                else:
                    r[prefix + '.' + k]['type'] = 'object'
                    r.update(get_indexed_properties_in_dict(v['properties'], prefix + '.' + k))
                if ('include_in_all' in v) and v['include_in_all']:
                    r[prefix + '.' + k]['include_in_all'] = True
                else:
                    r[prefix + '.' + k]['include_in_all'] = False
            return r

        r = {}
        for (k, v) in get_indexed_properties_in_dict(es_mapping, '').items():
            k1 = k.lstrip('.')
            if (search and search in k1) or (prefix and k1.startswith(prefix)) or (not search and not prefix):
                r[k1] = v
                if k1 in notes:
                    r[k1]['notes'] = notes[k1]
        self.return_json(OrderedDict(sorted(r.items(), key=lambda x: x[0])))
