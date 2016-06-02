import re
import json
from tornado.web import HTTPError
from biothings.www.helper import BaseHandler
from biothings.utils.common import split_ids
from biothings.utils.version import get_python_version
from biothings.utils.version import get_repository_information
from biothings.utils.version import get_biothings_commit
from biothings.settings import BiothingSettings

biothing_settings = BiothingSettings()

class BiothingHandler(BaseHandler):

    def _ga_event_object(self, action, data={}):
        ''' Returns the google analytics object for requests on this endpoint (annotation handler).'''
        return biothing_settings.ga_event_object(endpoint=biothing_settings._annotation_endpoint, action=action, data=data)


    def _examine_kwargs(self, action, kwargs):
        ''' A function for sub-classing.  This will be run after the get_query_params but before the actual
            elasticsearch querying. '''
        if action == 'GET':
            pass
        elif action == 'POST':
            pass
        pass

    def get(self, bid=None):
        '''
        '''
        if bid:
            kwargs = self.get_query_params()
            self._examine_kwargs('GET', kwargs)
            biothing_object = self.esq.get_biothing(bid, **kwargs)
            if biothing_object:
                self.return_json(biothing_object)
                self.ga_track(event=self._ga_event_object('GET'))
            else:
                raise HTTPError(404)
        else:
            raise HTTPError(404)

    def post(self, ids=None):
        '''
           This is essentially the same as post request in QueryHandler, with different defaults.

           parameters:
            ids
            fields
            email
            jsonld
        '''
        kwargs = self.get_query_params()
        self._examine_kwargs('POST', kwargs)
        ids = kwargs.pop('ids', None)
        if ids:
            ids = re.split('[\s\r\n+|,]+', ids)
            res = self.esq.mget_biothings(ids, **kwargs)
        else:
            res = {'success': False, 'error': "Missing required parameters."}
        encode = not isinstance(res, str)    # when res is a string, e.g. when rawquery is true, do not encode it as json
        self.return_json(res, encode=encode)
        self.ga_track(event=self._ga_event_object('POST', {'qsize': len(ids) if ids else 0}))


class QueryHandler(BaseHandler):

    def _ga_event_object(self, action, data={}):
        ''' Returns the google analytics object for requests on this endpoint (query handler).'''
        return biothing_settings.ga_event_object(endpoint=biothing_settings._query_endpoint, action=action, data=data)

    def _examine_kwargs(self, action, kwargs):
        ''' A function for sub-classing.  This will be run after the get_query_params but before the actual
            elasticsearch querying. '''
        if action == 'GET':
            pass
        elif action == 'POST':
            pass
        pass

    def get(self):
        '''
        parameters:
            q
            fields
            from
            size
            sort
            facets
            callback
            email
            fetch_all
            jsonld
            explain
            raw
        '''
        kwargs = self.get_query_params()
        self._examine_kwargs('GET', kwargs)
        q = kwargs.pop('q', None)
        scroll_id = kwargs.pop('scroll_id', None)
        _has_error = False
        if scroll_id:
            res = self.esq.scroll(scroll_id, **kwargs)
        elif q:
            for arg in ['from', 'size']:
                value = kwargs.get(arg, None)
                if value:
                    try:
                        kwargs[arg] = int(value)
                    except ValueError:
                        res = {'success': False, 'error': 'Parameter "{}" must be an integer.'.format(arg)}
                        _has_error = True
            if not _has_error:
                res = self.esq.query(q, **kwargs)
                if kwargs.get('fetch_all', False):
                    self.ga_track(event=self._ga_event_object('fetch_all', {'total': res.get('total', None)}))
        else:
            res = {'success': False, 'error': "Missing required parameters."}

        self.return_json(res)
        self.ga_track(event=self._ga_event_object('GET', {'qsize': len(q) if q else 0}))

    def post(self):
        '''
        parameters:
            q
            scopes
            fields
            email
            jsonld
            jsoninput   if true, input "q" is a json string, must be decoded as a list.
        '''
        kwargs = self.get_query_params()
        self._examine_kwargs('POST', kwargs)
        q = kwargs.pop('q', None)
        jsoninput = kwargs.pop('jsoninput', None) in ('1', 'true')
        if q:
            # ids = re.split('[\s\r\n+|,]+', q)
            try:
                ids = json.loads(q) if jsoninput else split_ids(q)
                if not isinstance(ids, list):
                    raise ValueError
            except ValueError:
                ids = None
                res = {'success': False, 'error': 'Invalid input for "q" parameter.'}
            if ids:
                scopes = kwargs.pop('scopes', None)
                fields = kwargs.pop('fields', None)
                res = self.esq.mget_biothings(ids, fields=fields, scopes=scopes, **kwargs)
        else:
            res = {'success': False, 'error': "Missing required parameters."}

        encode = not isinstance(res, str)    # when res is a string, e.g. when rawquery is true, do not encode it as json
        self.return_json(res, encode=encode)
        self.ga_track(event=self._ga_event_object('POST', {'qsize': len(q) if q else 0}))


class Neo4jQueryHandler(BaseHandler):
    ''' Implements a graph query endpoint for HTML GET. '''

    def _ga_event_object(self, action, data={}):
        ''' Returns the google analytics object for requests on this endpoint (query handler).'''
        return biothing_settings.ga_event_object(endpoint=biothing_settings._graph_query_endpoint, action=action, data=data)

    def _examine_kwargs(self, action, kwargs):
        ''' A function for sub-classing.  This will be run after the get_query_params but before the actual
            elasticsearch querying. '''
        pass

    def get(self):
        kwargs = self.get_query_params()
        self._examine_kwargs('GET', kwargs)
        q = kwargs.pop('q', None)
        if q:
            res = self.neo4jq.query(q, **kwargs)
        else:
            res = {'success': False, 'error': "Missing required parameters."}

        self.return_json(res)
        self.ga_track(event=self._ga_event_object('GET'))


class MetaDataHandler(BaseHandler):
    
    def get(self):
        _meta = self.esq.get_mapping_meta()
        _meta['software'] = {
            'python-package-info': get_python_version(),
            'codebase': get_repository_information(),
            'biothings': get_biothings_commit()
        }
        self.return_json(_meta)


class FieldsHandler(BaseHandler):

    def get(self):
        es_mapping = self.esq.query_fields()
        if biothing_settings.field_notes_path:
            notes = json.load(open(biothing_settings.field_notes_path, 'r'))
        else:
            notes = {}
        kwargs = self.get_query_params()

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
        search = kwargs.pop('search', None)
        prefix = kwargs.pop('prefix', None)
        for (k, v) in get_indexed_properties_in_dict(es_mapping, '').items():
            k1 = k.lstrip('.')
            if (search and search in k1) or (prefix and k1.startswith(prefix)) or (not search and not prefix):
                r[k1] = v
                if k1 in notes:
                    r[k1]['notes'] = notes[k1]
        self.return_json(r)

class StatusHandler(BaseHandler):
    ''' Handles requests to check the status of the server. '''

    def head(self):
        r = self.esq.status_check(biothing_settings.status_check_id)
        if r is None:
            # we failed to retrieve ref/test doc, something is wrong -> service unavailable
            raise HTTPError(503)

    def get(self):
        self.head()
        self.write('OK')
