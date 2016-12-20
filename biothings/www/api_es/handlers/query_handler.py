import re
import json
from tornado.web import HTTPError
from biothings.www.helper import BaseHandler
from biothings.utils.common import split_ids
from biothings.utils.version import get_software_info
from collections import OrderedDict

class QueryHandler(BaseHandler):
    def _extra_control_options(self, options, kwargs):
        pass

    def _extra_es_options(self, options, kwargs):
        pass

    def _extra_esqb_options(self, options, kwargs):
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
                res = self.esq.mquery_biothings(ids, fields=fields, scopes=scopes, **kwargs)
        else:
            res = {'success': False, 'error': "Missing required parameters."}

        encode = not isinstance(res, str)    # when res is a string, e.g. when rawquery is true, do not encode it as json
        self.return_json(res, encode=encode)
        self.ga_track(event=self._ga_event_object('POST', {'qsize': len(q) if q else 0}))
