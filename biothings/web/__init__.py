import os
import sys
import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.escape
from biothings.web.settings import BiothingESWebSettings
from biothings.utils.common import is_str
from tempfile import NamedTemporaryFile
from string import Template

def _is_module(m):
    try:
        import types
        if type(m) == types.ModuleType:
            return True
        import importlib
        _ret = importlib.import_module(m)
    except ImportError:
        return False

    return _ret

def _is_file(f):
    return os.path.exists(os.path.abspath(f)) and os.path.isfile(os.path.abspath(f))

class BiothingsAPIApp(object):
    def __init__(self, *args, **kwargs):
        if kwargs.get('object_name', False) or (len(args) >= 1 and not _is_module(args[0]) and not _is_file(args[0])):
            _arg = args[0] if args else ''
            self._configure_by_object_name(object_name=kwargs.get('object_name', _arg))
        elif ((kwargs.get('config_file', False) and _file_exists(kwargs['config_file'])) or
            (len(args) >= 1 and _is_file(args[0]))):
            _arg = args[0] if args else ''
            self._configure_by_file(config_file=os.path.abspath(kwargs.get('config_file', _arg)))
        elif ((kwargs.get('config_module', False) and _is_module(kwargs['config_module'])) or 
            (len(args) >= 1 and _is_module(args[0]))):
            _arg = args[0] if args else ''
            self._configure_by_module(config_module=kwargs.get('config_module', _arg))
        else:
            self._configure_by_kwargs(**kwargs)

    def get_server(self,config_mod, **app_settings):
        settings = BiothingESWebSettings(config=config_mod) 
        app = tornado.web.Application(settings.generate_app_list(), **app_settings)
        server = tornado.httpserver.HTTPServer(app)
        return server

    def start(self, debug=True, port=8000, address='127.0.0.1', app_settings={}):
        if debug:
            #import tornado.autoreload
            import logging
            logging.getLogger().setLevel(logging.DEBUG)
            address='0.0.0.0'

        with NamedTemporaryFile(mode='w', suffix='.py', dir=os.path.abspath('.')) as _tempfile:
            if not getattr(self, 'settings_mod', False):
                if not getattr(self, 'settings_str', False):
                    raise("Could not configure biothings app")
                _tempfile.file.write(self.settings_str)
                _tempfile.file.flush()
                self.settings_mod = os.path.split(_tempfile.name)[1].split('.')[0]
            self.settings = BiothingESWebSettings(config=self.settings_mod)
            application = tornado.web.Application(self.settings.generate_app_list(), **app_settings)
            http_server = tornado.httpserver.HTTPServer(application)
            http_server.listen(port, address)
            loop = tornado.ioloop.IOLoop.instance()
            if debug:
                #tornado.autoreload.start(loop)
                logging.info('Server is running on "%s:%s"...' % (address, port))
            loop.start() 

    def _configure_by_object_name(self, object_name):
        # get the config file template
        config_string = """from biothings.web.settings.default import *\n""" + \
                        """from biothings.web.api.es.handlers import *\n""" + \
                        """ES_INDEX = '${src_package}_current'\n""" + \
                        """ES_DOC_TYPE = '${es_doctype}'\n""" + \
                        """API_VERSION = 'v1'\n""" + \
                        """APP_LIST = [(r'/status', StatusHandler), (r'/metadata/?', MetadataHandler), (r'/metadata/fields/?', MetadataHandler), (r'/{}/${annotation_endpoint}/(.+)/?'.format(API_VERSION), BiothingHandler), (r'/{}/${annotation_endpoint}/?$$'.format(API_VERSION), BiothingHandler), (r'/{}/query/?'.format(API_VERSION), QueryHandler), (r'/{}/metadata/?'.format(API_VERSION), MetadataHandler), (r'/{}/metadata/fields/?'.format(API_VERSION), MetadataHandler),]\n""" + \
                        """GA_RUN_IN_PROD = False"""
        settings_dict = {'src_package': 'my' + object_name.lower(),
                         'es_doctype': object_name.lower(),
                         'annotation_endpoint': object_name.lower()}
       
        self.settings_str = Template(config_string).substitute(settings_dict)            

    def _configure_by_module(self, config_module):
        self.settings_mod = config_module

    def _configure_by_kwargs(self, **kwargs):
        self.settings_str = """from biothings.web.settings.default import *\nfrom biothings.web.api.es.handlers import *\n"""
        for (k,v) in kwargs.items():
            if k == 'APP_LIST':
                self.settings_str += '{k}=['.format(k=k)
                for (reg, handler_str) in v:
                    self.settings_str += "(r'{reg}', {handler}),".format(reg=reg, handler=handler_str)
                self.settings_str += ']\n'
            elif k in ['ES_QUERY_BUILDER', 'ES_QUERY', 'ES_RESULT_TRANSFORMER']:
                self.settings_str += '{k}={v}\n'
            elif is_str(v):
                self.settings_str += '{k}="{v}"\n'.format(k=k, v=v)
            else:
                self.settings_str += '{k}={v}\n'
        
    def _configure_by_file(self, config_file):
        with open(config_file, 'r') as config_handle:
            self.settings_str = config_handle.read()
