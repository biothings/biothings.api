import sys, os, asyncio, types
import logging
import concurrent.futures
from .version import MAJOR_VER, MINOR_VER, MICRO_VER

def get_version():
    return '{}.{}.{}'.format(MAJOR_VER, MINOR_VER, MICRO_VER)


class ConfigurationError(Exception):
    pass


class ConfigurationValue(object):
    """
    type to wrap default value when it's code and needs to be interpreted later
    code is passed to eval() in the context of the whole "config" dict
    (so for instance, paths declared before in the configuration file can be used
    in the code passed to eval)
    code will also be executed through exec() *if* eval() raised a syntax error. This
    would happen when code contains statements, not just expression. In that case,
    a variable should be created in these statements (named the same as the original
    config variable) so the proper value can be through ConfigWrapper.
    """
    def __init__(self,code):
        self.code = code

class ConfigurationDefault(object):
    def __init__(self,default,desc):
        self.default = default
        self.desc = desc

def check_config(config_mod):
    for attr in dir(config_mod):
        if isinstance(getattr(config_mod,attr),ConfigurationError):
            raise ConfigurationError("%s: %s" % (attr,str(getattr(config_mod,attr))))


class ConfigWrapper(types.ModuleType):
    def __init__(self,conf):
        self.conf = conf
    def __getattr__(self,name):
        try:
            val = getattr(self.conf,name)
            if isinstance(val,ConfigurationDefault):
                if isinstance(val.default,ConfigurationValue):
                    try:
                        return eval(val.default.code,self.conf.__dict__)
                    except SyntaxError:
                        # try exec, maybe it's a statement (not just an expression).
                        # in that case, it eeans user really knows what he's doing...
                        exec(val.default.code,self.conf.__dict__)
                        # there must be a variable named the same same, in that dict,
                        # coming from code's statements
                        return self.conf.__dict__[name]
                else:
                    return val.default
            else:
                return val
        except AttributeError:
            raise
    def __repr__(self):
        return "<%s over %s>" % (self.__class__.__name__,self.conf.__name__)

def config_for_app(config_mod, check=True):
    if check == True:
        check_config(config_mod)
    app_path = os.path.split(config_mod.__file__)[0]
    sys.path.insert(0,app_path)
    # this will create a "biothings.config" module
    # so "from biothings from config" will get app config at lib level
    # (but "import biothings.config" won't b/c not a real module within biothings
    globals()["config"] = ConfigWrapper(config_mod)
    config.APP_PATH = app_path
    if not hasattr(config_mod,"HUB_DB_BACKEND"):
        raise AttributeError("Can't find HUB_DB_BACKEND in configutation module")
    else:
        import importlib
        config.hub_db = importlib.import_module(config_mod.HUB_DB_BACKEND["module"])
        import biothings.utils.hub_db
        biothings.utils.hub_db.setup(config)
    from biothings.utils.loggers import EventRecorder
    logger = logging.getLogger()
    fmt = logging.Formatter('%(asctime)s [%(process)d:%(threadName)s] - %(name)s - %(levelname)s -- %(message)s', datefmt="%H:%M:%S")
    erh = EventRecorder()
    erh.name = "event_recorder"
    erh.setFormatter(fmt)
    if not erh.name in [h.name for h in logger.handlers]:
        logger.addHandler(erh)


def get_loop(max_workers=None):
    loop = asyncio.get_event_loop()
    executor = concurrent.futures.ProcessPoolExecutor(max_workers=max_workers)
    loop.set_default_executor(executor)
    return loop

