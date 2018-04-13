import sys, os, asyncio
import logging
import concurrent.futures
from .version import MAJOR_VER, MINOR_VER, MICRO_VER

def get_version():
    return '{}.{}.{}'.format(MAJOR_VER, MINOR_VER, MICRO_VER)


class ConfigurationError(Exception):
    pass


def check_config(config_mod):
    for attr in dir(config_mod):
        if isinstance(getattr(config_mod,attr),ConfigurationError):
            raise ConfigurationError("%s: %s" % (attr,str(getattr(config_mod,attr))))


def config_for_app(config_mod, check=True):
    if check == True:
        check_config(config_mod)
    app_path = os.path.split(config_mod.__file__)[0]
    sys.path.insert(0,app_path)
    # this will create a "biothings.config" module
    # so "from biothings from config" will get app config at lib level
    # (but "import biothings.config" won't b/c not a real module within biothings
    globals()["config"] = config_mod
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

