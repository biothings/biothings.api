import sys, os, asyncio, types
import inspect, importlib, re
import logging
import concurrent.futures
from .version import MAJOR_VER, MINOR_VER, MICRO_VER
from .utils.dotfield import merge_object, make_object

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
    config variable) so the proper value can be through ConfigurationManager.
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


class ConfigurationManager(types.ModuleType):
    """
    Wraps and manages configuration access and edit. A singleton
    instance is available throughout all hub apps using biothings.config
    after biothings.config_for_app(conf_mod) as been called.
    In addition to providing config value access, either from config files
    or database, config manager can supersede atrributes of a class or instance
    with values coming from the database, allowing dynamic configuration of hub's
    elements.
    """

    def __init__(self, conf):
        self.conf = conf
        self.hub_config = None # set when wrapped, see config_for_app()
        self.bykeys = {} # caching values from hub db
        self.byroots = {} # for dotfield notation, all config names starting with first elem

    def __getattr__(self, name):
        # first try value from Hub DB, they have precedence
        # if nothing, then take it from file
        val = self.get_value_from_db(name) or self.get_value_from_file(name)
        return val

    def __getitem__(self, name):
        # for dotfield notation
        return self.__getattr__(name)

    def reset_cache(self):
        self.bykeys = {}
        self.byroots = {}

    def get_path_from_db(self, name):
        return self.byroots.get(name,[])

    def merge_with_path_from_db(self, name, val):
        roots = self.get_path_from_db(name)
        for root in roots:
            dotfieldname,value = root["_id"],root["value"]
            val = merge_object({name:val},make_object(dotfieldname,value))
        return val

    def store_value_to_db(self, name, value, scope="config"):
        """
        Stores a configuration "value" named "name" in hub_config.
        "scope" defines what the configuration value applies on:
        - 'config': a config value which could be find in config*.py files
        - 'class': applied to a class (supersedes class attributes)
        - 'instance': appliced to an instance (supersedes instance props)
        """
        assert self.hub_config, "No hub_config collection set"
        res = self.hub_config.update_one({"_id" : name},
                                         {"$set" : {
                                             "scope" : scope,
                                             "value": value,
                                             }
                                         },
                                         upsert=True) 
        return res.upserted_id

    def get_value_from_db(self, name, scope="config"):
        if self.hub_config:
            # cache on first call
            if not self.bykeys:
                for d in self.hub_config.find():
                    # tricky: get it from file to cast to correct type
                    val = d["value"]
                    try:
                        tval = self.get_value_from_file(d["_id"])
                        typ = type(tval)
                        val = typ(val) # recast
                    except AttributeError:
                        # only exists in db
                        pass
                    # fill in cache, by scope then by config key
                    scope = d.get("scope","config")
                    self.bykeys.setdefault(scope,{})
                    self.bykeys[scope][d["_id"]] = val
                    elems = d["_id"].split(".")
                    if len(elems) > 1: # we have a dotfield notation there
                        self.byroots.setdefault(elems[0],[]).append({"_id" : d["_id"], "value": val})
            return self.bykeys.get(scope,{}).get(name)

    def get_value_from_file(self, name):
        # if "name" corresponds to a dict, we may have
        # dotfield paths in DB overridiing some of the content
        # we'd need to merge that path with 
        val = getattr(self.conf,name)
        val = self.merge_with_path_from_db(name,val)
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

    def patch(self, something, confvals):
        for confval in confvals:
            print(confval)
            key = confval["_id"]
            value = confval["value"]
            # key looks like dotfield notation, with first elem being "something"'s name
            # we can remove that first elem as it represents "something"
            # TODO: sanity check ? first elem must match "something" to make sure we patch what
            # we think we should patch ?
            attrs = ".".join(key.split(".")[1:])
            # we need to set attributes to "something" with "value", in recursive way, moving
            # in depth in "something" to find attributes to patch. Note: all attrs must exists.
            def set(what, name, value):
                if "." in name:
                    # need to go deeper
                    set(getattr(what,name.split(".")[0]),".".join(name.split(".")[1:]),value)
                else:
                    setattr(what,name,value)
            set(something,attrs,value)

    def supersede(self, something):
        # find config values with scope corresponding to something's type
        # Note: any conf key will look like a dotfield notation, eg. MyClass.myattr
        # so we search 1st by root (MyClass) to see if we have a config key in DB, 
        # then we fetch all 
        scope = None
        if isinstance(something,type):
            scope = "class"
        elif isinstance(something,object):
            # it's an instance. conf key looks the same as when it's a class but scope is different
            # (because classes have a name, but instances don't)
            scope = "instance"
        else:
            raise TypeError("Don't know how to supersede type '%s'" % type(something))

        assert scope
        # it's a class, get by roots using string repr
        confvals = self.byroots.get(something.__name__,[])
        # check/filter by scope
        valids = []
        for conf in confvals:
            match = self.get_value_from_db(conf["_id"],scope=scope)
            if match:
                # we actually have a conf key/value matching that scope, keep it
                valids.append(conf)
        self.patch(something,valids)

    def __repr__(self):
        return "<%s over %s>" % (self.__class__.__name__,self.conf.__name__)


class ConfigParser(object):
    """
    Parse configuration module and extract documentation from it.
    Documentation can be found in different place (in order):
    1. the configuration value is a ConfigurationDefault instance (specify a default value)
       or a ConfigurationError instance, in whic case the documentation is taken
       from the instance doc.
    2. the documentation can be specified as an inline comment
    3. the documentation can be specified as comments above
    If the configuration module also import another (or more) config modules, those
    modules will be searched as well, if nothing could be found in the main module.
    As soon as a documentation is found, the search stops (importance of module imports order)
    """

    def __init__(self, config_mod):
        self.config = config_mod
        self.lines = inspect.getsourcelines(self.config)[0]
        self.find_base_config()

    def find_base_config(self):
        self.config_bases = []
        pat = re.compile("^from\s+(.*?)\s+import\s+\*")
        for l in self.lines:
            m = pat.match(l)
            if m:
                base = m.groups()[0]
                base_mod = importlib.import_module(base)
                self.config_bases.append(base_mod)

    def find_docstring(self, field):
        for conf in [self.config] + self.config_bases:
            doc = self.find_docstring_in_config(conf, field)
            if doc:
                return doc

    def find_docstring_in_config(self, config, field):
        field = field.strip()
        if "." in field:
            # dotfield notiation, explore a dict
            raise ValueError("no fot")
            pass
        if not hasattr(config,field):
            return None
        confval = getattr(config,field)
        if isinstance(confval,ConfigurationDefault):
            return confval.desc
        if isinstance(confval,ConfigurationError):
            # it's an Exception, just take the text
            return confval.args[0]
        else:
            found_field = False
            for i,l in enumerate(self.lines):
                if l.startswith(field):
                    found_field = True
                    break
            if found_field:
                return self.find_comment(field,i)

    def find_comment(self,field,lineno):
        pat = re.compile(".*\s*#\s*(.*)$")
        # inline comment
        line = self.lines[lineno]
        m = pat.match(line)
        if m:
            return m.groups()[0].strip()
        else:
            # comment above
            docs = []
            i = lineno
            while i > 0:
                i -= 1
                l = self.lines[i]
                if l.startswith("\n"):
                    break
                else:
                    m = pat.match(l)
                    if m:
                        docs.insert(0,m.groups()[0])
                    else:
                        break
            return "\n".join(docs)


def config_for_app(config_mod, check=True):
    if check == True:
        check_config(config_mod)
    app_path = os.path.split(config_mod.__file__)[0]
    sys.path.insert(0,app_path)
    # this will create a "biothings.config" module
    # so "from biothings from config" will get app config at lib level
    # (but "import biothings.config" won't b/c not a real module within biothings
    wrapper = ConfigurationManager(config_mod)
    globals()["config"] = wrapper
    config.APP_PATH = app_path
    if not hasattr(config_mod,"HUB_DB_BACKEND"):
        raise AttributeError("Can't find HUB_DB_BACKEND in configutation module")
    else:
        import importlib
        config.hub_db = importlib.import_module(config_mod.HUB_DB_BACKEND["module"])
        import biothings.utils.hub_db
        biothings.utils.hub_db.setup(config)
        wrapper.hub_config = biothings.utils.hub_db.get_hub_config()
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

