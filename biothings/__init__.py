import sys, os, asyncio, types, copy
import inspect, importlib, re
import logging
import concurrent.futures
from .version import MAJOR_VER, MINOR_VER, MICRO_VER
from .utils.dotfield import merge_object, make_object
from .utils.jsondiff import make as jsondiff
from .utils.common import is_scalar

def get_version():
    return '{}.{}.{}'.format(MAJOR_VER, MINOR_VER, MICRO_VER)


# re pattern to find config param
# (by convention, all upper caps, _ allowed, that's all)
PARAM_PAT = re.compile("^([A-Z_]+)$")

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
    or database, config manager can supersede attributes of a class with values
    coming from the database, allowing dynamic configuration of hub's elements.
    """

    def __init__(self, conf):
        self.conf = conf
        self.conf_parser = ConfigParser(conf)
        self.hub_config = None # collection containing config value, set when wrapped, see config_for_app()
        self.bykeys = {} # caching values from hub db
        self.byroots = {} # for dotfield notation, all config names starting with first elem
        self.allow_edits = False
        self.dirty = False # gets dirty (needs reload) when config is changed in db
        self._original_params = {} # cache config params as defined in config files

    @property
    def original_params(self):
        if not self._original_params:
            self._original_params = self.get_config_params()
        return self._original_params

    def __getattr__(self, name):
        # first try value from Hub DB, they have precedence
        # if nothing, then take it from file
        val = self.get_value_from_db(name) or self.get_value_from_file(name)
        return val

    def __delattr__(self, name):
        delattr(self.conf,name)

    def __getitem__(self, name):
        # for dotfield notation
        return self.__getattr__(name)

    def editable(self, allow):
        self.allow_edits = bool(allow)

    def show(self):
        origparams = copy.deepcopy(self.original_params)
        byscopes = {"scope" : {}}
        # some of these could have been superseded by values from DB
        for key,info in origparams.items():
            # search whether param named "key" has been superseded
            # using a dotfield notation (inside a dict) or if it's
            # a simple config param (value = scalar)
            if key in self.byroots or key in self.bykeys.get("config",{}):
                # it's been superseded
                newvalue = getattr(self,key)
                diff = jsondiff(info["value"],newvalue)
                origparams[key]["superseded"] = {"value" : newvalue, "diff" : diff}
        byscopes["scope"]["config"] = origparams
        byscopes["scope"]["class"] = self.bykeys.get("class",{})

        # set a flag to indicate if config is dirty and the hub needs to reload
        # (something's changed but not taken yet into account)
        byscopes["_dirty"] = self.dirty
        return byscopes

    def clear_cache(self):
        self.bykeys = {}
        self.byroots = {}

    def get_path_from_db(self, name):
        return self.byroots.get(name,[])

    def merge_with_path_from_db(self, name, val):
        roots = self.get_path_from_db(name)
        for root in roots:
            dotfieldname,value = root["_id"],root["value"]
            val = merge_object(val,make_object(dotfieldname,value)[dotfieldname.split(".")[0]])
        return val

    def check_editable(self, name, scope):
        assert self.allow_edits, "Configuration is read-only"
        assert self.hub_config, "No hub_config collection set"
        assert not name == "CONFIG_READONLY", "I won't allow to store/supersede that parameter. Nice try though..."
        # check if param is invisble
        # (and even if using dotfield notation, if a dict is "invisible"
        # then any of its keys are also invisible)
        if scope == "config":
            name = name.split(".")[0]
            assert name in self.original_params, "Unknown configuration parameter"
            assert not self.original_params[name].get("readonly"), "This parameter is not editable"

    def reset(self, name, scope="config"):
        self.check_editable(name,scope)
        res = self.hub_config.delete_one({"_id" : name,
                                          "scope" : "config"})
        self.dirty = True # may need a reload
        return res.acknowledged

    def store_value_to_db(self, name, value, scope="config"):
        """
        Stores a configuration "value" named "name" in hub_config.
        "scope" defines what the configuration value applies on:
        - 'config': a config value which could be find in config*.py files
        - 'class': applied to a class (supersedes class attributes)
        """
        self.check_editable(name,scope)
        res = self.hub_config.update_one({"_id" : name},
                                         {"$set" : {
                                             "scope" : scope,
                                             "value": value,
                                             }
                                         },
                                         upsert=True) 
        self.dirty = True # may need a reload
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
                        # tricky; read the comments below, extracting the root has a different meaning depending on the scope
                        if scope == "config":
                            # first elem in the path a config variable, the rest is a path inside that variable (which
                            # is a dict)
                            self.byroots.setdefault(elems[0],[]).append({"_id" : d["_id"], "value": val})
                        else:
                            # the root is everything up to the last element in the path, that is, the full path
                            # of the class, etc... The last element is the attribute to set.
                            self.byroots.setdefault(".".join(elems[:-1]),[]).append({"_id" : d["_id"], "value": val})
            return self.bykeys.get(scope,{}).get(name)

    def get_value_from_file(self, name):
        # if "name" corresponds to a dict, we may have
        # dotfield paths in DB overridiing some of the content
        # we'd need to merge that path with 
        val = getattr(self.conf,name)
        try:
            copiedval = copy.deepcopy(val) # we want to keep original value (if it's a dict)
                                 # as this will be merged with value from db
        except TypeError as e:
            # it can't be copied, it probably means it can't even be stored in db
            # so no risk of overriding original value
            copiedval = val
            pass
        copiedval = self.merge_with_path_from_db(name,copiedval)
        if isinstance(copiedval,ConfigurationDefault):
            if isinstance(copiedval.default,ConfigurationValue):
                try:
                    return eval(copiedval.default.code,self.conf.__dict__)
                except SyntaxError:
                    # try exec, maybe it's a statement (not just an expression).
                    # in that case, it eeans user really knows what he's doing...
                    exec(copiedval.default.code,self.conf.__dict__)
                    # there must be a variable named the same same, in that dict,
                    # coming from code's statements
                    return self.conf.__dict__[name]
            else:
                return copiedval.default
        else:
            return copiedval

    def patch(self, something, confvals, scope):
        for confval in confvals:
            key = confval["_id"]
            value = confval["value"]
            # key looks like dotfield notation, last elem is the attribute, and what's before
            # is path to the "something" object (eg. hub.dataload.sources.mysrc.dump.Dumper)
            attr = key.split(".")[-1]
            setattr(something,attr,value)

    def get_config_params(self):
        """
        Return all configuration parameters with their description, by section"
        """
        params = {}
        for attrname in self.conf_parser.list():
            value = getattr(self,attrname)
            info = self.conf_parser.find_information(attrname)
            if info is None:
                # if no information could be found, not even the field,
                # (if field was found in a config file but without any information,
                # we would have had a {"found" : True}), it means the parameter (field)
                # as been set dynamically somewhere in the code. It's not coming from 
                # config files, we need to tag it as-is, and make it readonly
                # (we don't want to allow config changes other than those specified in
                # config files)
                params[attrname] = {
                        "value" : value,
                        "dynamic" : True,
                        "readonly" : True
                        }
            else:
                if info["invisible"]:
                    continue
                params[attrname] = {
                        "value" : info["hidden"] and "********" or value,
                        "section" : info["section"],
                        "desc" : info["desc"]}
                if info["hidden"]:
                    params[attrname]["hidden"] = True
                if info["readonly"]:
                    params[attrname]["readonly"] = True

        return params

    def supersede(self, something):
        # find config values with scope corresponding to something's type
        # Note: any conf key will look like a dotfield notation, eg. MyClass.myattr
        # so we search 1st by root (MyClass) to see if we have a config key in DB, 
        # then we fetch all 
        scope = None
        if isinstance(something,type):
            fullname = "%s.%s" % (something.__module__,something.__name__)
            scope = "class"
        else:
            raise TypeError("Don't know how to supersede type '%s'" % type(something))

        assert scope
        # it's a class, get by roots using string repr
        confvals = self.byroots.get(fullname,[])
        # check/filter by scope
        valids = []
        for conf in confvals:
            match = self.get_value_from_db(conf["_id"],scope=scope)
            if match:
                # we actually have a conf key/value matching that scope, keep it
                valids.append(conf)
            self.patch(something,valids,scope)

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
    
    There are several special comment formats used to organize and manager params:
    - all comments above a param are considered as documentation/description for the parameter,
      until a empty line or a non-comment line is found.
    - to be considered as documentation, comments also need to have a space after "#". So:
            # my comment
      will be kepts as documentation, but:
            #my comment
      will just be ignored (so python code can be commented that way, without then being part
      of the documentation)
    - A section title can be added to organize parameters. A section title looks like this:
            #* my section title *#
      It can be added for each parameters, or the first section found above is the section
      the parameter will be associated to. An empty section title can be used to reset the
      section title and associate the current parameter to default one:
            #* *#
      If no section is found, all parameters are part of the default one (None).
    - some parameters needs to be kept secret (like passwords for instance):
            #- invisible -#
      will hide the parameter, including the name, value, description, from the configuration
            #- hidden -#
      will keep the parameter in the configuration displayed to users, but its value will be omitted
            #- readonly -#
      will allow the parameter to shown, but not editable

      Note: special comments can stacked:
            #- readonly -#
            #- hidden -#
      will make the parameter read-only, and its value won't be displayed
    """

    def __init__(self, config_mod):
        self.config = config_mod
        self.lines = inspect.getsourcelines(self.config)[0]
        self.find_base_config()

    def list(self):
        """
        Return a list of all config parameters' names found in config file
        (including base config files)
        """
        for attrname in dir(self.config):
            if PARAM_PAT.match(attrname):
                yield attrname

    def find_base_config(self):
        self.config_bases = []
        pat = re.compile("^from\s+(.*?)\s+import\s+\*")
        for l in self.lines:
            m = pat.match(l)
            if m:
                base = m.groups()[0]
                base_mod = importlib.import_module(base)
                self.config_bases.append(base_mod)

    def find_information(self, field):
        # search all config files, trying to get max infor
        # (field can be set in a base config file containing the description
        # and re-defined in main config without description)
        infos = []
        for conf in [self.config] + self.config_bases:
            info = self.find_docstring_in_config(conf, field)
            if info["found"]:
                infos.insert(0,info)

        if infos:
            # merge everything we have about the field, in the order
            # so most recent config in import history has precedence
            master = infos[0]
            for info in infos[1:]:
                for k,v in info.items():
                    if v:
                        master[k] = v
            return master

        # if we get there, it means field couldn't be found in any config files
        return None



    def find_docstring_in_config(self, config, field):
        field = field.strip()
        if "." in field:
            # dotfield notiation, explore a dict
            raise NotImplementedError("docstring no supported in dict")
            pass
        if not hasattr(config,field):
            return {"found" : False}
        confval = getattr(config,field)
        desc = None
        section = None
        invisible = None
        hidden = None
        readonly = None
        if isinstance(confval,ConfigurationDefault):
            desc = confval.desc
        if isinstance(confval,ConfigurationError):
            # it's an Exception, just take the text
            desc = confval.args[0]

        found_field = False
        lines = inspect.getsourcelines(config)[0]
        for i,l in enumerate(lines):
            if l.startswith(field):
                found_field = True
                break
        if found_field:
            if not desc:
                # no desc previously found in ConfigurationDefault or ConfigurationError
                desc = self.find_description(field,lines,i)
                section = self.find_section(field,lines,i)
                invisible = self.is_invisible(field,lines,i)
                hidden = self.is_hidden(field,lines,i)
                readonly = self.is_readonly(field,lines,i)
        return {"found" : found_field,
                "section" : section,
                "desc" : desc,
                "invisible" : invisible,
                "hidden" : hidden,
                "readonly" : readonly}

    def find_description(self, field, lines, lineno):
        # at least one space after # to consider this a description
        descpat = re.compile(".*\s*#\s+(.*)$")
        # inline comment
        line = lines[lineno]
        m = descpat.match(line)
        if m:
            return m.groups()[0].strip()
        else:
            # comment above
            cmts = []
            i = lineno
            while i > 0:
                i -= 1
                l = lines[i]
                if l.startswith("\n"):
                    break
                else:
                    m = descpat.match(l)
                    if m:
                        cmts.insert(0,m.groups()[0])
                    else:
                        break
            return "\n".join(cmts)

    def find_special_comment(self, pattern, field, lines, lineno, stop_on_eol=True):
        pat = re.compile(pattern)
        i = lineno
        while i > 0:
            i -= 1
            l = lines[i]
            if stop_on_eol and l.startswith("\n"):
                break
            m = pat.match(l)
            if m:
                return m

    def find_section(self, field, lines, lineno):
        match = self.find_special_comment("^#\*\s*(.*)\s*\*#$",field,lines,lineno,stop_on_eol=False)
        if match:
            section = match.groups()[0]
            if not section: # if we have "#* *#" it's a section breaker
                section = None # back to None if empty string
            return section

    def is_invisible(self, field, lines, lineno):
        match = self.find_special_comment("^#-\s*invisible\s*-#$",field,lines,lineno)
        if match:
            return True
        else:
            return False

    def is_hidden(self, field, lines, lineno):
        match = self.find_special_comment("^#-\s*hide\s*-#$",field,lines,lineno)
        if match:
            return True
        else:
            return False

    def is_readonly(self, field, lines, lineno):
        match = self.find_special_comment("^#-\s*readonly\s*-#$",field,lines,lineno)
        if match:
            return True
        else:
            return False


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

