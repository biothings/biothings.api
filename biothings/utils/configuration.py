
import re
from copy import deepcopy
from collections import UserList, UserString
from dataclasses import asdict, dataclass, field


import copy
import inspect
import json
import re
from collections import UserList, UserString, deque
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from importlib import import_module

from biothings.utils.dataload import dict_traverse
from biothings.utils.dotfield import make_object, merge_object
from biothings.utils.jsondiff import make as jsondiff


class ConfigurationError(Exception):
    pass


class ConfigurationValue:
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

    def __init__(self, code):
        self.code = code

    def get_value(self, name, conf):
        """
        Return value by eval'ing code in self.code, in the context of given configuration
        dict (namespace), for given config parameter name.
        """
        try:
            return eval(self.code, conf.__dict__)
        except SyntaxError:
            # try exec, maybe it's a statement (not just an expression).
            # in that case, it eeans user really knows what he's doing...
            exec(self.code, conf.__dict__)
            # there must be a variable named the same same, in that dict,
            # coming from code's statements
            return conf.__dict__[name]


class ConfigurationDefault:
    def __init__(self, default, desc):
        self.default = default
        self.desc = desc


class ConfigurationManager():
    """
    Wraps and manages configuration access and edit. A singleton
    instance is available throughout all hub apps using biothings.config
    after biothings.config_for_app(conf_mod) as been called.
    In addition to providing config value access, either from config files
    or database, config manager can supersede attributes of a class with values
    coming from the database, allowing dynamic configuration of hub's elements.
    """

    # This class contains mostly Sebastien's original design
    # It has been moved from biothings.__init__ module to here.

    def __init__(self, conf):
        self.conf = conf
        self.hub_config = None  # collection containing config value, set when wrapped, see config_for_app()
        self.bykeys = {}  # caching values from hub db
        self.byroots = {}  # for dotfield notation, all config names starting with first elem
        self.dirty = False  # gets dirty (needs reload) when config is changed in db
        self._original_params = {}  # cache config params as defined in config files

        self.allow_edits = False
        if hasattr(self.conf, "CONFIG_READONLY"):
            self.allow_edits = not self.conf.CONFIG_READONLY
            delattr(self.conf, "CONFIG_READONLY")

    @property
    def original_params(self):
        if not self._original_params:

            params = {}
            conf_markups = _parse_conf_comments(self.conf)
            for attrname in _list_hub_attributes(self.conf):
                value = getattr(self, attrname)
                info = conf_markups.get(attrname)
                if info is None:
                    # if no information could be found, not even the field,
                    # (if field was found in a config file but without any information,
                    # we would have had a {"found" : True}), it means the parameter (field)
                    # as been set dynamically somewhere in the code. It's not coming from
                    # config files, we need to tag it as-is, and make it readonly
                    # (we don't want to allow config changes other than those specified in
                    # config files)
                    params[attrname] = {
                        "value": value,
                        "dynamic": True,
                        "readonly": True,
                        "default": value  # for compatibilty
                    }
                else:
                    if info["invisible"]:
                        continue
                    origvalue = getattr(info["confmod"], attrname)

                    # TODO
                    # this is in a way repeating what's in get_value_from_file, 
                    # maybe possible to extract it?

                    def eval_default_value(k, v):
                        if isinstance(v, ConfigurationDefault):
                            if isinstance(v.default, ConfigurationValue):
                                return (k, v.default.get_value(attrname, self.conf))
                            else:
                                return (k, v.default)
                        elif isinstance(v, ConfigurationValue):
                            return (k, v.get_value(k, self.conf))
                        elif isinstance(v, ConfigurationError):
                            return k, repr(v)
                        else:
                            return k, v

                    if isinstance(origvalue, dict):
                        # walk the dict and instantiate values when special
                        dict_traverse(origvalue, eval_default_value, traverse_list=True)
                    else:
                        # just use the same func but ignore "k" key, not a dict
                        # pass unhashable "k" to make sure we'd raise an error
                        # while dict traversing  if we're not supposed to be here
                        _, origvalue = eval_default_value({}, origvalue)

                    params[attrname] = {
                        "value": info["hidden"] and "********" or value,
                        "section": info["section"],
                        "desc": info["description"],
                        "default": info["hidden"] and "********" or origvalue
                    }
                    if info["hidden"]:
                        params[attrname]["hidden"] = True
                    if info["readonly"]:
                        params[attrname]["readonly"] = True

            self._original_params = params
        return self._original_params

    def __getattr__(self, name):
        # first try value from Hub DB, they have precedence
        # if nothing, then take it from file

        try:
            val = self.get_value_from_db(name)
        except (KeyError, ValueError):
            val = self.get_value_from_file(name)

        return val

    def __delattr__(self, name):
        delattr(self.conf, name)

    def __getitem__(self, name):
        # for dotfield notation
        return self.__getattr__(name)

    def show(self):
        origparams = copy.deepcopy(self._original_params)
        byscopes = {"scope": {}}
        # some of these could have been superseded by values from DB
        for key, info in origparams.items():
            # search whether param named "key" has been superseded
            if info["default"] != info["value"]:
                diff = jsondiff(info["default"], info["value"])
                origparams[key]["diff"] = diff
        byscopes["scope"]["config"] = origparams
        byscopes["scope"]["class"] = self.bykeys.get("class", {})

        # set a flag to indicate if config is dirty and the hub needs to reload
        # (something's changed but not taken yet into account)
        byscopes["_dirty"] = self.dirty
        byscopes["allow_edits"] = self.allow_edits
        return byscopes

    def clear_cache(self):
        self.bykeys = {}
        self.byroots = {}
        self._original_params = {}

    def get_path_from_db(self, name):
        return self.byroots.get(name, [])

    def merge_with_path_from_db(self, name, val):
        roots = self.get_path_from_db(name)
        for root in roots:
            dotfieldname, value = root["_id"], root["value"]
            val = merge_object(
                val,
                make_object(dotfieldname, value)[dotfieldname.split(".")[0]])
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
            assert not self.original_params[name].get(
                "readonly"), "This parameter is not editable"

    def reset(self, name, scope="config"):
        self.check_editable(name, scope)
        res = self.hub_config.remove({"_id": name})
        self.dirty = True  # may need a reload
        self.clear_cache()  # will force reload everything to get up-to-date values
        return res["ok"]

    def store_value_to_db(self, name, value, scope="config"):
        """
        Stores a configuration "value" named "name" in hub_config.
        "scope" defines what the configuration value applies on:
        - 'config': a config value which could be find in config*.py files
        - 'class': applied to a class (supersedes class attributes)
        """
        self.check_editable(name, scope)
        res = self.hub_config.update_one(
            {"_id": name},
            {"$set": {
                "scope": scope,
                "value": json.loads(value),
            }},
            upsert=True)
        self.dirty = True  # may need a reload
        self.clear_cache()
        return res

    def get_value_from_db(self, name, scope="config"):
        if not self.hub_config:
            raise ValueError("hub_config not set yet")

        # cache on first call
        if not self.bykeys:
            for d in self.hub_config.find():
                # tricky: get it from file to cast to correct type
                val = d["value"]
                try:
                    tval = self.get_value_from_file(d["_id"])
                    typ = type(tval)
                    val = typ(val)  # recast
                except AttributeError:
                    # only exists in db
                    pass
                # fill in cache, by scope then by config key
                scope = d.get("scope", "config")
                self.bykeys.setdefault(scope, {})
                self.bykeys[scope][d["_id"]] = val
                elems = d["_id"].split(".")
                if len(elems) > 1:  # we have a dotfield notation there
                    # tricky; read the comments below, extracting the root has a different meaning depending on the scope
                    if scope == "config":
                        # first elem in the path a config variable, the rest is a path inside that variable (which
                        # is a dict)
                        self.byroots.setdefault(elems[0], []).append({
                            "_id": d["_id"],
                            "value": val
                        })
                    else:
                        # the root is everything up to the last element in the path, that is, the full path
                        # of the class, etc... The last element is the attribute to set.
                        self.byroots.setdefault(".".join(elems[:-1]), []).append({
                            "_id": d["_id"],
                            "value": val
                        })

        return self.bykeys.get(scope, {})[name]

    def get_value_from_file(self, name):
        # if "name" corresponds to a dict, we may have
        # dotfield paths in DB overridiing some of the content
        # we'd need to merge that path with
        val = getattr(self.conf, name)
        try:
            copiedval = copy.deepcopy(val)  # we want to keep original value (if it's a dict)
            # as this will be merged with value from db
        except TypeError:
            # it can't be copied, it probably means it can't even be stored in db
            # so no risk of overriding original value
            copiedval = val
            pass
        copiedval = self.merge_with_path_from_db(name, copiedval)

        def eval_default_value(k, v):
            if isinstance(v, ConfigurationDefault):
                if isinstance(v.default, ConfigurationValue):
                    return (k, v.default.get_value(name, self.conf))
                else:
                    return (k, v.default)
            elif isinstance(v, ConfigurationValue):
                return (k, v.get_value(k, self.conf))
            else:
                return (k, v)

        if isinstance(copiedval, dict):
            # walk the dict and instantiate values when special
            dict_traverse(copiedval, eval_default_value, traverse_list=True)
        else:
            # just use the same func but ignore "k" key, not a dict
            # pass unhashable "k" to make sure we'd raise an error
            # while dict traversing  if we're not supposed to be here
            _, copiedval = eval_default_value({}, copiedval)

        return copiedval

    def patch(self, something, confvals, scope):
        for confval in confvals:
            key = confval["_id"]
            value = confval["value"]
            # key looks like dotfield notation, last elem is the attribute, and what's before
            # is path to the "something" object (eg. hub.dataload.sources.mysrc.dump.Dumper)
            attr = key.split(".")[-1]
            setattr(something, attr, value)

    def supersede(self, something):
        # find config values with scope corresponding to something's type
        # Note: any conf key will look like a dotfield notation, eg. MyClass.myattr
        # so we search 1st by root (MyClass) to see if we have a config key in DB,
        # then we fetch all
        scope = None
        if isinstance(something, type):
            fullname = "%s.%s" % (something.__module__, something.__name__)
            scope = "class"
        else:
            raise TypeError("Don't know how to supersede type '%s'" %
                            type(something))

        assert scope
        # it's a class, get by roots using string repr
        confvals = self.byroots.get(fullname, [])
        # check/filter by scope
        valids = []
        for conf in confvals:
            match = self.get_value_from_db(conf["_id"], scope=scope)
            if match:
                # we actually have a conf key/value matching that scope, keep it
                valids.append(conf)
            self.patch(something, valids, scope)

    def __repr__(self):
        return "<%s over %s>" % (self.__class__.__name__, str(self.conf))


def _list_hub_attributes(conf_mod):
    attrs = set()
    for attrname in dir(conf_mod):
        # re pattern to find config param
        # (by convention, all upper caps, _ allowed, that's all)
        if re.compile("^([A-Z_]+)$").match(attrname):
            attrs.add(attrname)
    return attrs


def _parse_conf_comments(conf_mod):
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
    attrs = _list_hub_attributes(conf_mod)

    configs = []
    _configs = deque([conf_mod])
    while _configs:
        config = _configs.popleft()
        lines = inspect.getsourcelines(config)[0]
        lines = ConfigLines(ConfigLine(line) for line in lines)

        pat = re.compile(r"^from\s+(.*?)\s+import\s+\*")
        for line in lines:
            match = pat.match(line.data)
            if match:
                base = match.groups()[0]
                base_mod = import_module(base)
                _configs.append(base_mod)

        result = lines.parse(attrs)
        configs.append((config, result))

    _info = {}  # merged
    for config, result in configs:
        for attr, meta in result.items():
            meta.confmod.feed(config)
            if attr in _info:
                _info[attr].update(meta)
            else:
                _info[attr] = meta

    return {k: v.asdict() for k, v in _info.items()}

class MetaField():
    default = type(None)

    def __init__(self, value=None):
        self._value = value or self.default()

    @property
    def value(self):
        return self._value

    def feed(self, value):
        self._value = value

    def clear(self):
        self._value = self.default()

class Text(MetaField):

    def feed(self, value):
        assert isinstance(value, str)
        self._value = value.strip()

class Flag(MetaField):
    default = bool

class Paragraph(MetaField):
    default = list

    @property
    def value(self):
        _value = ' '.join(self._value)
        return _value or None

    def feed(self, value):
        if isinstance(value, Paragraph):
            value = value.value
        assert isinstance(value, str)
        self._value.append(value.strip())


# TODO currently not processing SKIPs
# I noticed SKIPs in the config file
# But it seems that it wasn't processed
# in the previous version either

@dataclass
class ConfigAttrMeta():

    confmod: MetaField = field(default_factory=MetaField)
    section: Text = field(default_factory=Text)  # persistent
    description: Paragraph = field(default_factory=Paragraph)
    readonly: Flag = field(default_factory=Flag)
    hidden: Flag = field(default_factory=Flag)
    invisible: Flag = field(default_factory=Flag)

    def update(self, meta):
        assert isinstance(meta, ConfigAttrMeta)
        for field, value in meta.asdict().items():
            self.feed(field, value)

    def asdict(self):
        confmod, self.confmod = self.confmod, MetaField()
        result = {k: v.value for k, v in asdict(self).items()}
        result['confmod'] = confmod.value  # cannot pickle in asdict
        return result

    ## ------------------------------------

    def feed(self, field, value):
        if field and value:
            getattr(self, field).feed(value)

    def reset(self):
        self.description.clear()
        self.readonly.clear()
        self.hidden.clear()
        self.invisible.clear()

    def commit(self):
        result = deepcopy(self)
        self.reset()
        return result


class ConfigLines(UserList):

    def parse(self, attrs=()):

        result = {}
        attrs = set(attrs)
        current = ConfigAttrMeta()

        for line in self.data:

            # feed
            field, value = line.match()
            current.feed(field, value)

            # commit
            if '=' in line:
                attr = line.split("=")[0].strip()
                if attr in attrs:
                    result[attr] = current.commit()

            # reset
            if not line.strip():
                current.reset()

        return result


class ConfigLine(UserString):

    PATTERNS = (
        ("hidden", re.compile(r"^#-\s*hide\s*-#\s*$"), lambda m: bool(m)),
        ("invisible", re.compile(r"^#-\s*invisible\s*-#\s*$"), lambda m: bool(m)),
        ("readonly", re.compile(r"^#-\s*readonly\s*-#\s*$"), lambda m: bool(m)),
        ("section", re.compile(r"^#\*\s*(.*)\s*\*#\s*$"), lambda m: m.groups()[0]),
        ("description", re.compile(r".*\s*#\s+(.*)$"), lambda m: m.groups()[0]),
    )

    def match(self):
        for _type, pattern, func in self.PATTERNS:
            match = pattern.match(self.data)
            if match:
                return _type, func(match)
        return None, None
