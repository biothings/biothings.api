import inspect
import json
import os
import re
from collections import UserList, UserString, deque
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from importlib import import_module
from json import JSONDecodeError

from biothings.utils.dataload import dict_traverse
from biothings.utils.jsondiff import make as jsondiff
from biothings.utils.loggers import setup_default_log


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


# the above dynamic value types must be evaluated at runtime
# and not cached, because they may contain reference to the
# current time, which is commonly used in hub operations.


def is_jsonable(x):
    try:
        json.dumps(x)
        return True
    except (TypeError, OverflowError):
        return False


def set_default_folder(data_archive_root, sub_folder):
    """set default sub folder based on data_archive_root"""

    # trim off "datasources" to get the data_root folder, otherwise, use it as is.
    # '/data/mydisease/datasources' --> '/data/mydisease/'
    # "datasources" was reserved for downloaded data sources files
    _data_root = (
        data_archive_root[: -len("datasources")] if data_archive_root.endswith("datasources") else data_archive_root
    )
    return os.path.join(_data_root, sub_folder)


class ConfigurationWrapper:
    """
    Wraps and manages configuration access and edit. A singleton
    instance is available throughout all hub apps using biothings.config
    or biothings.hub.config after calling import biothings.hub.
    In addition to providing config value access, either from config files
    or database, config manager can supersede attributes of a class with values
    coming from the database, allowing dynamic configuration of hub's elements.
    """

    # This class contains some of Sebastien's original design
    # It has been moved from biothings.__init__ module to here.

    def __init__(self, default_config, conf):
        """
        When constructing a ConfigurationWrapper instance,
        variables will be defined with default values coming from default_config,
        then they can be overridden by conf's values,
        or new variables will be added if not defined in default_conf.
        Only metadata come from default_config will be used.
        """

        # Store current pid to use later for checking with the runtime's pid.
        self._pid = os.getpid()

        self._module = conf  # python module, typically config.py
        # update self._module with default value for missing configrations
        for attr in dir(default_config):
            default_value = getattr(default_config, attr)
            if not hasattr(self._module, attr):
                setattr(self._module, attr, default_value)

        self._annotations = _parse_comments(default_config, conf)  # section, visibility..
        self._db = None  # typically set by _config_for_app()

        self._modified = False
        self._readonly = True

        if hasattr(self._module, "CONFIG_READONLY"):
            self._readonly = self._module.CONFIG_READONLY

        # When config is readonly, or for readonly config keys,
        # we do a one-time evaluation for those config values set to a ConfigurationDefault instance.
        # This will save repeated code evaluation.
        for attr in dir(self._module):
            if attr == "logger":
                continue  # config.logger will be handled specifically later
            if isinstance(getattr(self._module, attr), ConfigurationDefault):
                if self._readonly or self._annotations.get(attr, {}).get("readonly", False):
                    setattr(self._module, attr, self.get_value_from_file(attr))

        # setup config.logger if not set yet
        logger = getattr(self._module, "logger", None)
        if not logger or isinstance(logger, ConfigurationDefault):
            if hasattr(self._module, "LOG_FOLDER"):
                # set logger if LOG_FOLDER is set
                self._module.logger = setup_default_log(
                    getattr(self._module, "LOGGER_NAME", "hub"),
                    self._module.LOG_FOLDER,
                )
            elif isinstance(logger, ConfigurationDefault):
                # set logger based on default value from default_config
                self._module.logger = self.get_value_from_file("logger")
                self._module.logger.warning('Missing "LOG_FOLDER" setting, default logger set to console only')
            else:
                raise ConfigurationError('Cannot set "config.logger", check "LOG_FOLDER" or "logger" setting')

    @property
    def modified(self):
        if self._db:
            self._modified = bool(self._db.count())
        return self._modified

    @property
    def readonly(self):
        return self._readonly

    def __getattr__(self, name):
        try:
            val = self.get_value_from_db(name)
        except (KeyError, ValueError, AttributeError):
            val = self.get_value_from_file(name)
        return val

    def __delattr__(self, name):
        # TODO I don't think this is a good idea
        delattr(self._module, name)

    def __getitem__(self, name):
        # for dotfield notation,
        # like MyClass.CLS_ATTR
        return self.__getattr__(name)

    def show(self):
        # correspond to /config endpoint
        # the result strcture is designed to
        # maintain compatibility with the frontend

        _config = {}
        _class = {}

        # basic information
        for key in self._annotations:
            x = self._annotations[key]
            # y = serializable(x)

            # "invisible" describes the visibility of keys
            # "hidden" describes the visibility of values

            if x["invisible"]:
                continue

            y = dict(x)
            y["confmod"] = str(y.pop("confmod", ""))
            y["desc"] = y.pop("description")

            default = getattr(self._module, key)
            if is_jsonable(default):
                y["default"] = default

            value = getattr(self, key)
            if is_jsonable(value):
                y["value"] = value

            if self._db.find_one({"_id": key}):
                y["dynamic"] = True

            if x["hidden"]:
                y["value"] = "********"
                y["default"] = "********"

            if y.get("default") and y.get("value"):
                _diff = jsondiff(y["default"], y["value"])
                if _diff:  # frontend shows "reset" button
                    y["diff"] = _diff

            _config[key] = y

        # process-level transient parameters
        for key in _list_attrs(self):
            _config[key] = {
                "value": getattr(self, key),
                "readonly": True,  # wrt to outside
            }

        # transient parameters on self._module are
        # not shown here, like self._module.logger

        # class attr superseding
        for doc in self._db.find():
            key = doc["_id"]
            if len(key.split(".")) == 2:
                _class[key] = json.loads(doc["json"])

        return {
            "scope": {
                "config": _config,
                "class": _class,
            },
            "_dirty": self.modified,
            "allow_edits": not self.readonly,
        }

    def reset(self, name=None):
        if not name:  # global reset
            self._modified = False
            return self._db.remove({})

        res = self._db.remove({"_id": name})
        # res = {'n': 1, 'ok': 1.0}
        return res["ok"]

    def store_value_to_db(self, name, value):
        if not self._db:
            raise RuntimeError("Transient parameter requires DB setup.")
        if self._readonly:
            raise RuntimeError("Configuration is globally read-only.")
        if self._annotations.get(name, {}).get("readonly"):
            raise RuntimeError("Parameter read-only.")
        if self._annotations.get(name, {}).get("invisible"):
            raise RuntimeError("Parameter reserved.")
        if name == "CONFIG_READONLY":  # False -> True also not allowed.
            raise RuntimeError("Runtime modification not allowed.")

        try:  # already json?
            json.loads(value)
        except JSONDecodeError:
            value = json.dumps(value)

        res = self._db.update_one(
            {"_id": name},
            {"$set": {"json": value}},
            upsert=True,
        )

        self._modified = True
        try:  # serializable result for mongo
            return res.raw_result
        except AttributeError:  # for sqlite, etc..
            return res

    def get_value_from_db(self, name):
        if not self._db:  # without db, only support module params.
            raise AttributeError("Transient parameter requires DB setup.")

        # Our db instance is shared with child process when this process is folked, and that action it not fork-safe.
        # So we must recreate db instance in the child process.
        # Ref: https://pymongo.readthedocs.io/en/stable/faq.html#id3
        if os.getpid() != self._pid:
            self._pid = os.getpid()
            self._db = None
            self._db = self._get_db_function()

        doc = self._db.find_one({"_id": name})
        if not doc:
            raise AttributeError(name)

        val = json.loads(doc["json"])
        return val

    def get_value_from_file(self, name):
        # raw value might require eval
        val = getattr(self._module, name)

        def eval_default_value(k, v):
            if isinstance(v, ConfigurationDefault):
                if isinstance(v.default, ConfigurationValue):
                    return (k, v.default.get_value(name, self._module))
                else:
                    return (k, v.default)
            elif isinstance(v, ConfigurationValue):
                return (k, v.get_value(k, self._module))
            elif isinstance(v, ConfigurationError):
                # default ConfigurationError value needs to be
                # set in hub's config file
                raise v
            else:
                return (k, v)

        if isinstance(val, dict):
            # walk the dict and instantiate values when special
            dict_traverse(val, eval_default_value, traverse_list=True)
        else:
            # just use the same func but ignore "k" key, not a dict
            # pass unhashable "k" to make sure we'd raise an error
            # while dict traversing  if we're not supposed to be here
            _, val = eval_default_value({}, val)

        return val

    def supersede(self, klass):
        """supersede class variable with db values"""

        if not isinstance(klass, type):
            raise TypeError("Don't know how to supersede type '%s'" % type(klass))

        for doc in self._db.find():
            attr = doc["_id"]  # MyClass.EXAMPLE_CLASS_VAR
            if attr.startswith(klass.__name__):
                splits = attr.split(".")
                if len(splits) == 2:
                    value = json.loads(doc["json"])
                    setattr(klass, splits[1], value)

    def __repr__(self):
        return "<%s over %s>" % (self.__class__.__name__, str(self._module))


def _list_attrs(conf_mod):
    attrs = set()  # hub-supported attributes
    for attrname in dir(conf_mod):
        # re pattern to find config param
        # (by convention, all upper caps, _ allowed, that's all)
        if re.compile("^([A-Z_]+)$").match(attrname):
            attrs.add(attrname)
    return attrs


def _parse_comments(default_conf_mod, conf_mod):
    """
    TODO NEED REVIEW

    Parse configuration module and extract documentation from default_conf_mod.
    Documentation can be found in different place (in order):
    1. the configuration value is a ConfigurationDefault instance (specify a default value)
       or a ConfigurationError instance, in which case the documentation is taken
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
    attrs = _list_attrs(default_conf_mod)
    attrs.update(_list_attrs(conf_mod))

    try:
        configs = []
        _configs = deque([default_conf_mod, conf_mod])

        while _configs:
            config = _configs.popleft()
            lines = inspect.getsourcelines(config)[0]

            # ignore special comment lines which donot come from default_config_mod
            if config != default_conf_mod:
                lines = [line for line in lines if not line.strip().startswith("#")]

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
        for config, result in reversed(configs):
            for attr, meta in result.items():
                meta.confmod.feed(config)
                if attr in _info:
                    _info[attr].update(meta)
                else:
                    _info[attr] = meta

        return {k: v.asdict() for k, v in _info.items()}
    except Exception:
        return dict.fromkeys(attrs, {})


class MetaField:
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
        self._value = value.strip() or None


class Flag(MetaField):
    default = bool

    def feed(self, value):
        if value:  # cannot unset a flag
            self._value = value


class Paragraph(MetaField):
    default = list

    @property
    def value(self):
        _value = " ".join(self._value)
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
class ConfigAttrMeta:
    confmod: MetaField = field(default_factory=MetaField)
    section: Text = field(default_factory=Text)  # persistent
    description: Paragraph = field(default_factory=Paragraph)
    readonly: Flag = field(default_factory=Flag)
    hidden: Flag = field(default_factory=Flag)
    invisible: Flag = field(default_factory=Flag)

    def update(self, meta):
        assert isinstance(meta, ConfigAttrMeta)
        for _field, value in meta.asdict().items():
            self.feed(_field, value)

    def asdict(self):
        confmod, self.confmod = self.confmod, MetaField()
        result = {k: v.value for k, v in asdict(self).items()}
        result["confmod"] = confmod.value  # cannot pickle in asdict
        return result

    # ------------------------------------

    def feed(self, field, value):
        if field and value is not None:
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
            if "=" in line:
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
