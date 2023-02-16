"""
Request Argument Standardization
"""

import copy
import orjson
import logging
import re
from collections import UserDict, abc, defaultdict, namedtuple
from datetime import datetime as dt
from pprint import pformat
from types import MappingProxyType

try:
    from re import Pattern  # py>=3.7
except ImportError:
    from typing import Pattern

try:
    from functools import singledispatchmethod  # py>=3.7
except ImportError:
    from singledispatchmethod import singledispatchmethod

from biothings.utils.common import dotdict, split_ids


class OptionError(ValueError):

    def __init__(self, reason=None, **kwargs):
        super().__init__()
        self.info = {"reason": reason}
        self.info.update(kwargs)

    def simplify(self):
        self.info = {k: v for k, v in self.info.items() if v}

    def __str__(self):
        return f"OptionError({pformat(self.info)})"

class Converter():
    """
        A generic HTTP request argument processing unit.
        Only perform one level of validation at this moment.
        The strict switch controls the type conversion rules.
    """

    def __init__(self, **kwargs):

        self.type_ = kwargs.get("type", str)
        self.strict = kwargs.get("strict", True)
        translations = kwargs.get("translations", ())

        # Opinion on what value self.type_ can take
        # ---------------------------------------------
        # Although it may sound attractive to use non-native
        # "type" here to simplify certain object constuction
        # through the converstion process, and indeed it is
        # supported through self.to_type, this could lead to
        # difficulties in OptionSet serialization and other
        # problems in integration with this module.

        self.translations = []

        if isinstance(translations, dict):
            translations = translations.items()

        for pattern, repl in translations:
            if isinstance(pattern, Pattern):
                self.translations.append(
                    (pattern, repl))
            elif isinstance(pattern, tuple):
                self.translations.append(
                    (re.compile(*pattern), repl))
            elif isinstance(pattern, str):
                self.translations.append(
                    (re.compile(pattern), repl))
            else:  # https://docs.python.org/3/library/re.html#re.compile
                raise TypeError("Invalid Regex Pattern.")

    def __call__(self, value, to_type):
        return self.convert_to(value, to_type)

    @classmethod
    def subclasses(cls, kwargs):
        for kls in cls.__subclasses__():
            name = kls.__name__[:-len("ArgCvter")]
            yield name.lower(), kls(**kwargs)

    def convert(self, value):
        return self.convert_to(value, self.type_)

    def convert_to(self, value, to_type):

        # default implementation
        # only works for strings
        assert isinstance(value, str)

        if to_type is None:
            to_type = self.type_

        if to_type is str:
            return value  # pass through

        if to_type is bool:
            return self.str_to_bool(value)

        if to_type is int:
            return self.str_to_int(value)

        if to_type in (list, tuple, set):
            lst = self.str_to_list(value)
            return self.to_type(lst, to_type)

        return self.to_type(value, to_type)

    def translate(self, value):

        if isinstance(value, (tuple, list, set)):
            return (type(value))(self.translate(item) for item in value)

        if not isinstance(value, str):
            return value  # can only perform translations to strings

        # https://docs.python.org/3/library/re.html#re.sub
        for pattern, repl in self.translations:
            value = re.sub(pattern, repl, value)

        return value

    @staticmethod
    def str_to_bool(val):
        """ Interpret string representation of bool values. """
        assert isinstance(val, str)
        try:  # if it is a number
            return float(val) > 0
        except ValueError:
            pass  # process as keywords
        return val.lower() in ('1', 'true', 'yes', 'y', 't')

    # Opinion on str -> list
    #
    # It appears to have become more problematic recently, as the variety
    # of data increase in biothings applications, causing the identifiers
    # and field values hard to escape properly when used in queries.
    #
    # Consider implementing a very safe splitting algorithm that only works
    # for basic cases like a,b,c and use other methods like JSON input to
    # pass in complex queries.
    #
    # For example, for field reagent.GNF_mm+hs-MGC in Mygene:
    # the current algorithm will split it into ['reagent.GNF_mm', 'hs-MGC'

    @staticmethod
    def str_to_list(val):
        """ Cast Biothings-style str to list. """
        try:  # core splitting algorithm
            lst = split_ids(str(val))
        except ValueError as err:
            raise OptionError(str(err))
        return lst

    def str_to_int(self, val):
        """ Convert a numerical string to an integer. """
        assert isinstance(val, str)
        if not self.strict:
            val = self.to_type(val, float)
        return self.to_type(val, int)

    @staticmethod
    def to_type(val, type_):
        """
        Native type casting in Python.
        Fallback approach for type casting.
        """
        try:
            result = (type_)(val)
        except (ValueError, TypeError):
            raise OptionError(f"Expect type {type_.__name__}.")
        return result

class PathArgCvter(Converter):
    """
        Dedicated argument converter for path arguments.
        Correspond to arguments received in tornado for
            RequestHandler.path_args
            RequestHandler.path_kwargs
        See https://www.tornadoweb.org/en/stable/web.html
    """

class QueryArgCvter(Converter):
    """
        Dedicated argument converter for url query arguments.
        Correspond to arguments received in tornado from
            RequestHandler.get_query_argument
        See https://www.tornadoweb.org/en/stable/web.html
    """

    @classmethod
    def str_to_bool(cls, val):
        """ Biothings-style str to bool interpretation """
        # empty string indicates the presence of its key
        # we consider this to be a positive boolean value
        # this is especially useful in url when the user
        # may specify endpoint?op1&op2&op3 in which case
        # it makes sense to consider their keys true.
        return super().str_to_bool(val) or val.lower() == ''

class FormArgCvter(Converter):
    """
        Dedicated argument converter for HTTP body arguments.
        Additionally support JSON seriealization format as values.
        Correspond to arguments received in tornado from
            RequestHandler.get_body_argument
        See https://www.tornadoweb.org/en/stable/web.html
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # If jsoninput evaluates to true,
        # try to load the str as a json dump
        jsoninput = kwargs.get("jsoninput", False)
        if isinstance(jsoninput, bool):
            self.jsoninput = jsoninput
        else:  # it itself can be in argument format
            self.jsoninput = self.convert_to(jsoninput, bool)

    # Opinion on JsonInput
    #
    # The aforementioned feature was partly for convenience and partly
    # for backward compatibility with the original published design.
    #
    # It might be more beneficial to use standard data serialization
    # format like JSON to indicate data type unequivocally.

    def convert_to(self, value, to_type):
        if self.jsoninput:
            try:  # attempt to load as json first
                _value = orjson.loads(value)
            except orjson.JSONDecodeError as exc:
                logging.debug(repr(exc))
            else:  # no more conversions
                if isinstance(_value, to_type):
                    return _value
        if isinstance(value, to_type):
            return value
        if isinstance(value, str):
            return super().convert_to(value, to_type)
        return self.to_type(value, to_type)

class JsonArgCvter(Converter):
    """
        Dedicated argument converter for JSON HTTP bodys.
        Here it is used for dict JSON objects, with their
        first level keys considered as parameters and
        their values considered as arguments to process.

        May correspond to this tornado implementation:
        https://www.tornadoweb.org/en/stable/web.html#input
    """

    def convert_to(self, value, to_type):

        if isinstance(value, to_type):
            return value   # type matches

        if self.strict:
            # since JSON support value types
            # strict mode enforces it and essentially
            # makes this step a validation step.
            raise OptionError(f"Expect type {to_type.__name__}.")

        # mostly for biothings client 0.2.3 compatibility
        if isinstance(value, str) and to_type is list:
            return self.str_to_list(value)

        return self.to_type(value, to_type)

    def to_type(self, val, type_):

        if issubclass(type_, (list, tuple, set)) and not self.strict:
            val = (val, )  # "abc" -> ["abc"] instead of ["a", "b", "c"]

        return super().to_type(val, type_)


class ReqResult(dotdict):

    # besides multi-level attribute(dot)
    # read and write access, this class
    # also returns None value for missing
    # keys instead of raising an error

    def __str__(self):
        return f"ReqResult({pformat(self)})"

class ReqArgs():

    class Path():

        def __init__(self, args=(), kwargs={}):
            assert isinstance(args, (tuple, list))
            assert isinstance(kwargs, dict)
            self.args = args or ()
            self.kwargs = kwargs or {}

        def __getitem__(self, key):
            try:
                if isinstance(key, int):
                    return self.args[key]
                if isinstance(key, str):
                    return self.kwargs[key]
            except (KeyError, IndexError):
                return None

        def __bool__(self):
            return bool(self.args) or bool(self.kwargs)

        def __str__(self):
            lines = []
            for src in ("args", "kwargs"):
                if getattr(self, src):
                    lines.append("{}={}".format(
                        src, str(getattr(self, src))
                    ))
            return "Path(" + ", ".join(lines) + (")")

    def __init__(self, path=None, query=None, form=None, json_=None):

        assert isinstance(query, (dict, type(None)))
        assert isinstance(form, (dict, type(None)))

        if not isinstance(path, (self.Path, type(None))):
            path = self.Path(*path)

        self.path = path  # positional and named capture group in a routing pattern
        self.query = query  # key value pairs after a question mark at the end of an url
        self.form = form  # type multipart/form-data and application/x-www-form-urlencoded
        self.json = json_ if isinstance(json_, dict) else {}  # type application/json

    def lookup(self, locator, order=None, src=False):

        if isinstance(locator, str):
            locator = Locator(dict(keyword=locator))
        elif isinstance(locator, dict):
            locator = Locator(locator)
        elif not isinstance(locator, Locator):
            raise TypeError("Unknown Locator.")

        if order is None:
            order = ('path', 'query', 'form', 'json')
        elif isinstance(order, str):
            order = (order, )
        elif not isinstance(order, abc.Iterable):
            raise TypeError("Unknown Order.")

        for loc in order:
            try:
                args = getattr(self, loc)
                val = locator.lookin(args)
            except AttributeError:
                _ = "No such location: %s."
                logging.warning(_, loc)
            else:
                if val is not None:
                    return (val, loc) if src else val

        return (None, None) if src else None

    def __str__(self):
        lines = []
        for src in ("path", "query", "form", "json"):
            if getattr(self, src):
                lines.append("{}={}".format(
                    src, str(getattr(self, src))
                ))
        return "ReqArgs(" + ",\n".join(lines) + (")")


class Locator():
    """
        Describes the location of an argument in ReqArgs.
        {
            "keyword": <str>,
            "path": <int or str>,
            "alias": <str or [<str>, ...]>
        }
    """

    def __init__(self, defdict):

        self.keyword = defdict.get('keyword')
        self.path = defdict.get('path')
        aliases = defdict.get('alias', [])

        assert isinstance(self.path, (str, int, type(None)))
        assert isinstance(self.keyword, (str, type(None)))

        if isinstance(aliases, (list, tuple)):
            self.aliases = aliases
        elif isinstance(aliases, str):
            self.aliases = [aliases]
        else:  # validation failed
            raise ValueError("Unknown Alias.")

    @singledispatchmethod
    def lookin(self, location):
        """
        Find an argument in the specified location.
        Use directions indicated in this locator.
        """

    @lookin.register(ReqArgs.Path)
    def _(self, path):
        if self.path is not None:
            return path[self.path]
        return None

    @lookin.register(dict)  # all others
    def _(self, dic):
        if self.keyword in dic:
            return dic[self.keyword]
        for alias in self.aliases:
            if alias in dic:
                return dic[alias]
        return None

class Existentialist():
    """
        Describes the requirement of
        the existance of an argument.
        {
            "default": <object>,
            "required": <bool>,
        }
    """

    def __init__(self, defdict):

        self._defdict = MappingProxyType(defdict)
        self.keyword = defdict.get('keyword')
        self.required = bool(defdict.get('required'))
        self.default = defdict.get('default')

        if self.default and self.required:
            logging.warning(
                ("A default value is set for parameter '%s' "
                 "while 'required' is set to True, making it"
                 "ineffective."), self.keyword)

    def inquire(self, obj):

        if obj is None:
            if self.required:
                raise OptionError(
                    missing=self.keyword,
                    keyword=None,  # empty this field
                    alias=self._defdict.get('alias'))

            obj = self.default

        return obj


class Validator():
    """
        Describes the requirement of
        the existance of an argument.
        {
            "enum": <container>,
            "max": <int>,
            "min": <int>,
            "date_format": <str>,
        }
    """

    def __init__(self, defdict):

        self._defdict = MappingProxyType(defdict)
        self.keyword = defdict.get('keyword')
        self.strict = defdict.get('strict', True)
        self.enum = defdict.get('enum', ())
        self.max = defdict.get('max')
        self.min = defdict.get('min')
        self.date_format = defdict.get('date_format')

        assert isinstance(self.enum, abc.Container)
        assert isinstance(self.max, (int, type(None)))
        assert isinstance(self.min, (int, type(None)))
        assert isinstance(self.date_format, (str, type(None)))

    def validate(self, obj):

        if self.enum and not self._in_enum(obj):
            raise OptionError(
                keyword=self.keyword,
                allowed=self.enum,
                alias=self._defdict.get('alias'))

        if self.max:
            if isinstance(obj, (list, tuple, set)):
                self._check_list_max(obj)
            elif isinstance(obj, (int, float, complex)):
                self._check_num_max(obj)

        if self.min:
            if isinstance(obj, (list, tuple, set)):
                self._check_list_min(obj)
            elif isinstance(obj, (int, float, complex)):
                self._check_num_min(obj)

        if self.date_format and isinstance(obj, str):
            self._check_date_format(obj)
        return obj

    def _in_enum(self, value):

        if isinstance(value, (list, tuple, set)):
            for val in value:
                if not self._in_enum(val):
                    return False
            return True

        return value in self.enum

    def _check_list_max(self, container):

        if len(container) > self.max:
            raise OptionError(
                keyword=self.keyword,
                max=self.max,
                size=len(container)
            )

    def _check_num_max(self, num):

        if isinstance(num, bool):
            return

        if num > self.max:
            raise OptionError(
                keyword=self.keyword,
                max=self.max,
                num=num)

    def _check_list_min(self, container):
        if len(container) > self.min:
            raise OptionError(
                keyword=self.keyword,
                min=self.min,
                size=len(container)
            )

    def _check_num_min(self, num):
        if isinstance(num, bool):
            return

        if num < self.min:
            raise OptionError(
                keyword=self.keyword,
                min=self.min,
                num=num)

    def _check_date_format(self, value):
        try:
            dt.strptime(value, self.date_format)
        except Exception:
            raise OptionError(keyword=self.keyword, date_format=self.date_format, num=value)

# For Future Work
# ------------------

# Consider supporting the OpenAPI-compatible JSON Schema
# definitions as a subset of keys to describe the Option.
# By doing this, it is both easier to validate the input
# and to generate an OpenAPI Specification. Validation
# can be performed by existing packages like "jsonschema",
# and components of the OpenAPI Specifications can be
# generated by directly taking the relevant keys from the
# Option definition. However, keep in mind that validation
# and converstion will still not be straightforward when
# dealing with query parameters and form-encoded requests.

# To illustrate the benefit of using a subset of JSON Schema
# as our option validation language, take the example of
# validating complex objects. Currently, nested objects or
# containers are only validated at the top level, with the
# help of JSON schema, we could easily find the syntax needed
# to define the object structure and perform validation.
# Comparing to designing the schema language ourselves,
# and writing validation code for that purpose, using an
# existing framework should save time and improve accuracy,
# not to mention the additional benefit of spec generation.

# For more about OpenAPI-compatible JSON Schema:
# https://swagger.io/specification/#schema-object

class Option(UserDict):
    """
        A parameter for end applications to consume.
        Find the value of it in the desired *location*.

        For example:
        {
            "keyword": "q",
            "location": ("query", "form", "json"),
            "default": "__all__",
            "type": "str"
        }
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._locater = Locator(self)
        self._exists = Existentialist(self)
        self._typify = dict(Converter.subclasses(self))
        self._validator = Validator(self)

        # default argument parsing location order
        self.order = self.get("location", None)
        if self.order == "body":  # shortcut
            self.order = ("form", "json")

    def parse(self, reqargs):

        if not isinstance(reqargs, ReqArgs):
            reqargs = ReqArgs(*reqargs)

        # ------- debug here -------
        # if self.get("keyword") == "q":
        #     print()  # breakpoint
        # ---------------------------

        # find the user input
        val, loc = reqargs.lookup(
            locator=self._locater,
            order=self.order,
            src=True)

        if val is None:
            val = self._exists.inquire(val)
        else:  # type conversion and transform
            val = self._typify[loc].convert(val)
            val = self._typify[loc].translate(val)

        # additional conditions
        val = self._validator.validate(val)

        return val

class OptionSet(UserDict):
    """
        A collection of options that a specific endpoint consumes.
        Divided into *groups* and by the *request methods*.

        For example:
        {
            "*":{"raw":{...},"size":{...},"dotfield":{...}},
            "GET":{"q":{...},"from":{...},"sort":{...}},
            "POST":{"q":{...},"scopes":{...}}
        }
    """

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.groups = set()  # explicit result access groups
        self.setup()  # populate self.optset variable

    def __missing__(self, key):
        self.data[key] = {}
        return self[key]

    def setup(self):
        """
        Apply the wildcard method configurations dict.
        Must call this method after changes to this object.
        """
        # store Option objects used for parse method
        self.optset = defaultdict(dict)

        for method, options in self.data.items():
            for keyword, defdict in options.items():
                option = dict(defdict)
                option["keyword"] = keyword
                self.optset[method][keyword] = Option(option)
                if "group" in option:
                    if isinstance(option["group"], (list, tuple, set)):
                        self.groups.update(option["group"])
                    elif isinstance(option["group"], str):
                        self.groups.add(option["group"])

        wildcards = self.optset.get("*", {})
        for method, options in self.optset.items():
            for keyword, option in wildcards.items():
                options.setdefault(keyword, option)

    def parse(self, method, reqargs):
        """
        Parse a HTTP request, represented by its method and args,
        with this OptionSet and return an attribute dictionary.
        """

        options = self.optset.get(method, self.optset["*"])
        result = defaultdict(dict)  # to accomodate groups

        for keyword, option in options.items():
            try:
                val = option.parse(reqargs)
            except OptionError as err:
                err.info.setdefault("keyword", keyword)
                err.info["alias"] = option.get("alias")
                err.simplify()  # remove empty fields
                raise err  # with helpful info

            if val is not None:
                if 'group' in option:
                    group = option['group']
                    if isinstance(group, str):
                        result[group][keyword] = val
                    else:  # assume iterable
                        for _group in group:
                            result[_group][keyword] = val
                else:  # top level keywords
                    result[keyword] = val
            elif 'default' in option:  # explicit None
                result[keyword] = None

        # make sure all named groups exist
        for group in self.groups:
            if group not in result:
                result[group] = {}

        return ReqResult(result)

class OptionsManager(UserDict):
    """
    A collection of OptionSet(s) that makes up an application.
    Provide an interface to setup and serialize.

    Example:
    {
        "annotation": {"*": {...}, "GET": {...}, "POST": {... }},
        "query": {"*": {...},  "GET": {...}, "POST": {... }},
        "metadata": {"GET": {...}, "POST": {... }}
    }
    """

    def add(self, name, optionset, groups=()):
        if not name:
            logging.warning("Ignore unnamed optionset:\n%s", optionset)
        if name not in self.data:
            self.data[name] = OptionSet(optionset)
            self.data[name].groups.update(groups)
        else:  # update existing optionset
            for method, options in optionset.items():
                # merge second level objects
                self.data[name][method].update(options)
                self.data[name].groups.update(groups)
                self.data[name].setup()  # required

    def log(self):
        # serializable API-ready format
        return self._serialize(self)

    def _serialize(self, obj):
        if isinstance(obj, abc.Mapping):
            _obj = {}
            items = list(obj.items())
            for key, val in sorted(items):
                _obj[key] = self._serialize(val)
            return _obj
        if isinstance(obj, (list, tuple)):
            return [self._serialize(item) for item in obj]
        if isinstance(obj, (str, int)):
            return obj
        if hasattr(obj, "__name__"):
            return obj.__name__
        return str(obj)  # best effort
