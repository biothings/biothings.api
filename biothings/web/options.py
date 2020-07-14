import copy
import json
import re
from collections import defaultdict
from functools import partial

from biothings.utils.common import dotdict, split_ids


class OptionArgError(Exception):

    def __init__(self, **info):
        self.info = info

class StringTranslate():
    """
    Translate the input parameter to the desired type.
    If jsoninput is set to true, try to load str in json.
    """

    def __init__(self, jsoninput=False):

        if isinstance(jsoninput, str):
            self.jsoninput = self.str_to_bool(jsoninput)
        else:
            self.jsoninput = bool(jsoninput)

    def convert(self, value, to_type):
        if isinstance(value, to_type):
            return value
        if isinstance(value, str) and to_type == bool:  # TODO use dispatch syntax
            return self.str_to_bool(value)
        elif isinstance(value, str) and to_type == list:
            return self.str_to_list(value)
        elif isinstance(value, (int, float)):
            return self.convert(str(value), to_type)
        else:
            return self.obj_to_type(value, to_type)

    @staticmethod
    def str_to_bool(val):
        return val.lower() in ('1', 'true', 'y', 't', '')

    def str_to_list(self, val, param=''):
        if self.jsoninput:
            try:
                val = json.loads(val)
            except Exception:
                pass
        if not isinstance(val, list):
            try:
                val = split_ids(str(val))
            except ValueError as e:
                raise OptionArgError(reason=str(e), param=param)
        return val

    @staticmethod
    def obj_to_type(val, type_, param=''):
        try:
            result = (type_)(val)
        except ValueError:
            raise OptionArgError(reason=f"Expect type {type_.__name__}.", param=param)
        return result

class OptionArg():
    """
    Interpret a setting file for a keyword like
    {
        'type': list,
        'default': None,
        'max': 1000,
        'alias': [
            'fields', 'field', 'filter'
        ]
        'translations': [
           (re.compile(r'rs[0-9]+', re.I), 'dbsnp.rsid')
        ]
    }
    And apply the setting for a keyword in input args.
    """

    def __init__(self, keyword, setting):

        self.keyword = keyword
        self.setting = setting

        self.list_size_cap = 1000

    def parse(self, args, path_args, path_kwargs):

        value = args.get(self.keyword)
        jsoninput = args.get('jsoninput')  # deprecated

        if 'alias' in self.setting and not value:
            value = self._alias(args)

        if 'path' in self.setting:
            value = self._path(path_args, path_kwargs)

        if value is None:
            return self._default()

        if 'type' in self.setting:
            value = self._typify(value, jsoninput)

        if 'required' in self.setting:
            value = self._required(value)

        if 'translations' in self.setting:
            value = self._translate(value)

        if 'enum' in self.setting:
            value = self._enum(value)

        if 'max' in self.setting or self.list_size_cap:
            value = self._max(value)

        return value

    def _path(self, path_args, path_kwargs):
        # find path args from index number
        # find path kwargs from handler regex captures
        path = self.setting['path']
        if isinstance(path, int):
            if len(path_args) <= path:
                raise OptionArgError(missing=self.keyword)
            return path_args[path]
        elif isinstance(path, str):
            if path not in path_kwargs:
                raise OptionArgError(missing=self.keyword)
            return path_kwargs[path]

    def _alias(self, args):
        aliases = self.setting['alias']
        if not isinstance(aliases, list):
            aliases = [aliases]
        for _alias in aliases:
            if _alias in args:
                return args[_alias]

    def _default(self):
        # fallback to default values or raise error
        if self.setting.get('required'):
            raise OptionArgError(missing=self.keyword)
        return self.setting.get('default')

    def _required(self, value):
        if self.setting.get('required'):
            if isinstance(value, (str, list)):
                if not value:  # should evaluate to true
                    raise OptionArgError(missing=self.keyword)
        return value

    def _typify(self, value, jsoninput):
        # convert to the desired value type and format
        if isinstance(value, self.setting['type']):
            return value
        param = StringTranslate(jsoninput)
        return param.convert(value, self.setting['type'])

    def _translate(self, obj):
        translations = self.setting['translations']
        if isinstance(obj, str):
            for (regex, translation) in translations:
                obj = re.sub(regex, translation, obj)
            return obj
        if isinstance(obj, list):
            return [self._translate(item)
                    for item in obj]
        raise TypeError()

    def _max(self, value):
        # list size and int value validation
        if isinstance(value, list):
            max_allowed = self.setting.get('max')
            max_allowed = max_allowed or self.list_size_cap
            if len(value) > max_allowed:
                raise OptionArgError(
                    keyword=self.keyword,
                    max=max_allowed,
                    lst=value)
        elif isinstance(value, (int, float, complex)) \
                and not isinstance(value, bool):
            if 'max' in self.setting:
                if value > self.setting['max']:
                    raise OptionArgError(
                        keyword=self.keyword,
                        max=self.setting['max'],
                        num=value)
        return value

    def _enum(self, value):

        # allow only specified values
        allowed = self.setting['enum']

        if isinstance(value, list):
            for val in value:
                self._enum(val)
        elif value not in allowed:
            raise OptionArgError(
                keyword=self.keyword,
                allowed=allowed
            )
        return value

class Options():

    def __init__(self, options, groups, methods):

        self._options = options  # example: {'dev':{'type':str }}
        self._groups = groups or ()  # example: ('es', 'transform')
        self._methods = list(method.upper() for method in methods or ())  # 'GET'

    def parse(self, method, args, path_args, path_kwargs):

        result = defaultdict(dict)
        options = {}

        rules = []  # expand * to kwarg_methods setting
        if not self._methods or method in self._methods:
            rules += list(self._options['*'].items())
        rules += list(self._options[method].items())

        # method precedence: specific > *
        for keyword, setting in rules:
            options[keyword] = setting

        # setting + inputs -> arg value
        for keyword, setting in options.items():
            arg = OptionArg(keyword, setting)
            val = arg.parse(args, path_args, path_kwargs)
            # discard no default value
            if val is not None:
                if 'group' in setting:
                    group = setting['group']
                    if isinstance(group, str):
                        result[group][keyword] = val
                    else:  # assume iterable
                        for _group in group:
                            result[_group][keyword] = val
                else:  # top level keywords
                    result[keyword] = val

        # make sure all named groups exist
        for group in self._groups:
            if group not in result:
                result[group] = {}

        return dotdict(result)

class OptionSets():

    def __init__(self):

        self.options = defaultdict(partial(defaultdict, dict))
        self.groups = defaultdict()
        self.methods = defaultdict()

    def add(self, name, optionset):
        if name:
            for method, options in optionset.items():
                self.options[name][method.upper()].update(options)

    def get(self, name):

        return Options(
            self.options[name],
            self.groups[name],
            self.methods[name]
        )

    def log(self):

        res = copy.deepcopy(self.options)
        res = self._serialize(res)
        for api, groups in self.groups.items():
            res[api]['_groups'] = groups
        for api, methods in self.methods.items():
            res[api]['_methods'] = methods
        return res

    def _serialize(self, obj):
        if isinstance(obj, dict):
            for key, val in obj.items():
                obj[key] = self._serialize(val)
            return obj
        elif isinstance(obj, (list, tuple)):
            return [self._serialize(item) for item in obj]
        elif isinstance(obj, (str, int)):
            return obj
        else:  # best effort
            return str(obj)
