import json
import re

from biothings.utils.common import split_ids
from biothings.web.api.helper import BadRequest


class StringParamParser():
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
                raise BadRequest(reason=str(e), param=param)
        return val

    @staticmethod
    def obj_to_type(val, type_, param=''):
        try:
            result = (type_)(val)
        except ValueError:
            raise BadRequest(reason=f"Expect type {type_.__name__}.", param=param)
        return result

class OptionArgsParser():
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
        self.string_as_json = False

    def parse(self, args, path_args, path_kwargs):

        value = args.get(self.keyword)

        if 'alias' in self.setting and not value:
            value = self._alias(args)

        if 'path' in self.setting:
            value = self._path(path_args, path_kwargs)

        if value is None:
            return self._default()

        if 'type' in self.setting:
            value = self._typify(value)

        if 'translations' in self.setting:
            value = self._translate(value)

        if 'max' in self.setting or self.list_size_cap:
            value = self._max(value)

        return value

    def _path(self, path_args, path_kwargs):
        # find path args from index number
        # find path kwargs from handler regex captures
        path = self.setting['path']
        if isinstance(path, int):
            if len(path_args) <= path:
                raise BadRequest(missing=self.keyword)
            return path_args[path]
        elif isinstance(path, str):
            if path not in path_kwargs:
                raise BadRequest(missing=self.keyword)
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
            raise BadRequest(missing=self.keyword)
        return self.setting.get('default')

    def _typify(self, value):
        # convert to the desired value type and format
        if isinstance(value, self.setting['type']):
            return value
        param = StringParamParser(self.string_as_json)
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
                raise BadRequest(
                    keyword=self.keyword,
                    max=max_allowed,
                    lst=value)
        elif isinstance(value, (int, float, complex)) \
                and not isinstance(value, bool):
            if 'max' in self.setting:
                if value > self.setting['max']:
                    raise BadRequest(
                        keyword=self.keyword,
                        max=self.setting['max'],
                        num=value)
        return value
