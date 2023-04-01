"""
Utility functions for parsing flatfiles,
mapping to JSON, cleaning.
"""
# see tabfile_feeder(coerce_unicode) if needed
# from __future__ import unicode_literals
import itertools
import csv
import os
import os.path
import json
from collections.abc import Mapping
from collections import OrderedDict, Counter
from functools import total_ordering

from .common import open_anyfile, is_str, safewfile, anyfile
from .dotstring import key_value, set_key_value

csv.field_size_limit(10000000)   # default is 131072, too small for some big files


def dict_sweep(d, vals=None, remove_invalid_list=False):
    """
    Remove keys whose values are ".", "-", "", "NA", "none", " "; and remove empty dictionaries

    Args:
        d (dict): a dictionary
        vals (str or list): a string or list of strings to sweep, or None to use the default values
        remove_invalid_list (boolean): when true, will remove key for which
            list has only one value, which is part of "vals".
            Ex::

                test_dict = {'gene': [None, None], 'site': ["Intron", None], 'snp_build' : 136}

            with `remove_invalid_list == False`::

                {'gene': [None], 'site': ['Intron'], 'snp_build': 136}

            with `remove_invalid_list == True`::

                {'site': ['Intron'], 'snp_build': 136}
    """
    # set default supported vals for empty values
    vals = vals or {".", "-", "", "NA", "none", " ", "Not Available", "unknown"}
    for key, val in list(d.items()):
        if val in vals:
            del d[key]
        elif isinstance(val, list):
            if remove_invalid_list:
                val = [v for v in val if v not in vals]
                for item in val:
                    if isinstance(item, dict):
                        dict_sweep(item, vals, remove_invalid_list=remove_invalid_list)
                # if len(val) == 0:
                if not val:
                    del d[key]
                else:
                    d[key] = val
            else:
                for item in val:
                    if item in vals:
                        val.remove(item)
                    elif isinstance(item, dict):
                        dict_sweep(item, vals, remove_invalid_list=remove_invalid_list)
                # if len(val) == 0:
                if not val:
                    del d[key]
        elif isinstance(val, dict):
            dict_sweep(val, vals, remove_invalid_list=remove_invalid_list)
            # if len(val) == 0:
            if not val:
                del d[key]
    return d


def safe_type(f, val):
    """
    Convert an input string to int/float/... using passed function.
    If the conversion fails then None is returned.
    If value of a type other than a string
    then the original value is returned.
    """
    if is_str(val):
        try:
            return f(val)
        except ValueError:
            pass
    return val


def to_float(val):
    """convert an input string to int"""
    return safe_type(float, val)


def to_int(val):
    """convert an input string to float"""
    return safe_type(int, val)


def to_number(val):
    """convert an input string to int/float."""
    if is_str(val):
        try:
            return int(val)
        except ValueError:
            try:
                return float(val)
            except ValueError:
                pass
    return val


def boolean_convert(d, convert_keys=None, level=0):
    """
    Convert values specified by `convert_keys` in document `d` to boolean. Dotfield notation can be used to specify inner keys.

    Note that `None` values are converted to `False` in Python. Use `dict_sweep()` before calling this function if such `False` values are not expected.
    See https://github.com/biothings/biothings.api/issues/274 for details.
    """
    convert_keys = convert_keys or []
    for key, val in d.items():
        if isinstance(val, dict):
            d[key] = boolean_convert(val, convert_keys)
        if key in [ak.split(".")[level] for ak in convert_keys if len(ak.split(".")) > level]:
            if isinstance(val, list) or isinstance(val, tuple):
                if val and isinstance(val[0], dict):
                    d[key] = [boolean_convert(v, convert_keys, level+1) for v in val]
                else:
                    d[key] = [to_boolean(x) for x in val]
            elif isinstance(val, dict) or isinstance(val, OrderedDict):
                d[key] = boolean_convert(val, convert_keys, level+1)
            else:
                d[key] = to_boolean(val)
    return d


def float_convert(d, include_keys=None, exclude_keys=None):
    """Convert elements in a document to floats.

    By default, traverse all keys
    If include_keys is specified, only convert the list from include_keys a.b, a.b.c
    If exclude_keys is specified, only exclude the list from exclude_keys

    :param d: a dictionary to traverse keys on
    :param include_keys: only convert these keys (optional)
    :param exclude_keys: exclude all other keys except these keys (optional)
    :return: generate key, value pairs
    """
    return value_convert_incexcl(d, to_float, include_keys, exclude_keys)


def int_convert(d, include_keys=None, exclude_keys=None):
    """Convert elements in a document to integers.

    By default, traverse all keys
    If include_keys is specified, only convert the list from include_keys a.b, a.b.c
    If exclude_keys is specified, only exclude the list from exclude_keys

    :param d: a dictionary to traverse keys on
    :param include_keys: only convert these keys (optional)
    :param exclude_keys: exclude all other keys except these keys (optional)
    :return: generate key, value pairs
    """
    return value_convert_incexcl(d, to_int, include_keys, exclude_keys)


def to_boolean(val, true_str=None, false_str=None):
    """Normalize str value to boolean value"""
    # set default true_str and false_str
    true_str = true_str or {'true', '1', 't', 'y', 'yes', 'Y', 'Yes', 'YES', 1}
    false_str = false_str or {'false', '0', 'f', 'n', 'N', 'No', 'no', 'NO', 0}
    # if type(val)!=str:
    if not isinstance(val, str):
        return bool(val)
    else:
        if val in true_str:
            return True
        elif val in false_str:
            return False


def merge_duplicate_rows(rows, db):
    """
    @param rows: rows to be grouped by
    @param db: database name, string
    """
    rows = list(rows)

    keys = set()
    for row in rows:
        for k in row[db]:
            keys.add(k)

    first_row = rows[0]
    other_rows = rows[1:]
    for row in other_rows:
        for i in keys:
            try:
                aa = first_row[db][i]
            except KeyError:
                try:
                    first_row[db][i] = row[db][i]
                except KeyError:
                    pass
                continue
            if i in row[db]:
                if row[db][i] != first_row[db][i]:
                    if not isinstance(aa, list):
                        aa = [aa]
                    aa.append(row[db][i])
                    first_row[db][i] = aa
            else:
                continue
    return first_row


def unique_ids(src_module):
    i = src_module.load_data()
    out = list(i)
    id_list = [a['_id'] for a in out if a]
    myset = set(id_list)
    print(len(out), "Documents produced")
    print(len(myset), "Unique IDs")
    return out


def rec_handler(infile, block_end='\n', skip=0, include_block_end=False, as_list=False):
    """
    A generator to return a record (block of text) at once from the `infile`.
    The record is separated by one or more empty lines by default.
    `skip` can be used to skip top n-th lines if `include_block_end` is True, the line matching block_end will also be returned.
    If `as_list` is True, return a list of lines in one record.
    """
    with open_anyfile(infile) as in_f:
        if skip:
            for i in range(skip):
                in_f.readline()
                del i
        for key, group in itertools.groupby(in_f, lambda line: line == block_end):
            if not key:
                if include_block_end:
                    _g = itertools.chain(group, (block_end,))
                yield list(_g) if as_list else ''.join(_g)


# ===============================================================================
# List Utility functions
# ===============================================================================

# if dict value is a list of length 1, unlist
def unlist(d):
    for key, val in d.items():
        if isinstance(val, list):
            if len(val) == 1:
                d[key] = val[0]
        elif isinstance(val, dict):
            unlist(val)
    return d


def unlist_incexcl(d, include_keys=None, exclude_keys=None):
    """Unlist elements in a document.

    If there is 1 value in the list, set the element to that value.  Otherwise,
    leave the list unchanged.

    By default, traverse all keys
    If include_keys is specified, only traverse the list from include_keys a.b, a.b.c
    If exclude_keys is specified, only exclude the list from exclude_keys

    :param d: a dictionary to unlist
    :param include_keys: only unlist these keys (optional)
    :param exclude_keys: exclude all other keys except these keys (optional)
    :return: generate key, value pairs
    """
    def unlist_helper(d, include_keys=None, exclude_keys=None, keys=None):
        include_keys = include_keys or []
        exclude_keys = exclude_keys or []
        keys = keys or []
        if isinstance(d, dict):
            for key, val in d.items():
                if isinstance(val, list):
                    if len(val) == 1:
                        path = '.'.join(keys + [key])
                        if include_keys:
                            if path in include_keys:
                                d[key] = val[0]
                        elif path not in exclude_keys:
                            d[key] = val[0]
                elif isinstance(val, dict):
                    unlist_helper(val, include_keys, exclude_keys, keys + [key])
    unlist_helper(d, include_keys, exclude_keys, [])
    return d


def list_split(d, sep):
    """Split fields by sep into comma separated lists, strip."""
    for key, val in d.items():
        if isinstance(val, dict):
            list_split(val, sep)
        try:
            if len(val.split(sep)) > 1:
                d[key] = val.rstrip().rstrip(sep).split(sep)
        except AttributeError:
            pass
    return d


def id_strip(id_list):
    id_list = id_list.split("|")
    ids = []
    for _id in id_list:
        ids.append(_id.rstrip().lstrip())
    return ids


def llist(li, sep='\t'):
    """Nicely output the list with each item a line."""
    for x in li:
        if isinstance(x, (li, tuple)):
            xx = sep.join([str(i) for i in x])
        else:
            xx = str(x)
        print(xx)


def listitems(a_list, *idx):
    """Return multiple items from list by given indexes."""
    if isinstance(a_list, tuple):
        return tuple(a_list[i] for i in idx)
    else:
        return [a_list[i] for i in idx]


def list2dict(a_list, keyitem, alwayslist=False):     # pylint: disable=redefined-outer-name
    """
    Return a dictionary with specified `keyitem` as key, others as values.
    `keyitem` can be an index or a sequence of indexes.
    For example::

        li=[['A','a',1],
            ['B','a',2],
            ['A','b',3]]
        list2dict(li,0)---> {'A':[('a',1),('b',3)],
                             'B':('a',2)}

    If `alwayslist` is True, values are always a list even there is only one item in it::

        list2dict(li,0,True)---> {'A':[('a',1),('b',3)],
                                  'B':[('a',2),]}
    """
    _dict = {}
    for x in a_list:
        if isinstance(keyitem, int):  # single item as key
            key = x[keyitem]
            value = tuple(x[:keyitem] + x[keyitem + 1:])
        else:
            key = tuple(x[i] for i in keyitem)
            value = tuple(x[i] for i in range(len(a_list)) if i not in keyitem)
        if len(value) == 1:  # single value
            value = value[0]
        if key not in _dict:
            if alwayslist:
                _dict[key] = [value, ]
            else:
                _dict[key] = value
        else:
            current_value = _dict[key]
            if not isinstance(current_value, list):
                current_value = [current_value, ]
            current_value.append(value)
            _dict[key] = current_value
    return _dict


def listsort(a_list, by, reverse=False, cmp=None, key=None):
    """
    Given `a_list` is a list of sub(list/tuple.), return a new list sorted by the ith (given from "by" item) item of each sublist.
    """
    new_li = [(x[by], x) for x in a_list]
    new_li.sort(cmp=cmp, key=key, reverse=reverse)
    return [x[1] for x in new_li]


def list_itemcnt(a_list):
    """Return number of occurrence for each item in the list."""
    return list(Counter(a_list).items())


def alwayslist(value):
    """If input value is not a list/tuple type, return it as a single value list."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return value
    else:
        return [value]

# ===============================================================================
# File Utility functions
# ===============================================================================


def tabfile_tester(datafile, header=1, sep='\t'):
    reader = csv.reader(anyfile(datafile), delimiter=sep)
    lineno = 0
    try:
        for _ in range(header):
            next(reader)
            lineno += 1

        for _ in reader:
            lineno += 1

        del _
    except Exception:
        print("Error at line number:", lineno)
        raise


def dupline_seperator(dupline, dup_sep, dup_idx=None, strip=False):
    """
    for a line like this::

        a   b1,b2  c1,c2

    return a generator of this list (breaking out of the duplicates in each field)::

        [(a,b1,c1),
         (a,b2,c1),
         (a,b1,c2),
         (a,b2,c2)]

    Example::

         dupline_seperator(dupline=['a', 'b1,b2', 'c1,c2'],
                           dup_idx=[1,2],
                           dup_sep=',')

    if dup_idx is None, try to split on every field.
    if strip is True, also tripe out of extra spaces.
    """
    value_li = list(dupline)
    for idx, value in enumerate(value_li):
        if dup_idx:
            if idx in dup_idx:
                value = value.split(dup_sep)
                if strip:
                    value = [x.strip() for x in value]
            else:
                value = [value]
        else:
            value = value.split(dup_sep)
            if strip:
                value = [x.strip() for x in value]
        value_li[idx] = value
    return itertools.product(*value_li)    # itertools.product fits exactly the purpose here


def tabfile_feeder(datafile, header=1, sep='\t', includefn=None, coerce_unicode=True, assert_column_no=None):
    """a generator for each row in the file."""

    in_f = anyfile(datafile)
    reader = csv.reader(in_f, delimiter=sep)
    lineno = 0
    try:
        for _ in range(header):
            next(reader)
            lineno += 1

        for ld in reader:
            if assert_column_no:
                if len(ld) != assert_column_no:
                    err = "Unexpected column number: got {}, should be {}".format(len(ld), assert_column_no)
                    raise ValueError(err)
            if not includefn or includefn(ld):
                lineno += 1
                if coerce_unicode:
                    yield [str(x) for x in ld]
                else:
                    yield ld
    except ValueError:
        print("Error at line number:", lineno)
        raise


def tab2list(datafile, cols, **kwargs):
    if os.path.exists(datafile):
        if isinstance(cols, int):
            return [ld[cols] for ld in tabfile_feeder(datafile, **kwargs)]
        else:
            return [listitems(ld, *cols) for ld in tabfile_feeder(datafile, **kwargs)]
    else:
        print('Error: missing "%s". Skipped!' % os.path.split(datafile)[1])
        return {}


def tab2dict(datafile, cols, key, alwayslist=False, **kwargs):     # pylint: disable=redefined-outer-name
    if isinstance(datafile, tuple):
        _datafile = datafile[0]
    else:
        _datafile = datafile
    if os.path.exists(_datafile):
        return list2dict([listitems(ld, *cols) for ld in tabfile_feeder(datafile, **kwargs)], key, alwayslist=alwayslist)
    else:
        print('Error: missing "%s". Skipped!' % os.path.split(_datafile)[1])
        return {}


def tab2dict_iter(datafile, cols, key, alwayslist=False, **kwargs):     # pylint: disable=redefined-outer-name
    """
    Args:
        cols (array of int): an array of indices (of a list) indicating which element(s) are kept in bulk
        key (int): an index (of a list) indicating which element is treated as a bulk key

    Iterate `datafile` by row, subset each row (as a list of strings) by `cols`. Adjacent rows sharing the same value at the `key` index are put into one bulk.
    Each bulk is then transformed to a dict with the value at the `key` index as the dict key.

    E.g. given the following datafile, cols=[0,1,2], and key=1, two bulks are generated:

        key
    a1	b1	c1  --------------------------------------------------
    a2	b1	c2  # bulk_1 => {b1: [(a1, c1), (a2, c2), (a3, c3)]} #
    a3	b1	c3  --------------------------------------------------
    a4	b2	c4  --------------------------------------------------
    a5	b2	c5  # bulk_2 => {b2: [(a4, c4), (a5, c5), (a6, c6)]} #
    a6	b2	c6  --------------------------------------------------
    """
    if isinstance(datafile, tuple):
        _datafile = datafile[0]
    else:
        _datafile = datafile

    if not os.path.exists(_datafile):
        print('Error: missing "%s". Skipped!' % os.path.split(_datafile)[1])
        return {}

    bulk = []
    current_key = None
    for ld in tabfile_feeder(datafile, **kwargs):
        li = listitems(ld, *cols)
        if current_key is None or (li[key] == current_key):
            # same key, put into bulk
            bulk.append(li)
            current_key = li[key]
        else:
            # key changed
            # first step: yield the current bulk
            di = list2dict(bulk, key, alwayslist=alwayslist)
            yield di

            # key changed
            # second step: start a new bulk
            bulk = [li]
            current_key = li[key]

    # flush remaining bulk
    if bulk:
        di = list2dict(bulk, key, alwayslist=alwayslist)
        yield di


def file_merge(infiles, outfile=None, header=1, verbose=1):
    """
    Merge a list of input files with the same format.
    If `header` is n then the top n lines will be discarded since reading the 2nd file in the list.
    """
    outfile = outfile or '_merged'.join(os.path.splitext(infiles[0]))
    out_f, outfile = safewfile(outfile)
    if verbose:
        print("Merging...")
    cnt = 0
    for i, fn in enumerate(infiles):
        print(os.path.split(fn)[1], '...', end='')
        line_no = 0
        in_f = anyfile(fn)
        if i > 0:
            for k in range(header):
                in_f.readline()
                del k
        for line in in_f:
            out_f.write(line)
            line_no += 1
        in_f.close()
        cnt += line_no
        print(line_no)
    out_f.close()
    print("=" * 20)
    print("Done![total %d lines output]" % cnt)

# ===============================================================================
# Dictionary & other structures Utility functions
# ===============================================================================


# http://stackoverflow.com/questions/12971631/sorting-list-by-an-attribute-that-can-be-none
# used to sort list with None element (because python3 suddenly decided it wasn't possible
# anymore. because...)
# from functools import total_ordering
@total_ordering
class MinType(object):
    def __le__(self, other):
        return True

    def __eq__(self, other):
        return self is other


Min = MinType()


def traverse_keys(d, include_keys=None, exclude_keys=None):
    """Return all key, value pairs for a document.

    By default, traverse all keys
    If include_keys is specified, only traverse the list from include_kes a.b, a.b.c
    If exclude_keys is specified, only exclude the list from exclude_keys

    if a key in include_keys/exclude_keys is not found in d, it's skipped quietly.

    :param d: a dictionary to traverse keys on
    :param include_keys: only traverse these keys (optional)
    :param exclude_keys: exclude all other keys except these keys (optional)
    :return: generate key, value pairs
    """
    include_keys = include_keys or []
    exclude_keys = exclude_keys or []

    def traverse_helper(d, keys):
        if isinstance(d, dict):
            for k in d.keys():
                yield from traverse_helper(d[k], keys + [k])
        elif isinstance(d, list):
            for i in d:
                yield from traverse_helper(i, keys)
        else:
            yield keys, d

    if include_keys:
        for k in include_keys:
            for val in key_value(d, k):
                if val:
                    # only yield non-empty value
                    # when val is None, it could be either:
                    #   1. k is not found in d
                    #   2. the value of k in d is indeed None
                    # For now, we cannot tell which case, just skip it
                    yield k, val
    else:
        for kl, val in traverse_helper(d, []):
            key = '.'.join(kl)
            if key not in exclude_keys:
                yield key, val


# from mygene, originally
def value_convert(_dict, fn, traverse_list=True):
    """
    For each value in _dict, apply fn and then update _dict with return the value.
    If `traverse_list` is True and a value is a list, apply `fn` to each item of the list.
    """
    for k in _dict:
        if traverse_list and isinstance(_dict[k], list):
            _dict[k] = [fn(x) for x in _dict[k]]
        else:
            _dict[k] = fn(_dict[k])
    return _dict


def value_convert_incexcl(d, fn, include_keys=None, exclude_keys=None):
    """Convert elements in a document using a function fn.

    By default, traverse all keys
    If include_keys is specified, only convert the list from include_keys a.b, a.b.c
    If exclude_keys is specified, only exclude the list from exclude_keys

    :param d: a dictionary to traverse keys on
    :param fn: function to convert elements with
    :param include_keys: only convert these keys (optional)
    :param exclude_keys: exclude all other keys except these keys (optional)
    :return: generate key, value pairs
    """
    for path, value in traverse_keys(d, include_keys, exclude_keys):
        new_value = fn(value)
        set_key_value(d, path, new_value)
    return d


# from biothings, originally
# closed to value_convert, could be refactored except this one
# is recursive for dict typed values
def value_convert_to_number(d, skipped_keys=None):
    """
    Convert string numbers into integers or floats; skip converting certain keys in skipped_keys list.
    """
    skipped_keys = skipped_keys or []
    for key, val in d.items():
        if isinstance(val, dict):
            value_convert_to_number(val, skipped_keys)
        if key not in skipped_keys:
            if isinstance(val, list):
                d[key] = [to_number(x) if not isinstance(x, dict) else value_convert_to_number(x, skipped_keys) for x in val]
            elif isinstance(val, tuple):
                d[key] = tuple(to_number(x) if not isinstance(x, dict) else value_convert_to_number(x, skipped_keys) for x in val)
            else:
                d[key] = to_number(val)
    return d


def dict_convert(_dict, keyfn=None, valuefn=None):
    """Return a new dict with each key converted by keyfn (if not None),
       and each value converted by valuefn (if not None).
    """
    if keyfn is None and valuefn is not None:
        for k in _dict:
            _dict[k] = valuefn(_dict[k])
        return _dict

    elif keyfn is not None:
        out_dict = {}
        for k in _dict:
            out_dict[keyfn(k)] = valuefn(_dict[k]) if valuefn else _dict[k]
        return out_dict
    else:
        return _dict


def updated_dict(_dict, attrs):
    """Same as `dict.update`, but return the updated dictionary."""
    out = _dict.copy()
    out.update(attrs)
    return out


def update_dict_recur(d, u):
    """
    Update dict `d` with dict `u`'s values, recursively (so existing values in `d` but not in `u` are kept even if nested)
    """
    for k, v in u.items():
        if isinstance(v, Mapping):
            r = update_dict_recur(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d


def merge_dict(dict_li, attr_li, missingvalue=None):
    """
    Merging multiple dictionaries into a new one.
    Example::

        In [136]: d1 = {'id1': 100, 'id2': 200}
        In [137]: d2 = {'id1': 'aaa', 'id2': 'bbb', 'id3': 'ccc'}
        In [138]: merge_dict([d1,d2], ['number', 'string'])
        Out[138]:
        {'id1': {'number': 100, 'string': 'aaa'},
        'id2': {'number': 200, 'string': 'bbb'},
        'id3': {'string': 'ccc'}}
        In [139]: merge_dict([d1,d2], ['number', 'string'], missingvalue='NA')
        Out[139]:
        {'id1': {'number': 100, 'string': 'aaa'},
        'id2': {'number': 200, 'string': 'bbb'},
        'id3': {'number': 'NA', 'string': 'ccc'}}
    """
    dd = dict(zip(attr_li, dict_li))
    key_set = set()
    for attr in dd:
        key_set = key_set | set(dd[attr])

    out_dict = {}
    for k in key_set:
        value = {}
        for attr in dd:
            if k in dd[attr]:
                value[attr] = dd[attr][k]
            elif missingvalue is not None:
                value[attr] = missingvalue
        out_dict[k] = value
    return out_dict


def normalized_value(value, sort=True):
    """Return a "normalized" value:
           1. if a list, remove duplicate and sort it
           2. if a list with one item, convert to that single item only
           3. if a list, remove empty values
           4. otherwise, return value as it is.
    """
    if isinstance(value, list):
        value = [x for x in value if x]   # remove empty values
        try:
            _v = list(set(value))
        except TypeError:
            _v = [json.loads(x) for x in {json.dumps(x) for x in value}]
        if _v and sort:
            # py3 won't sort dict anymore...
            if isinstance(_v[0], dict):
                _v = sorted(_v, key=lambda x: sorted(x.keys()))
            else:
                try:
                    _v = sorted(_v)
                except TypeError:
                    # probably some None values to sort, not handle anymore in py3
                    # let's use a trick...
                    _v = sorted(_v, key=lambda x: Min if x is None or (not isinstance(x, str) and None in x) else x)
        if len(_v) == 1:
            _v = _v[0]
    else:
        _v = value

    return _v


def dict_nodup(_dict, sort=True):
    for k in _dict:
        _dict[k] = normalized_value(_dict[k], sort=sort)
    return _dict


def dict_attrmerge(dict_li, removedup=True, sort=True, special_fns=None):
    """
    dict_attrmerge([{'a': 1, 'b':[2,3]},
                    {'a': [1,2], 'b':[3,5], 'c'=4}])
    should return
         {'a': [1,2], 'b':[2,3,5], 'c'=4}

    special_fns is a dictionary of {attr:  merge_fn} used for some special attr, which need special merge_fn
    e.g., {'uniprot': _merge_uniprot}
    """
    special_fns = special_fns or {}
    out_dict = {}
    keys = []
    for d in dict_li:
        keys.extend(d.keys())
    keys = set(keys)
    for k in keys:
        _value = []
        for d in dict_li:
            if d.get(k, None):
                if isinstance(d[k], list):
                    _value.extend(d[k])
                else:
                    _value.append(d[k])
        if len(_value) == 1:
            out_dict[k] = _value[0]
        else:
            out_dict[k] = _value

        if k in special_fns:
            out_dict[k] = special_fns[k](out_dict[k])

    if removedup:
        out_dict = dict_nodup(out_dict, sort=sort)
    return out_dict


def merge_root_keys(doc1, doc2, exclude=None):
    """
    Ex: d1 = {"_id":1,"a":"a","b":{"k":"b"}}
        d2 = {"_id":1,"a":"A","b":{"k":"B"},"c":123}

        Both documents have the same _id, and 2 root keys, "a" and "b".
        Using this storage, the resulting document will be:

        {'_id': 1, 'a': ['A', 'a'], 'b': [{'k': 'B'}, {'k': 'b'}],"c":123}
    """
    # we'll "eat" from doc2 so clean it first as needed
    exclude = exclude or []
    for k in exclude:
        doc2.pop(k, None)
    for k1 in doc1:
        if k1 in exclude:
            continue
        v2 = doc2.pop(k1, None)
        if not isinstance(v2, list):
            v2 = [v2]
        if v2:
            if isinstance(doc1[k1], list):
                doc1[k1].extend(v2)
            else:
                doc1[k1] = [doc1[k1]] + v2
    # merge what's remaining in doc2 that wasn't in doc1
    doc1.update(doc2)

    return doc1


def dict_apply(d, key, value, sort=True):
    """add value to d[key], append it if key exists

        >>> d = {'a': 1}
        >>> dict_apply(d, 'a', 2)
         {'a': [1, 2]}
        >>> dict_apply(d, 'a', 3)
         {'a': [1, 2, 3]}
        >>> dict_apply(d, 'b', 2)
         {'a': 1, 'b': 2}
    """
    if key in d:
        _value = d[key]
        if not isinstance(_value, list):
            _value = [_value]
        if isinstance(value, list):
            _value.extend(value)
        else:
            _value.append(value)
    else:
        _value = value

    d[key] = normalized_value(_value, sort=sort)


def dict_to_list(gene_d):
    """return a list of genedoc from genedoc dictionary and
       make sure the "_id" field exists.
    """
    doc_li = [updated_dict(gene_d[k], {'_id': str(k)}) for k in sorted(gene_d.keys())]
    return doc_li


def merge_struct(v1, v2, aslistofdict=None):
    if isinstance(v1, list):
        if isinstance(v2, list):
            v1 = v1 + [x for x in v2 if x not in v1]
        else:
            if v2 not in v1:
                v1.append(v2)

    elif isinstance(v2, list) and isinstance(v1, dict):
        if v1 not in v2:
            v2.append(v1)

    elif isinstance(v1, dict):
        assert isinstance(v2, dict), "v2 %s not a dict (v1: %s)" % (v2, v1)
        for k in list(v1.keys()):
            if k in v2:
                if aslistofdict == k:
                    v1elem = v1[k]
                    v2elem = v2[k]
                    if not isinstance(v1elem, list):
                        v1elem = [v1elem]
                    if not isinstance(v2elem, list):
                        v2elem = [v2elem]
                    # v1elem and v2elem may be the same, in this case as a result
                    # we may have transformed it in a list (no merge, but just type change).
                    # if so, back to scalar
                    if v1elem != v2elem:
                        v1[k] = merge_struct(v1elem, v2elem)
                else:
                    v1[k] = merge_struct(v1[k], v2[k])
            else:
                v2[k] = v1[k]
        for k in v2:
            if k in v1:
                pass  # already done
            else:
                v1[k] = v2[k]

    elif isinstance(v1, str) or isinstance(v1, int) or isinstance(v1, float):
        if isinstance(v2, str) or isinstance(v2, int) or isinstance(v2, float):
            if v1 != v2:
                v1 = [v1, v2]
            else:
                pass
        else:
            return merge_struct(v2, v1)
    else:
        raise TypeError("dunno how to merge type %s" % type(v1))

    return v1


def dict_walk(dictionary, key_func):
    """Recursively apply key_func to dict's keys"""
    if not isinstance(dictionary, dict):
        return dictionary
    return {key_func(k): dict_walk(v, key_func) for k, v in dictionary.items()}


def dict_traverse(d, func, traverse_list=False):
    """
    Recursively traverse dictionary d, calling func(k,v) for each key/value found. func must return a tuple(new_key,new_value)
    """
    try:
        items = sorted(d.items(), key=lambda x: x[0])
    except TypeError:
        # not sortable
        # need to make a copy first, because d will be updated during
        #   the iteration, a RuntimeError will be raised otherwise:
        #   RuntimeError: dictionary keys changed during iteration
        items = d.copy().items()
    for k, v in items:
        if isinstance(v, dict):
            dict_traverse(v, func, traverse_list=traverse_list)
        elif traverse_list and isinstance(v, list):
            for e in v:
                if isinstance(e, dict):
                    dict_traverse(e, func, traverse_list=traverse_list)
        else:
            newk, newv = func(k, v)
            d.pop(k)
            d[newk] = newv
