"""
Utility functions for parsing flatfiles,
mapping to JSON, cleaning.
"""
# see tabfile_feeder(coerce_unicode) if needed
#from __future__ import unicode_literals
import itertools
import csv
import os, os.path
import json
import collections

from .common import open_anyfile, is_str, ask, safewfile, anyfile

csv.field_size_limit(10000000)   # default is 131072, too small for some big files


# remove keys whos values are ".", "-", "", "NA", "none", " "
# and remove empty dictionaries
def dict_sweep(d, vals=[".", "-", "", "NA", "none", " ", "Not Available", "unknown"]):
    """
    @param d: a dictionary
    @param vals: a string or list of strings to sweep
    """
    for key, val in list(d.items()):
        if val in vals:
            del d[key]
        elif isinstance(val, list):
            val = [v for v in val if v not in vals]
            for item in val:
                if isinstance(item, dict):
                    dict_sweep(item, vals)
            if len(val) == 0:
                del d[key]
            else:
                d[key] = val
        elif isinstance(val, dict):
            dict_sweep(val, vals)
            if len(val) == 0:
                del d[key]
    return d


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


def boolean_convert(d, convert_keys=[], level=0):
    """Explore document d and specified convert keys to boolean.
    Use dotfield notation for inner keys"""
    for key, val in d.items():
        if isinstance(val, dict):
            d[key] = boolean_convert(val, convert_keys)
        if key in [ak.split(".")[level] for ak in convert_keys if len(ak.split(".")) > level]:
            if isinstance(val, list) or isinstance(val, tuple):
                if val and isinstance(val[0],dict):
                    d[key] = [boolean_convert(v,convert_keys,level+1) for v in val]
                else:
                    d[key] = [to_boolean(x) for x in val]
            elif isinstance(val, dict) or isinstance(val, collections.OrderedDict):
                d[key] = boolean_convert(val, convert_keys, level+1)
            else:
                d[key] = to_boolean(val)
    return d


def to_boolean(val,true_str=['true','1', 't', 'y', 'yes', 'Y','Yes','YES',1],false_str=['false','0','f','n','N','No','no','NO',0]):
    if type(val)!=str:
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
                except:
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
    '''A generator to return a record (block of text)
       at once from the infile. The record is separated by
       one or more empty lines by default.
       skip can be used to skip top n-th lines
       if include_block_end is True, the line matching block_end will also be returned.
       if as_list is True, return a list of lines in one record.
    '''
    rec_separator = lambda line: line == block_end
    with open_anyfile(infile) as in_f:
        if skip:
            for i in range(skip):
                in_f.readline()
        for key, group in itertools.groupby(in_f, rec_separator):
            if not key:
                if include_block_end:
                    _g = itertools.chain(group, (block_end,))
                yield (list(_g) if as_list else ''.join(_g))


#===============================================================================
# List Utility functions
#===============================================================================

# if dict value is a list of length 1, unlist
def unlist(d):
    for key, val in d.items():
            if isinstance(val, list):
                if len(val) == 1:
                    d[key] = val[0]
            elif isinstance(val, dict):
                unlist(val)
    return d


# split fields by sep into comma separated lists, strip.
def list_split(d, sep):
    for key, val in d.items():
        if isinstance(val, dict):
            list_split(val, sep)
        try:
            if len(val.split(sep)) > 1:
                d[key] = val.rstrip().rstrip(sep).split(sep)
        except (AttributeError):
            pass
    return d


def id_strip(id_list):
    id_list = id_list.split("|")
    ids = []
    for id in id_list:
        ids.append(id.rstrip().lstrip())
    return ids

def llist(list, sep='\t'):
    '''Nicely output the list with each item a line.'''
    for x in list:
        if isinstance(x, (list, tuple)):
            xx = sep.join([str(i) for i in x])
        else:
            xx = str(x)
        print(xx)


def listitems(a_list, *idx):
    '''Return multiple items from list by given indexes.'''
    if isinstance(a_list, tuple):
        return tuple([a_list[i] for i in idx])
    else:
        return [a_list[i] for i in idx]


def list2dict(a_list, keyitem, alwayslist=False):
    '''Return a dictionary with specified keyitem as key, others as values.
       keyitem can be an index or a sequence of indexes.
       For example: li=[['A','a',1],
                        ['B','a',2],
                        ['A','b',3]]
                    list2dict(li,0)---> {'A':[('a',1),('b',3)],
                                         'B':('a',2)}
       if alwayslist is True, values are always a list even there is only one item in it.
                    list2dict(li,0,True)---> {'A':[('a',1),('b',3)],
                                              'B':[('a',2),]}
    '''
    _dict = {}
    for x in a_list:
        if isinstance(keyitem, int):      # single item as key
            key = x[keyitem]
            value = tuple(x[:keyitem] + x[keyitem + 1:])
        else:
            key = tuple([x[i] for i in keyitem])
            value = tuple([x[i] for i in range(len(a_list)) if i not in keyitem])
        if len(value) == 1:      # single value
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


def list_nondup(a_list):
    # TODO: use set in caller and remove func
    x = {}
    for item in a_list:
        x[item] = None
    return x.keys()


def listsort(a_list, by, reverse=False, cmp=None, key=None):
    '''Given list is a list of sub(list/tuple.)
       Return a new list sorted by the ith(given from "by" item)
       item of each sublist.'''
    new_li = [(x[by], x) for x in a_list]
    new_li.sort(cmp=cmp, key=key, reverse=reverse)
    return [x[1] for x in new_li]


def list_itemcnt(a_list):
    '''Return number of occurrence for each type of item in the list.'''
    x = {}
    for item in a_list:
        # TODO= use list.count(value)
        if item in x:
            x[item] += 1
        else:
            x[item] = 1
    return [(i, x[i]) for i in x]


def alwayslist(value):
    """If input value if not a list/tuple type, return it as a single value list."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return value
    else:
        return [value]

#===============================================================================
# File Utility functions
#===============================================================================

def tabfile_tester(datafile, header=1, sep='\t'):
    reader = csv.reader(anyfile(datafile), delimiter=sep)
    lineno = 0
    try:
        for i in range(header):
            next(reader)
            lineno += 1

        for ld in reader:
            lineno += 1
    except:
        print("Error at line number:", lineno)
        raise


def dupline_seperator(dupline, dup_sep, dup_idx=None, strip=False):
    '''
    for a line like this:
        a   b1,b2  c1,c2

    return a generator of this list (breaking out of the duplicates in each field):
        [(a,b1,c1),
         (a,b2,c1),
         (a,b1,c2),
         (a,b2,c2)]
    example:
         dupline_seperator(dupline=['a', 'b1,b2', 'c1,c2'],
                           dup_idx=[1,2],
                           dup_sep=',')
    if dup_idx is None, try to split on every field.
    if strip is True, also tripe out of extra spaces.
    '''
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


def tabfile_feeder(datafile, header=1, sep='\t',
                   includefn=None,
                   coerce_unicode=True,
                   assert_column_no=None):
    '''a generator for each row in the file.'''

    in_f = anyfile(datafile)
    reader = csv.reader(in_f, delimiter=sep)
    lineno = 0
    try:
        for i in range(header):
            next(reader)
            lineno += 1

        for ld in reader:
            if assert_column_no:
                if len(ld) != assert_column_no:
                    err = "Unexpected column number:" \
                          " got {}, should be {}".format(len(ld), assert_column_no)
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


def tab2dict(datafile, cols, key, alwayslist=False, **kwargs):
    if isinstance(datafile, tuple):
        _datafile = datafile[0]
    else:
        _datafile = datafile
    if os.path.exists(_datafile):
        return list2dict([listitems(ld, *cols) for ld in tabfile_feeder(datafile, **kwargs)], key, alwayslist=alwayslist)
    else:
        print('Error: missing "%s". Skipped!' % os.path.split(_datafile)[1])
        return {}


def tab2dict_iter(datafile, cols, key, alwayslist=False, **kwargs):
    if isinstance(datafile, tuple):
        _datafile = datafile[0]
    else:
        _datafile = datafile
    if os.path.exists(_datafile):
        bulk = []
        prev_id = None
        for ld in tabfile_feeder(datafile, **kwargs):
            #print(ld)
            li = listitems(ld, *cols)
            #print("key %s len bulk %s prev %s" % (li[key],len(bulk),prev_id))
            if prev_id == None or (li[key] == prev_id):
                #print("\t\tfound same")
                bulk.append(li)
                prev_id = li[key]
            else:
                #print("bulk size: %s" % bulk)
                di = list2dict(bulk, key, alwayslist=alwayslist)
                bulk = []
                # changed key, init next bulk
                bulk.append(li)
                prev_id = li[key]
                #print("on yield: %s" % di)
                yield di
        # flush remaining bulk
        if bulk:
            di = list2dict(bulk, key, alwayslist=alwayslist)
            yield di
    else:
        print('Error: missing "%s". Skipped!' % os.path.split(_datafile)[1])
        return {}


def file_merge(infiles, outfile=None, header=1, verbose=1):
    '''merge a list of input files with the same format.
       if header will be removed from the 2nd files in the list.
    '''
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
        for line in in_f:
            out_f.write(line)
            line_no += 1
        in_f.close()
        cnt += line_no
        print(line_no)
    out_f.close()
    print("=" * 20)
    print("Done![total %d lines output]" % cnt)

#===============================================================================
# Dictionary & other structures Utility functions
#===============================================================================

# http://stackoverflow.com/questions/12971631/sorting-list-by-an-attribute-that-can-be-none
# used to sort list with None element (because python3 suddenly decided it wwasn't possible
# anymore. because...)
from functools import total_ordering
@total_ordering
class MinType(object):
    def __le__(self, other):
        return True

    def __eq__(self, other):
        return (self is other)
Min = MinType()

# from mygene, originally
def value_convert(_dict, fn, traverse_list=True):
    '''For each value in _dict, apply fn and then update
       _dict with return the value.
       if traverse_list is True and a value is a list,
       apply fn to each item of the list.
    '''
    for k in _dict:
        if traverse_list and isinstance(_dict[k], list):
            _dict[k] = [fn(x) for x in _dict[k]]
        else:
            _dict[k] = fn(_dict[k])
    return _dict

# from biothings, originally
# closed to value_convert, could be refactored except this one
# is recursive for dict typed values
def value_convert_to_number(d, skipped_keys=[]):
    """convert string numbers into integers or floats
       skip converting certain keys in skipped_keys list"""
    for key, val in d.items():
        if isinstance(val, dict):
            value_convert_to_number(val, skipped_keys)
        if key not in skipped_keys:
            if isinstance(val, list):
                d[key] = [to_number(x) for x in val]
            elif isinstance(val, tuple):
                d[key] = tuple([to_number(x) for x in val])
            else:
                d[key] = to_number(val)
    return d



def dict_convert(_dict, keyfn=None, valuefn=None):
    '''Return a new dict with each key converted by keyfn (if not None),
       and each value converted by valuefn (if not None).
    '''
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
    '''Same as dict.update, but return the updated dictionary.'''
    out = _dict.copy()
    out.update(attrs)
    return out


def update_dict_recur(d,u):
    """
    Update dict d with dict u's values, recursively
    (so existing values in d but not in u are kept even if nested)
    """
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            r = update_dict_recur(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d


def merge_dict(dict_li, attr_li, missingvalue=None):
    '''
    Merging multiple dictionaries into a new one.
    Example:
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
    '''
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
    '''Return a "normalized" value:
           1. if a list, remove duplicate and sort it
           2. if a list with one item, convert to that single item only
           3. if a list, remove empty values
           4. otherwise, return value as it is.
    '''
    if isinstance(value, list):
        value = [x for x in value if x]   # remove empty values
        try:
            _v = list(set(value))
        except TypeError:
            #use alternative way
            _v = [json.loads(x) for x in set([json.dumps(x) for x in value])]
        if len(_v) and sort:
            # py3 won't sort dict anymore...
            if isinstance(_v[0],dict):
                _v = sorted(_v,key=lambda x: sorted(x.keys()))
            else:
                try:
                    _v = sorted(_v)
                except TypeError:
                    # probably some None values to sort, not handle anymore in py3
                    # let's use a trick...
                    _v = sorted(_v,key=lambda x: Min if x is None or (type(x) != str and None in x) else x) 
        if len(_v) == 1:
            _v = _v[0]
    else:
        _v = value

    return _v


def dict_nodup(_dict, sort=True):
    for k in _dict:
        _dict[k] = normalized_value(_dict[k], sort=sort)
    return _dict


def dict_attrmerge(dict_li, removedup=True, sort=True, special_fns={}):
    '''
        dict_attrmerge([{'a': 1, 'b':[2,3]},
                        {'a': [1,2], 'b':[3,5], 'c'=4}])
        sould return
             {'a': [1,2], 'b':[2,3,5], 'c'=4}

        special_fns is a dictionary of {attr:  merge_fn}
         used for some special attr, which need special merge_fn
         e.g.,   {'uniprot': _merge_uniprot}
    '''
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


def dict_apply(dict, key, value, sort=True):
    '''

    '''
    if key in dict:
        _value = dict[key]
        if not isinstance(_value, list):
            _value = [_value]
        if isinstance(value, list):
            _value.extend(value)
        else:
            _value.append(value)
    else:
        _value = value

    dict[key] = normalized_value(_value, sort=sort)


def dict_to_list(gene_d):
    '''return a list of genedoc from genedoc dictionary and
       make sure the "_id" field exists.
    '''
    doc_li = [updated_dict(gene_d[k], {'_id': str(k)}) for k in sorted(gene_d.keys())]
    return doc_li


def merge_struct(v1, v2,aslistofdict=None):

    #print("v1 = %s" % repr(v1))
    #print("v2 = %s" % repr(v2))

    if isinstance(v1, list):
        #print("v1 is list ", end="")
        if isinstance(v2, list):
            #print("v2 is list -> extend")
            #v1.extend(v2)
            #v1 = list(set(v1))
            v1 = v1 + [x for x in v2 if x not in v1]
        else:
            #print("v2 not list -> append")
            if v2 not in v1:
                v1.append(v2)

    elif isinstance(v2, list) and isinstance(v1, dict):
        #return merge_struct(v2,v1)
        if v1 not in v2:
            v2.append(v1)

    elif isinstance(v1, dict):
        assert isinstance(v2, dict),"v2 %s not a dict (v1: %s)" % (v2,v1)
        #print("v1 & v2 is dict")
        for k in list(v1.keys()):
            #print("v1[%s]" % k)
            if k in v2:
                #print("%s is both dict -> merge/update v1[%s],v2[%s]" % (k, k, k))
                #v1.update(merge(v1[k], v2[k]))
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
                        v1[k] = merge_struct(v1elem,v2elem)
                else:
                    v1[k] = merge_struct(v1[k], v2[k])
            else:
                #print("%s not in v2 -> update v1 with v2" % k)
                #print("v2 before %s" % repr(v2))
                #v1.update(v2)
                v2[k] = v1[k]
                #print("v2 after %s" % repr(v2))
        for k in v2:
            #print("v2[%s]" % k)
            if k in v1:
                pass  # already done
            else:
                v1[k] = v2[k]

    elif isinstance(v1, str) or isinstance(v1, int) or isinstance(v1, float):
        if isinstance(v2, str) or isinstance(v2, int) or isinstance(v2, float):
            if v1 != v2:
                #print("v1 & v2 not iterable -> list of 2")
                v1 = [v1, v2]
            else:
                pass
                #print("v1 == v2, skip")
        else:
            #print("v2 iterable, reverse merge")
            return merge_struct(v2, v1)
    else:
        raise TypeError("dunno how to merge type %s" % type(v1))

    #print("return %s" % v1)
    return v1

