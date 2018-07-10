from biothings.utils.common import is_seq
from biothings.utils.doc_traversal import depth_first_traversal
from collections import OrderedDict

def flatten_doc_2(doc, outfield_sep='.', sort=True):
    _ret = {}
    for _path, _val in depth_first_traversal(doc):
        if not isinstance(_val, dict) and not is_seq(_val):
            if outfield_sep:
                _new_path = outfield_sep.join(_path)
            else:
                _new_path = _path
            _ret.setdefault(_new_path, []).append(_val)
    if sort and outfield_sep:
        return OrderedDict(sorted([(k,v[0]) if len(v) == 1 else (k, v) for (k, v) in _ret.items()], key=lambda x: x[0]))
    return dict([(k, v[0]) if len(v) == 1 else (k, v) for (k, v) in _ret.items()])


def flatten_doc(doc, outfield_sep='.', sort=True):
    ''' This function will flatten an elasticsearch document (really any json object).
        outfield_sep is the separator between the fields in the return object.
        sort specifies whether the output object should be sorted alphabetically before returning
            (otherwise output will remain in traveral order) '''

    def _recursion_helper(_doc, _ret, out):
        if isinstance(_doc, dict):
            for key in _doc:
                if outfield_sep:
                    new_key = key if not out else outfield_sep.join([out, key])
                else:
                    new_key = tuple([key]) if not out else tuple(list(tuple(out)) + [key])
                _recursion_helper(_doc[key], _ret, new_key)
        elif is_seq(_doc):
            for _obj in _doc:
                _recursion_helper(_obj, _ret, out)
        else:
            # this is a leaf
            _ret.setdefault(out, []).append(_doc)

    ret = {}
    _recursion_helper(doc, ret, '')
    if sort and outfield_sep:
        return OrderedDict(sorted([(k,v[0]) if len(v) == 1 else (k,v) for (k,v) in ret.items()], key=lambda x: x[0]))
    return dict([(k,v[0]) if len(v) == 1 else (k,v) for (k,v) in ret.items()])
