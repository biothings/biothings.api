from biothings.utils.common import is_seq
from collections import OrderedDict

def flatten_doc(doc, outfield_sep='.', sort=True):
    ''' This function will flatten an elasticsearch document (really any json object).
        outfield_sep is the separator between the fields in the return object.
        sort specifies whether the output object should be sorted alphabetically before returning
            (otherwise output will remain in traveral order) '''

    def _recursion_helper(_doc, _ret, out):
        if isinstance(_doc, dict):
            for key in _doc:
                new_key = key if not out else outfield_sep.join([out, key])
                _recursion_helper(_doc[key], _ret, new_key)
        elif is_seq(_doc):
            for _obj in _doc:
                _recursion_helper(_obj, _ret, out)
        else:
            # this is a leaf
            _ret.setdefault(out, []).append(_doc)

    ret = {}
    _recursion_helper(doc, ret, '')
    if sort:
        return OrderedDict(sorted([(k,v[0]) if len(v) == 1 else (k,v) for (k,v) in ret.items()], key=lambda x: x[0]))
    return dict([(k,v[0]) if len(v) == 1 else (k,v) for (k,v) in ret.items()])
