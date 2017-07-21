''' web utils go here. '''
# randos
def sum_arg_dicts(*arg, **kwarg):
    ''' any dicts passed to the args of this function (either positional or keyword) are combined (using update) '''
    _ret = {}
    for _val in arg + tuple(kwarg.values()):
        if isinstance(_val, dict):
            _ret.update(_val)
    return _ret
