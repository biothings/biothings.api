
class IDMapperBase(object):
    """
    Provide mapping between different sources
    """

    def __init__(self, name=None, convert_func=None, *args, **kwargs):
        """
        'name' may match an id_type (see uploaders). If None, mapper 
        will be applied to any document from a resource without id_type argument
        """
        self.map = None
        self.convert_func = convert_func or (lambda x: x)
        self.name = name

    def load(self):
        """
        Do whatever is required to fill mapper with mapping data
        Can be called multiple time, the first time only will load data
        """
        raise NotImplementedError("sub-class and implement me")

    def translate(self,_id,transparent=False):
        """
        Return _id translated through mapper, or _id itself if not part of mapper
        If 'transparent' and no match, original _id will be returned
        """
        if self.map is None:
            self.load()
        default = transparent and _id or None
        return self.map.get(self.convert_func(_id),default)

    def __contains__(self,_id):
        if self.map is None:
            self.load()
        return _id in self.map

    def __len__(self):
        if self.map is None:
            self.load()
        return len(self.map)

    def convert(self,docs,key_to_convert,transparent=False):
        """
        Convert a 'key_to_convert' document key using mapping.
        If transparent and no match, original key will be used
        (so there's no change). Else, if no match, document will
        be discarded.
        Warning: key to be translated must not be None (it's considered
        a non-match)
        """
        for doc in docs:
            _new = self.translate(doc[key_to_convert],transparent)
            if _new is None and not transparent:
                continue
            doc[key_to_convert] = _new
            yield doc
