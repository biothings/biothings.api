from biothings.hub.datatransform import IDStruct


class CIIDStruct(IDStruct):
    """
    CIIDStruct - id structure for use with the DataTransform classes.  The basic idea
    is to provide a structure that provides a list of (original_id, current_id)
    pairs.

    This is a case-insensitive version of IDStruct.
    """

    def add(self, left, right):
        """add a (original_id, current_id) pair to the list,
        All string values are typecast to lowercase"""
        if not left or not right:
            return  # identifiers cannot be None
        if self.lookup(left, right):
            return  # tuple already in the list
        # ensure it's hashable
        if not type(left) in [list,tuple]:
            left = [left]
        if not type(right) in [list,tuple]:
            right = [right]
        if type(left) == list:
            left = tuple(left)
        if type(right) == list:
            right = tuple(right)
        for v in left:
            # After some thought, this data structure should be case insensitive
            if isinstance(v, str):
                v = v.lower()
            if v not in self.forward.keys():
                self.forward[v] = right
            else:
                self.forward[v] = self.forward[v] + right
        for v in right:
            # After some thought, this data structure should be case insensitive
            if isinstance(v, str):
                v = v.lower()
            if v not in self.inverse.keys():
                self.inverse[v] = left
            else:
                self.inverse[v] = self.inverse[v] + left

    def find(self,where,ids):
        """Case insensitive lookup of ids"""
        if not ids:
            return
        if not type(ids) in (list,tuple):
            ids = [ids]
        for id in ids:
            # This find is case insensitive
            if isinstance(id, str):
                id = id.lower()
            if id in where.keys():
                for i in where[id]:
                    yield i

