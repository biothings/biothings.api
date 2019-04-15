"""
CIIDStruct - case insenstive id matching data structure
"""
# pylint: disable=E0611
from biothings.hub.datatransform import IDStruct


class CIIDStruct(IDStruct):
    """
    CIIDStruct - id structure for use with the DataTransform classes.  The basic idea
    is to provide a structure that provides a list of (original_id, current_id)
    pairs.

    This is a case-insensitive version of IDStruct.
    """

    def add(self, left, right):
        # pylint: disable=R0912
        """add a (original_id, current_id) pair to the list,
        All string values are typecast to lowercase"""
        if not left or not right:
            return  # identifiers cannot be None
        if self.lookup(left, right):
            return  # tuple already in the list
        # ensure it's hashable
        if not isinstance(left, (list, tuple)):
            left = [left]
        if not isinstance(right, (list, tuple)):
            right = [right]
        if isinstance(left, list):
            left = tuple(left)
        if isinstance(right, list):
            right = tuple(right)
        for val in left:
            # After some thought, this data structure should be case insensitive
            if isinstance(val, str):
                val = val.lower()
            if val not in self.forward.keys():
                self.forward[val] = right
            else:
                self.forward[val] = self.forward[val] + right
        for val in right:
            # After some thought, this data structure should be case insensitive
            if isinstance(val, str):
                val = val.lower()
            if val not in self.inverse.keys():
                self.inverse[val] = left
            else:
                self.inverse[val] = self.inverse[val] + left

    def find(self, where, ids):
        """Case insensitive lookup of ids"""
        if not ids:
            return
        if not isinstance(ids, (list, tuple)):
            ids = [ids]
        for key in ids:
            # This find is case insensitive
            if isinstance(key, str):
                key = key.lower()
            if key in where.keys():
                for i in where[key]:
                    yield i
