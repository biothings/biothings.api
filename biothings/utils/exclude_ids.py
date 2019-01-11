from .dotstring import list_length
from .dotstring import remove_key


class ExcludeFieldsById(object):
    """
    This class provides a framework to exclude fields for certain
    identifiers. Up to three arguments are passed to this class, an
    identifier list, a list of fields to remove, and minimum list
    size.  The identifier list is a list of document identifiers to act
    on.  The list of fields are fields that will be removed; they are
    specified using a dotstring notation.  The minimum list size is
    the minimum number of elements that should be in a list in order
    for it to be removed.  The 'drugbank', 'chebi', and 'ndc' data
    sources were manually tested with this class.
    """

    def __init__(self, exclusion_ids, field_lst, min_list_size=1000):
        """
        Fields to truncate are specified by field_lst.  The
        dot-notation is accepted.
        """
        self.exclusion_ids = exclusion_ids
        self.field_lst = field_lst
        self.min_list_size = min_list_size

    def __call__(self, f):
        """
        Truncate specified fields on documents on call.
        :param f: function to apply to, this function should return documents
        :return:
        """
        def wrapped_f(*args):
            input_docs = f(*args)
            for doc in input_docs:
                if doc['_id'] in self.exclusion_ids:
                    for field in self.field_lst:
                        # min_list_size check
                        if list_length(doc, field) > self.min_list_size:
                            remove_key(doc, field)
                yield doc
        return wrapped_f
