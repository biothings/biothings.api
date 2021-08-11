from .common import is_str


def last_element(d, key_list):
    """Return the last element and key for a document d given a docstring.

    A document d is passed with a list of keys key_list.  A generator is then
    returned for all elements that match all keys.  Not that there may be
    a 1-to-many relationship between keys and elements due to lists in the document.

    :param d: document d to return elements from
    :param key_list: list of keys that specify elements in the document d
    :return: generator for elements that match all keys
    """
    # preconditions
    if not d or not key_list:
        return
    k = key_list.pop(0)
    # termination
    if not key_list:
        yield k, d
    # recursion
    else:
        try:
            t = d[k]
        except KeyError:
            return    # key does not exist
        except TypeError:
            return    # not sub-scriptable
        if isinstance(t, dict):
            yield from last_element(t, key_list)
        elif isinstance(t, list):
            for l in t:
                yield from last_element(l, key_list.copy())
        elif isinstance(t, tuple):
            # unsupported type
            raise ValueError("unsupported type in key {}".format(k))


def key_value(dictionary, key):
    """Return a generator for all values in a dictionary specific by a dotstirng (key)
       if key is not found from the dictionary, None is returned.

    :param dictionary: a dictionary to return values from
    :param key: key that specifies a value in the dictionary
    :return: generator for values that match the given key
    """
    def safe_ref(k, d):
        if d:
            try:
                return d[k]
            except KeyError:
                pass

    if not is_str(key):
        raise TypeError("key argument must of be of type 'str'")
    key_list = key.split('.')
    for k, le in last_element(dictionary, key_list):
        yield safe_ref(k, le)


def set_key_value(dictionary, key, value):
    """Set values all values in dictionary matching a dotstring key to a specified value.
       if key is not found in dictionary, it just skip quietly.

    :param dictionary: a dictionary to set values in
    :param key: key that specifies an element in the dictionary
    :return: dictionary after changes have been made
    """
    def safe_assign(k, d):
        if d:
            try:
                d[k] = value
            except KeyError:
                pass

    if not is_str(key):
        raise TypeError("key argument must of be of type 'str'")
    key_list = key.split('.')
    for k, le in last_element(dictionary, key_list):
        safe_assign(k, le)
    return dictionary


def remove_key(dictionary, key):
    """Remove field specified by the docstring key

    :param dictionary: a dictionary to remove the value from
    :param key: key that specifies an element in the dictionary
    :return: dictionary after changes have been made
    """
    if not is_str(key):
        raise TypeError("key argument must of be of type 'str'")
    key_list = key.split('.')
    for k, le in last_element(dictionary, key_list):
        try:
            del le[k]
        except KeyError:
            pass
    return dictionary


def list_length(d, field):
    """Return the length of a list specified by field.

    If field represents a list in the document, then return its length.
    Otherwise return 0.

    :param d: a dictionary
    :param field: the dotstring field specifying a list
    """
    default_value = 0

    try:
        lst = next(key_value(d, field))
    except StopIteration:
        return default_value

    if isinstance(lst, list):
        return len(lst)
    else:
        return default_value
