''' Some utility functions that do document traversal '''
from biothings.utils.common import is_seq

# doc labelled in breadth first way with letters
test_doc = {
    'a': {
        'b': {
            'e': {
                'i': 'i value', 
                'j': 'j value'
            }, 
            'f': 'f value'
        },
        'c': 'c value',
        'd': {
            'g': {
                'k': 'k value', 
                'l': 'l value'
            }, 
            'h': 'h value'
        }
    }
}

class QueueEmptyError(Exception):
    pass


class StackEmptyError(Exception):
    pass


class Stack(object):
    def __init__(self):
        self.stack = []

    def push(self, obj):
        ''' put obj on stack '''
        self.stack.insert(0, obj)

    def pop(self):
        try:
            return self.stack.pop(0)
        except IndexError:
            raise StackEmptyError("Can't pop object from an empty stack")

    def isempty(self):
        return len(self.stack) == 0


class Queue(object):
    def __init__(self):
        self.queue = []

    def push(self, obj):
        ''' put obj on queue '''
        self.queue.append(obj)

    def pop(self):
        ''' get next obj from queue '''
        try:
            return self.queue.pop(0)
        except IndexError:
            ''' empty queue '''
            raise QueueEmptyError("Can't pop object from an empty queue")

    def isempty(self):
        return len(self.queue) == 0

def breadth_first_traversal(doc):
    ''' Yield a 2 element tuple for every k, v pair in document items (nodes are visited in breadth first order
        k is itself a tuple of keys annotating the path for this node (v) to root
        v is the node value
    '''
    return _generic_traversal(doc, Queue)

def depth_first_traversal(doc):
    ''' Yield a 2 element tuple for every k, v pair in document items (nodes are visited in depth first order
        k is itself a tuple of keys annotating the path for this node (v) to root
        v is the node value
    '''
    return _generic_traversal(doc, Stack)

def _generic_traversal(doc, structure):
    _struct = structure()

    # push first level
    for (k, v) in doc.items():
        _struct.push( (tuple([k]), v) )

    while not _struct.isempty():
        _next = _struct.pop()
        yield _next
        if isinstance(_next[1], dict):
            # push this level
            for (k, v) in _next[1].items():
                _struct.push( (tuple(list(_next[0]) + [k]), v) )
        elif is_seq(_next[1]):
            # push all elements in a list/tuple
            for o in _next[1]:
                _struct.push( (_next[0], o) )

def breadth_first_recursive_traversal(doc, path=[]):
    ''' doesn't exactly implement breadth first ordering it seems, not sure why... '''
    #TODO fix this...
    if isinstance(doc, dict):
        for (k, v) in doc.items():
            yield ( tuple(list(path) + [k]), v)
        for (k, v) in doc.items():
            yield from breadth_first_recursive_traversal(v, tuple(list(path) + [k]))
    elif is_seq(doc):
        for o in doc:
            yield ( tuple(list(path)), o )
        for o in doc:
            yield from breadth_first_recursive_traversal(o, tuple(list(path)))

def depth_first_recursive_traversal(doc, path=[]):
    if isinstance(doc, dict):
        for (k, v) in doc.items():
            _path = tuple(list(path) + [k])
            yield ( _path, v)
            yield from depth_first_recursive_traversal(v, _path)
    elif is_seq(doc):
        for o in doc:
            _path = tuple(list(path))
            yield (_path, o)
            yield from depth_first_recursive_traversal(o, _path)
