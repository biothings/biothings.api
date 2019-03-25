"""
DataTransform Histogram class - track keylookup statistics
"""


class Histogram(object):
    """
    Histogram - track keylookup statistics
    """

    def __init__(self):
        self.io_histogram = {}
        self.edge_histogram = {}

    def __str__(self):
        res = {
            'io_report': self.io_histogram,
            'edge_report': self.edge_histogram
            }
        return str(res)

    def update_edge(self, vert1, vert2, size):
        """
        Update the edge histogram
        """
        key = self._construct_key(vert1, vert2)
        self._increment(self.edge_histogram, key, size)

    def update_io(self, input_type, output_type, size):
        """
        Update the edge histogram
        """
        key = self._construct_key(input_type, output_type)
        self._increment(self.io_histogram, key, size)

    @staticmethod
    def _construct_key(obj1, obj2):
        return "{}-->{}".format(obj1, obj2)

    @staticmethod
    def _increment(hist, key, size):
        if size > 0:
            if key not in hist.keys():
                hist[key] = int(size)
            else:
                hist[key] += int(size)
