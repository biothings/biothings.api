

class Histogram(object):

    def __init__(self):
        self.io_histogram = {}
        self.edge_histogram = {}

    def __str__(self):
        res = {
            'io_report': self.io_histogram,
            'edge_report': self.edge_histogram
            }
        return str(res)

    def update_edge(self, v1, v2, size):
        """
        Update the edge histogram
        """
        key = self._construct_key(v1, v2)
        self._increment(self.edge_histogram, key, size)

    def update_io(self, input_type, output_type, size):
        """
        Update the edge histogram
        """
        key = self._construct_key(input_type, output_type)
        self._increment(self.io_histogram, key, size)

    def _construct_key(self, obj1, obj2):
        return "{}-->{}".format(obj1, obj2)

    def _increment(self, hist, key, size):
        if size > 0:
            if key not in hist.keys():
                hist[key] = int(size)
            else:
                hist[key] += int(size)
