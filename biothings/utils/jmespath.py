"""
We can define jmespath custom functions here

See: https://jmespath.org/tutorial.html#custom-functions

from biothings.utils.jmespath import options as jmp_options

jmespath.search("unique(`foo`)", {}, options=jmp_options)
or
jmespath.compile("unique(`foo`)").search({}, options=jmp_options)

"""
import jmespath
from jmespath import functions


class CustomFunctions(functions.Functions):
    """Create a subclass of functions.Functions.
    The function.Functions base class has logic
    that introspects all of its methods and automatically
    registers your custom functions in its function table.
    """

    @functions.signature({"types": ["array"]})
    def _func_unique(self, arr):
        """return a list of unique values in an array"""
        return sorted(set(arr))

    @functions.signature({"types": ["array"]})
    def _func_unique_count(self, arr):
        """return the number of unique values in an array"""
        return len(set(arr))


# pass this jmespath_options to search to use custom functions
options = jmespath.Options(custom_functions=CustomFunctions())
