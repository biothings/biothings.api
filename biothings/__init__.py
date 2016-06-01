import pkg_resources

__version__ = pkg_resources.require("biothings")[0].version

def get_version():
    return __version__
