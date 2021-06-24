from typing import NamedTuple
# do NOT import non standard library packages, doing so will break setup.py


class _version_info(NamedTuple):
    # similar to sys._version_info
    major: int
    minor: int
    micro: int


version_info = _version_info(0, 10, 0)
version_str = '.'.join(map(str, version_info))
