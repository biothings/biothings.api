import time
from functools import wraps

try:
    from biothings.web.options import OptionSet, ReqArgs
except ImportError:
    from biothings.web.options import Options as OptionSet

from biothings.web.settings.default import QUERY_KWARGS


def timethis(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        r = func(*args, **kwargs)
        end = time.perf_counter()
        print("{}.{} : {}".format(func.__module__, func.__name__, end - start))
        return r

    return wrapper


class Request:
    def __init__(self, method, reqargs):
        self.method = method
        self.reqargs = reqargs

    def get_new_calling_signature(self):
        return self.method, ReqArgs(*self.reqargs)

    def get_old_calling_signature(self):
        args = {}
        for i in range(1, 4):
            try:
                args.update(self.reqargs[i])
            except (IndexError, TypeError):
                pass
        path_args = ()
        path_kwargs = {}
        if self.reqargs[0]:
            path_args = self.reqargs[0][0]
            path_kwargs = self.reqargs[0][1]

        return self.method, args, path_args, path_kwargs


LONG_IDS = ",".join("cdk" + str(num) for num in range(100))

_REQUESTS = (
    ("GET", (None, {"q": "cdk2"})),
    ("GET", (None, {"q": "cdk", "size": "1000"})),
    ("GET", (None, {"q": "cdk2"}, {"aggs": "symbol"})),
    ("GET", (None, {"q": "cdk2"}, None, {"from": 1000, "explain": True})),
    ("GET", (((), {"ver": "v3"}), {"q": "cdk2"}, {"sort": "taxid"})),
    ("GET", (None, {"q": "cdk2", "userquery": "yourgene"})),
    ("POST", (None, {"format": "yaml"}, {"q": "cdk2,cdk3"})),
    ("POST", (None, {"format": "yaml"}, None, {"q": ["cdk2"]})),
    ("POST", (None, {"format": "html"}, {"q": "cdk2 cdk3 cdk9"})),
    ("POST", (None, {"format": "html"}, {"q": LONG_IDS})),
)

REQUESTS = (Request(*request) for request in _REQUESTS)
try:
    OPTIONSET = OptionSet(QUERY_KWARGS)
except TypeError:
    OPTIONSET = OptionSet(QUERY_KWARGS, (), ())


@timethis
def main():
    for _ in range(100000000):
        for request in REQUESTS:
            OPTIONSET.parse(*request.get_new_calling_signature())


def test():
    for request in REQUESTS:
        print(OPTIONSET.parse(*request.get_new_calling_signature()))


if __name__ == "__main__":
    main()
