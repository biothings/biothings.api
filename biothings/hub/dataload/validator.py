'''
Deprecated. This module is not used any more
'''

import types
import logging

enc = None
dec = None
try:
    import bson
    enc = bson.BSON.encode
    dec = bson.BSON.decode
except ImportError:
    import json
    enc = json.dumps
    dec = json.loads



from biothings.utils.common import is_scalar, infer_types

class ParserValidator(object):

    def __init__(self, logger=logging, max_kept_errors=None):
        self.logger = logger
        self.max_kept_errors = max_kept_errors
        self.data_map = {}
        self.summary = {"total": 0}
        self.errs = {}

    def set_err(self,err_type,data):
        self.errs.set_default(err_type,{"count": 0, "data": []})
        self.errs[err_type]["count"] += 1
        if not self.max_kept_errors is None and self.errs[err_type]["count"] <= self.max_kept_errors:
            self.errs[err_type]["data"].append(data)

    def analyze(self, data, check_id=True, root_key=None):
        map_type = {}
        if check_id and not "_id" in data:
            self.set_err("_id_missing",data)
        infer_types(data,self.data_map)

    def check(self,data):
        try:
            data = dec(enc(data))
        except TypeError:
            self.set_err("not_json_serializable",data)
        self.analyze(data)

    def validate(self,func,*args,**kwargs):
        """
        Validate parsing function "func". args and kwargs are
        passed as arguments to this function.
        """
        assert callable(func),"%s is not callable" % func
        res = func(*args,**kwargs)
        assert type(res) == list or type(res) == types.GeneratorType, "%s returned a wrong type: %s" % (func,type(res))
        self.logger.info("Return type is %s, now iterating over content" % type(res))
        cnt = 0
        for data in res:
            if not type(data) == dict:
                self.set_err("wrong_data_type",oata)
            self.check(data)
            cnt += 1
            if cnt % 1000 == 0:
                self.logger.debug("Processed %s records" % cnt)
