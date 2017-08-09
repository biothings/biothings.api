from tornado.web import RequestHandler
from tornado import escape
from biothings.utils.common import json_encode
escape.json_encode = json_encode


import logging
class BaseHandler(RequestHandler):

    def initialize(self,managers,**kwargs):
        self.managers = managers

    def set_default_headers(self):
        self.set_header('Access-Control-Allow-Origin','*')
        self.set_header('Content-Type', 'application/json')

    def write(self,result):
        super(BaseHandler,self).write(
                {"result":result,
                 "status" : "ok"})

    def write_error(self,status_code, **kwargs):
        super(BaseHandler,self).write(
                {"error":str(kwargs.get("exc_info",[None,None,None])[1]),
                 "status" : "error",
                 "code" : status_code})
