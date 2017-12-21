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


class GenericHandler(BaseHandler):
    def get(self):
        self.write_error(405,exc_info=(None,"Method GET not allowed",None))
    def post(self):
        self.write_error(405,exc_info=(None,"Method POST not allowed",None))
    def put(self):
        self.write_error(405,exc_info=(None,"Method PUT not allowed",None))
    def delete(self):
        self.write_error(405,exc_info=(None,"Method DELETE not allowed",None))
    def options(self):
        self.write_error(405,exc_info=(None,"Method OPTIONS not allowed",None))
    def head(self):
        self.write_error(405,exc_info=(None,"Method HEAD not allowed",None))

