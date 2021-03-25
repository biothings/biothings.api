from tornado.web import HTTPError

class EndRequest(HTTPError):
    """
        Similar to tornado.web.Finish() but write_error handles it.
    """

    def __init__(self, status_code=200, log_message=None, *args, **kwargs):
        super().__init__(status_code, log_message, *args, **kwargs)
        self.kwargs = dict(kwargs) or {}
        self.kwargs.pop('reason', None)

class BadRequest(EndRequest):

    def __init__(self, log_message=None, *args, **kwargs):
        super().__init__(400, log_message, *args, **kwargs)
        self.kwargs = dict(kwargs) or {}
        self.kwargs.pop('reason', None)
