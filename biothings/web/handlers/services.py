from biothings.web.handlers import BaseHandler, BaseAPIHandler
from biothings.web.options.openapi import OpenAPIDocumentBuilder


class StatusHandler(BaseHandler):
    '''
    Handles requests to check the status of the server.
    Use set_status instead of raising exception so that
    no error will be propogated to sentry monitoring. # TODO IS IT A GOOD IDEA?
    '''

    def head(self):
        return self._check()

    async def get(self):

        dev = self.get_argument('dev', None)
        res = await self._check(dev is not None)
        self.finish(res)

    async def _check(self, dev=False):

        try:  # some db connections support async operations
            response = await self.biothings.health.async_check(info=dev)
        except (AttributeError, NotImplementedError):
            response = self.biothings.health.check()

        if not dev:
            return {
                # this endpoint might be accessed frequently,
                # keep the default response minimal. This is
                # especially useful when the document payload
                # is very large. Also useful when the automated
                # healch check only support GET requests.
                "success": True,
                "status": response.get("status")
            }

        return dict(response)

class FrontPageHandler(BaseHandler):

    def get(self):
        self.render(
            template_name="home.html",
            alert='Front Page Not Configured.',
            title='Biothings API',
            contents=self.biothings.handlers.keys(),
            support=self.biothings.metadata.types,
            url='http://biothings.io/'
        )

    def get_template_path(self):
        import biothings.web.templates
        return next(iter(biothings.web.templates.__path__))

class APISpecificationHandler(BaseAPIHandler):

    def get(self):
        # openapi = OpenAPIDocumentBuilder()
        # openapi.info(title='Biothings API', version='v1')
        # self.finish(openapi.document)
        self.finish(self.biothings.optionsets.log())
