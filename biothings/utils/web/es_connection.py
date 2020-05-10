import logging

from elasticsearch_async.connection import AIOHttpConnection

logger = logging.getLogger("elasticsearch")
tracer = logging.getLogger("elasticsearch.trace")

class BiothingsAIOHttpConnection(AIOHttpConnection):

    def _log_trace(self, method, path, body, status_code, response, duration):
        if not tracer.isEnabledFor(logging.INFO) or not tracer.handlers:
            return

        # include pretty in trace curls
        path = path.replace("?", "?pretty&", 1) if "?" in path else path + "?pretty"
        if self.url_prefix:
            path = path.replace(self.url_prefix, "", 1)
        tracer.info(
            "curl %s-X%s '%s' -d '%s'",  # override: path is full url
            "-H 'Content-Type: application/json' " if body else "",
            method,
            path,
            self._pretty_json(body) if body else "",
        )

        if tracer.isEnabledFor(logging.DEBUG):
            # override
            if response:
                tracer.debug(
                    "#[%s] (%.3fs)\n#%s",
                    status_code,
                    duration,
                    self._pretty_json(response).replace("\n", "\n#") if response else "",
                )
            else:  # remove empty body output
                tracer.debug(
                    "#[%s] (%.3fs)",
                    status_code,
                    duration
                )

    def log_request_success(
        self, method, full_url, path, body, status_code, response, duration
    ):
        """ Log a successful API call.  """

        # body has already been serialized to utf-8, deserialize it for logging
        if body:
            try:
                body = body.decode("utf-8", "ignore")
            except AttributeError:
                pass

        logger.info(
            "%s %s [status:%s request:%.3fs]", method, full_url, status_code, duration
        )
        logger.debug("> %s", body)
        logger.debug("< %s", response)

        # override, pass in full url instead of path
        self._log_trace(method, full_url, body, status_code, response, duration)

    def log_request_fail(
        self,
        method,
        full_url,
        path,
        body,
        duration,
        status_code=None,
        response=None,
        exception=None,
    ):
        """ Log an unsuccessful API call.  """
        # do not log 404s on HEAD requests
        if method == "HEAD" and status_code == 404:
            return
        logger.warning(
            "%s %s [status:%s request:%.3fs]",
            method,
            full_url,
            status_code or "N/A",
            duration,
            # Override
            # exc_info=exception is not None,
        )

        # body has already been serialized to utf-8, deserialize it for logging
        # TODO: find a better way to avoid (de)encoding the body back and forth
        if body:
            try:
                body = body.decode("utf-8", "ignore")
            except AttributeError:
                pass

        logger.debug("> %s", body)

        # override, pass in full url instead of path
        self._log_trace(method, full_url, body, status_code, response, duration)
